//! Puppet-pool overflow + FCFS promotion (catalogue #7) — the **live 3-speaker
//! variant** of the hermetic `puppet.rs::scenario_overflow_fcfs_promotion_order`
//! test. It proves the pool's overflow and first-come-first-served promotion
//! end-to-end against the real Discord/Matrix stack.
//!
//! The bridge-under-test runs with a **restricted 1-bot pool**
//! ([`BridgeUnderTest::start_with_pool_size`]) so overflow is deterministic no
//! matter how many tokens the shared `NVB_E2E_PUPPET_TOKENS` secret holds. Three
//! membership-only [`MatrixPhantom`]s join Auto Test 01 in order:
//!
//! * **A** (`nvbtest_alice`) — the first participant takes the single bot as a
//!   dedicated puppet and is elected the channel's capture anchor.
//! * **B** (`nvbtest_bob`) — the pool is now exhausted, so B is mixed into the
//!   anchor's shared **overflow** mix (no new bot appears in Discord voice).
//! * **C** (`nvbtest_carol`) — likewise overflows, arriving after B.
//!
//! Then A leaves. `release()` promotes the **oldest** waiting overflow speaker
//! (lowest `seq`) into the freed slot — that must be **B, not C** (FCFS). The
//! assertion keys the `promoted` field of the promotion log against the `key`
//! B's own overflow log carried, so the exact participant-key format never has
//! to be hard-coded here.
//!
//! Membership-driven (like the reaper/leave-grace scenarios): no audio, no audio
//! oracle — low-flake. Runs with prod up (dedicated harness bots + the nvbtest
//! appservice).

use super::audio::{await_quiescent, puppet_ids, sweep_test_memberships};
use super::lifecycle::await_puppet_present;
use crate::actors::matrix::MatrixPhantom;
use crate::bridge_proc::{BridgeUnderTest, tenant_entry};
use crate::observers::{ConnCountProbe, RoomStatePoller};
use crate::scenario::{Ctx, Expectation, ScenarioDef};
use anyhow::{Context as _, Result, bail};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

/// #7 — overflow beyond the pool, then FCFS promotion of the oldest overflow.
pub static OVERFLOW_FCFS_PROMOTION: ScenarioDef = ScenarioDef {
    name: "overflow_fcfs_promotion",
    expectation: Expectation::Pass,
    run: |ctx| Box::pin(run_overflow_fcfs(ctx)),
};

/// Pool-level allocation logs (`crates/discord/src/puppet.rs`), matched as
/// substrings of the JSON `message` field.
const ASSIGNED_PUPPET: &str = "assigned participant to puppet";
const OVERFLOW: &str = "overflow to anchor mix";
const PROMOTED: &str = "promoted overflow speaker into freed slot";

async fn run_overflow_fcfs(ctx: Arc<Ctx>) -> Result<()> {
    let cfg = &ctx.cfg;
    // The phantoms masquerade under the nvbtest appservice.
    cfg.test_as_token().context("overflow_fcfs_promotion needs the test appservice")?;
    let bridge_name = "Auto Test 01";
    let poller = RoomStatePoller::new(cfg)?;
    sweep_test_memberships(cfg, &poller, &cfg.room_a).await?;

    let tenants = tenant_entry(bridge_name, &cfg.room_a, &cfg.guild_id, &cfg.channel_a);
    // A 1-bot pool: 3 participants → 1 dedicated + 2 overflow, deterministically.
    let bridge =
        BridgeUnderTest::start_with_pool_size(cfg, ctx.profile, &tenants, &ctx.artifacts.join("bridge"), 1)
            .await
            .context("starting bridge-under-test")?;
    if let Err(e) = bridge.await_ready(&[bridge_name], Duration::from_secs(120)).await {
        bridge.kill().await;
        return Err(e);
    }

    let result = drive_overflow_fcfs(&ctx, &bridge).await;
    match result {
        Ok(()) => {
            let _ = await_quiescent(&poller, &cfg.room_a).await;
            bridge.sigterm_and_wait(Duration::from_secs(20)).await.context("SIGTERM drain")
        }
        Err(e) => {
            bridge.kill().await;
            Err(e)
        }
    }
}

async fn drive_overflow_fcfs(ctx: &Ctx, bridge: &BridgeUnderTest) -> Result<()> {
    let cfg = &ctx.cfg;
    let targets = puppet_ids(cfg)?;
    let probe =
        ConnCountProbe::connect(cfg.speaker_tokens.first().context("probe token")?, &cfg.guild_id)
            .await?;

    let outcome = async {
        // ── A: the first participant claims the single bot + becomes anchor. ──
        let a = MatrixPhantom::join(cfg, &cfg.room_a, false, false, "nvbtest_alice", "NVB Test Alice")
            .await?;
        bridge
            .scraper
            .await_event(Duration::from_secs(90), |e| e.message.contains(ASSIGNED_PUPPET))
            .await
            .context("A never got a dedicated puppet")?;
        let anchor = await_puppet_present(&probe, &targets, Duration::from_secs(60))
            .await
            .context("A's puppet never appeared in Discord voice")?;
        assert_one_puppet(&probe, &targets, anchor, "after A joined").await?;
        eprintln!("   A anchored on puppet {anchor}");

        // ── B: pool exhausted → overflow. Capture B's participant key. ──
        let b = MatrixPhantom::join(cfg, &cfg.room_a, false, false, "nvbtest_bob", "NVB Test Bob")
            .await?;
        let b_ev = bridge
            .scraper
            .await_event(Duration::from_secs(60), |e| {
                e.message.contains(OVERFLOW) && e.field("display_name") == Some("NVB Test Bob")
            })
            .await
            .context("B never overflowed (did it wrongly get a second bot?)")?;
        let bob_key = b_ev
            .field("key")
            .context("B's overflow log carried no `key` field")?
            .to_owned();
        assert_one_puppet(&probe, &targets, anchor, "after B overflowed").await?;
        eprintln!("   B overflowed (key={bob_key})");

        // ── C: overflows too, strictly after B. ──
        let c = MatrixPhantom::join(cfg, &cfg.room_a, false, false, "nvbtest_carol", "NVB Test Carol")
            .await?;
        bridge
            .scraper
            .await_event(Duration::from_secs(60), |e| {
                e.message.contains(OVERFLOW) && e.field("display_name") == Some("NVB Test Carol")
            })
            .await
            .context("C never overflowed")?;
        assert_one_puppet(&probe, &targets, anchor, "after C overflowed (3 members, 1 bot)").await?;
        eprintln!("   C overflowed — 3 members, still 1 bot in voice (overflow proven)");

        // ── A leaves → FCFS: the OLDEST overflow (B) is promoted, not C. ──
        a.leave().await;
        let promo = bridge
            .scraper
            .await_event(Duration::from_secs(60), |e| e.message.contains(PROMOTED))
            .await
            .context("no overflow speaker was promoted after A left")?;
        match promo.field("promoted") {
            Some(k) if k == bob_key => {}
            other => bail!(
                "FCFS violated: expected the oldest overflow (B, key={bob_key}) promoted, \
                 got promoted={other:?}"
            ),
        }
        assert_one_puppet(&probe, &targets, anchor, "after B was promoted").await?;
        eprintln!("   B (oldest overflow) promoted into the freed slot — FCFS holds");

        // ── Teardown: everyone leaves → guild-wide baseline. ──
        b.leave().await;
        c.leave().await;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    let baseline = match &outcome {
        Ok(()) => probe.await_baseline(&targets, Duration::from_secs(90)).await,
        Err(_) => Ok(()), // don't mask the driving error with a baseline one
    };
    probe.shutdown().await;
    outcome?;
    baseline?;

    let errors = bridge.scraper.error_events();
    if !errors.is_empty() {
        bail!("bridge logged {} ERROR event(s); first: {}", errors.len(), errors[0].message);
    }
    Ok(())
}

/// Assert that exactly one watched bot — `expected` — is in the guild's voice,
/// proving overflow never spun up a second bot. Polls briefly so a cache miss
/// (`None`) or a not-yet-settled snapshot doesn't false-fail (#72 F6-3).
async fn assert_one_puppet(
    probe: &ConnCountProbe,
    targets: &HashSet<u64>,
    expected: u64,
    when: &str,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut last = None;
    loop {
        if let Some(present) = probe.voice_members_among(targets) {
            last = Some(present.clone());
            if present.len() == 1 && present[0] == expected {
                return Ok(());
            }
            if present.len() > 1 {
                bail!(
                    "expected exactly 1 puppet in voice {when}, found {} ({present:?}) — \
                     overflow wrongly allocated an extra bot",
                    present.len()
                );
            }
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("expected puppet {expected} alone in voice {when}, saw {last:?}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
