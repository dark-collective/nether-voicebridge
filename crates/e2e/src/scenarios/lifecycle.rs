//! Membership-lifecycle scenarios (catalogue #8/#13): the two **real-timing**
//! scenarios whose whole point is the production grace windows, so they run
//! under `--profile real` (nightly) rather than the compressed `--all` pass.
//!
//! * `reap_release` (#8) — a phantom `m.call.member` with no live LiveKit
//!   participant is emptied by the #25 reaper after `reap_grace` (45 s real),
//!   and the reap must RELEASE the puppet the membership had been assigned
//!   (the #54 fix: a bot-sent retraction is attributed to the phantom's MXID,
//!   not the bot, so the leave-grace release actually runs).
//! * `leave_grace_debounce` (#13) — a membership that leaves and rejoins the
//!   same MXID within `leave_grace` (12 s real) must NOT tear the puppet down:
//!   the reappear cancels the pending release (Element Call memberships flap
//!   spuriously; an immediate release would needlessly churn the puppet,
//!   anchor, and subscriber).
//!
//! Both drive a **presence-only** [`MatrixPhantom`]: puppet presence is
//! membership-driven (assign on join, release on leave), so a phantom exercises
//! the exact assign/reap/debounce paths without a LiveKit track — and, for
//! #13, without the reconnect latency that would jeopardise the 12 s window.
//! Both run with prod up (dedicated harness bots + the nvbtest appservice).

use super::audio::{await_quiescent, puppet_ids, sweep_test_memberships};
use crate::actors::matrix::MatrixPhantom;
use crate::bridge_proc::{BridgeUnderTest, tenant_entry};
use crate::config::Profile;
use crate::observers::{ConnCountProbe, RoomStatePoller, SenderClass};
use crate::scenario::{Ctx, Expectation, ScenarioDef};
use anyhow::{Context as _, Result, bail};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

/// #8 — the reaper empties a stale phantom membership and releases its puppet.
pub static REAP_RELEASE: ScenarioDef = ScenarioDef {
    name: "reap_release",
    expectation: Expectation::Pass,
    run: |ctx| Box::pin(run_reap_release(ctx)),
};
/// #13 — a leave+rejoin within the leave grace never tears the puppet down.
pub static LEAVE_GRACE_DEBOUNCE: ScenarioDef = ScenarioDef {
    name: "leave_grace_debounce",
    expectation: Expectation::Pass,
    run: |ctx| Box::pin(run_leave_grace_debounce(ctx)),
};

/// The em-dash-free anchors of the membership-lifecycle logs
/// (`crates/matrix-rtc/src/matrix.rs`), matched as substrings so the exact
/// em-dash in each message never has to round-trip through this source.
const ASSIGNING_PUPPET: &str = "assigning puppet"; // "member joined — assigning puppet"
const GRACE_TIMER_ARMED: &str = "starting leave grace timer"; // "membership gone — ..."
const RELEASE_CANCELLED: &str = "cancelled pending release"; // "membership re-appeared within grace — ..."
const GRACE_ELAPSED: &str = "leave grace elapsed"; // "leave grace elapsed — releasing puppet"
const REAPED: &str = "reaped phantom"; // "reaped phantom m.call.member (...)"

// ─── #8 reap_release ─────────────────────────────────────────────────────────────

async fn run_reap_release(ctx: Arc<Ctx>) -> Result<()> {
    if skip_unless_real(&ctx, "reap_release", "reap_grace is 3 s compressed / 45 s real") {
        return Ok(());
    }
    let cfg = &ctx.cfg;
    let bridge_name = "Auto Test 01";
    let poller = RoomStatePoller::new(cfg)?;
    sweep_test_memberships(cfg, &poller, &cfg.room_a).await?;

    let tenants = tenant_entry(bridge_name, &cfg.room_a, &cfg.guild_id, &cfg.channel_a);
    let bridge = BridgeUnderTest::start(cfg, ctx.profile, &tenants, &ctx.artifacts.join("bridge"))
        .await
        .context("starting bridge-under-test")?;
    if let Err(e) = bridge.await_ready(&[bridge_name], Duration::from_secs(120)).await {
        bridge.kill().await;
        return Err(e);
    }

    let result = drive_reap_release(&ctx, &bridge).await;
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

async fn drive_reap_release(ctx: &Ctx, bridge: &BridgeUnderTest) -> Result<()> {
    let cfg = &ctx.cfg;
    let poller = RoomStatePoller::new(cfg)?;
    let targets = puppet_ids(cfg)?;
    let probe =
        ConnCountProbe::connect(cfg.speaker_tokens.first().context("probe token")?, &cfg.guild_id)
            .await?;

    let outcome = async {
        // A membership-only phantom: it posts `m.call.member` — opening the call,
        // so the subscriber joins LiveKit and the reaper ticks — but NEVER joins
        // LiveKit itself, so it has no live participant: exactly the shape the #25
        // reaper hunts.
        let phantom =
            MatrixPhantom::join(cfg, &cfg.room_a, false, false, "nvbtest_alice", "NVB Test Alice")
                .await?;

        // The bridge assigns a puppet purely on the membership, and that puppet
        // joins Discord voice. The #54 guard needs a puppet actually IN voice so
        // the reap below can prove it gets RELEASED.
        bridge
            .scraper
            .await_event(Duration::from_secs(90), |e| e.message.contains(ASSIGNING_PUPPET))
            .await
            .context("bridge never assigned a puppet for the phantom membership")?;
        let puppet = await_puppet_present(&probe, &targets, Duration::from_secs(60))
            .await
            .context("phantom's puppet never appeared in Discord voice")?;
        eprintln!(
            "   phantom's puppet {puppet} is in voice; killing the phantom (stale membership left behind)"
        );

        // Die WITHOUT retracting: the membership lingers with no live participant.
        phantom.kill().await;

        // The reaper (no-live-participant past `reap_grace`) empties the phantom.
        bridge
            .scraper
            .await_event(Duration::from_secs(150), |e| e.message.contains(REAPED))
            .await
            .context("reaper never emptied the stale phantom membership within reap_grace")?;

        // Out-of-band confirmation (not trusting the bridge log): no active
        // non-bridge membership remains in room state.
        poller
            .await_members(&cfg.room_a, Duration::from_secs(30), "phantom membership retracted", |m| {
                !m.iter().any(|e| e.class == SenderClass::Other && e.active)
            })
            .await?;

        // The #54 assertion: the reap is a bot-sent retraction on the phantom's
        // state_key; the fix attributes it to the phantom's MXID so the leave-grace
        // release runs (the pre-fix bug left the puppet in voice until restart).
        bridge
            .scraper
            .await_event(Duration::from_secs(30), |e| e.message.contains(GRACE_ELAPSED))
            .await
            .context("reaped membership never triggered the leave-grace puppet release (#54)")?;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    // After the reap+release the puppet must be gone from Discord voice.
    let baseline = match &outcome {
        Ok(()) => probe.await_baseline(&targets, Duration::from_secs(60)).await,
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

// ─── #13 leave_grace_debounce ────────────────────────────────────────────────────

async fn run_leave_grace_debounce(ctx: Arc<Ctx>) -> Result<()> {
    if skip_unless_real(&ctx, "leave_grace_debounce", "leave_grace is 2 s compressed / 12 s real") {
        return Ok(());
    }
    let cfg = &ctx.cfg;
    let bridge_name = "Auto Test 01";
    let poller = RoomStatePoller::new(cfg)?;
    sweep_test_memberships(cfg, &poller, &cfg.room_a).await?;

    let tenants = tenant_entry(bridge_name, &cfg.room_a, &cfg.guild_id, &cfg.channel_a);
    let bridge = BridgeUnderTest::start(cfg, ctx.profile, &tenants, &ctx.artifacts.join("bridge"))
        .await
        .context("starting bridge-under-test")?;
    if let Err(e) = bridge.await_ready(&[bridge_name], Duration::from_secs(120)).await {
        bridge.kill().await;
        return Err(e);
    }

    let result = drive_leave_grace_debounce(&ctx, &bridge).await;
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

async fn drive_leave_grace_debounce(ctx: &Ctx, bridge: &BridgeUnderTest) -> Result<()> {
    let cfg = &ctx.cfg;
    let targets = puppet_ids(cfg)?;
    let probe =
        ConnCountProbe::connect(cfg.speaker_tokens.first().context("probe token")?, &cfg.guild_id)
            .await?;

    let outcome = async {
        // First membership → puppet assigned and in Discord voice.
        let p1 =
            MatrixPhantom::join(cfg, &cfg.room_a, false, false, "nvbtest_alice", "NVB Test Alice")
                .await?;
        bridge
            .scraper
            .await_event(Duration::from_secs(90), |e| e.message.contains(ASSIGNING_PUPPET))
            .await
            .context("bridge never assigned a puppet for the first membership")?;
        let puppet = await_puppet_present(&probe, &targets, Duration::from_secs(60))
            .await
            .context("puppet never appeared in Discord voice")?;
        eprintln!("   puppet {puppet} in voice; flapping the membership within the leave grace");

        // Leave, then WAIT for the bridge to arm the grace timer before rejoining.
        // This proves the debounce path is armed AND guarantees ordering (the
        // bridge sees gone THEN appeared): without the wait, a reordered sync
        // could apply the rejoin as a fresh join and the leave as a real leave.
        p1.leave().await;
        bridge
            .scraper
            .await_event(Duration::from_secs(30), |e| e.message.contains(GRACE_TIMER_ARMED))
            .await
            .context("bridge never armed the leave-grace timer on the membership leave")?;

        // Rejoin the SAME MXID within the grace window (real 12 s). The reappear
        // must cancel the pending release. (A fresh phantom posts a new device
        // state_key, but the debounce keys on the bare MXID — the realistic EC
        // flap shape.)
        let p2 =
            MatrixPhantom::join(cfg, &cfg.room_a, false, false, "nvbtest_alice", "NVB Test Alice")
                .await?;
        bridge
            .scraper
            .await_event(Duration::from_secs(30), |e| e.message.contains(RELEASE_CANCELLED))
            .await
            .context("rejoin within the grace did not cancel the pending release (debounce broken)")?;

        // Wait past the original grace window (plus margin) to rule out a late
        // release fire, then assert the puppet never left voice.
        let settle = match ctx.profile {
            Profile::Compressed => Duration::from_secs(4),
            Profile::Real => Duration::from_secs(16),
        };
        tokio::time::sleep(settle).await;
        let released = bridge.scraper.events_matching(|e| e.message.contains(GRACE_ELAPSED));
        if !released.is_empty() {
            bail!("the puppet was released despite a rejoin within the leave grace (debounce failed)");
        }
        match probe.voice_members_among(&targets) {
            Some(present) if present.contains(&puppet) => {}
            other => bail!(
                "puppet {puppet} left Discord voice during the flap (debounce failed): {other:?}"
            ),
        }

        p2.leave().await;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    probe.shutdown().await;
    outcome?;

    let errors = bridge.scraper.error_events();
    if !errors.is_empty() {
        bail!("bridge logged {} ERROR event(s); first: {}", errors.len(), errors[0].message);
    }
    Ok(())
}

// ─── shared ──────────────────────────────────────────────────────────────────────

/// These two scenarios exist to exercise the PRODUCTION grace windows, so they
/// only run under `--profile real`; under compressed the windows shrink to a
/// few seconds — too tight for a real Matrix sync round-trip (a compressed
/// leave-grace rejoin would race the timer) — so they no-op with a note. The
/// nightly workflow runs them as a dedicated real-profile subset; the compressed
/// `--all` pass skips them here (kept in the registry so `--scenario <name>
/// --profile real` still selects them). Returns `true` when the caller should
/// skip.
fn skip_unless_real(ctx: &Ctx, name: &str, why: &str) -> bool {
    if ctx.profile == Profile::Real {
        return false;
    }
    eprintln!("   {name} skipped — real-timing scenario ({why}); run under --profile real");
    true
}

/// Poll until one of `ids` is in ANY voice channel of the test guild, returning
/// its user id (`None`-returning cache misses keep polling, #72 F6-3).
pub(super) async fn await_puppet_present(
    probe: &ConnCountProbe,
    ids: &HashSet<u64>,
    timeout: Duration,
) -> Result<u64> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(uid) =
            probe.voice_members_among(ids).and_then(|present| present.into_iter().next())
        {
            return Ok(uid);
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("no watched bot appeared in the test guild's voice channels");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
