//! Cross-channel steal → release → re-promotion (catalogue #6, issue #24) — the
//! **live variant** of the hermetic
//! `puppet.rs::scenario_steal_release_repromotion_two_rounds` test. It proves,
//! end-to-end against the real Discord/Matrix stack, that a donor channel's
//! speaker demoted by a steal is re-promoted once the stealing channel returns
//! the bot to the pool (`promote_overflow_into_free_bots`, merged in `258eef7`)
//! instead of lingering in the overflow mix forever.
//!
//! Two same-guild tenants — Auto Test 01 (donor, room_a↔channel_a) and Auto
//! Test 03 (thief, room_c↔channel_c) — run in one process on a **restricted
//! 2-bot pool** ([`BridgeUnderTest::start_with_pool_size`]). Membership-only
//! [`MatrixPhantom`]s drive it:
//!
//! * **Alice** joins room A → takes bot 0 as dedicated puppet, elected anchor.
//! * **Bob** joins room A → takes bot 1; the pool is now exhausted. Both bots
//!   sit in channel A.
//! * **Carol** joins room C → C has no anchor and no free bot, so
//!   `steal_for_activation` takes A's newest non-anchor puppet: Bob's bot hops
//!   to channel C and **Bob is demoted into A's overflow mix**.
//! * **Carol leaves** → C's bot returns to the pool; the cross-channel
//!   `rebalance` sweep must promote Bob back into the freed bot — the
//!   `promoted overflow speaker into freed pool bot (cross-channel)` log, with
//!   Bob's bot rejoining channel A as his dedicated puppet.
//!
//! Presence is asserted per-channel via the read-only [`ConnCountProbe`]
//! gateway cache (no `DiscordListener` joins voice: a Discord human sitting in
//! channel C would defer the bot's release behind the capture-anchor idle
//! grace). Membership-driven, no audio oracle — low-flake. Runs with prod up
//! (dedicated harness bots + the nvbtest appservice).
//!
//! The issue's guild-isolation half (a freed bot only serves guilds it is a
//! member of) stays hermetic-only — the live env has a single test guild — see
//! `puppet.rs::freed_bot_only_repromotes_guild_capable_channel`.

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

/// #6 — steal for activation, then cross-channel re-promotion when the bot frees.
pub static STEAL_RELEASE_REPROMOTION: ScenarioDef = ScenarioDef {
    name: "steal_release_repromotion",
    expectation: Expectation::Pass,
    run: |ctx| Box::pin(run_steal_repromotion(ctx)),
};

/// Pool-level allocation logs (`crates/discord/src/puppet.rs`), matched as
/// substrings of the JSON `message` field.
const ASSIGNED_PUPPET: &str = "assigned participant to puppet";
const STOLE_FROM_DONOR: &str = "stole bot from donor to activate channel";
const STOLE_ANCHOR: &str = "stole bot to activate channel anchor";
const PROMOTED_CROSS: &str = "promoted overflow speaker into freed pool bot (cross-channel)";

/// The donor and thief tenants (bridge labels are also the pool's channel keys,
/// carried verbatim in the allocation logs' `bridge`/`donor`/`channel` fields).
const NAME_A: &str = "Auto Test 01";
const NAME_C: &str = "Auto Test 03";

async fn run_steal_repromotion(ctx: Arc<Ctx>) -> Result<()> {
    let cfg = &ctx.cfg;
    // The phantoms masquerade under the nvbtest appservice.
    cfg.test_as_token().context("steal_release_repromotion needs the test appservice")?;
    let (room_c, channel_c) = cfg.at03_pair()?;
    let (room_c, channel_c) = (room_c.to_owned(), channel_c.to_owned());
    if cfg.puppet_tokens.len() < 2 {
        bail!(
            "steal_release_repromotion needs ≥2 harness puppet bots: the donor channel \
             must hold two dedicated puppets before the thief channel has anything to \
             steal. Provision a second nvb-test-puppet and add its token to \
             NVB_E2E_PUPPET_TOKENS."
        );
    }

    let poller = RoomStatePoller::new(cfg)?;
    sweep_test_memberships(cfg, &poller, &cfg.room_a).await?;
    sweep_test_memberships(cfg, &poller, &room_c).await?;

    // Two same-guild tenants, pool restricted to exactly 2 bots — so Carol's
    // activation can only be satisfied by stealing, deterministically.
    let mut tenants = tenant_entry(NAME_A, &cfg.room_a, &cfg.guild_id, &cfg.channel_a);
    tenants.push_str(&tenant_entry(NAME_C, &room_c, &cfg.guild_id, &channel_c));
    let bridge =
        BridgeUnderTest::start_with_pool_size(cfg, ctx.profile, &tenants, &ctx.artifacts.join("bridge"), 2)
            .await
            .context("starting bridge-under-test")?;
    if let Err(e) = bridge.await_ready(&[NAME_A, NAME_C], Duration::from_secs(120)).await {
        bridge.kill().await;
        return Err(e);
    }

    let result = drive_steal_repromotion(&ctx, &bridge, &room_c, &channel_c).await;
    match result {
        Ok(()) => {
            let _ = await_quiescent(&poller, &cfg.room_a).await;
            let _ = await_quiescent(&poller, &room_c).await;
            bridge.sigterm_and_wait(Duration::from_secs(20)).await.context("SIGTERM drain")
        }
        Err(e) => {
            bridge.kill().await;
            Err(e)
        }
    }
}

async fn drive_steal_repromotion(
    ctx: &Ctx,
    bridge: &BridgeUnderTest,
    room_c: &str,
    channel_c: &str,
) -> Result<()> {
    let cfg = &ctx.cfg;
    let targets = puppet_ids(cfg)?;
    let chan_a: u64 = cfg.channel_a.parse().context("channel_a is not a numeric Discord id")?;
    let chan_c: u64 = channel_c.parse().context("channel_c is not a numeric Discord id")?;
    let probe =
        ConnCountProbe::connect(cfg.speaker_tokens.first().context("probe token")?, &cfg.guild_id)
            .await?;

    let outcome = async {
        // ── Alice: first participant in A claims bot 0 + becomes anchor. ──
        let alice =
            MatrixPhantom::join(cfg, &cfg.room_a, false, false, "nvbtest_alice", "NVB Test Alice")
                .await?;
        bridge
            .scraper
            .await_event(Duration::from_secs(90), |e| {
                e.message.contains(ASSIGNED_PUPPET)
                    && e.field("display_name") == Some("NVB Test Alice")
            })
            .await
            .context("Alice never got a dedicated puppet")?;
        let anchor = await_puppet_present(&probe, &targets, Duration::from_secs(60))
            .await
            .context("Alice's puppet never appeared in Discord voice")?;
        eprintln!("   Alice anchored on puppet {anchor} in channel A");

        // ── Bob: second dedicated puppet in A — the pool is now exhausted. ──
        let bob =
            MatrixPhantom::join(cfg, &cfg.room_a, false, false, "nvbtest_bob", "NVB Test Bob")
                .await?;
        let bob_ev = bridge
            .scraper
            .await_event(Duration::from_secs(60), |e| {
                e.message.contains(ASSIGNED_PUPPET)
                    && e.field("display_name") == Some("NVB Test Bob")
            })
            .await
            .context("Bob never got a dedicated puppet (steal needs a 2-puppet donor)")?;
        let bob_key = bob_ev
            .field("key")
            .context("Bob's assignment log carried no `key` field")?
            .to_owned();
        let in_a = await_channel_members(&probe, &targets, chan_a, 2, "after Bob joined").await?;
        let bob_bot = *in_a
            .iter()
            .find(|uid| **uid != anchor)
            .context("second puppet in channel A is the anchor itself?")?;
        eprintln!("   Bob got dedicated puppet {bob_bot} (key={bob_key}) — pool exhausted");

        // ── Carol: C is anchor-less + pool empty → steal Bob's bot from A. ──
        let carol =
            MatrixPhantom::join(cfg, room_c, false, false, "nvbtest_carol", "NVB Test Carol")
                .await?;
        let steal = bridge
            .scraper
            .await_event(Duration::from_secs(60), |e| e.message.contains(STOLE_FROM_DONOR))
            .await
            .context("Carol's activation never stole a bot (did she wrongly get a free one?)")?;
        if steal.field("donor") != Some(NAME_A) {
            bail!("steal took a bot from {:?}, expected donor {NAME_A}", steal.field("donor"));
        }
        match steal.field("demoted") {
            Some(k) if k == bob_key => {}
            other => bail!(
                "steal demoted {other:?}, expected the donor's newest non-anchor \
                 puppet (Bob, key={bob_key})"
            ),
        }
        bridge
            .scraper
            .await_event(Duration::from_secs(60), |e| {
                e.message.contains(STOLE_ANCHOR) && e.field("bridge") == Some(NAME_C)
            })
            .await
            .context("the stolen bot was never assigned as channel C's anchor")?;
        // Bob's bot hops A→C; Bob is now blended into A's anchor mix.
        let in_c =
            await_channel_members(&probe, &targets, chan_c, 1, "after the steal (channel C)")
                .await?;
        if in_c[0] != bob_bot {
            bail!("channel C got bot {}, expected the stolen bot {bob_bot}", in_c[0]);
        }
        let in_a =
            await_channel_members(&probe, &targets, chan_a, 1, "after the steal (channel A)")
                .await?;
        if in_a[0] != anchor {
            bail!("channel A kept bot {}, expected the anchor {anchor}", in_a[0]);
        }
        eprintln!("   Carol's activation stole {bob_bot} from A — Bob demoted to overflow");

        // ── Carol leaves → the bot frees → cross-channel sweep re-promotes Bob. ──
        carol.leave().await;
        let promo = bridge
            .scraper
            .await_event(Duration::from_secs(60), |e| e.message.contains(PROMOTED_CROSS))
            .await
            .context(
                "no cross-channel re-promotion after the thief channel freed its bot — \
                 Bob is stranded in overflow (the #24 regression)",
            )?;
        if promo.field("channel") != Some(NAME_A) {
            bail!("re-promotion targeted {:?}, expected channel {NAME_A}", promo.field("channel"));
        }
        match promo.field("promoted") {
            Some(k) if k == bob_key => {}
            other => bail!("re-promotion picked {other:?}, expected Bob (key={bob_key})"),
        }
        let in_a = await_channel_members(&probe, &targets, chan_a, 2, "after re-promotion").await?;
        if !in_a.contains(&bob_bot) {
            bail!("channel A has {in_a:?} after re-promotion, expected {bob_bot} back in it");
        }
        await_channel_members(&probe, &targets, chan_c, 0, "after re-promotion (channel C)")
            .await?;
        eprintln!("   Bob re-promoted into freed bot {bob_bot} — back in channel A, C empty");

        // ── Teardown: everyone leaves → guild-wide baseline. ──
        alice.leave().await;
        bob.leave().await;
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

/// Poll until exactly `want` of the watched bots sit in the given voice channel,
/// returning them. Cache misses (`None`) and transitional counts keep polling
/// (#72 F6-3) — a steal hop takes a moment to show in the gateway cache.
async fn await_channel_members(
    probe: &ConnCountProbe,
    targets: &HashSet<u64>,
    channel: u64,
    want: usize,
    when: &str,
) -> Result<Vec<u64>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut last = None;
    loop {
        if let Some(present) = probe.voice_members_in_channel(targets, channel) {
            if present.len() == want {
                return Ok(present);
            }
            last = Some(present);
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("expected {want} puppet(s) in channel {channel} {when}, saw {last:?}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
