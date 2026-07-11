//! Multi-bridge isolation (catalogue #15) — two `[[bridge]]` tenants in one
//! process must stay fully independent: **no cross-talk**.
//!
//! Two unencrypted bridges run in the bridge-under-test — Auto Test 01
//! (room_a ↔ channel_a) and Auto Test 03 (room_c ↔ channel_c), both in the same
//! Discord guild. Because a bot holds one voice `Call` per guild, the two
//! channels must be served by **distinct anchor bots** — so the harness pool
//! needs ≥2 puppets.
//!
//! A Matrix speaker in each room drives a seeded probe; a Discord listener in
//! each channel records its puppet. The isolation assertions:
//!
//! * **distinct anchors** — the puppet serving room_a ≠ the puppet serving room_c;
//! * **presence isolation** — room_a's puppet is only ever in channel_a (never
//!   channel_c) and vice-versa;
//! * **no audio cross-talk (both directions)** — channel_a's puppet plays back
//!   only room_a's probe (`seedA`) and NOT room_c's (`seedC`), and channel_c's
//!   puppet only `seedC` and NOT `seedA`;
//! * **teardown independence** — one bridge's speaker leaving releases only that
//!   bridge's puppet; the other bridge's puppet stays in voice.
//!
//! Runs with prod up (dedicated harness bots + the nvbtest appservice).

use super::audio::{
    DRAIN, PROBE_MS, SETTLE, await_quiescent, loudest, oracle_absent, oracle_assert, puppet_ids,
    sweep_test_memberships,
};
use crate::actors::discord::DiscordListener;
use crate::actors::matrix::MatrixSpeaker;
use crate::bridge_proc::{BridgeUnderTest, tenant_entry};
use crate::observers::RoomStatePoller;
use crate::scenario::{Ctx, Expectation, ScenarioDef};
use anyhow::{Context as _, Result, bail};
use std::sync::Arc;
use std::time::Duration;

/// #15 — two bridges in one process, no cross-talk.
pub static MULTI_BRIDGE_ISOLATION: ScenarioDef = ScenarioDef {
    name: "multi_bridge_isolation",
    expectation: Expectation::Pass,
    run: |ctx| Box::pin(run_multi_bridge_isolation(ctx)),
};

/// Distinct probe seeds so a channel's recording can be scored against BOTH —
/// its own (must match) and the other's (must be absent).
const SEED_A: u64 = 0x15A0;
const SEED_C: u64 = 0x15C0;
/// A matching stream globally aligns high (own-probe crossings clear the
/// oracle's 0.75 mean floor); two *different* seeds stay well below even under
/// the wide alignment search, so 0.5 separates "same probe" from "different
/// probe / silence" with margin on both sides (leak → red, clean → green).
const CROSS_TALK_MAX_NCC: f32 = 0.5;

/// The two bridged tenants (also their log-span labels for `in_bridge`).
const NAME_A: &str = "Auto Test 01";
const NAME_C: &str = "Auto Test 03";

async fn run_multi_bridge_isolation(ctx: Arc<Ctx>) -> Result<()> {
    let cfg = &ctx.cfg;
    let (room_c, channel_c) = cfg.at03_pair()?;
    let (room_c, channel_c) = (room_c.to_owned(), channel_c.to_owned());
    cfg.test_as_token().context("multi_bridge_isolation needs the test appservice")?;
    if cfg.puppet_tokens.len() < 2 {
        bail!(
            "multi_bridge_isolation needs ≥2 harness puppet bots: the two same-guild \
             channels each need a distinct anchor bot (one voice Call per guild per bot). \
             Provision a second nvb-test-puppet and add its token to NVB_E2E_PUPPET_TOKENS."
        );
    }
    if cfg.speaker_tokens.len() < 2 {
        bail!(
            "multi_bridge_isolation needs ≥2 speaker bot tokens (one Discord listener per \
             channel) — set NVB_E2E_SPEAKER_TOKENS to at least two nvb-test-speaker tokens"
        );
    }

    let poller = RoomStatePoller::new(cfg)?;
    sweep_test_memberships(cfg, &poller, &cfg.room_a).await?;
    sweep_test_memberships(cfg, &poller, &room_c).await?;

    // Two tenants, one process, full pool.
    let mut tenants = tenant_entry(NAME_A, &cfg.room_a, &cfg.guild_id, &cfg.channel_a);
    tenants.push_str(&tenant_entry(NAME_C, &room_c, &cfg.guild_id, &channel_c));
    let bridge = BridgeUnderTest::start(cfg, ctx.profile, &tenants, &ctx.artifacts.join("bridge"))
        .await
        .context("starting bridge-under-test")?;
    if let Err(e) = bridge.await_ready(&[NAME_A, NAME_C], Duration::from_secs(120)).await {
        bridge.kill().await;
        return Err(e);
    }

    let result = drive_isolation(&ctx, &bridge, &room_c, &channel_c).await;
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

async fn drive_isolation(
    ctx: &Ctx,
    bridge: &BridgeUnderTest,
    room_c: &str,
    channel_c: &str,
) -> Result<()> {
    let cfg = &ctx.cfg;
    let targets = puppet_ids(cfg)?;

    // A speaker in each room; a listener in each channel (distinct tokens).
    let la = DiscordListener::connect(
        &cfg.speaker_tokens[0],
        &cfg.guild_id,
        &cfg.channel_a,
        targets.clone(),
    )
    .await?;
    let lc =
        DiscordListener::connect(&cfg.speaker_tokens[1], &cfg.guild_id, channel_c, targets.clone())
            .await?;

    // One async block that OWNS both speakers (so `leave(self)` can consume them
    // inline for the teardown-independence step) and only BORROWS the listeners
    // (so `la`/`lc` survive to `stop()` after). On an error path a not-yet-left
    // speaker is dropped; the next run's `sweep_test_memberships` clears it.
    let outcome: Result<(u64, u64)> = async {
        // Listeners join voice + start recording BEFORE the puppets arrive, so
        // each is present to catch the puppet's SpeakingStateUpdate (the SSRC→
        // user mapping). A listener that joins after a puppet is already speaking
        // continuously never sees a fresh speaking transition and records nothing
        // for it — the mirror of `audio_m2d`'s listener-joins-first ordering.
        la.join().await?;
        lc.join().await?;

        let speaker_a =
            MatrixSpeaker::join(cfg, &cfg.room_a, false, "nvbtest_alice", "NVB Test Alice").await?;
        let speaker_c =
            MatrixSpeaker::join(cfg, room_c, false, "nvbtest_carol", "NVB Test Carol").await?;

        // Each bridge must subscribe its speaker's track and start playback
        // before its puppet transmits anything.
        bridge
            .scraper
            .await_event(Duration::from_secs(90), |e| {
                e.message.contains("playback consumer started") && e.in_bridge(NAME_A)
            })
            .await
            .context("bridge A never subscribed room_a's speaker")?;
        bridge
            .scraper
            .await_event(Duration::from_secs(90), |e| {
                e.message.contains("playback consumer started") && e.in_bridge(NAME_C)
            })
            .await
            .context("bridge C never subscribed room_c's speaker")?;

        // Each speaker's puppet joins ITS channel.
        la.await_target_in_voice(Duration::from_secs(90))
            .await
            .context("no puppet joined channel_a for room_a's speaker")?;
        lc.await_target_in_voice(Duration::from_secs(90))
            .await
            .context("no puppet joined channel_c for room_c's speaker")?;
        let puppet_a = la
            .targets_in_channel()
            .and_then(|p| p.into_iter().next())
            .context("puppet in channel_a not visible in the listener cache")?;
        let puppet_c = lc
            .targets_in_channel()
            .and_then(|p| p.into_iter().next())
            .context("puppet in channel_c not visible in the listener cache")?;

        // ── Distinct anchors + presence isolation (both directions). ──
        if puppet_a == puppet_c {
            bail!(
                "both bridges served by the SAME bot {puppet_a} — two same-guild channels \
                 must use distinct anchor bots"
            );
        }
        if la.targets_in_channel().is_some_and(|p| p.contains(&puppet_c)) {
            bail!("room_c's puppet {puppet_c} leaked into channel_a (presence cross-talk)");
        }
        if lc.targets_in_channel().is_some_and(|p| p.contains(&puppet_a)) {
            bail!("room_a's puppet {puppet_a} leaked into channel_c (presence cross-talk)");
        }
        eprintln!("   distinct anchors: puppet_a={puppet_a} (chan A), puppet_c={puppet_c} (chan C)");

        // ── Drive both probes (listeners already recording). ──
        tokio::time::sleep(SETTLE).await;
        speaker_a.speak_probe(SEED_A, PROBE_MS).await?;
        tokio::time::sleep(Duration::from_millis(PROBE_MS as u64)).await;
        tokio::time::sleep(DRAIN).await;
        speaker_c.speak_probe(SEED_C, PROBE_MS).await?;
        tokio::time::sleep(Duration::from_millis(PROBE_MS as u64)).await;
        tokio::time::sleep(DRAIN).await;

        // ── Teardown independence: A leaving must not disturb bridge C. ──
        // (Done while the listeners' gateways are still live so channel presence
        // is observable; the probe audio is already buffered in the recordings.)
        speaker_a.leave().await;
        await_target_gone(&la, puppet_a, Duration::from_secs(60))
            .await
            .context("room_a's puppet never left channel_a after its speaker left")?;
        if !lc.targets_in_channel().is_some_and(|p| p.contains(&puppet_c)) {
            bail!(
                "bridge C's puppet {puppet_c} vanished when bridge A's speaker left — \
                 teardown is not isolated per bridge"
            );
        }
        eprintln!("   bridge A torn down; bridge C's puppet {puppet_c} intact");
        speaker_c.leave().await;
        Ok((puppet_a, puppet_c))
    }
    .await;

    // Stop both listeners to harvest recordings regardless of outcome.
    let rec_a = la.stop().await;
    let rec_c = lc.stop().await;
    let (puppet_a, puppet_c) = outcome?;

    // ── No audio cross-talk, both directions. ──
    let (uid_a, cap_a) =
        loudest(rec_a).context("channel_a listener recorded nothing from any puppet")?;
    if uid_a != puppet_a {
        bail!("channel_a recording came from {uid_a}, expected room_a's puppet {puppet_a}");
    }
    oracle_assert(&ctx.artifacts, "chanA_ownprobe", SEED_A, PROBE_MS, &cap_a)?;
    oracle_absent(&ctx.artifacts, "chanA_no_seedC", SEED_C, PROBE_MS, &cap_a, CROSS_TALK_MAX_NCC)?;

    let (uid_c, cap_c) =
        loudest(rec_c).context("channel_c listener recorded nothing from any puppet")?;
    if uid_c != puppet_c {
        bail!("channel_c recording came from {uid_c}, expected room_c's puppet {puppet_c}");
    }
    oracle_assert(&ctx.artifacts, "chanC_ownprobe", SEED_C, PROBE_MS, &cap_c)?;
    oracle_absent(&ctx.artifacts, "chanC_no_seedA", SEED_A, PROBE_MS, &cap_c, CROSS_TALK_MAX_NCC)?;

    let errors = bridge.scraper.error_events();
    if !errors.is_empty() {
        bail!("bridge logged {} ERROR event(s); first: {}", errors.len(), errors[0].message);
    }
    Ok(())
}

/// Poll a listener's gateway cache until `user` is no longer in its channel
/// (`None` cache misses keep polling, #72 F6-3).
async fn await_target_gone(
    listener: &DiscordListener,
    user: u64,
    timeout: Duration,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(present) = listener.targets_in_channel() {
            if !present.contains(&user) {
                return Ok(());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("user {user} still in the listener's channel after {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
