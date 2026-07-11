//! Mod-driven adversarial scenarios (catalogue #9/#10): the [`DiscordMod`]
//! REST actor voice-kicks / force-moves the bridge's puppet mid-call and the
//! bridge must recover on its own.
//!
//! * `external_disconnect_recovery` — a moderator disconnects the puppet from
//!   voice; the #46 recovery (songbird `DriverDisconnect` → same-bot rejoin)
//!   must bring it back AND audio must resume (oracled).
//! * `force_move_recovery` — a moderator drags the puppet into another
//!   channel; the connection survives, so no `DriverDisconnect` fires — only
//!   the bridge's own voice-state probe (`VerifyConnected` → `voice_displaced`)
//!   notices and rejoins the assigned channel. Guards the #62 fix
//!   (v0.3.0-alpha.11); the original catalogue listed this as the #62
//!   KnownFail, filed before that fix landed.
//!
//! Both run with prod up (dedicated harness bots + the nvbtest appservice).

use super::audio::{
    DRAIN, PROBE_MS, SETTLE, await_quiescent, loudest, oracle_assert, puppet_ids,
    sweep_test_memberships,
};
use crate::actors::discord::{DiscordListener, DiscordMod};
use crate::actors::matrix::MatrixSpeaker;
use crate::bridge_proc::{BridgeUnderTest, tenant_entry};
use crate::observers::{ConnCountProbe, RoomStatePoller};
use crate::scenario::{Ctx, Expectation, ScenarioDef};
use anyhow::{Context as _, Result, bail};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

/// #9 — a mod voice-kick mid-call must self-heal: rejoin + audio resumes.
pub static EXTERNAL_DISCONNECT_RECOVERY: ScenarioDef = ScenarioDef {
    name: "external_disconnect_recovery",
    expectation: Expectation::Pass,
    run: |ctx| Box::pin(run_external_disconnect_recovery(ctx)),
};
/// #10 — a mod force-move mid-call must self-heal: rejoin the ASSIGNED channel.
pub static FORCE_MOVE_RECOVERY: ScenarioDef = ScenarioDef {
    name: "force_move_recovery",
    expectation: Expectation::Pass,
    run: |ctx| Box::pin(run_force_move_recovery(ctx)),
};

/// The recovery-claim log (`puppet.rs`, shared by the `DriverDropped` and
/// `VerifyConnected` paths) — the bridge NOTICED the adversarial event.
const RECOVERING: &str = "recovering (rejoin)";
/// The recovery-success log — the SAME bot is back in its assigned channel.
const RECOVERED: &str = "puppet voice connection recovered";

// ─── #9 external_disconnect_recovery ─────────────────────────────────────────────

async fn run_external_disconnect_recovery(ctx: Arc<Ctx>) -> Result<()> {
    let cfg = &ctx.cfg;
    let bridge_name = "Auto Test 01";
    let seed = 0xD3E9;
    let poller = RoomStatePoller::new(cfg)?;
    let targets = puppet_ids(cfg)?;
    sweep_test_memberships(cfg, &poller, &cfg.room_a).await?;

    let tenants = tenant_entry(bridge_name, &cfg.room_a, &cfg.guild_id, &cfg.channel_a);
    let bridge = BridgeUnderTest::start(cfg, ctx.profile, &tenants, &ctx.artifacts.join("bridge"))
        .await
        .context("starting bridge-under-test")?;
    if let Err(e) = bridge.await_ready(&[bridge_name], Duration::from_secs(120)).await {
        bridge.kill().await;
        return Err(e);
    }

    let result = drive_disconnect_recovery(&ctx, &bridge, bridge_name, seed, targets).await;
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

async fn drive_disconnect_recovery(
    ctx: &Ctx,
    bridge: &BridgeUnderTest,
    bridge_name: &str,
    seed: u64,
    targets: HashSet<u64>,
) -> Result<()> {
    let cfg = &ctx.cfg;
    let moderator = DiscordMod::connect(cfg.mod_token()?, &cfg.guild_id).await?;

    // The far-side ear doubles as the voice-presence view (its gateway cache).
    let listener = DiscordListener::connect(
        cfg.speaker_tokens.first().context("listener token")?,
        &cfg.guild_id,
        &cfg.channel_a,
        targets,
    )
    .await?;
    let outcome = async {
        listener.join().await?;

        let speaker =
            MatrixSpeaker::join(cfg, &cfg.room_a, false, "nvbtest_alice", "NVB Test Alice").await?;
        let driven = async {
            bridge
                .scraper
                .await_event(Duration::from_secs(90), |e| {
                    e.message.contains("playback consumer started") && e.in_bridge(bridge_name)
                })
                .await
                .context("bridge never subscribed the test speaker's track")?;
            listener
                .await_target_in_voice(Duration::from_secs(60))
                .await
                .context("no puppet joined the voice channel for the test speaker")?;
            let puppet = listener
                .targets_in_channel()
                .and_then(|p| p.into_iter().next())
                .context("puppet in voice but not visible in the listener's cache")?;

            // ── The adversarial event: a moderator voice-kicks the puppet. ──
            moderator.disconnect(puppet).await?;
            eprintln!("   mod voice-kicked puppet {puppet}; awaiting #46 recovery");

            // The bridge must notice (DriverDisconnect and/or the voice-state
            // probe — the recovery latch dedupes them) and rejoin the SAME bot.
            bridge
                .scraper
                .await_event(Duration::from_secs(60), |e| e.message.contains(RECOVERING))
                .await
                .context("bridge never noticed the external disconnect (#46 recovery)")?;
            bridge
                .scraper
                .await_event(Duration::from_secs(60), |e| e.message.contains(RECOVERED))
                .await
                .context("puppet never recovered from the external disconnect (#46 rejoin)")?;
            listener
                .await_target_in_voice(Duration::from_secs(60))
                .await
                .context("recovery logged but no puppet reappeared in the voice channel")?;
            eprintln!("   puppet back in voice; probing post-recovery audio");

            // ── Audio must RESUME through the recovered connection. ──
            tokio::time::sleep(SETTLE).await;
            speaker.speak_probe(seed, PROBE_MS).await?;
            tokio::time::sleep(Duration::from_millis(PROBE_MS as u64)).await;
            tokio::time::sleep(DRAIN).await;
            Ok::<(), anyhow::Error>(())
        }
        .await;
        speaker.leave().await;
        driven?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    let recordings = listener.stop().await;
    outcome?;

    let (uid, captured) = loudest(recordings)
        .context("the Discord listener recorded nothing from any puppet")?;
    eprintln!("   recorded {} samples from puppet {uid}", captured.len());
    oracle_assert(&ctx.artifacts, "disconnect_recovery", seed, PROBE_MS, &captured)?;

    let errors = bridge.scraper.error_events();
    if !errors.is_empty() {
        bail!("bridge logged {} ERROR event(s); first: {}", errors.len(), errors[0].message);
    }
    Ok(())
}

// ─── #10 force_move_recovery ─────────────────────────────────────────────────────

async fn run_force_move_recovery(ctx: Arc<Ctx>) -> Result<()> {
    let cfg = &ctx.cfg;
    let bridge_name = "Auto Test 01";
    // The move target: AT03's voice channel (unbridged in this tenant set).
    let (_, channel_c) = cfg.at03_pair()?;
    let channel_c: u64 = channel_c.parse().context("AT03 channel id")?;
    let channel_a: u64 = cfg.channel_a.parse().context("AT01 channel id")?;
    let poller = RoomStatePoller::new(cfg)?;
    let targets = puppet_ids(cfg)?;
    sweep_test_memberships(cfg, &poller, &cfg.room_a).await?;

    let tenants = tenant_entry(bridge_name, &cfg.room_a, &cfg.guild_id, &cfg.channel_a);
    let bridge = BridgeUnderTest::start(cfg, ctx.profile, &tenants, &ctx.artifacts.join("bridge"))
        .await
        .context("starting bridge-under-test")?;
    if let Err(e) = bridge.await_ready(&[bridge_name], Duration::from_secs(120)).await {
        bridge.kill().await;
        return Err(e);
    }

    let result =
        drive_force_move(&ctx, &bridge, bridge_name, channel_a, channel_c, &targets).await;
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

async fn drive_force_move(
    ctx: &Ctx,
    bridge: &BridgeUnderTest,
    bridge_name: &str,
    channel_a: u64,
    channel_c: u64,
    targets: &HashSet<u64>,
) -> Result<()> {
    let cfg = &ctx.cfg;
    let moderator = DiscordMod::connect(cfg.mod_token()?, &cfg.guild_id).await?;
    // No audio assertion here (the #9 oracle covers the shared rejoin path),
    // so a bare gateway-cache probe is the presence view — no listener needed.
    let probe =
        ConnCountProbe::connect(cfg.speaker_tokens.first().context("probe token")?, &cfg.guild_id)
            .await?;

    let outcome = async {
        let speaker =
            MatrixSpeaker::join(cfg, &cfg.room_a, false, "nvbtest_alice", "NVB Test Alice").await?;
        let driven = async {
            bridge
                .scraper
                .await_event(Duration::from_secs(90), |e| {
                    e.message.contains("playback consumer started") && e.in_bridge(bridge_name)
                })
                .await
                .context("bridge never subscribed the test speaker's track")?;
            let puppet =
                await_puppet_in_channel(&probe, targets, channel_a, Duration::from_secs(60))
                    .await
                    .context("no puppet joined AT01's voice channel for the test speaker")?;

            // ── The adversarial event: a moderator drags the puppet to AT03. ──
            // (HTTP success = Discord executed the move; the bridge's 1 s
            // delayed VerifyConnected probe may rejoin before a cache poll
            // could even observe the displacement, so it isn't asserted.)
            moderator.force_move(puppet, channel_c).await?;
            eprintln!("   mod force-moved puppet {puppet} to AT03; awaiting #62 recovery");

            // The bridge's own voice-state probe must see the displacement
            // (no DriverDisconnect fires — the connection survived) and
            // rejoin the SAME bot to its ASSIGNED channel.
            bridge
                .scraper
                .await_event(Duration::from_secs(60), |e| e.message.contains(RECOVERING))
                .await
                .context("bridge never noticed the force-move displacement (#62 probe)")?;
            bridge
                .scraper
                .await_event(Duration::from_secs(60), |e| e.message.contains(RECOVERED))
                .await
                .context("puppet never recovered from the force-move (#62 rejoin)")?;
            let returned =
                await_puppet_in_channel(&probe, targets, channel_a, Duration::from_secs(60))
                    .await
                    .context("recovery logged but no puppet returned to AT01's channel")?;
            eprintln!("   puppet {returned} back in AT01's channel");

            // Nothing may linger in the move-target channel (a failover that
            // stranded the displaced bot in AT03 would pass the check above).
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            loop {
                match probe.voice_members_in_channel(targets, channel_c) {
                    Some(left) if left.is_empty() => break,
                    state => {
                        if tokio::time::Instant::now() >= deadline {
                            bail!("puppet(s) still in the AT03 channel after recovery: {state:?}");
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;
        speaker.leave().await;
        driven
    }
    .await;

    // The speaker's leave releases the puppet (leave grace) → guild-wide
    // baseline, force-move notwithstanding.
    let baseline = match outcome {
        Ok(()) => probe.await_baseline(targets, Duration::from_secs(90)).await,
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

/// Poll until one of `ids` is in the given voice channel, returning its user
/// id (`None`-returning cache misses keep polling, #72 F6-3).
async fn await_puppet_in_channel(
    probe: &ConnCountProbe,
    ids: &HashSet<u64>,
    channel: u64,
    timeout: Duration,
) -> Result<u64> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(uid) = probe
            .voice_members_in_channel(ids, channel)
            .and_then(|present| present.into_iter().next())
        {
            return Ok(uid);
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("no watched bot appeared in voice channel {channel}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
