//! `smoke_open_close` — catalogue #1, the lifecycle base case.
//!
//! A Discord speaker joins Auto Test 01 → the bridge minds a presence ghost
//! (membership in the Matrix room + log events) → the speaker plays a probe →
//! leaves → the ghost membership retracts → connection count and room state
//! return to baseline → SIGTERM drains cleanly.
//!
//! No Matrix human takes part, so the ghost stays presence-only (voice-less
//! puppets, no LiveKit publish — by design since #17/#46); audio-crossing
//! assertions arrive with the D2 Matrix actors.

use crate::actors::ParticipantDriver;
use crate::actors::discord::DiscordSpeaker;
use crate::bridge_proc::{BridgeUnderTest, tenant_entry};
use crate::observers::{ConnCountProbe, RoomStatePoller, SenderClass, decode_bot_user_id};
use crate::scenario::{Ctx, Expectation, ScenarioDef};
use anyhow::{Context as _, Result, bail};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

pub static SMOKE_OPEN_CLOSE: ScenarioDef = ScenarioDef {
    name: "smoke_open_close",
    expectation: Expectation::Pass,
    run: boxed,
};

fn boxed(ctx: Arc<Ctx>) -> crate::scenario::ScenarioFuture {
    Box::pin(run(ctx))
}

const BRIDGE_NAME: &str = "Auto Test 01";

async fn run(ctx: Arc<Ctx>) -> Result<()> {
    let cfg = &ctx.cfg;
    let poller = RoomStatePoller::new(cfg)?;
    let puppet_ids: HashSet<u64> =
        cfg.puppet_tokens.iter().filter_map(|t| decode_bot_user_id(t)).collect();
    if puppet_ids.len() != cfg.puppet_tokens.len() {
        bail!("could not decode every puppet token's bot user id");
    }

    // ── Start the bridge-under-test with the one tenant ─────────────────────
    let tenants = tenant_entry(BRIDGE_NAME, &cfg.room_a, &cfg.guild_id, &cfg.channel_a);
    let bridge = BridgeUnderTest::start(cfg, ctx.profile, &tenants, &ctx.artifacts.join("bridge"))
        .await
        .context("starting bridge-under-test")?;
    let ready = bridge.await_ready(&[BRIDGE_NAME], Duration::from_secs(120)).await;
    if let Err(e) = ready {
        bridge.kill().await;
        return Err(e);
    }

    // Everything below tears the bridge down on ANY failure path so a red
    // scenario never leaks a live child (and its Discord voice sessions).
    let result = drive(cfg, &poller, &puppet_ids, &bridge).await;
    match result {
        Ok(()) => {
            // ── Graceful shutdown IS part of the scenario ────────────────────
            bridge
                .sigterm_and_wait(Duration::from_secs(20))
                .await
                .context("SIGTERM drain")?;
            Ok(())
        }
        Err(e) => {
            bridge.kill().await;
            Err(e)
        }
    }
}

async fn drive(
    cfg: &crate::config::E2eConfig,
    poller: &RoomStatePoller,
    puppet_ids: &HashSet<u64>,
    bridge: &BridgeUnderTest,
) -> Result<()> {
    // ── Precondition: quiescent room ─────────────────────────────────────────
    // The bridge's own startup stale-ghost sweep clears residue from a prior
    // crashed run; wait for the room to show no asserted ghost membership.
    poller
        .await_members(&cfg.room_a, Duration::from_secs(30), "quiescent precondition", |m| {
            !m.iter().any(|e| e.class == SenderClass::Ghost && e.active)
        })
        .await?;

    // ── A Discord human appears ──────────────────────────────────────────────
    let mut speaker = DiscordSpeaker::connect(
        cfg.speaker_tokens.first().context("speaker token")?,
        &cfg.guild_id,
        &cfg.channel_a,
    )
    .await?;
    let opened = async {
        speaker.join().await.context("speaker voice join")?;

        // The bridge minds a presence ghost: log event + real room state.
        bridge
            .scraper
            .await_event(Duration::from_secs(60), |e| {
                e.message.contains("ghost present") || e.message.contains("ghost assigned")
            })
            .await
            .context("ghost lifecycle log event")?;
        poller
            .await_members(&cfg.room_a, Duration::from_secs(60), "ghost membership visible", |m| {
                m.iter().any(|e| e.class == SenderClass::Ghost && e.active)
            })
            .await?;

        // Exercise the capture path with the deterministic Phase-B probe (a
        // presence-only ghost publishes no media — the assert here is only
        // that nothing crashes/errors while a speaker actually speaks).
        speaker.speak_probe(0xD1, 1_500).await?;

        // ── The human leaves: ghost retracts, room returns to baseline ──────
        speaker.leave().await.context("speaker voice leave")?;
        poller
            .await_members(&cfg.room_a, Duration::from_secs(90), "ghost membership retracted", |m| {
                !m.iter().any(|e| e.class == SenderClass::Ghost && e.active)
            })
            .await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    speaker.shutdown().await;
    opened?;

    // ── Baseline: no bridge bot lingers in Discord voice ─────────────────────
    // Probed via a second gateway session on a harness token (gateway sessions
    // are not exclusive; only voice is — and the probe never joins voice).
    let probe = ConnCountProbe::connect(
        cfg.speaker_tokens.last().context("probe token")?,
        &cfg.guild_id,
    )
    .await?;
    let baseline = probe.await_baseline(puppet_ids, Duration::from_secs(60)).await;
    probe.shutdown().await;
    baseline?;

    // ── No ERROR events over the whole scenario ──────────────────────────────
    let errors = bridge.scraper.error_events();
    if !errors.is_empty() {
        bail!(
            "bridge logged {} ERROR event(s); first: {}",
            errors.len(),
            errors[0].message
        );
    }
    Ok(())
}
