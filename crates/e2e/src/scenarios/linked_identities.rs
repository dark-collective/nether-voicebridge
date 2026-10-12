//! Linked identities (ADR 0001): a paired person present on BOTH sides is one
//! person to the bridge — no ghost for the Discord half, no puppet for the
//! Matrix half — and the exclusion flips live as either half comes and goes.
//!
//! The "Discord half" is a harness bot (the bridge treats non-pool bots as
//! humans); the "Matrix half" is a presence-only [`MatrixPhantom`]. No Discord
//! *user* account is ever automated. The pairing is seeded into the bridge's
//! data dir before start (exactly what `pair` + `/bridge pair` would write).
//!
//! Order A: Matrix first. The phantom gets a puppet (alone it is ordinary);
//! the paired Discord account arriving RELEASES that puppet and gets NO ghost;
//! it leaving hands the puppet back (the failover-notice path).
//! Order B: Discord first. The Discord account gets a ghost; the phantom
//! arriving RETIRES that ghost and gets no puppet; the phantom leaving (after
//! the leave grace) mints the ghost again.

use super::audio::{await_quiescent, sweep_test_memberships};
use crate::actors::discord::DiscordListener;
use crate::actors::matrix::MatrixPhantom;
use crate::bridge_proc::{BridgeUnderTest, tenant_entry};
use crate::observers::{RoomStatePoller, decode_bot_user_id};
use crate::scenario::{Ctx, Expectation, ScenarioDef};
use anyhow::{Context as _, Result};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

pub static LINKED_IDENTITY_EXCLUSION: ScenarioDef = ScenarioDef {
    name: "linked_identity_exclusion",
    expectation: Expectation::Pass,
    run: |ctx| Box::pin(run(ctx)),
};

// Log anchors (substrings; the exact em-dashes never round-trip through here).
const ASSIGNING_PUPPET: &str = "assigning puppet";
const JOINED_NO_PUPPET: &str = "paired Discord account present"; // "member joined — paired Discord account present — no puppet, listening"
const RELEASING_PUPPET: &str = "member is now bridge-owned"; // "... — releasing puppet"
const PUPPET_BACK: &str = "member is no longer bridge-owned"; // "... — assigning puppet"
const GHOST_ASSIGNED: &str = "ghost assigned";
const NO_GHOST: &str = "voice member is bridge-owned"; // "... (paired/excluded) — no ghost"
const GHOST_RETIRED: &str = "ghost retired"; // "ghost retired — member is now bridge-owned (...)"
const GHOST_BACK: &str = "minting ghost"; // "member is no longer bridge-owned (...) — minting ghost"
const ANCHOR_KEPT_FOR_LISTENER: &str = "keeping capture-only anchor for listeners without a puppet";
const ANCHOR_ACQUIRED_FOR_LISTENER: &str = "capture-only anchor acquired for a listener";
const ANCHOR_IDLE_LAPSED: &str = "capture-only anchor idle grace elapsed";

async fn run(ctx: Arc<Ctx>) -> Result<()> {
    let cfg = &ctx.cfg;
    let bridge_name = "Auto Test 01";
    let poller = RoomStatePoller::new(cfg)?;
    sweep_test_memberships(cfg, &poller, &cfg.room_a).await?;

    // The Discord half: the first harness speaker bot. Its user id is in its token.
    let discord_token = cfg.speaker_tokens.first().context("a harness bot token")?;
    let discord_id = decode_bot_user_id(discord_token).context("decoding the harness bot id")?;
    // The Matrix half: the phantom's MXID is deterministic from its localpart.
    let localpart = "nvbtest_paired";
    let mxid = format!("@{localpart}:{}", cfg.primary_as()?.domain);

    // Seed the pairing before the bridge starts (what `pair` + `/bridge pair` persist).
    let workdir = ctx.artifacts.join("bridge");
    let data_dir = workdir.join("data");
    tokio::fs::create_dir_all(&data_dir).await?;
    tokio::fs::write(
        data_dir.join("pairings.toml"),
        format!("[[pairing]]\nmatrix_user = \"{mxid}\"\ndiscord_user = \"{discord_id}\"\ncreated = 0\n"),
    )
    .await
    .context("seeding pairings.toml")?;

    let tenants = tenant_entry(bridge_name, &cfg.room_a, &cfg.guild_id, &cfg.channel_a);
    // The Matrix half is a membership-only phantom (never in the LiveKit roster): to the #25
    // reaper that IS a stale membership. Since ADR 0003 the paired listener keeps a capture
    // anchor, so the subscriber is connected and the compressed 3 s `reap_grace` retracts the
    // phantom's m.call.member ~4 s after it joins — mid-choreography ("reaped phantom
    // m.call.member" in the bridge log; every nightly red 10-07→10-12). Same fix as
    // `steal_repromotion`; the reaper itself is covered by `reap_release`.
    let bridge = BridgeUnderTest::start_with_timing(
        cfg,
        ctx.profile,
        &tenants,
        &workdir,
        "reap_grace_ms = 300000\n",
    )
    .await
    .context("starting bridge-under-test")?;
    if let Err(e) = bridge.await_ready(&[bridge_name], Duration::from_secs(120)).await {
        bridge.kill().await;
        return Err(e);
    }

    let result = drive(&ctx, &bridge, discord_token, discord_id, localpart).await;
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

async fn drive(
    ctx: &Ctx,
    bridge: &BridgeUnderTest,
    discord_token: &str,
    discord_id: u64,
    localpart: &str,
) -> Result<()> {
    let cfg = &ctx.cfg;
    let wait = Duration::from_secs(90);
    let since_now = |needle: &'static str| {
        move |e: &crate::logscrape::LogEvent| e.message.contains(needle)
    };

    // ── Order A: Matrix first ────────────────────────────────────────────────
    eprintln!("   order A: Matrix half joins first");
    let phantom = MatrixPhantom::join(cfg, &cfg.room_a, false, false, localpart, "NVB Paired").await?;
    bridge
        .scraper
        .await_event(wait, since_now(ASSIGNING_PUPPET))
        .await
        .context("A: the phantom alone should get a puppet")?;

    let listener = DiscordListener::connect(discord_token, &cfg.guild_id, &cfg.channel_a, HashSet::new()).await?;
    listener.join().await.context("A: Discord half joining voice")?;
    bridge
        .scraper
        .await_event(wait, since_now(RELEASING_PUPPET))
        .await
        .context("A: the paired Discord account arriving should release the phantom's puppet")?;
    bridge
        .scraper
        .await_event(wait, since_now(NO_GHOST))
        .await
        .context("A: the paired Discord account should get no ghost")?;
    // ADR 0003: the paired person still LISTENS from Matrix — the capture anchor stays (capture-only) instead of
    // leaving after its idle grace (the 2026-10-06 silence: nobody counted, anchor gone, ghosts mute).
    bridge
        .scraper
        // Kept through the puppet's release, or — when the playback consumer released the puppet a moment before
        // the pairing transition said "listen" — re-acquired for the listener. Either way: capture continues.
        .await_event(wait, |e: &crate::logscrape::LogEvent| {
            e.message.contains(ANCHOR_KEPT_FOR_LISTENER) || e.message.contains(ANCHOR_ACQUIRED_FOR_LISTENER)
        })
        .await
        .context("A: the capture anchor should be kept (or acquired) for the paired listener")?;
    tokio::time::sleep(Duration::from_secs(5)).await; // > the compressed anchor-idle grace (3 s)
    let lapsed = bridge.scraper.events_matching(|e| e.message.contains(ANCHOR_IDLE_LAPSED));
    anyhow::ensure!(lapsed.is_empty(), "A: the capture anchor left although the paired person was still listening");
    eprintln!("   order A: both present — puppet released, no ghost, capture kept; dropping the Discord half");

    let _ = listener.stop().await;
    bridge
        .scraper
        .await_event(wait, since_now(PUPPET_BACK))
        .await
        .context("A: the Discord half dropping should hand the phantom its puppet back")?;
    phantom.leave().await;
    eprintln!("   order A passed");

    // ── Order B: Discord first ───────────────────────────────────────────────
    eprintln!("   order B: Discord half joins first");
    let listener = DiscordListener::connect(discord_token, &cfg.guild_id, &cfg.channel_a, HashSet::new()).await?;
    listener.join().await.context("B: Discord half joining voice")?;
    bridge
        .scraper
        .await_event(wait, {
            let id = discord_id.to_string();
            move |e| e.message.contains(GHOST_ASSIGNED) && e.raw.contains(&id)
        })
        .await
        .context("B: the Discord account alone should get a ghost")?;

    let phantom = MatrixPhantom::join(cfg, &cfg.room_a, false, false, localpart, "NVB Paired").await?;
    bridge
        .scraper
        .await_event(wait, since_now(GHOST_RETIRED))
        .await
        .context("B: the Matrix half arriving should retire the Discord account's ghost")?;
    bridge
        .scraper
        .await_event(wait, since_now(JOINED_NO_PUPPET))
        .await
        .context("B: the paired Matrix half should get no puppet")?;
    eprintln!("   order B: both present — ghost retired, no puppet; Matrix half leaves");

    phantom.leave().await;
    bridge
        .scraper
        .await_event(wait + Duration::from_secs(30), since_now(GHOST_BACK))
        .await
        .context("B: after the leave grace the Discord account should get its ghost back")?;
    let _ = listener.stop().await;
    eprintln!("   order B passed");
    Ok(())
}
