//! Companion interface, `stream:attach` (ADR 0004 §5): a companion attaches a stream to the ghost of a represented
//! Discord member, and a Matrix watcher receives it as that ghost's own stream device.
//!
//! The streamer is a harness bot in the voice channel (it gets a ghost). Bots cannot go live on Discord, so consent is
//! the member's personal grant, seeded into `companions.toml` before the bridge starts together with the companion's
//! key — exactly what `companion key create` and `/bridge companion allow` persist. The companion is the testkit's
//! test-pattern publisher, driven through the bridge's real HTTP listener.

use super::audio::{await_quiescent, sweep_test_memberships};
use crate::actors::discord::DiscordListener;
use crate::actors::matrix::MatrixWatcher;
use crate::bridge_proc::{BridgeUnderTest, tenant_entry};
use crate::observers::{RoomStatePoller, decode_bot_user_id};
use crate::scenario::{Ctx, Expectation, ScenarioDef};
use anyhow::{Context as _, Result, bail};
use base64::Engine as _;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

pub static COMPANION_STREAM_ATTACH: ScenarioDef = ScenarioDef {
    name: "companion_stream_attach",
    expectation: Expectation::Pass,
    run: |ctx| Box::pin(run(ctx)),
};

const GHOST_ASSIGNED: &str = "ghost assigned";
const STREAM_LIVE: &str = "stream device live";
const STREAM_DETACHED: &str = "stream device detached";

async fn run(ctx: Arc<Ctx>) -> Result<()> {
    let cfg = &ctx.cfg;
    let bridge_name = "Auto Test 01";
    let poller = RoomStatePoller::new(cfg)?;
    sweep_test_memberships(cfg, &poller, &cfg.room_a).await?;

    let token = cfg.speaker_tokens.first().context("a harness bot token")?;
    let discord_id = decode_bot_user_id(token).context("decoding the harness bot id")?;

    // Seed the companion key (room-scoped) and the member's personal grant.
    let minted = nvb_companion::token::mint();
    let workdir = ctx.artifacts.join("bridge");
    let data_dir = workdir.join("data");
    tokio::fs::create_dir_all(&data_dir).await?;
    tokio::fs::write(
        data_dir.join("companions.toml"),
        format!(
            "[[key]]\nid = \"{id}\"\nsecret_sha256 = \"{hash}\"\nissuer = \"config\"\nkinds = [\"observe\", \"stream:attach\"]\nlabel = \"e2e\"\ncreated = 0\n\n[key.scope]\ntype = \"room\"\nroom_id = \"{room}\"\n\n[[grant]]\ndiscord_user = \"{discord_id}\"\nkey_id = \"{id}\"\ncreated = 0\n",
            id = minted.id,
            hash = minted.secret_sha256,
            room = cfg.room_a,
        ),
    )
    .await
    .context("seeding companions.toml")?;

    // A free local port for the listener.
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0")?;
        l.local_addr()?.port()
    };
    let tenants = tenant_entry(bridge_name, &cfg.room_a, &cfg.guild_id, &cfg.channel_a);
    // The `[timing]` block is the config's last table, so `[companions]` can follow its lines.
    let extra = format!("reap_grace_ms = 300000\n\n[companions]\nenabled = true\nlisten = \"127.0.0.1:{port}\"\n");
    let bridge = BridgeUnderTest::start_with_timing(cfg, ctx.profile, &tenants, &workdir, &extra)
        .await
        .context("starting bridge-under-test")?;
    if let Err(e) = bridge.await_ready(&[bridge_name], Duration::from_secs(120)).await {
        bridge.kill().await;
        return Err(e);
    }
    let result = drive(&ctx, &bridge, token, discord_id, port, &minted.token).await;
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

async fn drive(ctx: &Ctx, bridge: &BridgeUnderTest, token: &str, discord_id: u64, port: u16, key: &str) -> Result<()> {
    let cfg = &ctx.cfg;
    let wait = Duration::from_secs(90);
    let http = reqwest::Client::new();
    let api = format!("http://127.0.0.1:{port}/_nvb/v1");

    // The streamer joins voice (ghost), a Matrix watcher joins the call (call active).
    let listener = DiscordListener::connect(token, &cfg.guild_id, &cfg.channel_a, HashSet::new()).await?;
    listener.join().await.context("the streamer joining voice")?;
    bridge.scraper.await_event(wait, |e: &crate::logscrape::LogEvent| e.message.contains(GHOST_ASSIGNED)).await.context("the streamer should get a ghost")?;
    let watcher = MatrixWatcher::join(cfg, &cfg.room_a, false, "nvbtest_watcher", "NVB Watcher").await?;

    // Ask for the stream; the active call can take a moment to show on the bridge, so retry call_inactive briefly.
    let mut grant: Option<serde_json::Value> = None;
    for _ in 0..20 {
        let r = http
            .post(format!("{api}/leases"))
            .bearer_auth(key)
            .json(&serde_json::json!({ "kind": "stream:attach", "participant": { "discord_user_id": discord_id.to_string() } }))
            .send()
            .await?;
        let status = r.status();
        let body: serde_json::Value = r.json().await.unwrap_or_default();
        if status.as_u16() == 201 {
            grant = Some(body);
            break;
        }
        if body["error"] != "call_inactive" {
            bail!("stream:attach refused: {status} {body}");
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let grant = grant.context("stream:attach never granted (call stayed inactive)")?;
    eprintln!("   granted: consent={} ghost={} device={}", grant["consent"], grant["participant"]["ghost_mxid"], grant["participant"]["device_id"]);
    if grant["consent"] != "personal_grant" {
        bail!("expected consent by personal grant, got {}", grant["consent"]);
    }
    let lease_id = grant["lease_id"].as_str().context("lease_id")?.to_string();
    let identity = grant["transport"]["identity"].as_str().context("transport.identity")?.to_string();
    let media_key = grant["media"]["key_b64"]
        .as_str()
        .map(|k| base64::engine::general_purpose::STANDARD.decode(k))
        .transpose()?
        .map(|v| <[u8; 16]>::try_from(v.as_slice()))
        .transpose()
        .map_err(|_| anyhow::anyhow!("media.key_b64 is not 16 bytes"))?;

    // The companion keeps the lease's event stream open (its liveness), publishes, then reports live.
    let events = http.get(format!("{api}/leases/{lease_id}/events")).bearer_auth(key).send().await?;
    if !events.status().is_success() {
        bail!("event stream refused: {}", events.status());
    }
    let screen = nvb_matrix_rtc::testkit::publish_test_screen(
        grant["transport"]["sfu_url"].as_str().context("sfu_url")?,
        grant["transport"]["jwt"].as_str().context("jwt")?,
        media_key,
    )
    .await?;
    let r = http.post(format!("{api}/leases/{lease_id}/live")).bearer_auth(key).send().await?;
    if !r.status().is_success() {
        bail!("live refused: {}", r.status());
    }
    bridge.scraper.await_event(wait, |e: &crate::logscrape::LogEvent| e.message.contains(STREAM_LIVE)).await.context("the stream device should post its membership")?;
    tokio::time::sleep(Duration::from_secs(8)).await;

    // End the lease: the device detaches (membership retracted).
    let r = http.delete(format!("{api}/leases/{lease_id}")).bearer_auth(key).send().await?;
    if !r.status().is_success() {
        bail!("release refused: {}", r.status());
    }
    bridge.scraper.await_event(wait, |e: &crate::logscrape::LogEvent| e.message.contains(STREAM_DETACHED)).await.context("the stream device should detach")?;
    drop(events);
    screen.stop().await;
    let seen = watcher.stop().await?;
    let _ = listener.stop().await;

    eprintln!("   watcher saw: {seen:?}");
    let ours = seen.iter().find(|v| v.identity == identity).with_context(|| format!("the watcher never received {identity}"))?;
    if !ours.source.contains("Screenshare") {
        bail!("{identity} arrived as {} instead of a screen share", ours.source);
    }
    // A decode check, not a quality check: the first live run decoded 24 frames in ~8 s (the encoder ramps up from a
    // cold bandwidth estimate on a fresh connection), so require a steady trickle, not the full 15 fps.
    if ours.frames < 10 {
        bail!("{identity}: only {} frames decoded in ~8 s", ours.frames);
    }
    Ok(())
}
