//! Audio-crossing scenarios (catalogue #2–#5, the #32/#36/#51 regression
//! classes): real audio over the real bridge in both directions, verified by
//! the Phase-B oracle.
//!
//! * `audio_m2d_unenc` — MatrixSpeaker probe → puppet → DiscordListener WAV → oracle
//! * `audio_d2m_unenc` — DiscordSpeaker probe → ghost → MatrixListener WAV → oracle
//! * `audio_m2d_enc`   — same as m2d in the ENCRYPTED room (the bridge must
//!   receive the speaker's Olm-delivered media key to decrypt)
//! * `audio_d2m_enc`   — encrypted Discord→Matrix: ghost key delivery is
//!   asserted from logs and the audio is oracled at Tap B (the exact PCM the
//!   ghost hands LiveKit) — a *decrypting* Matrix listener is deferred (D2.5/D3).
//!
//! All four run with prod up (dedicated harness bots + the nvbtest appservice).

use crate::actors::ParticipantDriver;
use crate::actors::discord::{DiscordListener, DiscordSpeaker};
use crate::actors::matrix::{MatrixListener, MatrixSpeaker};
use crate::bridge_proc::{BridgeUnderTest, tenant_entry, tenant_entry_opts};
use crate::config::E2eConfig;
use crate::observers::{RoomStatePoller, SenderClass, decode_bot_user_id};
use crate::scenario::{Ctx, Expectation, ScenarioDef};
use anyhow::{Context as _, Result, bail};
use nvb_audio::oracle::{OracleConfig, compare_mono_48k};
use nvb_audio::probe::synth_probe_mono_48k;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

pub static AUDIO_M2D_UNENC: ScenarioDef = ScenarioDef {
    name: "audio_m2d_unenc",
    expectation: Expectation::Pass,
    run: |ctx| Box::pin(run_m2d(ctx, false)),
};
pub static AUDIO_D2M_UNENC: ScenarioDef = ScenarioDef {
    name: "audio_d2m_unenc",
    expectation: Expectation::Pass,
    run: |ctx| Box::pin(run_d2m_unenc(ctx)),
};
pub static AUDIO_M2D_ENC: ScenarioDef = ScenarioDef {
    name: "audio_m2d_enc",
    expectation: Expectation::Pass,
    run: |ctx| Box::pin(run_m2d(ctx, true)),
};
pub static AUDIO_D2M_ENC: ScenarioDef = ScenarioDef {
    name: "audio_d2m_enc",
    expectation: Expectation::Pass,
    run: |ctx| Box::pin(run_d2m_enc(ctx)),
};

/// Probe length: 6 cycles gives the oracle plenty of active segments while
/// keeping a scenario comfortably inside the compressed-profile timeout.
const PROBE_MS: u32 = 6_000;

/// Post-probe drain before reading the far side: LiveKit's capture buffer can
/// run ~1 s ahead of real time, plus jitter/mixer latency on both ends.
const DRAIN: Duration = Duration::from_secs(4);
/// Settle after the media path reports live before speaking (DAVE epoch
/// changes, mixer priming, subscription plumbing).
const SETTLE: Duration = Duration::from_secs(3);

/// Live-path oracle thresholds: defaults (Opus in the loop), with the
/// alignment bound widened — the capture window opens well before the probe
/// plays, so the discovered delay is seconds, not the default 2 s.
fn oracle_cfg() -> OracleConfig {
    OracleConfig { max_align_ms: 20_000, ..OracleConfig::default() }
}

/// Compare `captured` against `probe`, persist both WAVs + the report JSON
/// into the scenario artifacts, and fail with the scores when below threshold.
fn oracle_assert(artifacts: &Path, label: &str, probe: &[i16], captured: &[i16]) -> Result<()> {
    let wav = |suffix: &str, data: &[i16]| -> Result<()> {
        let path = artifacts.join(format!("{label}_{suffix}.wav"));
        nvb_audio::wav::write_wav_mono_48k(path.to_str().context("artifact path")?, data)
    };
    wav("probe", probe)?;
    wav("captured", captured)?;

    let report = compare_mono_48k(probe, captured, &oracle_cfg());
    let json = serde_json::json!({
        "label": label,
        "pass": report.pass,
        "ncc": report.ncc,
        "seg_snr_db": report.seg_snr_db,
        "gain": report.gain,
        "delay_samples": report.delay_samples,
        "dropouts": report.dropouts.len(),
        "glitches": report.glitches.len(),
    });
    std::fs::write(
        artifacts.join(format!("{label}_oracle.json")),
        serde_json::to_string_pretty(&json)?,
    )?;
    eprintln!(
        "   oracle[{label}]: ncc={:.3} seg_snr={:.1}dB gain={:.2} delay={} dropouts={} glitches={} → {}",
        report.ncc,
        report.seg_snr_db,
        report.gain,
        report.delay_samples,
        report.dropouts.len(),
        report.glitches.len(),
        if report.pass { "PASS" } else { "FAIL" },
    );
    if !report.pass {
        bail!(
            "oracle failed for {label}: ncc={:.3} seg_snr={:.1}dB dropouts={} glitches={} \
             (WAVs + report in artifacts)",
            report.ncc,
            report.seg_snr_db,
            report.dropouts.len(),
            report.glitches.len(),
        );
    }
    Ok(())
}

/// Retract any `m.call.member` a prior crashed run's `@nvbtest_*` participant
/// left behind (the bridge's own startup sweep only clears ITS ghosts). A
/// stale test membership would flip the bridge call-active at boot and skew
/// the scenario's lifecycle assertions.
async fn sweep_test_memberships(cfg: &E2eConfig, poller: &RoomStatePoller, room: &str) -> Result<()> {
    let as_token = cfg.test_as_token()?;
    let stale: Vec<_> = poller
        .call_members(room)
        .await?
        .into_iter()
        .filter(|m| m.active && m.sender.starts_with("@nvbtest_"))
        .collect();
    if stale.is_empty() {
        return Ok(());
    }
    let http = reqwest::Client::builder()
        .use_rustls_tls()
        .user_agent("Mozilla/5.0 (compatible; nvb-e2e)")
        .build()?;
    for m in stale {
        let url = format!(
            "{}/_matrix/client/v3/rooms/{}/state/org.matrix.msc3401.call.member/{}?user_id={}",
            cfg.homeserver_url.trim_end_matches('/'),
            urlencoding(room),
            urlencoding(&m.state_key),
            urlencoding(&m.sender),
        );
        let resp = http
            .put(url)
            .bearer_auth(as_token)
            .json(&serde_json::json!({}))
            .send()
            .await
            .context("retracting stale test membership")?;
        if resp.status().is_success() {
            eprintln!("   swept stale test membership: {} ({})", m.sender, m.state_key);
        } else {
            bail!("failed to sweep stale test membership {}: {}", m.state_key, resp.status());
        }
    }
    Ok(())
}

fn urlencoding(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// Wait for the room to show no active ghost membership (the bridge's startup
/// sweep clears residue) — the same precondition the smoke scenario uses.
async fn await_quiescent(poller: &RoomStatePoller, room: &str) -> Result<()> {
    poller
        .await_members(room, Duration::from_secs(30), "quiescent precondition", |m| {
            !m.iter().any(|e| e.class == SenderClass::Ghost && e.active)
        })
        .await?;
    Ok(())
}

/// Decode the harness puppet ids (the users a DiscordListener records).
fn puppet_ids(cfg: &E2eConfig) -> Result<HashSet<u64>> {
    let ids: HashSet<u64> =
        cfg.puppet_tokens.iter().filter_map(|t| decode_bot_user_id(t)).collect();
    if ids.len() != cfg.puppet_tokens.len() {
        bail!("could not decode every puppet token's bot user id");
    }
    Ok(ids)
}

/// Pick the loudest recording (by RMS) — the puppet actually carrying the
/// probe; the anchor's overflow track and idle puppets record near-silence.
fn loudest(recordings: HashMap<u64, Vec<i16>>) -> Option<(u64, Vec<i16>)> {
    recordings
        .into_iter()
        .filter(|(_, s)| !s.is_empty())
        .max_by(|a, b| {
            nvb_audio::rms_dbfs(&a.1)
                .partial_cmp(&nvb_audio::rms_dbfs(&b.1))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
}

// ─── Matrix → Discord (unenc + enc share the flow; the room differs) ─────────────

async fn run_m2d(ctx: Arc<Ctx>, encrypted: bool) -> Result<()> {
    let cfg = &ctx.cfg;
    let (room, channel, bridge_name, seed) = if encrypted {
        let (room_b, channel_b) = cfg.encrypted_pair()?;
        (room_b.to_owned(), channel_b.to_owned(), "Auto Test 02", 0xD2C3)
    } else {
        (cfg.room_a.clone(), cfg.channel_a.clone(), "Auto Test 01", 0xD2A1)
    };
    let poller = RoomStatePoller::new(cfg)?;
    let targets = puppet_ids(cfg)?;
    sweep_test_memberships(cfg, &poller, &room).await?;

    let tenants = tenant_entry(bridge_name, &room, &cfg.guild_id, &channel);
    let bridge = BridgeUnderTest::start(cfg, ctx.profile, &tenants, &ctx.artifacts.join("bridge"))
        .await
        .context("starting bridge-under-test")?;
    if let Err(e) = bridge.await_ready(&[bridge_name], Duration::from_secs(120)).await {
        bridge.kill().await;
        return Err(e);
    }

    let result = drive_m2d(&ctx, &bridge, &room, &channel, bridge_name, seed, encrypted, targets)
        .await;
    match result {
        Ok(()) => {
            let _ = await_quiescent(&poller, &room).await;
            bridge.sigterm_and_wait(Duration::from_secs(20)).await.context("SIGTERM drain")
        }
        Err(e) => {
            bridge.kill().await;
            Err(e)
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn drive_m2d(
    ctx: &Ctx,
    bridge: &BridgeUnderTest,
    room: &str,
    channel: &str,
    bridge_name: &str,
    seed: u64,
    encrypted: bool,
    targets: HashSet<u64>,
) -> Result<()> {
    let cfg = &ctx.cfg;

    // The far-side ear: a harness bot receiving in the voice channel.
    let listener = DiscordListener::connect(
        cfg.speaker_tokens.first().context("listener token")?,
        &cfg.guild_id,
        channel,
        targets,
    )
    .await?;
    let outcome = async {
        listener.join().await?;

        // The Matrix human: publishes a track and posts membership — the
        // bridge assigns a puppet and subscribes.
        let speaker =
            MatrixSpeaker::join(cfg, room, encrypted, "nvbtest_alice", "NVB Test Alice").await?;
        let spoken = async {
            bridge
                .scraper
                .await_event(Duration::from_secs(90), |e| {
                    e.message.contains("playback consumer started") && e.in_bridge(bridge_name)
                })
                .await
                .context("bridge never subscribed the test speaker's track")?;
            // The puppet must actually be IN the channel before the probe
            // plays — its playback buffer only drains once it's connected.
            listener
                .await_target_in_voice(Duration::from_secs(60))
                .await
                .context("no puppet joined the voice channel for the test speaker")?;
            if encrypted {
                // The bridge must have Olm-received + installed the speaker's
                // media key, or its subscriber outputs undecryptable noise.
                bridge
                    .scraper
                    .await_event(Duration::from_secs(90), |e| {
                        e.message.contains("installed peer media key")
                    })
                    .await
                    .context("bridge never installed the test speaker's media key")?;
            }
            tokio::time::sleep(SETTLE).await;
            speaker.speak_probe(seed, PROBE_MS).await?;
            // The probe runs ~real-time from here (LiveKit self-pacing);
            // wait it out plus the drain margin.
            tokio::time::sleep(Duration::from_millis(PROBE_MS as u64)).await;
            tokio::time::sleep(DRAIN).await;
            Ok::<(), anyhow::Error>(())
        }
        .await;
        speaker.leave().await;
        spoken?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    let recordings = listener.stop().await;
    outcome?;

    let (uid, captured) = loudest(recordings)
        .context("the Discord listener recorded nothing from any puppet")?;
    eprintln!("   recorded {} samples from puppet {uid}", captured.len());
    let probe = synth_probe_mono_48k(seed, PROBE_MS);
    oracle_assert(&ctx.artifacts, "m2d", &probe, &captured)?;

    let errors = bridge.scraper.error_events();
    if !errors.is_empty() {
        bail!("bridge logged {} ERROR event(s); first: {}", errors.len(), errors[0].message);
    }
    Ok(())
}

// ─── Discord → Matrix, unencrypted (full oracle on the Matrix listener) ──────────

async fn run_d2m_unenc(ctx: Arc<Ctx>) -> Result<()> {
    let cfg = &ctx.cfg;
    let seed = 0xD2B2;
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

    let result = drive_d2m_unenc(&ctx, &bridge, bridge_name, seed).await;
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

async fn drive_d2m_unenc(
    ctx: &Ctx,
    bridge: &BridgeUnderTest,
    bridge_name: &str,
    seed: u64,
) -> Result<()> {
    let cfg = &ctx.cfg;

    // The far-side ear FIRST: its membership activates the call, so the
    // Discord speaker's ghost publishes as soon as it appears.
    let listener =
        MatrixListener::join(cfg, &cfg.room_a, false, "nvbtest_listener", "NVB Test Listener")
            .await?;

    let mut speaker = DiscordSpeaker::connect(
        cfg.speaker_tokens.first().context("speaker token")?,
        &cfg.guild_id,
        &cfg.channel_a,
    )
    .await?;
    let outcome = async {
        speaker.join().await.context("speaker voice join")?;
        bridge
            .scraper
            .await_event(Duration::from_secs(90), |e| {
                e.message.contains("ghost publishing Discord speaker audio")
                    && e.in_bridge(bridge_name)
            })
            .await
            .context("ghost never went live for the Discord speaker")?;
        tokio::time::sleep(SETTLE).await;
        // speak_probe waits out the probe duration itself.
        speaker.speak_probe(seed, PROBE_MS).await?;
        tokio::time::sleep(DRAIN).await;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    // Stop the listener BEFORE the speaker leaves: the ghost retires on the
    // speaker's leave and we want the recording intact, not raced.
    let recordings = match outcome {
        Ok(()) => listener.stop().await,
        Err(e) => {
            let _ = listener.stop().await;
            speaker.shutdown().await;
            return Err(e);
        }
    };
    speaker.shutdown().await;
    let recordings = recordings?;

    let ghost_marker = format!("@{}", cfg.ghost_prefix());
    let captured = recordings
        .iter()
        .filter(|r| r.identity.starts_with(&ghost_marker) && !r.samples.is_empty())
        .max_by(|a, b| {
            nvb_audio::rms_dbfs(&a.samples)
                .partial_cmp(&nvb_audio::rms_dbfs(&b.samples))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .with_context(|| {
            format!(
                "the Matrix listener recorded no ghost track (got: {:?})",
                recordings.iter().map(|r| (&r.identity, r.samples.len())).collect::<Vec<_>>()
            )
        })?;
    eprintln!(
        "   recorded {} samples from ghost {}",
        captured.samples.len(),
        captured.identity
    );
    let probe = synth_probe_mono_48k(seed, PROBE_MS);
    oracle_assert(&ctx.artifacts, "d2m", &probe, &captured.samples)?;

    let errors = bridge.scraper.error_events();
    if !errors.is_empty() {
        bail!("bridge logged {} ERROR event(s); first: {}", errors.len(), errors[0].message);
    }
    Ok(())
}

// ─── Discord → Matrix, encrypted (Tap B oracle + key-delivery log assertions) ────

async fn run_d2m_enc(ctx: Arc<Ctx>) -> Result<()> {
    let cfg = &ctx.cfg;
    let seed = 0xD2D4;
    let bridge_name = "Auto Test 02";
    let (room_b, channel_b) = {
        let (r, c) = cfg.encrypted_pair()?;
        (r.to_owned(), c.to_owned())
    };
    let poller = RoomStatePoller::new(cfg)?;
    sweep_test_memberships(cfg, &poller, &room_b).await?;

    // capture_wav=true: the bridge writes Tap B (`publish_<uid>.wav`) — the
    // exact mono PCM the ghost encrypts into LiveKit — into its workdir.
    let tenants = tenant_entry_opts(bridge_name, &room_b, &cfg.guild_id, &channel_b, true);
    let bridge = BridgeUnderTest::start(cfg, ctx.profile, &tenants, &ctx.artifacts.join("bridge"))
        .await
        .context("starting bridge-under-test")?;
    if let Err(e) = bridge.await_ready(&[bridge_name], Duration::from_secs(120)).await {
        bridge.kill().await;
        return Err(e);
    }

    let result = drive_d2m_enc(&ctx, &bridge, bridge_name, &room_b, &channel_b, seed).await;
    match result {
        Ok(()) => {
            let _ = await_quiescent(&poller, &room_b).await;
            bridge.sigterm_and_wait(Duration::from_secs(20)).await.context("SIGTERM drain")
        }
        Err(e) => {
            bridge.kill().await;
            Err(e)
        }
    }
}

async fn drive_d2m_enc(
    ctx: &Ctx,
    bridge: &BridgeUnderTest,
    bridge_name: &str,
    room_b: &str,
    channel_b: &str,
    seed: u64,
) -> Result<()> {
    let cfg = &ctx.cfg;

    // The listener carries a REAL crypto device (media_e2ee=true), so the
    // ghost's Olm key delivery targets a device that can answer /keys/claim.
    // Its LiveKit recordings stay encrypted (decrypting listener = D2.5/D3)
    // and are deliberately discarded — Tap B is the audio assertion here.
    let listener =
        MatrixListener::join(cfg, room_b, true, "nvbtest_listener", "NVB Test Listener").await?;

    let speaker_token = cfg.speaker_tokens.first().context("speaker token")?;
    let speaker_uid = decode_bot_user_id(speaker_token)
        .context("could not decode the speaker token's bot user id (needed for the Tap B path)")?;
    let mut speaker = DiscordSpeaker::connect(speaker_token, &cfg.guild_id, channel_b).await?;

    let outcome = async {
        speaker.join().await.context("speaker voice join")?;
        bridge
            .scraper
            .await_event(Duration::from_secs(90), |e| {
                e.message.contains("ghost publishing Discord speaker audio")
                    && e.in_bridge(bridge_name)
            })
            .await
            .context("ghost never went live for the Discord speaker")?;
        // Key delivery must succeed before the probe: "sent" = all recipient
        // devices reached, "delivered" = the live device reached with stale
        // ones skipped — both healthy.
        bridge
            .scraper
            .await_event(Duration::from_secs(90), |e| {
                e.message.contains("ghost media key sent")
                    || e.message.contains("ghost media key delivered")
            })
            .await
            .context("the ghost never delivered its media key to the test listener")?;
        tokio::time::sleep(SETTLE).await;
        speaker.speak_probe(seed, PROBE_MS).await?;
        tokio::time::sleep(DRAIN).await;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    // Leaving voice retires the ghost, whose audio pump finalizes Tap B.
    let left = speaker.leave().await;
    let _ = listener.stop().await;
    let tap_b_final = bridge
        .scraper
        .await_event(Duration::from_secs(60), |e| {
            e.message.contains("publish-tap WAV (Tap B) finalised")
        })
        .await
        .context("Tap B WAV was never finalised (ghost pump did not exit cleanly?)");
    speaker.shutdown().await;
    outcome?;
    left?;
    tap_b_final?;

    // Key-delivery health over the whole run: a cold-start "reached NO
    // recipient devices" retry or two is expected while the ghost's crypto
    // sync catches up; a PERSISTENT stream of them means delivery was broken.
    let reached_nobody = bridge
        .scraper
        .events_matching(|e| e.message.contains("reached NO recipient devices"))
        .len();
    if reached_nobody > 5 {
        bail!(
            "ghost media key reached NO recipient devices {reached_nobody} times — \
             encrypted delivery to the test listener is broken"
        );
    }

    // Tap B: the exact mono PCM the ghost fed LiveKit's frame crypto. This is
    // pre-Opus, so a healthy capture path scores far above the live thresholds.
    let tap_b = bridge.workdir.join(format!("publish_{speaker_uid}.wav"));
    let captured = nvb_audio::wav::read_wav_mono_48k(
        tap_b.to_str().context("tap B path")?,
    )
    .with_context(|| format!("reading Tap B WAV at {}", tap_b.display()))?;
    eprintln!("   Tap B: {} samples from {}", captured.len(), tap_b.display());
    let probe = synth_probe_mono_48k(seed, PROBE_MS);
    oracle_assert(&ctx.artifacts, "d2m_enc_tap_b", &probe, &captured)?;

    let errors = bridge.scraper.error_events();
    if !errors.is_empty() {
        bail!("bridge logged {} ERROR event(s); first: {}", errors.len(), errors[0].message);
    }
    Ok(())
}
