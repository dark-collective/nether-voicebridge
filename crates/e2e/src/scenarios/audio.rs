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
//! * `audio_m2d_enc_freshjoin` — the #73 regression: the speaker streams into
//!   the encrypted call BEFORE the bridge boots (frames beat the media key),
//!   the one-shot heal re-subscribe must fire, and audio must cross after it.
//!
//! All run with prod up (dedicated harness bots + the nvbtest appservice).

use crate::actors::ParticipantDriver;
use crate::actors::discord::{DiscordListener, DiscordSpeaker};
use crate::actors::matrix::{MatrixListener, MatrixPhantom, MatrixSpeaker};
use crate::bridge_proc::{BridgeUnderTest, tenant_entry, tenant_entry_opts};
use crate::config::E2eConfig;
use crate::observers::{ConnCountProbe, RoomStatePoller, SenderClass, decode_bot_user_id};
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
    run: |ctx| Box::pin(run_m2d(ctx, false, PROBE_MS)),
};
pub static AUDIO_D2M_UNENC: ScenarioDef = ScenarioDef {
    name: "audio_d2m_unenc",
    expectation: Expectation::Pass,
    run: |ctx| Box::pin(run_d2m_unenc(ctx)),
};
pub static AUDIO_M2D_ENC: ScenarioDef = ScenarioDef {
    name: "audio_m2d_enc",
    expectation: Expectation::Pass,
    run: |ctx| Box::pin(run_m2d(ctx, true, PROBE_MS)),
};
pub static AUDIO_D2M_ENC: ScenarioDef = ScenarioDef {
    name: "audio_d2m_enc",
    expectation: Expectation::Pass,
    run: |ctx| Box::pin(run_d2m_enc(ctx)),
};
pub static AUDIO_M2D_ENC_FRESHJOIN: ScenarioDef = ScenarioDef {
    name: "audio_m2d_enc_freshjoin",
    expectation: Expectation::Pass,
    run: |ctx| Box::pin(run_m2d_enc_freshjoin(ctx)),
};
/// #14 — SIGTERM mid-call drains cleanly and returns to baseline.
pub static SHUTDOWN_DRAIN: ScenarioDef = ScenarioDef {
    name: "shutdown_drain",
    expectation: Expectation::Pass,
    run: |ctx| Box::pin(run_shutdown_drain(ctx)),
};
/// #16 — sustained-audio drift sweep (opt-in via NVB_E2E_LONG; nightly-only).
pub static LONG_GLITCH_SWEEP: ScenarioDef = ScenarioDef {
    name: "long_glitch_sweep",
    expectation: Expectation::Pass,
    run: |ctx| Box::pin(run_long_glitch_sweep(ctx)),
};
/// #11 — a wiped crypto store re-mints a fresh device; encrypted audio must
/// still cross (#30/#51/#53 self-heal).
pub static FRESH_STORE_REKEY: ScenarioDef = ScenarioDef {
    name: "fresh_store_rekey",
    expectation: Expectation::Pass,
    run: |ctx| Box::pin(run_fresh_store_rekey(ctx)),
};
/// #12 — a membership change past the 90s min-interval rotates the ghost's Olm
/// outbound sessions (#52).
pub static GHOST_SESSION_ROTATION: ScenarioDef = ScenarioDef {
    name: "ghost_session_rotation",
    expectation: Expectation::Pass,
    run: |ctx| Box::pin(run_ghost_session_rotation(ctx)),
};

/// Long-probe length for the nightly drift sweep (#16). 3 minutes — needs the
/// `real` profile's 900 s scenario timeout (the compressed ceiling is 300 s).
const LONG_PROBE_MS: u32 = 180_000;

/// Probe length: 8 cycles — the two boundary cycles absorb startup/teardown
/// transients (see `oracle_assert`), leaving 6 interior cycles as the
/// load-bearing signal, all inside the compressed-profile timeout.
const PROBE_MS: u32 = 8_000;

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
    // 60 s: the encrypted m2d capture opens when the puppet first transmits,
    // which precedes the probe by the whole key-delivery + settle window.
    OracleConfig { max_align_ms: 60_000, ..OracleConfig::default() }
}

/// The probe's cycle length (`nvb_audio::probe` synthesizes per-1 s cycles
/// with seed-jittered layouts, so each cycle's envelope is distinct enough to
/// alignment-lock independently).
const CYCLE_MS: u32 = 1_000;

/// Compare `captured` against the seeded probe **cycle by cycle** and gate on
/// robust AGGREGATES, persisting the WAVs + a per-cycle report JSON.
///
/// Why per-cycle: a live receive path time-warps by design — libwebrtc's NetEQ
/// (and Discord-side suppression resume) inserts/removes ~10 ms to track
/// network jitter (live-measured 2026-07-07: cycles align at +0 ms before a
/// mid-stream slip and +9.7 ms after; per-cycle ncc 0.92–0.99 while a single
/// global alignment collapsed to 0.22). One global alignment can therefore
/// never hold across seconds of live audio, so each 1 s cycle is scored with
/// its own local alignment.
///
/// Why aggregate gates, not per-cycle pass/fail: the oracle's default
/// thresholds allow ZERO dropouts/glitches, which suits a hermetic chain but
/// not a live public SFU, where a single transient per multi-second stream is
/// normal (live-measured: an otherwise-pristine cycle at ncc 0.97 tripped on
/// one glitch). The robust discriminator is **mean per-cycle NCC**, which
/// showed a wide, stable margin across every live run — 0.86–0.99 when audio
/// crossed intact vs 0.0–0.47 when it did not (e.g. an undelivered E2EE key
/// collapsing every cycle). So the gate is: mean NCC over a floor, a majority
/// of cycles individually well-correlated (catches a half-broken direction),
/// and a whole-stream transient budget (a #36-class corruption produces dozens
/// of glitches, not one). Every per-cycle score is still recorded so a nightly
/// trend catches slow drift a single pass/fail would miss.
fn oracle_assert(
    artifacts: &Path,
    label: &str,
    seed: u64,
    probe_ms: u32,
    captured: &[i16],
) -> Result<()> {
    let probe = synth_probe_mono_48k(seed, probe_ms);
    let wav = |suffix: &str, data: &[i16]| -> Result<()> {
        let path = artifacts.join(format!("{label}_{suffix}.wav"));
        nvb_audio::wav::write_wav_mono_48k(path.to_str().context("artifact path")?, data)
    };
    wav("probe", &probe)?;
    wav("captured", captured)?;

    // Stage 1: one global envelope alignment for the BASE delay. Its ncc may
    // be poor when mid-stream time-warp is present (that's what stage 2
    // absorbs), but the envelope-derived delay is robust to ±tens of ms.
    let global = compare_mono_48k(&probe, captured, &oracle_cfg());
    let base = global.delay_samples;
    eprintln!(
        "   oracle[{label}] global: ncc={:.3} delay={base} (base for per-cycle windows)",
        global.ncc
    );

    // Stage 2: score each cycle against a ±500 ms window around its PREDICTED
    // position, with a tight local alignment bound — a far-away mis-lock (a
    // cycle whose jittered layout happens to resemble another's) is then
    // structurally impossible.
    const PAD: isize = 24_000; // 500 ms
    // "Well-correlated" cycle floor — comfortably above live jitter (healthy
    // cycles sit 0.93–0.99), comfortably below a corrupted one.
    const GOOD_NCC: f32 = 0.85;
    let cycles = (probe_ms / CYCLE_MS) as usize;
    let cycle_len = CYCLE_MS as usize * 48;
    let mut entries = Vec::new();
    let mut ncc_sum = 0.0f64;
    let mut good = 0usize;
    let mut total_dropouts = 0usize;
    let mut total_glitches = 0usize;
    for k in 0..cycles {
        let src = &probe[k * cycle_len..((k + 1) * cycle_len).min(probe.len())];
        let start = (base + (k * cycle_len) as isize - PAD).max(0) as usize;
        let end = ((base + ((k + 1) * cycle_len) as isize + PAD).max(0) as usize)
            .min(captured.len());
        let window = if start < end { &captured[start..end] } else { &[][..] };
        let local_cfg = OracleConfig { max_align_ms: 600, ..OracleConfig::default() };
        let rep = compare_mono_48k(src, window, &local_cfg);
        let well_correlated = rep.ncc >= GOOD_NCC;
        eprintln!(
            "   oracle[{label}] cycle {k}: ncc={:.3} seg_snr={:.1}dB gain={:.2} delay={} dropouts={} glitches={}{}",
            rep.ncc,
            rep.seg_snr_db,
            rep.gain,
            rep.delay_samples,
            rep.dropouts.len(),
            rep.glitches.len(),
            if well_correlated { "" } else { "  <-- rough" },
        );
        ncc_sum += rep.ncc as f64;
        if well_correlated {
            good += 1;
        }
        total_dropouts += rep.dropouts.len();
        total_glitches += rep.glitches.len();
        entries.push(serde_json::json!({
            "cycle": k,
            "ncc": rep.ncc,
            "seg_snr_db": rep.seg_snr_db,
            "gain": rep.gain,
            "delay_samples": rep.delay_samples,
            "dropouts": rep.dropouts.len(),
            "glitches": rep.glitches.len(),
        }));
    }
    let mean_ncc = ncc_sum / cycles as f64;
    // Aggregate gates (see the doc comment): a healthy live path clears all
    // three with margin; a corrupted/undelivered stream fails the first two,
    // a #36-class corruption the third.
    let mean_ok = mean_ncc >= 0.75;
    let majority_ok = good * 2 >= cycles;
    let transients_ok = total_dropouts <= cycles && total_glitches <= cycles;
    let pass = mean_ok && majority_ok && transients_ok;
    let json = serde_json::json!({
        "label": label,
        "pass": pass,
        "mean_ncc": mean_ncc,
        "good_cycles": good,
        "cycles_total": cycles,
        "total_dropouts": total_dropouts,
        "total_glitches": total_glitches,
        "global_ncc": global.ncc,
        "base_delay_samples": base,
        "cycles": entries,
    });
    std::fs::write(
        artifacts.join(format!("{label}_oracle.json")),
        serde_json::to_string_pretty(&json)?,
    )?;
    eprintln!(
        "   oracle[{label}]: mean_ncc={mean_ncc:.3} good_cycles={good}/{cycles} \
         dropouts={total_dropouts} glitches={total_glitches} → {}",
        if pass { "PASS" } else { "FAIL" },
    );
    if !pass {
        bail!(
            "oracle failed for {label}: mean_ncc={mean_ncc:.3} (floor 0.75), \
             good_cycles={good}/{cycles} (need majority), dropouts={total_dropouts} \
             glitches={total_glitches} (budget {cycles} each) — audio did not cross intact \
             (WAVs + report in artifacts)"
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

async fn run_m2d(ctx: Arc<Ctx>, encrypted: bool, probe_ms: u32) -> Result<()> {
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

    let result =
        drive_m2d(&ctx, &bridge, &room, &channel, bridge_name, seed, encrypted, probe_ms, targets)
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
    probe_ms: u32,
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
            speaker.speak_probe(seed, probe_ms).await?;
            // The probe runs ~real-time from here (LiveKit self-pacing);
            // wait it out plus the drain margin.
            tokio::time::sleep(Duration::from_millis(probe_ms as u64)).await;
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
    oracle_assert(&ctx.artifacts, "m2d", seed, probe_ms, &captured)?;

    let errors = bridge.scraper.error_events();
    if !errors.is_empty() {
        bail!("bridge logged {} ERROR event(s); first: {}", errors.len(), errors[0].message);
    }
    Ok(())
}

// ─── Matrix → Discord, encrypted, FRESH JOIN (the #73 wedge ordering) ────────────

/// #73 regression: the speaker is in the encrypted call AND STREAMING before
/// the bridge process even starts, so the subscriber's first inbound frames
/// deterministically beat the Olm-delivered media key. On an unfixed bridge
/// the inbound decryptor wedges permanently (E2EE state reports Ok, output
/// stays digital silence); the fix must (a) arm the heal gate at subscribe,
/// (b) re-subscribe exactly once when the key lands, and (c) deliver real
/// audio to the puppet afterwards — the oracle is the proof.
async fn run_m2d_enc_freshjoin(ctx: Arc<Ctx>) -> Result<()> {
    let cfg = &ctx.cfg;
    let seed = 0x73F1;
    let bridge_name = "Auto Test 02";
    let (room, channel) = {
        let (r, c) = cfg.encrypted_pair()?;
        (r.to_owned(), c.to_owned())
    };
    let poller = RoomStatePoller::new(cfg)?;
    let targets = puppet_ids(cfg)?;
    sweep_test_memberships(cfg, &poller, &room).await?;

    // The far-side ear first — its capture window must be open before the
    // puppet ever transmits.
    let listener = DiscordListener::connect(
        cfg.speaker_tokens.first().context("listener token")?,
        &cfg.guild_id,
        &channel,
        targets,
    )
    .await?;

    let outcome = async {
        listener.join().await?;

        // The Matrix human joins the call and starts speaking with NO bridge
        // running — the fresh-join half of the #73 race, made deterministic.
        let speaker =
            MatrixSpeaker::join(cfg, &room, true, "nvbtest_alice", "NVB Test Alice").await?;
        let filler = speaker.start_filler();

        // NOW boot the bridge into the already-live call.
        let tenants = tenant_entry(bridge_name, &room, &cfg.guild_id, &channel);
        let bridge =
            match BridgeUnderTest::start(cfg, ctx.profile, &tenants, &ctx.artifacts.join("bridge"))
                .await
                .context("starting bridge-under-test")
            {
                Ok(b) => b,
                Err(e) => {
                    filler.stop().await;
                    speaker.leave().await;
                    return Err(e);
                }
            };

        let drive = async {
            bridge.await_ready(&[bridge_name], Duration::from_secs(120)).await?;

            // 1. The subscribe must happen keyless (frames are already flowing,
            //    the key can only arrive after the bridge posts membership).
            bridge
                .scraper
                .await_event(Duration::from_secs(90), |e| {
                    e.message.contains("subscribed before its peer media key")
                        && e.in_bridge(bridge_name)
                })
                .await
                .context("bridge never recorded a keyless subscribe — fresh-join ordering broken?")?;

            // 2. The speaker's key lands…
            bridge
                .scraper
                .await_event(Duration::from_secs(90), |e| {
                    e.message.contains("installed peer media key")
                })
                .await
                .context("bridge never installed the test speaker's media key")?;

            // 3. …and triggers the one-shot heal.
            bridge
                .scraper
                .await_event(Duration::from_secs(60), |e| {
                    e.message.contains("re-subscribing to rebuild its decryptor")
                        && e.in_bridge(bridge_name)
                })
                .await
                .context("the #73 heal (re-subscribe on key install) never fired")?;

            // 4. The re-subscribe restarts the playback consumer — wait for the
            //    SECOND start (the first belongs to the wedged pre-key chain).
            let consumer_starts = || {
                bridge
                    .scraper
                    .events_matching(|e| {
                        e.message.contains("playback consumer started")
                            && e.in_bridge(bridge_name)
                    })
                    .len()
            };
            let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
            while consumer_starts() < 2 {
                if tokio::time::Instant::now() >= deadline {
                    bail!("the playback consumer never restarted after the #73 heal");
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }

            listener
                .await_target_in_voice(Duration::from_secs(60))
                .await
                .context("no puppet joined the voice channel for the test speaker")?;
            tokio::time::sleep(SETTLE).await;
            Ok::<(), anyhow::Error>(())
        }
        .await;

        // Stop the filler before the probe either way (heal proven or not);
        // its queued tail (~1.3 s) drains at LiveKit pace.
        filler.stop().await;

        let spoken = match drive {
            Ok(()) => {
                tokio::time::sleep(Duration::from_millis(1_500)).await;
                let r = speaker.speak_probe(seed, PROBE_MS).await;
                if r.is_ok() {
                    tokio::time::sleep(Duration::from_millis(PROBE_MS as u64)).await;
                    tokio::time::sleep(DRAIN).await;
                }
                r
            }
            Err(e) => Err(e),
        };
        speaker.leave().await;

        let checks = async {
            spoken?;
            // Exactly ONE heal: a second re-subscribe would mean the gate
            // loops (each key-index rotation re-wedging the track).
            let heals = bridge
                .scraper
                .events_matching(|e| e.message.contains("re-subscribing to rebuild its decryptor"))
                .len();
            if heals != 1 {
                bail!("expected exactly 1 #73 heal re-subscribe, saw {heals}");
            }
            let errors = bridge.scraper.error_events();
            if !errors.is_empty() {
                bail!(
                    "bridge logged {} ERROR event(s); first: {}",
                    errors.len(),
                    errors[0].message
                );
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;
        match checks {
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
    .await;

    let recordings = listener.stop().await;
    outcome?;

    let (uid, captured) = loudest(recordings)
        .context("the Discord listener recorded nothing from any puppet")?;
    eprintln!("   recorded {} samples from puppet {uid}", captured.len());
    oracle_assert(&ctx.artifacts, "m2d_enc_freshjoin", seed, PROBE_MS, &captured)
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
    oracle_assert(&ctx.artifacts, "d2m", seed, PROBE_MS, &captured.samples)?;

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
    oracle_assert(&ctx.artifacts, "d2m_enc_tap_b", seed, PROBE_MS, &captured)?;

    let errors = bridge.scraper.error_events();
    if !errors.is_empty() {
        bail!("bridge logged {} ERROR event(s); first: {}", errors.len(), errors[0].message);
    }
    Ok(())
}

// ─── #14 shutdown_drain: SIGTERM mid-call, clean drain + baseline ─────────────

/// Bring an unencrypted M→D call up with audio actively flowing, then SIGTERM
/// **mid-probe**. The graceful shutdown that every other scenario runs as an
/// afterthought is the whole assertion here: the bridge must exit 0, log
/// `all bridges drained cleanly`, and leave no puppet in Discord voice.
async fn run_shutdown_drain(ctx: Arc<Ctx>) -> Result<()> {
    let cfg = &ctx.cfg;
    let bridge_name = "Auto Test 01";
    let seed = 0x5D14;
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
    // Keep the log ring alive past sigterm_and_wait, which consumes the bridge.
    let scraper = bridge.scraper.clone();

    let driven = async {
        let speaker =
            MatrixSpeaker::join(cfg, &cfg.room_a, false, "nvbtest_alice", "NVB Test Alice").await?;
        let active = async {
            // The bridge assigns a puppet and subscribes the speaker's track.
            bridge
                .scraper
                .await_event(Duration::from_secs(90), |e| {
                    e.message.contains("playback consumer started") && e.in_bridge(bridge_name)
                })
                .await
                .context("bridge never subscribed the test speaker's track")?;
            tokio::time::sleep(SETTLE).await;
            // Start the probe, then let it run halfway before pulling the plug —
            // audio is genuinely crossing when the drain begins.
            speaker.speak_probe(seed, PROBE_MS).await?;
            tokio::time::sleep(Duration::from_millis((PROBE_MS / 2) as u64)).await;
            Ok::<(), anyhow::Error>(())
        }
        .await;
        // Retract the fake membership regardless of the bridge's state.
        speaker.leave().await;
        active
    }
    .await;
    if let Err(e) = driven {
        bridge.kill().await;
        return Err(e);
    }

    // ── The scenario proper: SIGTERM mid-call must drain cleanly (exit 0) ────
    bridge
        .sigterm_and_wait(Duration::from_secs(20))
        .await
        .context("SIGTERM drain (mid-call)")?;

    // The stdout pump can lag the child's exit by a few ms — poll the ring for
    // the supervisor's drain-complete line.
    let mut drained = false;
    for _ in 0..30 {
        if scraper
            .events_matching(|e| e.message.contains("all bridges drained cleanly"))
            .is_empty()
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
        } else {
            drained = true;
            break;
        }
    }
    if !drained {
        bail!("bridge exited 0 after SIGTERM but never logged 'all bridges drained cleanly'");
    }

    // ── Baseline: no puppet lingers in Discord voice after the drain ─────────
    // Generous window: SIGTERM tears the process down milliseconds after the
    // puppet's songbird leave, so the ACTIVE capture anchor's voice-leave may
    // not flush to Discord's gateway before exit — Discord then clears its voice
    // state via the RTC keepalive timeout (~90 s) rather than promptly. The
    // bridge's own "puppet left voice and released" + "all bridges drained
    // cleanly" (asserted above) prove the drain issued the leave; this only
    // confirms Discord eventually reflects baseline. (Prompter flush-on-shutdown
    // is a bridge follow-up, tracked separately.)
    let probe =
        ConnCountProbe::connect(cfg.speaker_tokens.last().context("probe token")?, &cfg.guild_id)
            .await?;
    let baseline = probe.await_baseline(&targets, Duration::from_secs(150)).await;
    probe.shutdown().await;
    baseline?;

    let errors = scraper.error_events();
    if !errors.is_empty() {
        bail!("bridge logged {} ERROR event(s); first: {}", errors.len(), errors[0].message);
    }
    Ok(())
}

// ─── #16 long_glitch_sweep: sustained-audio drift sweep (nightly, opt-in) ─────

/// The unencrypted M→D audio path over a 3-minute probe — a #32/#36 drift guard
/// that a single 8 s cycle can't surface. Opt-in via `NVB_E2E_LONG=1` (so the
/// default `--all` fast suite skips it) and run under `--profile real` (its
/// 900 s scenario timeout accommodates the long probe; compressed's 300 s does
/// not).
async fn run_long_glitch_sweep(ctx: Arc<Ctx>) -> Result<()> {
    if std::env::var("NVB_E2E_LONG").ok().filter(|v| !v.is_empty()).is_none() {
        eprintln!(
            "   long_glitch_sweep skipped — opt-in via NVB_E2E_LONG=1 \
             (nightly-only; run with --profile real for the {}s probe)",
            LONG_PROBE_MS / 1000
        );
        return Ok(());
    }
    run_m2d(ctx, false, LONG_PROBE_MS).await
}

// ─── #11 fresh_store_rekey: wiped crypto store → re-mint → audio still crosses ─

/// Prove the #30/#53 self-heal end to end: establish encrypted D→M audio, tear
/// the bridge down, **wipe its crypto store**, and restart on the SAME workdir.
/// The bridge re-provisions a brand-new device (a fresh store can't reuse the
/// old device's keys — #30) and encrypted audio must still cross.
///
/// Scope note: each pass uses a fresh `MatrixListener` (a send-only test-crypto
/// peer with no receive-side device memory), so this asserts the re-minted
/// device delivers keys to a fresh peer — the strongest check the current
/// testkit allows. The "peer remembers the OLD device and rejects a changed
/// key" path needs a full-crypto peer (workstream D) and is covered by the live
/// #53 deployment history.
async fn run_fresh_store_rekey(ctx: Arc<Ctx>) -> Result<()> {
    let cfg = &ctx.cfg;
    let bridge_name = "Auto Test 02";
    let (room_b, channel_b) = {
        let (r, c) = cfg.encrypted_pair()?;
        (r.to_owned(), c.to_owned())
    };
    let poller = RoomStatePoller::new(cfg)?;
    sweep_test_memberships(cfg, &poller, &room_b).await?;

    // One workdir reused across both starts so the crypto store persists (and
    // is ours to wipe). capture_wav=true → the Tap B oracle target.
    let workdir = ctx.artifacts.join("bridge");
    let tenants = tenant_entry_opts(bridge_name, &room_b, &cfg.guild_id, &channel_b, true);
    let data_dir = workdir.join("data");

    // ── Pass 1: establish encrypted D→M and confirm audio crosses ───────────
    fresh_store_pass(&ctx, &tenants, bridge_name, &room_b, &channel_b, &workdir, &poller, 0x11EC)
        .await
        .context("pass 1 (initial store) — encrypted audio should cross")?;

    // ── Wipe the crypto store so the restart re-mints a fresh device ────────
    // Remove the whole data dir (a single-tenant bridge keeps only this
    // bridge's bot + ghost stores there); `start` recreates it and rewrites
    // tenants.toml. This avoids depending on the private store-slug derivation.
    if !data_dir.exists() {
        bail!("expected a crypto store under {} after pass 1 — nothing to wipe", data_dir.display());
    }
    std::fs::remove_dir_all(&data_dir)
        .with_context(|| format!("wiping crypto store dir {}", data_dir.display()))?;
    eprintln!("   wiped crypto store dir {}", data_dir.display());

    // ── Pass 2: same workdir, fresh store → new device, audio must STILL cross ─
    fresh_store_pass(&ctx, &tenants, bridge_name, &room_b, &channel_b, &workdir, &poller, 0x11ED)
        .await
        .context("pass 2 (wiped store, re-minted device) — encrypted audio should still cross")?;
    Ok(())
}

/// One start→drive→drain cycle of the encrypted D→M path on a fixed workdir,
/// used by `run_fresh_store_rekey` for both the initial and post-wipe passes.
#[allow(clippy::too_many_arguments)]
async fn fresh_store_pass(
    ctx: &Ctx,
    tenants: &str,
    bridge_name: &str,
    room_b: &str,
    channel_b: &str,
    workdir: &Path,
    poller: &RoomStatePoller,
    seed: u64,
) -> Result<()> {
    let bridge = BridgeUnderTest::start(&ctx.cfg, ctx.profile, tenants, workdir)
        .await
        .context("starting bridge-under-test")?;
    if let Err(e) = bridge.await_ready(&[bridge_name], Duration::from_secs(120)).await {
        bridge.kill().await;
        return Err(e);
    }
    let result = drive_d2m_enc(ctx, &bridge, bridge_name, room_b, channel_b, seed).await;
    match result {
        Ok(()) => {
            let _ = await_quiescent(poller, room_b).await;
            bridge.sigterm_and_wait(Duration::from_secs(20)).await.context("SIGTERM drain")
        }
        Err(e) => {
            bridge.kill().await;
            Err(e)
        }
    }
}

// ─── #12 ghost_session_rotation: membership change rotates Olm sessions ───────

/// Guard #52: with an encrypted D→M call live, a membership change past the 90 s
/// `SESSION_ROTATE_MIN_INTERVAL` rotates the ghost's outbound Olm sessions with
/// `reason=membership`. Log-asserted (the rotation is internal to the ghost's
/// crypto).
///
/// Timing: the min-interval is a hardcoded 90 s const (not a `TimingConfig`
/// knob), so this scenario carries an inherent ~90 s floor even under the
/// compressed profile — it still fits compressed's 300 s ceiling. A future
/// follow-up could thread the interval into `TimingConfig` to compress it.
async fn run_ghost_session_rotation(ctx: Arc<Ctx>) -> Result<()> {
    let cfg = &ctx.cfg;
    let bridge_name = "Auto Test 02";
    let (room_b, channel_b) = {
        let (r, c) = cfg.encrypted_pair()?;
        (r.to_owned(), c.to_owned())
    };
    let poller = RoomStatePoller::new(cfg)?;
    sweep_test_memberships(cfg, &poller, &room_b).await?;

    let tenants = tenant_entry(bridge_name, &room_b, &cfg.guild_id, &channel_b);
    let bridge = BridgeUnderTest::start(cfg, ctx.profile, &tenants, &ctx.artifacts.join("bridge"))
        .await
        .context("starting bridge-under-test")?;
    if let Err(e) = bridge.await_ready(&[bridge_name], Duration::from_secs(120)).await {
        bridge.kill().await;
        return Err(e);
    }

    // A persistent Matrix listener keeps the call active (a claimable
    // encrypted-room key recipient) so the ghost stays LIVE and its
    // key-distribution loop — which owns the rotation timer — actually runs.
    // Without a Matrix member the ghost is presence-only and never publishes
    // (the smoke case), so the loop never starts and nothing can rotate.
    let listener = match MatrixListener::join(cfg, &room_b, true, "nvbtest_listener", "NVB Test Listener")
        .await
    {
        Ok(l) => l,
        Err(e) => {
            bridge.kill().await;
            return Err(e);
        }
    };

    // A Discord speaker gives the ghost audio to publish. Created outside the
    // inner block so its gateway/voice session is always torn down, even on
    // failure.
    let speaker_token = match cfg.speaker_tokens.first() {
        Some(t) => t,
        None => {
            let _ = listener.stop().await;
            bridge.kill().await;
            bail!("speaker token missing");
        }
    };
    let mut speaker = match DiscordSpeaker::connect(speaker_token, &cfg.guild_id, &channel_b).await {
        Ok(s) => s,
        Err(e) => {
            let _ = listener.stop().await;
            bridge.kill().await;
            return Err(e);
        }
    };
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
        // Confirm the key-distribution loop is actually delivering — it owns the
        // rotation timer.
        bridge
            .scraper
            .await_event(Duration::from_secs(90), |e| {
                e.message.contains("ghost media key sent")
                    || e.message.contains("ghost media key delivered")
            })
            .await
            .context("ghost never delivered its media key (key loop not running)")?;

        // The min-interval measures from the key loop's start; a membership
        // change only rotates once >90 s have elapsed. Wait it out, then join a
        // SECOND member (bob) — a membership change distinct from the listener —
        // to set the pending flag; the next distribution tick then rotates.
        tokio::time::sleep(Duration::from_secs(95)).await;
        let phantom =
            MatrixPhantom::join(cfg, &room_b, true, true, "nvbtest_bob", "NVB Test Bob").await?;
        tokio::time::sleep(Duration::from_secs(3)).await;
        phantom.leave().await;

        // The rotation fires on a distribution tick after the flap. Assert the
        // success log with reason=membership (the message is fixed; the reason
        // is a structured field on the raw JSON line).
        bridge
            .scraper
            .await_event(Duration::from_secs(90), |e| {
                e.message.contains("rotated Olm outbound sessions") && e.raw.contains("membership")
            })
            .await
            .context("ghost never rotated its Olm sessions with reason=membership")?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    let left = speaker.leave().await;
    speaker.shutdown().await;
    let _ = listener.stop().await;
    let driven = outcome.and(left.context("speaker voice leave"));

    match driven {
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
