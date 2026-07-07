//! Per-ghost lifecycle: register/join/connect/publish, post call membership, pump
//! the Discord speaker's audio, mirror mute, and retract on the way out.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use livekit::e2ee::key_provider::{KeyDerivationAlgorithm, KeyProvider, KeyProviderOptions};
use livekit::e2ee::{E2eeOptions, EncryptionType};
use livekit::id::ParticipantIdentity;
use livekit::track::LocalAudioTrack;
use livekit::webrtc::audio_frame::AudioFrame;
use livekit::webrtc::audio_source::native::NativeAudioSource;
use livekit::{Room, RoomOptions};
use matrix_sdk::Client;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use nvb_audio::jitter::JitterBuffer;
use nvb_audio::wav::WavMonoWriter;

use super::super::key_transport::GhostCryptoCache;
use super::super::keys;
use super::super::lk_jwt;
use super::super::publish;
use super::super::session::decode_jwt_claims;
use super::super::well_known::LivekitFocusInfo;
use super::appservice::{
    fetch_openid_token, invite_ghost, join_room, post_membership, post_membership_until_joined,
    register_ghost, set_avatar, set_displayname,
};
use super::keydist::distribute_key_task;
use super::GhostConfig;

/// Jitter buffer sizing for each ghost's outbound (Discord → LiveKit) stream —
/// mirrors the bridge bot's former single-stream consumer.
const JITTER_TARGET_MS: u32 = 60;
const JITTER_MAX_MS: u32 = 120;
const SAMPLE_RATE: u32 = 48_000;
const SAMPLES_10MS: u32 = 480;

/// Build a ghost's Matrix device id from its key and the per-store salt.
///
/// `D{key}` alone is identical on every machine; folding in the per-store salt
/// (`D{key}-{salt}`) makes it unique to this crypto store so peers never see one
/// device id carrying two different signing keys. An empty salt (no store) yields
/// the bare `D{key}` — unchanged legacy behavior.
pub(super) fn ghost_device_id(key: &str, salt: &str) -> String {
    if salt.is_empty() {
        format!("D{key}")
    } else {
        format!("D{key}-{salt}")
    }
}

/// Per-ghost media key reused across respawns, mirroring [`GhostCryptoCache`]: a Discord
/// speaker leaving and rejoining voice respawns `run_ghost`, which must reuse the SAME
/// 16-byte key under the SAME LiveKit identity at index 0. Otherwise a listener already
/// holding the old key at `(identity, 0)` has to re-receive the new one, and any missed
/// delivery (a known EC reliability gap) leaves that publisher silent until the identity
/// changes. Keyed by ghost MXID; never evicted (fresh only on full process restart, which
/// tears down all calls anyway).
pub(super) type GhostKeyCache = Arc<tokio::sync::Mutex<HashMap<String, [u8; 16]>>>;

/// One ghost's full lifecycle: register/join/connect/publish, post call
/// membership, then publish the speaker's audio until cancelled, retracting
/// membership and disconnecting on the way out.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_ghost(
    cfg: GhostConfig,
    focus: LivekitFocusInfo,
    client: Client,
    members_gen_rx: watch::Receiver<u64>,
    mut call_active_rx: watch::Receiver<bool>,
    key: String,
    display_name: String,
    avatar_url: Option<String>,
    frame_rx: mpsc::Receiver<Vec<i16>>,
    mute_rx: watch::Receiver<bool>,
    cancel: CancellationToken,
    crypto_cache: GhostCryptoCache,
    key_cache: GhostKeyCache,
) {
    let ghost_mxid = format!("@{}{key}:{}", cfg.ghost_localpart_prefix, cfg.server_name);
    let localpart = format!("{}{key}", cfg.ghost_localpart_prefix);
    // Stable per-ghost device id; must agree between the lk-jwt identity and the
    // m.call.member content so EC maps the ghost's media to its membership. The
    // per-store salt makes the otherwise machine-deterministic id unique to this
    // crypto store (no cross-store SigningKeyChanged — the #30 fix for ghosts).
    let device_id = ghost_device_id(&key, &cfg.ghost_device_salt);
    // Generate the media key up front (E2EE rooms only) so we can BOTH warm the
    // crypto + start key distribution during the presence window AND install the
    // same key on the LiveKit frame crypto when we publish. Decouples the ~10-20s
    // crypto bootstrap from `call_active` (see the early distribute_key_task spawn).
    // E2EE rooms only. Reuse this ghost's key across respawns (get-or-insert keyed by
    // MXID) so a Discord speaker's leave→rejoin keeps the SAME key at the SAME identity
    // — a listener already holding it stays able to decrypt, with nothing to re-deliver.
    // The key feeds BOTH the frame-encryptor install (connect_and_publish) and the
    // to-device send; the fp is logged here + at both consumers to prove send==encrypt.
    let media_key = if cfg.media_e2ee {
        use std::collections::hash_map::Entry;
        let mut cache = key_cache.lock().await;
        match cache.entry(ghost_mxid.clone()) {
            Entry::Occupied(e) => {
                let k = *e.get();
                tracing::info!(mxid = %ghost_mxid, key_fp = %keys::key_fp(&k), "ghost media key reused from cache (stable across respawn)");
                Some(k)
            }
            Entry::Vacant(e) => {
                let k = keys::generate_key();
                tracing::info!(mxid = %ghost_mxid, key_fp = %keys::key_fp(&k), "ghost media key generated");
                Some(*e.insert(k))
            }
        }
    } else {
        None
    };

    let span = tracing::info_span!("ghost", mxid = %ghost_mxid);
    async {
        // 1. Register (idempotent) and set the display name.
        if let Err(e) = register_ghost(&cfg, &localpart).await {
            tracing::error!(error = %e, "ghost register failed — skipping");
            return;
        }
        if let Err(e) = set_displayname(&cfg, &ghost_mxid, &display_name).await {
            tracing::warn!(error = %e, "ghost set_displayname failed (continuing)");
        }
        // Mirror the Discord avatar onto the ghost's Matrix profile (best-effort;
        // never blocks the ghost going live). Re-set on each assign — cheap, and
        // it tracks avatar changes across rejoins.
        if let Some(url) = avatar_url.as_deref() {
            if let Err(e) = set_avatar(&cfg, &ghost_mxid, url).await {
                tracing::warn!(error = %e, "ghost set_avatar failed (continuing)");
            }
        }
        // 2. Invite the ghost (as the bridge bot, already a room member) so it can
        // join a *restricted* room — a bare /join is refused there without an invite
        // (MSC3083). Best-effort: a public/non-restricted room may not need (or may
        // forbid) the invite, and the bare join below would still succeed — so a
        // failed invite must not block the ghost. `join_room` is the sole fatal gate:
        // if the room really was restricted, it fails there with a clear 403.
        if let Err(e) = invite_ghost(&client, &cfg.room_id, &ghost_mxid).await {
            tracing::warn!(
                error = %e,
                bot = %cfg.bot_user_id,
                room = %cfg.room_id,
                "ghost invite failed — trying join anyway (restricted rooms need the bot to have invite power)"
            );
        }
        // 3. Join the room (required before posting m.call.member).
        if let Err(e) = join_room(&cfg, &ghost_mxid).await {
            tracing::error!(error = %e, "ghost join_room failed — skipping");
            return;
        }
        // 3b. E2EE only: warm the crypto client + start key distribution NOW — on
        // presence, not on `call_active`. The ~10-20s crypto bootstrap (this ghost's
        // own /sync) then runs during the idle presence window, so when a human joins
        // every present ghost is already warm instead of stampeding its sync onto the
        // homeserver simultaneously. distribute_key_loop idles (no recipients) until a
        // human appears, then sends instantly. The SAME media_key is installed on the
        // LiveKit frame crypto in connect_and_publish below.
        if let Some(media_key) = media_key {
            tokio::spawn(
                distribute_key_task(
                    client,
                    cfg.clone(),
                    localpart.clone(),
                    ghost_mxid.clone(),
                    device_id.clone(),
                    media_key,
                    members_gen_rx,
                    cancel.clone(),
                    crypto_cache,
                )
                .instrument(tracing::Span::current()),
            );
        }
        // 4. Presence phase — ONLY while no Matrix human is in the call yet. Post
        // m.call.member (no media) so Element Call shows "a Discord user is waiting
        // here", then wait for a human. Skipped entirely when a human is already
        // present (e.g. a Discord user rejoining mid-call), so we go straight to the
        // publish-first path below — see step 5 for why ordering matters.
        let mut presence_membership: Option<tokio::task::JoinHandle<()>> = None;
        if !*call_active_rx.borrow_and_update() {
            presence_membership = Some(tokio::spawn(
                post_membership_until_joined(
                    cfg.clone(),
                    ghost_mxid.clone(),
                    device_id.clone(),
                    focus.clone(),
                    cancel.clone(),
                )
                .instrument(tracing::Span::current()),
            ));
            tracing::info!(display_name = %display_name, "ghost present (membership posting; no media until a Matrix user joins the call)");
            loop {
                if *call_active_rx.borrow_and_update() {
                    break;
                }
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => {
                        // Cancelled (Discord user left) before any Matrix human —
                        // retract and retire without ever touching LiveKit.
                        if let Some(h) = presence_membership.take() {
                            h.abort();
                        }
                        if let Err(e) = post_membership(&cfg, &ghost_mxid, &device_id, &focus, true).await {
                            tracing::warn!(error = %e, "ghost post_membership(leave) failed");
                        }
                        tracing::info!("ghost retired (was presence-only)");
                        return;
                    }
                    // Sender drops only on shutdown, which also cancels (handled above
                    // by the biased arm); ignore the result and re-check.
                    _ = call_active_rx.changed() => {}
                }
            }
        }

        // 5. Active phase — a Matrix human is present. Publish the LiveKit track
        // FIRST, THEN (re-)post m.call.member. Element Call subscribes to a ghost's
        // track when it processes the membership WITH the track already live; posting
        // membership before the track (as a naive presence-first flow does) leaves a
        // window where EC sees the member with no track and never re-subscribes to
        // the late track — which silenced a Discord user who left and rejoined
        // mid-call. The Room handle is moved into the event-drain task, which keeps
        // the connection up and closes it on cancel; we keep only the source to feed.
        // Honor cancellation across the connect: `connect_and_publish` does network
        // I/O that does not itself observe `cancel`, so race it. This is what makes a
        // self-heal effective — if this ghost was retracted mid-connect (e.g. it was
        // minted for one of our OWN puppet bots in a startup race, see
        // `retract_self_ghost`), we must abort BEFORE publishing a track or posting
        // membership, or the puppet would briefly surface in the call (echo).
        let (source, track) = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                if let Some(h) = presence_membership.take() {
                    h.abort();
                }
                if let Err(e) = post_membership(&cfg, &ghost_mxid, &device_id, &focus, true).await {
                    tracing::warn!(error = %e, "ghost post_membership(leave) failed");
                }
                tracing::info!("ghost retired (cancelled before publish)");
                return;
            }
            res = connect_and_publish(&cfg, &focus, &ghost_mxid, &device_id, media_key, cancel.clone()) => match res {
                Ok(out) => out,
                Err(e) => {
                    tracing::error!(error = %e, "ghost LiveKit connect/publish failed — retracting");
                    if let Some(h) = presence_membership.take() {
                        h.abort();
                    }
                    if let Err(e) = post_membership(&cfg, &ghost_mxid, &device_id, &focus, true).await {
                        tracing::warn!(error = %e, "ghost post_membership(leave) failed");
                    }
                    return;
                }
            }
        };
        // Track is live → post membership now (EC subscribes off this). Drop any
        // presence-phase post first so this is the last writer at the state key.
        if let Some(h) = presence_membership.take() {
            h.abort();
        }
        let membership_handle = tokio::spawn(
            post_membership_until_joined(
                cfg.clone(),
                ghost_mxid.clone(),
                device_id.clone(),
                focus.clone(),
                cancel.clone(),
            )
            .instrument(tracing::Span::current()),
        );
        tracing::info!(display_name = %display_name, "ghost publishing Discord speaker audio");
        // (Key distribution + crypto bootstrap were started on presence — step 3b —
        // so a pre-warmed ghost can deliver its key the instant a human joins.)

        // 6. Pump frames until cancelled, mirroring the Discord user's mute state
        // onto the track as it changes (and applying the current state up front, so
        // a user who joined muted starts muted).
        pump_audio(frame_rx, &source, &track, mute_rx, &cancel, cfg.capture_wav, &key).await;

        // 7. Teardown. Stop the membership-retry task first (it observes `cancel`)
        // and await it so a late join can't race past the leave below and linger.
        let _ = membership_handle.await;
        // Retract call membership (best-effort; a no-op if we never joined). The
        // LiveKit room is closed by the event-drain task once `cancel` fires.
        if let Err(e) = post_membership(&cfg, &ghost_mxid, &device_id, &focus, true).await {
            tracing::warn!(error = %e, "ghost post_membership(leave) failed");
        }
        tracing::info!("ghost retired");
    }
    .instrument(span)
    .await;
}

/// Drain the ghost's frame channel through a jitter buffer into its LiveKit
/// source as 10 ms mono frames. Mirrors the bridge bot's former audio consumer,
/// one per ghost. Returns when cancelled or the Discord side drops the sender.
///
/// Diagnostics (issue #32, always on): a periodic per-ghost health line (frames
/// in/out, jitter overflow drops + depth, `capture_frame` errors, RMS) and a
/// frame-shape warning. When `capture_wav` is set it also writes "Tap B" — the
/// exact mono PCM handed to LiveKit — to `publish_<key>.wav`.
async fn pump_audio(
    mut frame_rx: mpsc::Receiver<Vec<i16>>,
    source: &NativeAudioSource,
    track: &LocalAudioTrack,
    mut mute_rx: watch::Receiver<bool>,
    cancel: &CancellationToken,
    capture_wav: bool,
    key: &str,
) {
    // Mirror the Discord speaker's mute state onto the LiveKit track. `mute()`/
    // `unmute()` signal the server so Element Call peers see the mic-off indicator
    // (constraint: a muted Discord user must show muted in EC). Apply the current
    // state up front — covers a user who joined already muted — then track changes.
    apply_track_mute(track, *mute_rx.borrow_and_update());
    let mut jitter = JitterBuffer::new(JITTER_TARGET_MS, JITTER_MAX_MS, SAMPLE_RATE);

    // Tap B: stream the published PCM to a WAV for the bisection (off by default).
    let mut tap_b = if capture_wav {
        let path = format!("publish_{key}.wav");
        match WavMonoWriter::create(&path) {
            Ok(w) => {
                tracing::info!(path, "ghost publish-tap WAV (Tap B) capturing");
                Some(w)
            }
            Err(e) => {
                tracing::warn!(path, error = %e, "failed to open publish-tap WAV — skipping Tap B");
                None
            }
        }
    } else {
        None
    };

    // Health/diagnostic counters. Logged every ~250 input frames (~5 s) to match
    // the voicetick-health cadence, then again on exit so a short call still reports.
    let mut frames_in: u64 = 0;
    let mut frames_out: u64 = 0;
    let mut capture_errs: u64 = 0;
    let mut shape_warns: u64 = 0;
    let mut last_rms = f32::NEG_INFINITY;
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            res = mute_rx.changed() => {
                // Sender drops only when the ghost retires (which also cancels,
                // handled by the biased arm above); ignore Err and re-check.
                if res.is_ok() {
                    let muted = *mute_rx.borrow_and_update();
                    apply_track_mute(track, muted);
                    tracing::info!(muted, "ghost track mute mirrored from Discord");
                }
            }
            frame = frame_rx.recv() => {
                let Some(frame20) = frame else { break };
                frames_in += 1;
                jitter.push(&frame20);
                while let Some(f10) = jitter.pop_10ms() {
                    // Shape invariant: every popped frame must be exactly 10 ms.
                    // A mismatch would desync the encoder — warn (rate-limited) so a
                    // reframing bug shows up instead of silently corrupting audio.
                    if f10.len() != SAMPLES_10MS as usize {
                        shape_warns += 1;
                        if shape_warns.is_multiple_of(50) {
                            tracing::warn!(
                                key,
                                len = f10.len(),
                                expected = SAMPLES_10MS,
                                count = shape_warns,
                                "ghost publish frame is not 10 ms — reframing anomaly"
                            );
                        }
                    }
                    last_rms = nvb_audio::rms_dbfs(&f10);
                    if let Some(w) = tap_b.as_mut() {
                        w.write(&f10);
                    }
                    let af = AudioFrame {
                        data: Cow::Owned(f10),
                        sample_rate: SAMPLE_RATE,
                        num_channels: 1,
                        samples_per_channel: SAMPLES_10MS,
                    };
                    if let Err(e) = source.capture_frame(&af).await {
                        capture_errs += 1;
                        if capture_errs.is_multiple_of(50) {
                            tracing::warn!(key, error = %e, count = capture_errs, "ghost capture_frame failing — yielding");
                        }
                        tokio::task::yield_now().await;
                    } else {
                        frames_out += 1;
                    }
                }
                if frames_in.is_multiple_of(250) {
                    tracing::info!(
                        key,
                        frames_in,
                        frames_out,
                        jitter_dropped = jitter.dropped_samples(),
                        jitter_depth = jitter.depth_samples(),
                        capture_errs,
                        rms_db = format_args!("{last_rms:.1}"),
                        "ghost audio health"
                    );
                }
            }
        }
    }

    // Final health snapshot + finalize Tap B so the WAV header is valid.
    tracing::info!(
        key,
        frames_in,
        frames_out,
        jitter_dropped = jitter.dropped_samples(),
        capture_errs,
        shape_warns,
        "ghost audio pump exiting"
    );
    if let Some(w) = tap_b {
        match w.finalize() {
            Ok(()) => tracing::info!(key, "publish-tap WAV (Tap B) finalised"),
            Err(e) => tracing::warn!(key, error = %e, "publish-tap WAV finalise failed"),
        }
    }
}

/// Set the ghost track's mute state to `muted`. `LocalAudioTrack::mute`/`unmute`
/// are idempotent and cheap, so re-applying the same state (e.g. the up-front
/// apply) is harmless. The caller logs on an actual toggle.
fn apply_track_mute(track: &LocalAudioTrack, muted: bool) {
    if muted {
        track.mute();
    } else {
        track.unmute();
    }
}

/// Connect the ghost to LiveKit, publish its audio track, and spawn the
/// room-event drain. Returns the source to feed and, when media E2EE is on, the
/// 16-byte media key this ghost encrypts its frames under (to be distributed to
/// the call's humans by [`distribute_key_loop`]). The `Room` is owned by the
/// drain task (kept up until `cancel`, then closed).
async fn connect_and_publish(
    cfg: &GhostConfig,
    focus: &LivekitFocusInfo,
    ghost_mxid: &str,
    device_id: &str,
    media_key: Option<[u8; 16]>,
    cancel: CancellationToken,
) -> Result<(NativeAudioSource, LocalAudioTrack)> {
    let openid_token = fetch_openid_token(cfg, ghost_mxid).await?;
    let jwt_resp = lk_jwt::fetch_livekit_token(
        &cfg.http,
        cfg.lk_jwt_url.as_deref().unwrap_or(&focus.service_url),
        &cfg.room_id,
        device_id,
        &openid_token,
    )
    .await?;

    let (identity, _room) = decode_jwt_claims(&jwt_resp.jwt)?;
    tracing::info!(identity = %identity, "ghost LiveKit JWT obtained");

    // A ghost only publishes; disable auto-subscribe so it doesn't pull every
    // other participant's audio (the bridge bot is the sole subscriber). Without
    // this, N ghosts would each receive the whole call — needless bandwidth.
    let mut room_opts = RoomOptions::default();
    room_opts.auto_subscribe = false;

    // E2EE: configure HKDF/GCM frame encryption with the SAME key-provider options
    // the subscriber uses (`rtc::session`), generate a per-ghost media key, and set
    // it under this ghost's own LiveKit identity so its outbound frames encrypt
    // under it. Element Call peers install the same key (under the same identity)
    // from the to-device event `distribute_key_loop` sends → they can decrypt us.
    // Install the (pre-generated, in run_ghost) media key on the LiveKit frame
    // crypto so this ghost's outbound frames encrypt under it. The same key was
    // already (or is being) distributed to peers via to-device by the key task
    // started on presence — so it must be the SAME bytes here, hence passing it in.
    if let Some(key) = media_key {
        let key_provider = KeyProvider::new(KeyProviderOptions {
            ratchet_window_size: 16,
            ratchet_salt: b"LKFrameEncryptionKey".to_vec(),
            failure_tolerance: -1,
            key_ring_size: 256,
            key_derivation_algorithm: KeyDerivationAlgorithm::HKDF,
        });
        // The to-device `member.claimed_device_id` (= `device_id`) makes peers
        // reconstruct exactly this identity (`{mxid}:{device_id}`); they must agree
        // or peers install the key under the wrong identity and hear noise.
        if !key_provider.set_key(&ParticipantIdentity::from(identity.clone()), 0, key.to_vec()) {
            return Err(anyhow!("failed to set ghost media key on key provider"));
        }
        // #51 follow-up: fp of the key our OWN frames encrypt under, at index 0.
        // Must match this ghost's "media key generated" + "ghost media key sent" fps;
        // and peers' "installed peer media key" fp for this identity must match too.
        tracing::info!(
            identity = %identity,
            key_index = 0,
            key_fp = %keys::key_fp(&key),
            "ghost frame-encrypt key installed"
        );
        room_opts.encryption = Some(E2eeOptions {
            encryption_type: EncryptionType::Gcm,
            key_provider,
        });
    }

    let (room, events) = Room::connect(
        cfg.lk_url.as_deref().unwrap_or(&jwt_resp.url),
        &jwt_resp.jwt,
        room_opts,
    )
    .await
    .context("ghost LiveKit Room::connect")?;
    if media_key.is_some() {
        room.e2ee_manager().set_enabled(true);
        tracing::info!(identity = %identity, "ghost E2EE enabled — publishing encrypted frames");
    }
    let (source, track) = publish::publish(&room).await?;

    // Move the room + events into a drain task: it keeps the connection alive,
    // logs events so the unbounded channel never fills, and closes the room when
    // cancelled (ghost retired).
    tokio::spawn(async move {
        let mut events = events;
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                ev = events.recv() => match ev {
                    Some(livekit::RoomEvent::Disconnected { reason }) => {
                        tracing::warn!(reason = ?reason, "ghost LiveKit room disconnected");
                        break;
                    }
                    // The ghost's own frame-ENCRYPT state (it's a pure publisher). Surfaced
                    // at INFO to correlate with a listener that can't decrypt this ghost.
                    Some(livekit::RoomEvent::E2eeStateChanged { participant, state }) => {
                        tracing::info!(identity = %participant.identity(), ?state, "ghost LiveKit E2EE state changed");
                    }
                    Some(other) => tracing::trace!(event = ?other, "ghost LiveKit room event"),
                    None => break,
                },
            }
        }
        let _ = room.close().await;
        tracing::debug!("ghost LiveKit room closed");
    });

    Ok((source, track))
}
