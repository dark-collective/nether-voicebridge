//! MatrixRTC session management — Stage 2: LiveKit connect + E2EE + tone.
//!
//! # Lifecycle
//! Startup: discover SFU focus from .well-known.
//! Register to-device handler for incoming `io.element.call.encryption_keys`.
//!
//! On call-active (first human joins):
//!   1. Post m.call.member join
//!   2. Fetch fresh OpenID token + LiveKit JWT
//!   3. Connect to LiveKit room (HKDF E2EE, GCM)
//!   4. Generate media key; set it in provider under our own identity
//!   5. Publish 440 Hz sine tone
//!   6. Send our key to all current call members (encrypted to-device)
//!
//! On call-ended (last human leaves) or shutdown:
//!   1. Cancel tone task
//!   2. Disconnect from LiveKit room
//!   3. Post m.call.member leave

mod keys;
mod lk_jwt;
mod membership;
mod openid;
mod publish;
mod session;
mod well_known;

// Phase 4 tone diagnostic — kept on disk (and compiling) but not used in the
// live path now that Discord audio is forwarded. Re-wire in `publish` if a
// standalone tone test is ever needed again.
#[allow(dead_code)]
mod tone;

use anyhow::Result;
use livekit::e2ee::key_provider::KeyProvider;
use livekit::webrtc::audio_frame::AudioFrame;
use livekit::webrtc::audio_source::native::NativeAudioSource;
use matrix_sdk::{Client, ruma::RoomId};
use std::borrow::Cow;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, watch};

use nvb_audio::jitter::JitterBuffer;
use nvb_audio::routing::PlaybackRouter;
use nvb_config::{LiveKitConfig, MatrixConfig};
use keys::CallEncryptionKeysEventContent;
use well_known::LivekitFocusInfo;

/// Jitter buffer sizing for the outbound (Discord → LiveKit) stream.
const JITTER_TARGET_MS: u32 = 60;
const JITTER_MAX_MS: u32 = 120;
const SAMPLE_RATE: u32 = 48_000;
const SAMPLES_10MS: u32 = 480;

/// Control messages to the long-lived audio consumer: which LiveKit source (if
/// any) to feed.  Sent over an mpsc so source swaps happen *inside* the
/// consumer task — no shared mutex, and reconnects overwrite cleanly.
enum SourceControl {
    Activate(NativeAudioSource),
    Deactivate,
}

/// State held while the bridge is connected to a live call.
struct ActiveSession {
    lk_session: session::LiveSession,
    /// Handle to tell the audio consumer to stop feeding our source on teardown.
    ctrl_tx: mpsc::Sender<SourceControl>,
}

impl ActiveSession {
    /// Tear down the LiveKit connection.
    async fn close(self) {
        // Stop the consumer feeding the source *before* the source is dropped by
        // disconnect, so it never feeds a torn-down source.
        let _ = self.ctrl_tx.send(SourceControl::Deactivate).await;
        self.lk_session.disconnect().await;
        tracing::info!("LiveKit session closed");
    }
}

/// Long-lived task: drains forwarded Discord frames through a jitter buffer and
/// feeds 10 ms mono frames to the currently-active LiveKit source.
///
/// Pacing is driven by `audio_rx.recv()` — Discord's real-time 20 ms tick is
/// the producer clock, so (unlike the Phase 4 tone) there is no second clock
/// racing libwebrtc's drain.  Errors from `capture_frame` (e.g. feeding a
/// source mid-teardown) are swallowed; this task must never die, as its death
/// would silence the bridge for the rest of the process.
async fn audio_consumer(
    mut audio_rx: mpsc::Receiver<Vec<i16>>,
    mut ctrl_rx: mpsc::Receiver<SourceControl>,
) {
    let mut jitter = JitterBuffer::new(JITTER_TARGET_MS, JITTER_MAX_MS, SAMPLE_RATE);
    let mut source: Option<NativeAudioSource> = None;
    // ~5 s at 50 frames/s: periodic jitter-depth health log.
    let mut ticks: u64 = 0;

    loop {
        tokio::select! {
            biased;
            ctrl = ctrl_rx.recv() => match ctrl {
                Some(SourceControl::Activate(s)) => {
                    tracing::info!("audio consumer: source activated");
                    source = Some(s);
                    jitter.clear();
                }
                Some(SourceControl::Deactivate) => {
                    tracing::info!("audio consumer: source deactivated");
                    source = None;
                    jitter.clear();
                }
                None => break, // run() dropped ctrl_tx — shutting down
            },
            frame = audio_rx.recv() => {
                let Some(frame20) = frame else { break }; // Discord side gone
                jitter.push(&frame20);
                ticks += 1;
                if ticks.is_multiple_of(250) && source.is_some() {
                    tracing::debug!(
                        depth = jitter.depth_samples(),
                        target = jitter.target_samples(),
                        level_db = format_args!("{:.1}", nvb_audio::rms_dbfs(&frame20)),
                        "jitter buffer depth / inbound level"
                    );
                }
                while let Some(f10) = jitter.pop_10ms() {
                    // Drain even when inactive so latency stays bounded.
                    if let Some(src) = source.as_ref() {
                        let af = AudioFrame {
                            data: Cow::Owned(f10),
                            sample_rate: SAMPLE_RATE,
                            num_channels: 1,
                            samples_per_channel: SAMPLES_10MS,
                        };
                        if let Err(e) = src.capture_frame(&af).await {
                            tracing::warn!(error = %e, "capture_frame failed — yielding");
                            tokio::task::yield_now().await;
                        }
                    }
                }
            }
        }
    }
    tracing::info!("audio consumer task exited");
}

/// Run the RTC participation loop.
///
/// * `client` — a ready matrix-sdk client (produced by `matrix::build_client`)
/// * `lk_cfg` — LiveKit / lk-jwt config from TOML
/// * `mx_cfg`  — Matrix config (bot user_id + device_id + room_id)
/// * `call_active_rx` — watch receiver: `true` when humans are in the call
pub(crate) async fn run(
    client: Client,
    lk_cfg: &LiveKitConfig,
    mx_cfg: &MatrixConfig,
    mut call_active_rx: watch::Receiver<bool>,
    audio_rx: mpsc::Receiver<Vec<i16>>,
    router: Arc<dyn PlaybackRouter>,
) -> Result<()> {
    let span = tracing::info_span!("rtc");
    let _guard = span.enter();

    // Spawn the long-lived audio consumer once. `ctrl_tx` lives for the whole
    // run() so the consumer's control channel never closes until shutdown; a
    // clone is handed to each ActiveSession to activate/deactivate its source.
    let (ctrl_tx, ctrl_rx) = mpsc::channel::<SourceControl>(8);
    tokio::spawn(audio_consumer(audio_rx, ctrl_rx));

    // Build a dedicated HTTP client on rustls.
    let http = reqwest::Client::builder().use_rustls_tls().build()?;

    // Discover the LiveKit SFU focus once at startup.
    let focus = well_known::discover_livekit_focus(
        &http,
        &mx_cfg.homeserver_url,
        &mx_cfg.room_id,
    )
    .await?;

    // Shared key provider reference — populated when a session is active.
    // The to-device handler uses this to install incoming peer keys.
    let shared_provider: Arc<Mutex<Option<KeyProvider>>> = Arc::new(Mutex::new(None));

    // Register a handler to receive peers' media keys via to-device events.
    // If the room has Olm sessions the SDK auto-decrypts before delivery; if not
    // (unencrypted room) the event arrives in plaintext (_info is None).
    {
        let provider_ref = shared_provider.clone();
        client.add_event_handler(
            move |ev: matrix_sdk::ruma::events::ToDeviceEvent<CallEncryptionKeysEventContent>| {
                let provider_ref = provider_ref.clone();
                async move {
                    let sender = ev.sender.as_str().to_owned();
                    let content = ev.content;
                    let claimed_device_id = content.member.claimed_device_id.clone();
                    let member_id = content.member.id.clone();
                    let key_index = content.keys.index;
                    let key_b64 = content.keys.key.clone();

                    // Diagnostic: log all identity fields so we can verify EC's scheme.
                    // Never log the key bytes. These arrive plaintext (unencrypted room).
                    tracing::info!(
                        sender = %sender,
                        claimed_device_id = %claimed_device_id,
                        member_id = %member_id,
                        session_application = %content.session.application,
                        session_scope = %content.session.scope,
                        session_call_id = %content.session.call_id,
                        key_index,
                        "received io.element.call.encryption_keys to-device (plaintext)"
                    );

                    let guard = provider_ref.lock().expect("provider mutex poisoned");
                    if let Some(provider) = guard.as_ref() {
                        keys::install_peer_key(
                            &sender,
                            &claimed_device_id,
                            key_index,
                            &key_b64,
                            provider,
                        );
                    } else {
                        tracing::debug!(
                            sender = %sender,
                            "received encryption_keys but no active session — ignoring"
                        );
                    }
                }
            },
        );
    }

    // ── Static fields derived from config/client ──────────────────────────────
    let user_id = client
        .user_id()
        .ok_or_else(|| anyhow::anyhow!("Matrix client not authenticated"))?
        .to_owned();
    let device_id = client
        .device_id()
        .ok_or_else(|| anyhow::anyhow!("Matrix client has no device id"))?
        .to_owned();
    let room_id = RoomId::parse(&mx_cfg.room_id)?;
    let room_id_str = mx_cfg.room_id.clone();

    let mut active_session: Option<ActiveSession> = None;
    let mut joined = false;

    loop {
        let is_active = *call_active_rx.borrow_and_update();

        match (joined, is_active) {
            (false, true) => {
                // A human appeared — post membership and connect.
                tracing::info!("call became active — joining");

                // 1. Post m.call.member join first so EC maps our identity.
                if let Some(room) = client.get_room(&room_id) {
                    if let Err(e) = membership::join(&room, &user_id, &device_id, &focus).await {
                        tracing::error!(error = %e, "failed to post call membership");
                    }
                } else {
                    tracing::warn!("room not in local state");
                }

                // 2–6. Connect LiveKit, generate key, publish track, send key.
                match join_livekit_and_publish(
                    &client,
                    &http,
                    &focus,
                    &room_id,
                    &room_id_str,
                    mx_cfg.device_id.as_str(),
                    &shared_provider,
                    lk_cfg.media_e2ee,
                    &ctrl_tx,
                    router.clone(),
                )
                .await
                {
                    Ok(sess) => {
                        active_session = Some(sess);
                        joined = true;
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "failed to start LiveKit session");
                        // Retract the membership we just posted.
                        if let Some(room) = client.get_room(&room_id) {
                            let _ = membership::leave(&room, &user_id, &device_id).await;
                        }
                    }
                }
            }

            (true, false) => {
                // All humans left — tear down.
                tracing::info!("call ended — leaving");
                teardown(&mut active_session, &shared_provider, &router).await;
                if let Some(room) = client.get_room(&room_id) {
                    if let Err(e) = membership::leave(&room, &user_id, &device_id).await {
                        tracing::error!(error = %e, "failed to post call leave");
                    }
                }
                joined = false;
            }

            _ => {}
        }

        if call_active_rx.changed().await.is_err() {
            break;
        }
    }

    // Cleanup on shutdown.
    if joined {
        teardown(&mut active_session, &shared_provider, &router).await;
        if let Some(room) = client.get_room(&room_id) {
            let _ = membership::leave(&room, &user_id, &device_id).await;
        }
    }

    tracing::info!("RTC task exiting");
    Ok(())
}

/// Fetch a fresh JWT, connect to LiveKit, optionally set up E2EE, publish the
/// bridge audio track and activate it on the consumer, and (when E2EE is
/// enabled) send our key to all current call members.
#[allow(clippy::too_many_arguments)]
async fn join_livekit_and_publish(
    client: &Client,
    http: &reqwest::Client,
    focus: &LivekitFocusInfo,
    room_id: &RoomId,
    room_id_str: &str,
    device_id: &str,
    shared_provider: &Arc<Mutex<Option<KeyProvider>>>,
    use_e2ee: bool,
    ctrl_tx: &mpsc::Sender<SourceControl>,
    router: Arc<dyn PlaybackRouter>,
) -> Result<ActiveSession> {
    let user_id = client
        .user_id()
        .ok_or_else(|| anyhow::anyhow!("client not authenticated"))?
        .to_owned();

    let openid_token = openid::get_openid_token(client).await?;

    let jwt_resp = lk_jwt::fetch_livekit_token(
        http,
        &focus.service_url,
        room_id_str,
        device_id,
        &openid_token,
    )
    .await?;

    let lk_session = session::connect(&jwt_resp.url, &jwt_resp.jwt, use_e2ee, router).await?;

    if use_e2ee {
        if let Some(ref key_provider) = lk_session.key_provider {
            // Set our own key in the provider so LiveKit encrypts our outbound frames.
            let media_key = keys::generate_key();
            let our_identity =
                livekit::id::ParticipantIdentity::from(lk_session.our_identity.clone());
            let installed = key_provider.set_key(&our_identity, 0, media_key.to_vec());
            tracing::info!(
                identity = %lk_session.our_identity,
                installed,
                "our media key set in LiveKit key provider"
            );

            // Make the provider visible to the to-device receive handler.
            {
                let mut guard = shared_provider.lock().expect("provider mutex poisoned");
                *guard = Some(key_provider.clone());
            }

            // Send our key to all current call members (best-effort).
            if let Some(room) = client.get_room(room_id) {
                if let Err(e) = keys::send_key(
                    client,
                    &room,
                    &user_id,
                    device_id,
                    room_id_str,
                    0,
                    &media_key,
                )
                .await
                {
                    tracing::error!(error = %e, "failed to send media key to call members");
                }
            }
        }
    } else {
        tracing::info!("media E2EE disabled — connecting without frame encryption");
    }

    // Publish the bridge audio track and hand the source to the consumer.
    let source = publish::publish(&lk_session.room).await?;
    if ctrl_tx.send(SourceControl::Activate(source)).await.is_err() {
        tracing::error!("audio consumer gone — cannot activate source");
    }

    Ok(ActiveSession {
        lk_session,
        ctrl_tx: ctrl_tx.clone(),
    })
}

/// Tear down an active session, clearing the shared provider and releasing all
/// participant routes (puppets returned to the pool) so the next call starts clean.
async fn teardown(
    active_session: &mut Option<ActiveSession>,
    shared_provider: &Arc<Mutex<Option<KeyProvider>>>,
    router: &Arc<dyn PlaybackRouter>,
) {
    if let Some(sess) = active_session.take() {
        sess.close().await;
    }
    // Per-track consumers release their own routes on cancel; release_all() is
    // belt-and-suspenders in case a consumer was mid-push during teardown.
    router.release_all();
    let mut guard = shared_provider.lock().expect("provider mutex poisoned");
    *guard = None;
}
