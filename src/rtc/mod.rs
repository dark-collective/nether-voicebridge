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
mod session;
mod tone;
mod well_known;

use anyhow::Result;
use livekit::e2ee::key_provider::KeyProvider;
use matrix_sdk::{Client, ruma::RoomId};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::config::{LiveKitConfig, MatrixConfig};
use keys::CallEncryptionKeysEventContent;
use well_known::LivekitFocusInfo;

/// State held while the bridge is connected to a live call.
struct ActiveSession {
    lk_session: session::LiveSession,
    tone_task: tokio::task::JoinHandle<()>,
    tone_cancel: CancellationToken,
}

impl ActiveSession {
    /// Tear down the LiveKit connection and tone.
    async fn close(self) {
        self.tone_cancel.cancel();
        // Give the tone loop a moment to drain.
        let _ = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            self.tone_task,
        )
        .await;
        self.lk_session.disconnect().await;
        tracing::info!("LiveKit session closed");
    }
}

/// Run the RTC participation loop.
///
/// * `client` — a ready matrix-sdk client (produced by `matrix::build_client`)
/// * `_lk_cfg` — LiveKit / lk-jwt config from TOML (currently unused post-Stage-1)
/// * `mx_cfg`  — Matrix config (bot user_id + device_id + room_id)
/// * `call_active_rx` — watch receiver: `true` when humans are in the call
pub async fn run(
    client: Client,
    _lk_cfg: &LiveKitConfig,
    mx_cfg: &MatrixConfig,
    mut call_active_rx: watch::Receiver<bool>,
) -> Result<()> {
    let span = tracing::info_span!("rtc");
    let _guard = span.enter();

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

    // Register a handler to receive peers' media keys over encrypted to-device.
    // The matrix-sdk sync loop auto-decrypts m.room.encrypted to-device events
    // when e2e-encryption is enabled, delivering CallEncryptionKeysEventContent here.
    {
        let provider_ref = shared_provider.clone();
        client.add_event_handler(
            move |ev: matrix_sdk::ruma::events::ToDeviceEvent<CallEncryptionKeysEventContent>,
                  _info: Option<matrix_sdk::deserialized_responses::EncryptionInfo>| {
                let provider_ref = provider_ref.clone();
                async move {
                    let sender = ev.sender.as_str().to_owned();
                    let content = ev.content;
                    let device_id = content.member.claimed_device_id.clone();
                    let key_index = content.keys.index;
                    let key_b64 = content.keys.key.clone();

                    tracing::debug!(
                        sender = %sender,
                        device = %device_id,
                        key_index,
                        "received encryption_keys to-device"
                    );

                    let guard = provider_ref.lock().expect("provider mutex poisoned");
                    if let Some(provider) = guard.as_ref() {
                        keys::install_peer_key(&sender, &device_id, key_index, &key_b64, provider);
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

                // 2–6. Connect LiveKit, generate key, publish tone, send key.
                match join_livekit_and_start_tone(
                    &client,
                    &http,
                    &focus,
                    &room_id,
                    &room_id_str,
                    mx_cfg.device_id.as_str(),
                    &shared_provider,
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
                teardown(&mut active_session, &shared_provider).await;
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
        teardown(&mut active_session, &shared_provider).await;
        if let Some(room) = client.get_room(&room_id) {
            let _ = membership::leave(&room, &user_id, &device_id).await;
        }
    }

    tracing::info!("RTC task exiting");
    Ok(())
}

/// Fetch a fresh JWT, connect to LiveKit, generate our media key, publish the
/// tone, and send our key to all current call members.
async fn join_livekit_and_start_tone(
    client: &Client,
    http: &reqwest::Client,
    focus: &LivekitFocusInfo,
    room_id: &RoomId,
    room_id_str: &str,
    device_id: &str,
    shared_provider: &Arc<Mutex<Option<KeyProvider>>>,
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
        user_id.as_str(),
        &openid_token,
    )
    .await?;

    let lk_session = session::connect(&jwt_resp.url, &jwt_resp.jwt).await?;

    // Set our own key in the provider so LiveKit encrypts our outbound frames.
    let media_key = keys::generate_key();
    let our_identity = livekit::id::ParticipantIdentity::from(lk_session.our_identity.clone());
    let installed = lk_session.key_provider.set_key(&our_identity, 0, media_key.to_vec());
    tracing::info!(
        identity = %lk_session.our_identity,
        installed,
        "our media key set in LiveKit key provider"
    );

    // Make the provider visible to the to-device receive handler.
    {
        let mut guard = shared_provider.lock().expect("provider mutex poisoned");
        *guard = Some(lk_session.key_provider.clone());
    }

    // Publish the tone track.
    let tone_cancel = CancellationToken::new();
    let tone_task = tone::publish_and_run(&lk_session.room, tone_cancel.clone()).await?;

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

    Ok(ActiveSession { lk_session, tone_task, tone_cancel })
}

/// Tear down an active session, clearing the shared provider.
async fn teardown(
    active_session: &mut Option<ActiveSession>,
    shared_provider: &Arc<Mutex<Option<KeyProvider>>>,
) {
    if let Some(sess) = active_session.take() {
        sess.close().await;
    }
    let mut guard = shared_provider.lock().expect("provider mutex poisoned");
    *guard = None;
}
