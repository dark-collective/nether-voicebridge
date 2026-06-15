//! MatrixRTC session management.
//!
//! # Roles (Phase 8)
//! The bridge bot is the **subscriber**: it joins the call and subscribes to
//! every real Matrix participant's track, routing each to a Discord puppet
//! (Matrix → Discord). It no longer **publishes** — Discord → Matrix audio is
//! carried by per-speaker **ghosts** (`rtc::ghost`), one LiveKit participant per
//! Discord speaker. So this module connects + subscribes; it does not feed an
//! outbound source.
//!
//! On call-active (first human joins):
//!   1. Post the bridge bot's m.call.member join
//!   2. Fetch a fresh OpenID token + LiveKit JWT
//!   3. Connect to LiveKit and subscribe to remote tracks (HKDF E2EE when on)
//!
//! On call-ended (last human leaves) or shutdown:
//!   1. Disconnect from LiveKit (releases all puppet routes)
//!   2. Post the bridge bot's m.call.member leave

mod keys;
pub(crate) mod ghost;
mod lk_jwt;
mod membership;
mod openid;
mod publish;
mod session;
mod well_known;

// Phase 4 tone diagnostic — kept on disk (and compiling) but not used in the
// live path. Re-wire in `publish` if a standalone tone test is ever needed.
#[allow(dead_code)]
mod tone;

use anyhow::Result;
use livekit::e2ee::key_provider::KeyProvider;
use matrix_sdk::{Client, ruma::RoomId};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use tracing::Instrument;

use nvb_audio::routing::PlaybackRouter;
use nvb_config::{LiveKitConfig, MatrixConfig};
use keys::CallEncryptionKeysEventContent;
use well_known::LivekitFocusInfo;

/// State held while the bridge bot is connected to a live call (subscribe-only).
struct ActiveSession {
    lk_session: session::LiveSession,
}

impl ActiveSession {
    /// Tear down the LiveKit connection.
    async fn close(self) {
        self.lk_session.disconnect().await;
        tracing::info!("LiveKit session closed");
    }
}

/// Run the RTC participation loop (subscriber side).
///
/// * `client` — a ready matrix-sdk client (produced by `matrix::build_client`)
/// * `lk_cfg` — LiveKit / lk-jwt config from TOML
/// * `mx_cfg`  — Matrix config (bot user_id + device_id + room_id)
/// * `call_active_rx` — watch receiver: `true` when humans are in the call
#[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %bridge))]
pub(crate) async fn run(
    client: Client,
    lk_cfg: &LiveKitConfig,
    mx_cfg: &MatrixConfig,
    mut call_active_rx: watch::Receiver<bool>,
    router: Arc<dyn PlaybackRouter>,
    bridge: Arc<str>,
) -> Result<()> {
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
    // The to-device handler uses this to install incoming peer keys so the
    // subscriber can decrypt remote frames (only relevant when E2EE is on).
    let shared_provider: Arc<Mutex<Option<KeyProvider>>> = Arc::new(Mutex::new(None));

    // Register a handler to receive peers' media keys via to-device events.
    // If the room has Olm sessions the SDK auto-decrypts before delivery; if not
    // (unencrypted room) the event arrives in plaintext (_info is None).
    {
        let provider_ref = shared_provider.clone();
        // Runs in matrix-sdk's sync task (no inherited span) — tag per bridge.
        let bridge = bridge.clone();
        client.add_event_handler(
            move |ev: matrix_sdk::ruma::events::ToDeviceEvent<CallEncryptionKeysEventContent>| {
                let provider_ref = provider_ref.clone();
                let span = tracing::info_span!(parent: None, "bridge", name = %bridge);
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
                .instrument(span)
            },
        );
    }

    // ── Static fields derived from config/client ──────────────────────────────
    let room_id = RoomId::parse(&mx_cfg.room_id)?;
    let room_id_str = mx_cfg.room_id.clone();

    // The bridge bot is a hidden subscriber and never posts its own call
    // membership — but a prior process killed mid-call may have left one behind.
    // Retract any stale self-membership at startup so the bot stays out of EC's
    // roster (the puppets/ghosts are the visible identities).
    if let Some(uid) = client.user_id() {
        crate::matrix::retract_self_call_membership(&client, &room_id, uid).await;
    }

    let mut active_session: Option<ActiveSession> = None;
    let mut joined = false;

    loop {
        let is_active = *call_active_rx.borrow_and_update();

        match (joined, is_active) {
            (false, true) => {
                // A human appeared — connect as a subscribe-only LiveKit
                // participant. NOTE: the subscriber deliberately does NOT post an
                // m.call.member event. Element Call builds its participant roster
                // from m.call.member, so skipping it keeps the silent, non-
                // publishing bridge bot out of the call UI — it still receives
                // every Matrix speaker's track (the LiveKit JWT is authorized by
                // the OpenID token, not by membership). Ghosts post their own
                // membership (they must be seen); only this listener is hidden.
                tracing::info!("call became active — connecting subscriber (no membership)");

                match join_livekit(
                    &client,
                    &http,
                    &focus,
                    &room_id,
                    &room_id_str,
                    mx_cfg.device_id.as_str(),
                    &shared_provider,
                    lk_cfg.media_e2ee,
                    router.clone(),
                    bridge.clone(),
                )
                .await
                {
                    Ok(sess) => {
                        active_session = Some(sess);
                        joined = true;
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "failed to start LiveKit session");
                    }
                }
            }

            (true, false) => {
                // All humans left — tear down (no membership to retract).
                tracing::info!("call ended — disconnecting subscriber");
                teardown(&mut active_session, &shared_provider, &router).await;
                joined = false;
            }

            _ => {}
        }

        if call_active_rx.changed().await.is_err() {
            break;
        }
    }

    // Cleanup on shutdown (no membership to retract — subscriber never posts one).
    if joined {
        teardown(&mut active_session, &shared_provider, &router).await;
    }

    tracing::info!("RTC task exiting");
    Ok(())
}

/// Fetch a fresh JWT, connect to LiveKit and subscribe to remote tracks. When
/// E2EE is enabled, share the key provider so the to-device handler can install
/// peer keys for decryption. Does **not** publish — the bridge bot is the
/// subscriber; Discord → Matrix audio is published by ghosts.
#[allow(clippy::too_many_arguments)]
async fn join_livekit(
    client: &Client,
    http: &reqwest::Client,
    focus: &LivekitFocusInfo,
    room_id: &RoomId,
    room_id_str: &str,
    device_id: &str,
    shared_provider: &Arc<Mutex<Option<KeyProvider>>>,
    use_e2ee: bool,
    router: Arc<dyn PlaybackRouter>,
    bridge: Arc<str>,
) -> Result<ActiveSession> {
    let openid_token = openid::get_openid_token(client).await?;

    let jwt_resp = lk_jwt::fetch_livekit_token(
        http,
        &focus.service_url,
        room_id_str,
        device_id,
        &openid_token,
    )
    .await?;

    let lk_session = session::connect(
        &jwt_resp.url,
        &jwt_resp.jwt,
        use_e2ee,
        client.clone(),
        room_id.to_owned(),
        router,
        bridge,
    )
    .await?;

    if use_e2ee {
        if let Some(ref key_provider) = lk_session.key_provider {
            // Make the provider visible to the to-device receive handler so it can
            // install peers' keys and decrypt their inbound frames.
            let mut guard = shared_provider.lock().expect("provider mutex poisoned");
            *guard = Some(key_provider.clone());
            tracing::info!("E2EE key provider shared with to-device handler (subscribe decrypt)");
        }
    } else {
        tracing::info!("media E2EE disabled — connecting without frame encryption");
    }

    Ok(ActiveSession { lk_session })
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
