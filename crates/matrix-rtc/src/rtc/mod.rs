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
mod key_transport;
pub(crate) mod ghost;
mod lk_jwt;
mod membership;
mod openid;
mod publish;
mod session;
// `pub(crate)`: the management bot's link preflight probes RTC-foci discovery.
pub(crate) mod well_known;

// Phase 4 tone diagnostic — kept on disk (and compiling) but not used in the
// live path. Re-wire in `publish` if a standalone tone test is ever needed.
#[allow(dead_code)]
mod tone;

use anyhow::Result;
use livekit::e2ee::key_provider::{KeyDerivationAlgorithm, KeyProvider, KeyProviderOptions};
use matrix_sdk::{
    Client,
    ruma::{OwnedDeviceId, OwnedRoomId, OwnedUserId, RoomId},
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use nvb_audio::routing::PlaybackRouter;
use nvb_config::{LiveKitConfig, MatrixConfig};
use keys::CallEncryptionKeysEventContent;
use well_known::LivekitFocusInfo;

// The reaper's sweep interval (20 s default — with the default 45 s reap
// grace, a pre-existing phantom clears within ~a minute of the call becoming
// active) was a hardcoded const here until #38; it now comes from the resolved
// `[timing]` config (`TimingConfig::reap_interval`) as `run`'s `reap_interval`
// parameter. See [`matrix::reap_stale_memberships`].

/// Shared slot for the live session's key-install notifier (#73): the to-device
/// handler pushes the LiveKit identity of every peer whose media key it just
/// installed, and the session's room-event loop re-subscribes any track that
/// was subscribed before its key arrived. Set on join, cleared on teardown —
/// same lifecycle as the shared `KeyProvider`.
type KeyInstalledTx = Arc<Mutex<Option<tokio::sync::mpsc::UnboundedSender<String>>>>;

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
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run(
    client: Client,
    lk_cfg: &LiveKitConfig,
    mx_cfg: &MatrixConfig,
    // Resolved media-E2EE for this bridge (#14: auto-detected/overridden in
    // `lib::run`), NOT the raw `lk_cfg.media_e2ee` Option. Gates the subscriber's
    // `m.call.member` post and LiveKit frame-decrypt setup.
    media_e2ee: bool,
    // Stale-membership reaper timing (#25), from the resolved `[timing]`
    // config: how often to sweep, and how long a phantom must be continuously
    // absent from the LiveKit roster before it is emptied.
    reap_interval: Duration,
    reap_grace: Duration,
    mut call_active_rx: watch::Receiver<bool>,
    router: Arc<dyn PlaybackRouter>,
    bridge: Arc<str>,
    shutdown: CancellationToken,
) -> Result<()> {
    // Build a dedicated HTTP client on rustls.
    let http = reqwest::Client::builder().use_rustls_tls().build()?;
    let ghost_prefix: Arc<str> = Arc::from(
        ghost::ghost_localpart_prefix(mx_cfg.sender_localpart.as_deref()).as_str(),
    );

    // Discover the LiveKit SFU focus once at startup.
    let focus = well_known::discover_livekit_focus(
        &http,
        &mx_cfg.well_known_base_url(),
        &mx_cfg.room_id,
    )
    .await?;

    // Shared key provider reference — populated when a session is active.
    // The to-device handler uses this to install incoming peer keys so the
    // subscriber can decrypt remote frames (only relevant when E2EE is on).
    let shared_provider: Arc<Mutex<Option<KeyProvider>>> = Arc::new(Mutex::new(None));
    // Every peer key delivered this call, replayed into the fresh provider a
    // disconnect-recovery rejoin mints (#66) — see [`keys::PeerKeyCache`].
    let peer_keys: Arc<keys::PeerKeyCache> = Arc::new(keys::PeerKeyCache::default());
    // Key-install notifier for the live session's #73 heal gate.
    let key_installed_tx: KeyInstalledTx = Arc::new(Mutex::new(None));

    // Bridge-bot identity, used to post/retract call membership when E2EE is on.
    let bot_user_id: OwnedUserId = mx_cfg.bot_user_id.parse()?;
    let device_id: OwnedDeviceId = mx_cfg.device_id.as_str().into();

    // Register a handler to receive peers' media keys via to-device events.
    // With e2e on (encrypted room) EC/Commet send these Olm-encrypted and the SDK
    // auto-decrypts before delivery; in an unencrypted room they arrive in clear.
    {
        let provider_ref = shared_provider.clone();
        let peer_keys_ref = peer_keys.clone();
        let key_tx_ref = key_installed_tx.clone();
        // Runs in matrix-sdk's sync task (no inherited span) — tag per bridge.
        let bridge = bridge.clone();
        client.add_event_handler(
            move |ev: matrix_sdk::ruma::events::ToDeviceEvent<CallEncryptionKeysEventContent>| {
                let provider_ref = provider_ref.clone();
                let peer_keys_ref = peer_keys_ref.clone();
                let key_tx_ref = key_tx_ref.clone();
                let span = tracing::info_span!(parent: None, "bridge", name = %bridge);
                async move {
                    let sender = ev.sender.as_str().to_owned();
                    let content = ev.content;
                    let claimed_device_id = content.member.claimed_device_id.clone();
                    let member_id = content.member.id.clone();
                    let key_index = content.keys.index;
                    let key_b64 = content.keys.key.clone();

                    // Diagnostic: log all identity fields so we can verify EC's scheme.
                    // Never log the key bytes. (Olm-decrypted by the SDK when the
                    // room is encrypted; plaintext when it is not.)
                    tracing::info!(
                        sender = %sender,
                        claimed_device_id = %claimed_device_id,
                        member_id = %member_id,
                        session_application = %content.session.application,
                        session_scope = %content.session.scope,
                        session_call_id = %content.session.call_id,
                        key_index,
                        "received io.element.call.encryption_keys to-device"
                    );

                    // Record BEFORE installing: keys that arrive while the session
                    // is down (disconnect recovery, #66) must be retained so the
                    // rejoin's replay restores them into the fresh provider.
                    peer_keys_ref.record(&sender, &claimed_device_id, key_index, &key_b64);
                    let installed = {
                        let guard = provider_ref.lock().expect("provider mutex poisoned");
                        match guard.as_ref() {
                            Some(provider) => keys::install_peer_key(
                                &sender,
                                &claimed_device_id,
                                key_index,
                                &key_b64,
                                provider,
                            ),
                            None => {
                                tracing::debug!(
                                    sender = %sender,
                                    "received encryption_keys with no active session — cached for the next join"
                                );
                                false
                            }
                        }
                    };
                    // #73: nudge the session's heal gate — a track already
                    // subscribed keyless for this identity is re-subscribed
                    // now that its key exists.
                    if installed {
                        let slot = key_tx_ref.lock().expect("key-installed slot mutex poisoned");
                        if let Some(tx) = slot.as_ref() {
                            let _ = tx.send(keys::peer_livekit_identity(&sender, &claimed_device_id));
                        }
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
    // E2EE membership-retry task for the current activation (cancel + its handle).
    let mut membership_cancel: Option<CancellationToken> = None;
    let mut membership_handle: Option<tokio::task::JoinHandle<()>> = None;
    // True while an E2EE membership we posted may still be in room state. Usually
    // tracks `joined`, EXCEPT during disconnect recovery (#66): the recovery arm
    // sets `joined = false` but deliberately leaves the membership posted (we
    // intend to rejoin). If the call then ends before the rejoin lands, the
    // normal (true,false) teardown never runs — the guarded (false,false) arm
    // below retracts via this flag so the bot's membership can't linger forever
    // (the reaper skips bridge-owned entries, so nothing else would clean it).
    let mut membership_lingering = false;

    // Stale-membership reaper state: per-state-key "phantom since" grace clocks,
    // and the periodic tick that drives the sweep while a call is active (#25).
    let mut phantom_since: HashMap<String, Instant> = HashMap::new();
    let mut reap_interval = tokio::time::interval(reap_interval);
    // First tick fires immediately; skip the missed tick rather than bursting.
    reap_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        let is_active = *call_active_rx.borrow_and_update();

        match (joined, is_active) {
            (false, true) => {
                // A human appeared — connect as a subscribe-only LiveKit
                // participant. NOTE: in the UNENCRYPTED case the subscriber
                // deliberately posts NO m.call.member, so the silent, non-
                // publishing bridge bot stays out of the Element Call roster (it
                // still receives every track — the LiveKit JWT is authorized by
                // the OpenID token, not by membership). In the ENCRYPTED case it
                // MUST post membership (below), because EC/Commet only send media
                // keys to roster members — so there it becomes a visible, silent
                // participant. Ghosts always post their own membership.
                tracing::info!("call became active — connecting subscriber");

                match join_livekit(
                    &client,
                    &http,
                    &focus,
                    &room_id,
                    &room_id_str,
                    mx_cfg.device_id.as_str(),
                    &shared_provider,
                    &key_installed_tx,
                    &peer_keys,
                    media_e2ee,
                    lk_cfg.lk_jwt_url.as_deref(),
                    lk_cfg.url.as_deref(),
                    router.clone(),
                    bridge.clone(),
                    ghost_prefix.clone(),
                    shutdown.clone(),
                )
                .await
                {
                    Ok(sess) => {
                        active_session = Some(sess);
                        joined = true;
                        // E2EE only: become a roster member so EC/Commet target
                        // us with Olm-encrypted media keys. Posted AFTER the key
                        // provider is shared (inside join_livekit) so a fast
                        // incoming key has a live provider to land in. Retried until
                        // it lands (self-heals a transient/room-PL failure).
                        if media_e2ee {
                            let mc = shutdown.child_token();
                            membership_cancel = Some(mc.clone());
                            membership_handle = Some(tokio::spawn(
                                post_membership_until_joined(
                                    client.clone(),
                                    room_id.clone(),
                                    bot_user_id.clone(),
                                    device_id.clone(),
                                    focus.clone(),
                                    mc,
                                )
                                .instrument(tracing::info_span!(parent: None, "bridge", name = %bridge)),
                            ));
                            membership_lingering = true;
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "failed to start LiveKit session");
                    }
                }
            }

            (true, false) => {
                // All humans left — tear down. This is a normal call-end, so
                // release the puppets too (they leave voice).
                tracing::info!("call ended — disconnecting subscriber");
                teardown(&mut active_session, &shared_provider, &key_installed_tx, &router, true).await;
                joined = false;
                // No live roster anymore — drop the reaper's grace clocks so the
                // next call starts fresh.
                phantom_since.clear();
                // The next call's peer keys are all fresh — drop the replay cache.
                peer_keys.clear();
                // Stop the membership-retry task and await it before retracting, so
                // a late retry-join can't race past the leave and linger.
                if let Some(mc) = membership_cancel.take() {
                    mc.cancel();
                }
                if let Some(h) = membership_handle.take() {
                    let _ = h.await;
                }
                // Retract the E2EE roster membership we posted on join.
                if media_e2ee {
                    retract_membership(&client, &room_id, &bot_user_id, &device_id).await;
                }
                membership_lingering = false;
            }

            (false, false) if membership_lingering => {
                // The call ended while we were disconnected/rejoining (#66): the
                // (true,false) arm never ran because recovery had already dropped
                // `joined`, so the membership recovery left posted must be
                // retracted here or it lingers between calls.
                tracing::info!("call ended during disconnect recovery — retracting lingering membership");
                // This is the call-end path for a call that died disconnected —
                // same as (true,false): the next call's peer keys are all fresh.
                peer_keys.clear();
                if let Some(mc) = membership_cancel.take() {
                    mc.cancel();
                }
                if let Some(h) = membership_handle.take() {
                    let _ = h.await;
                }
                if media_e2ee {
                    retract_membership(&client, &room_id, &bot_user_id, &device_id).await;
                }
                membership_lingering = false;
            }

            _ => {}
        }

        // The current session's disconnect signal (#66). Snapshotted per
        // iteration: every teardown path takes `active_session` to `None` before
        // the next snapshot, so a close-induced Disconnected on an old session's
        // token is never selected (no spurious recovery from teardown echo).
        let disc = active_session
            .as_ref()
            .map(|s| s.lk_session.disconnected.clone());

        // Wait for the next call-active transition, a room disconnect, a reaper
        // tick, or shutdown. Ordering (biased): a simultaneous call-end +
        // disconnect must prefer the normal (true,false) teardown (which also
        // retracts membership) over the rejoin-intent recovery arm.
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            changed = call_active_rx.changed() => {
                if changed.is_err() {
                    break;
                }
            }
            _ = async {
                match &disc {
                    Some(t) => t.cancelled().await,
                    None => std::future::pending().await,
                }
            } => {
                // The LiveKit room died (SDK reconnect exhausted, or the event
                // stream ended). Tear the dead session down and rejoin: humans
                // are still in the call, so keep the puppet routes (the fresh
                // session's TrackSubscribed re-fires and consumers re-attach)
                // and keep the posted membership (we're coming back). The next
                // iteration's (false,true) arm re-runs join_livekit; failures
                // retry at reaper-tick cadence, same as an initial join.
                tracing::warn!("LiveKit room disconnected — tearing down for rejoin");
                teardown(&mut active_session, &shared_provider, &key_installed_tx, &router, false).await;
                joined = false;
                // The dead-room window produced bogus absence observations —
                // drop the grace clocks so nobody gets reaped off them.
                phantom_since.clear();
                // Stop the membership-retry task (the rejoin arm respawns it) but
                // do NOT retract: the membership is still valid and retract+repost
                // would flap the EC roster and rotate everyone's keys.
                if let Some(mc) = membership_cancel.take() {
                    mc.cancel();
                }
                if let Some(h) = membership_handle.take() {
                    let _ = h.await;
                }
                // membership_lingering stays true — the guarded (false,false) arm
                // retracts if the call ends before the rejoin lands.
            }
            _ = reap_interval.tick() => {
                // Only meaningful with a live LiveKit roster to compare against.
                // A dead room reads as an empty/frozen roster — every real human
                // would look phantom — so skip when the disconnect signal has
                // fired but the recovery arm hasn't run yet (#66 race window).
                if let Some(sess) = &active_session {
                    if sess.lk_session.disconnected.is_cancelled() {
                        tracing::debug!("skipping reap tick — room disconnected, awaiting rejoin");
                    } else {
                        let live_mxids = live_roster_mxids(&sess.lk_session.room);
                        crate::matrix::reap_stale_memberships(
                            &client, &room_id, &bot_user_id, &ghost_prefix, &live_mxids, &mut phantom_since,
                            reap_grace,
                        )
                        .await;
                    }
                }
            }
        }
    }

    // Cleanup on shutdown: close only our LiveKit subscriber. Puppet release is
    // owned by the Discord side's *awaited* shutdown drain — calling
    // `router.release_all()` here would be fire-and-forget and could empty the
    // assignment map out from under that drain (losing the await guarantee), so
    // we pass `release_routes = false`.
    // `joined` can be false with cleanup still owed: a shutdown that lands
    // mid-disconnect-recovery (#66) has no active session but a lingering
    // membership. Gate the two halves independently.
    if active_session.is_some() || joined {
        teardown(&mut active_session, &shared_provider, &key_installed_tx, &router, false).await;
    }
    if membership_lingering {
        if let Some(mc) = membership_cancel.take() {
            mc.cancel();
        }
        if let Some(h) = membership_handle.take() {
            let _ = h.await;
        }
        // Retract the E2EE roster membership so the bot doesn't linger in the
        // call view after the process exits.
        if media_e2ee {
            retract_membership(&client, &room_id, &bot_user_id, &device_id).await;
        }
    }

    tracing::info!("RTC task exiting — teardown complete");
    Ok(())
}

/// The set of bare MXIDs currently present in the LiveKit room — the reaper's
/// "who's really here" ground truth. LiveKit identities are `{mxid}:{device}`;
/// [`session::participant_mxid`] strips the device so we match on the bare MXID
/// (a present user is never reaped even if their membership device differs from
/// their track device).
fn live_roster_mxids(room: &livekit::Room) -> std::collections::HashSet<String> {
    room.remote_participants()
        .values()
        .map(|p| session::participant_mxid(&p.identity().to_string()).to_owned())
        .collect()
}

/// Post the bridge bot's `m.call.member` (E2EE only). Returns `Err` so the caller's
/// retry loop knows whether it landed — without membership EC/Commet send no media
/// keys, so receive is dead until it succeeds.
async fn post_membership(
    client: &Client,
    room_id: &RoomId,
    bot_user_id: &OwnedUserId,
    device_id: &OwnedDeviceId,
    focus: &LivekitFocusInfo,
) -> anyhow::Result<()> {
    let room = client
        .get_room(room_id)
        .ok_or_else(|| anyhow::anyhow!("room not in local state — cannot post E2EE membership"))?;
    membership::join(&room, bot_user_id, device_id, focus).await
}

/// Post the bridge bot's `m.call.member`, retrying with backoff until it lands or
/// `cancel` fires (call-end / shutdown). Self-heals a transient failure (e.g. an
/// encrypted room whose `org.matrix.msc3401.call.member` power level is wrong, then
/// fixed) so receive recovers without a reconnect.
async fn post_membership_until_joined(
    client: Client,
    room_id: OwnedRoomId,
    bot_user_id: OwnedUserId,
    device_id: OwnedDeviceId,
    focus: LivekitFocusInfo,
    cancel: CancellationToken,
) {
    let mut backoff = Duration::from_secs(2);
    let max = Duration::from_secs(30);
    loop {
        match post_membership(&client, &room_id, &bot_user_id, &device_id, &focus).await {
            Ok(()) => {
                tracing::info!("bridge participant posted to call (E2EE key receipt enabled)");
                return;
            }
            Err(e) => tracing::warn!(
                error = %e,
                retry_in_s = backoff.as_secs(),
                "failed to post bridge m.call.member — retrying (room may need m.call.member at PL 0); E2EE receive is down until it lands"
            ),
        }
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(max);
    }
}

/// Retract the bridge bot's `m.call.member` (E2EE only). Best-effort.
async fn retract_membership(
    client: &Client,
    room_id: &RoomId,
    bot_user_id: &OwnedUserId,
    device_id: &OwnedDeviceId,
) {
    if let Some(room) = client.get_room(room_id) {
        if let Err(e) = membership::leave(&room, bot_user_id, device_id).await {
            tracing::warn!(error = %e, "failed to retract bridge-bot m.call.member");
        }
    }
}

/// Fetch a fresh JWT, connect to LiveKit and subscribe to remote tracks. When
/// E2EE is enabled, the key provider is built, shared with the to-device
/// handler, and re-seeded from the peer-key cache **before** connecting:
/// inbound frames can only start after the post-connect subscribe, so every key
/// already delivered — and any arriving mid-connect — beats the first frame to
/// the decryptor. Frames-before-key permanently wedges libwebrtc's inbound
/// decryptor (#73), so this ordering is correctness, not style. Does **not**
/// publish — the bridge bot is the subscriber; Discord → Matrix audio is
/// published by ghosts.
#[allow(clippy::too_many_arguments)]
async fn join_livekit(
    client: &Client,
    http: &reqwest::Client,
    focus: &LivekitFocusInfo,
    room_id: &RoomId,
    room_id_str: &str,
    device_id: &str,
    shared_provider: &Arc<Mutex<Option<KeyProvider>>>,
    key_installed_tx: &KeyInstalledTx,
    peer_keys: &Arc<keys::PeerKeyCache>,
    use_e2ee: bool,
    // Optional config overrides; when `None` the values are auto-derived from
    // the focus (lk-jwt endpoint) and the lk-jwt response (SFU ws URL).
    lk_jwt_url_override: Option<&str>,
    url_override: Option<&str>,
    router: Arc<dyn PlaybackRouter>,
    bridge: Arc<str>,
    ghost_prefix: Arc<str>,
    shutdown: CancellationToken,
) -> Result<ActiveSession> {
    let openid_token = openid::get_openid_token(client).await?;

    let jwt_resp = lk_jwt::fetch_livekit_token(
        http,
        lk_jwt_url_override.unwrap_or(&focus.service_url),
        room_id_str,
        device_id,
        &openid_token,
    )
    .await?;

    let (key_provider, key_installed_rx) = if use_e2ee {
        // ratchet_salt is used as the HKDF salt when deriving the actual AES-GCM key
        // from the raw key bytes supplied via set_key. EC's livekit-client-js uses the
        // livekit SDK default "LKFrameEncryptionKey". Empty salt produces a different
        // derived key → decryption fails → raw encrypted bytes output as audio = noise.
        let key_provider = KeyProvider::new(KeyProviderOptions {
            ratchet_window_size: 16, // match livekit-client-js default
            ratchet_salt: b"LKFrameEncryptionKey".to_vec(),
            failure_tolerance: -1,
            key_ring_size: 256,
            key_derivation_algorithm: KeyDerivationAlgorithm::HKDF,
        });
        // Make the provider visible to the to-device receive handler so it can
        // install peers' keys and decrypt their inbound frames.
        {
            let mut guard = shared_provider.lock().expect("provider mutex poisoned");
            *guard = Some(key_provider.clone());
        }
        tracing::info!("E2EE key provider shared with to-device handler (subscribe decrypt)");
        // A rejoin after a disconnect (#66) mints this provider EMPTY; peers
        // won't re-send unprompted. Restore every key delivered this call
        // (no-op on the first join of a call).
        peer_keys.replay(&key_provider);
        // Key-install notifications for the session's heal gate (#73).
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        *key_installed_tx.lock().expect("key-installed slot mutex poisoned") = Some(tx);
        (Some(key_provider), Some(rx))
    } else {
        tracing::info!("media E2EE disabled — connecting without frame encryption");
        (None, None)
    };

    let lk_session = match session::connect(
        url_override.unwrap_or(&jwt_resp.url),
        &jwt_resp.jwt,
        key_provider,
        key_installed_rx,
        peer_keys.clone(),
        client.clone(),
        room_id.to_owned(),
        router,
        bridge,
        ghost_prefix.clone(),
        shutdown,
    )
    .await
    {
        Ok(sess) => sess,
        Err(e) => {
            // Keep the provider/notifier slots strictly session-scoped: a
            // failed connect leaves no session, so leave no slots behind (keys
            // arriving meanwhile are still cache-recorded and replay next try).
            clear_session_slots(shared_provider, key_installed_tx);
            return Err(e);
        }
    };

    Ok(ActiveSession { lk_session })
}

/// Clear the per-session shared slots (key provider + #73 key-install
/// notifier). Called on teardown and on a failed join, so the slots always
/// mirror "a session exists".
fn clear_session_slots(
    shared_provider: &Arc<Mutex<Option<KeyProvider>>>,
    key_installed_tx: &KeyInstalledTx,
) {
    *shared_provider.lock().expect("provider mutex poisoned") = None;
    *key_installed_tx.lock().expect("key-installed slot mutex poisoned") = None;
}

/// Tear down an active session, clearing the shared provider. When
/// `release_routes` is set (a normal call-end), also release all participant
/// routes so puppets return to the pool and the next call starts clean. On a
/// process shutdown this is `false`: the Discord side awaits its own puppet
/// drain, and a fire-and-forget `release_all()` here would race it.
async fn teardown(
    active_session: &mut Option<ActiveSession>,
    shared_provider: &Arc<Mutex<Option<KeyProvider>>>,
    key_installed_tx: &KeyInstalledTx,
    router: &Arc<dyn PlaybackRouter>,
    release_routes: bool,
) {
    if let Some(sess) = active_session.take() {
        sess.close().await;
    }
    if release_routes {
        // Per-track consumers release their own routes on cancel; release_all()
        // is belt-and-suspenders in case a consumer was mid-push during teardown.
        router.release_all();
    }
    clear_session_slots(shared_provider, key_installed_tx);
}
