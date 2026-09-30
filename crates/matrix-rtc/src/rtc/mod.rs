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
//!   4. Multi-SFU (MSC4195, Element Call ≥ 0.21): every human member publishes
//!      on the SFU their membership resolves to (`rtc::focus`) — normally their
//!      own homeserver's. For each such SFU that is not ours, run one extra
//!      subscribe-only session ([`ForeignSessions`]) so their tracks reach the
//!      same router. Reconciled on join, on membership change, on a foreign
//!      session dying, and on every reaper tick.
//!
//! On call-ended (last human leaves) or shutdown:
//!   1. Disconnect from LiveKit (releases all puppet routes)
//!   2. Post the bridge bot's m.call.member leave

mod keys;
mod key_transport;
pub(crate) mod focus;
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
    event_handler::RawEvent,
    ruma::{
        OwnedDeviceId, OwnedRoomId, OwnedUserId, RoomId,
        events::{AnyToDeviceEvent, ToDeviceEvent},
    },
};
use std::collections::{HashMap, HashSet};
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

/// Shared fan-out for the live sessions' key-install notifiers (#73): the
/// to-device handler pushes the LiveKit identity of every peer whose media key it
/// just installed, and each session's room-event loop re-subscribes any track
/// that was subscribed before its key arrived. One sender per live session (the
/// primary plus every foreign-SFU subscriber); senders whose session is gone are
/// dropped on the next push. Cleared on teardown — same lifecycle as the shared
/// `KeyProvider`.
type KeyInstalledTx = Arc<Mutex<Vec<tokio::sync::mpsc::UnboundedSender<String>>>>;

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
    // Bumped by the sync loop when a human joins the call: re-resolve which SFUs
    // the members publish on (multi-SFU fan-in) without waiting for a reaper tick.
    mut members_gen_rx: watch::Receiver<u64>,
    // The call's elected SFU (`rtc::focus::elected_focus`), republished after
    // every membership snapshot. Ghosts publish there and migrate when it moves
    // — a legacy (`oldest_membership`) client such as Commet listens only on the
    // elected SFU, so a ghost anywhere else is inaudible to it.
    call_focus_tx: watch::Sender<Option<LivekitFocusInfo>>,
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
    let key_installed_tx: KeyInstalledTx = Arc::new(Mutex::new(Vec::new()));

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
                    let member_id = content.member.id.clone().unwrap_or_else(|| "-".to_owned());
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
                        let identity = keys::peer_livekit_identity(&sender, &claimed_device_id);
                        let mut slot = key_tx_ref.lock().expect("key-installed slot mutex poisoned");
                        // Every live session gets the nudge; a closed receiver
                        // (session torn down) is pruned here.
                        slot.retain(|tx| tx.send(identity.clone()).is_ok());
                    }
                }
                .instrument(span)
            },
        );
    }

    // Interop tripwire (2026-09-27): the typed handler above never runs for an
    // `io.element.call.encryption_keys` event whose content fails to deserialize —
    // matrix-sdk logs that only under `matrix_sdk::event_handler=warn`, which prod
    // doesn't enable. That is exactly how Commet's key (no `member.id`) vanished:
    // no install, no log, one-way audio. Re-parse the raw JSON here and WARN with
    // the field *names* only — never the key bytes.
    {
        let bridge = bridge.clone();
        client.add_event_handler(move |ev: AnyToDeviceEvent, raw: RawEvent| {
            let span = tracing::info_span!(parent: None, "bridge", name = %bridge);
            async move {
                if ev.event_type().to_string() != keys::ENCRYPTION_KEYS_EVENT_TYPE {
                    return;
                }
                if let Err(e) =
                    serde_json::from_str::<ToDeviceEvent<CallEncryptionKeysEventContent>>(raw.get())
                {
                    tracing::warn!(
                        sender = %ev.sender(),
                        error = %e,
                        shape = %keys::describe_content_shape(raw.get()),
                        "unparseable io.element.call.encryption_keys to-device — peer media key NOT installed (client interop gap)"
                    );
                }
            }
            .instrument(span)
        });
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
    // The bot's roster membership age for the current activation (E2EE only):
    // fixed at the first post and reused by every refresh and by a rejoin after
    // a disconnect, so the call-focus election never reshuffles on our account;
    // cleared whenever the membership is retracted.
    let mut membership_created: Option<matrix_sdk::ruma::MilliSecondsSinceUnixEpoch> = None;
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

    // Multi-SFU fan-in: one extra subscribe-only session per SFU that a human
    // member publishes on and that isn't ours. Everything a foreign connect needs
    // is bundled once here (owned clones — the reconciler runs from several arms).
    let mut foreign = ForeignSessions::default();
    // Once the sync loop drops its sender, `changed()` errors immediately and
    // forever — go inert instead of spinning (the call-active arm, which shares
    // that lifetime, ends the loop anyway).
    let mut members_gen_closed = false;
    let fctx = ForeignCtx {
        client: client.clone(),
        http: http.clone(),
        room_id: room_id.clone(),
        bot_user_id: bot_user_id.clone(),
        device_id: mx_cfg.device_id.clone(),
        primary: focus.clone(),
        ghost_prefix: ghost_prefix.clone(),
        media_e2ee,
        shared_provider: shared_provider.clone(),
        key_installed_tx: key_installed_tx.clone(),
        peer_keys: peer_keys.clone(),
        router: router.clone(),
        bridge: bridge.clone(),
        shutdown: shutdown.clone(),
        call_focus_tx,
    };
    // Publish the election once up front (room state is synced by now) so a
    // ghost minted before the first call has a value to read.
    publish_call_focus(&fctx).await;

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
                // Re-elect BEFORE the (slow) primary connect: ghosts pick their
                // publish SFU off this watch the moment the call goes active.
                publish_call_focus(&fctx).await;

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
                        // Humans may already be publishing on other SFUs — fan in now.
                        foreign.reconcile(&fctx).await;
                        // E2EE only: become a roster member so EC/Commet target
                        // us with Olm-encrypted media keys. Posted AFTER the key
                        // provider is shared (inside join_livekit) so a fast
                        // incoming key has a live provider to land in. Retried until
                        // it lands (self-heals a transient/room-PL failure).
                        if media_e2ee {
                            let mc = shutdown.child_token();
                            membership_cancel = Some(mc.clone());
                            let created = *membership_created
                                .get_or_insert_with(matrix_sdk::ruma::MilliSecondsSinceUnixEpoch::now);
                            membership_handle = Some(tokio::spawn(
                                keep_membership_posted(
                                    client.clone(),
                                    room_id.clone(),
                                    bot_user_id.clone(),
                                    device_id.clone(),
                                    focus.clone(),
                                    created,
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
                foreign.disconnect_all().await;
                publish_call_focus(&fctx).await;
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
                membership_created = None;
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
                membership_created = None;
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
        // Same per-iteration snapshot for the foreign sessions: any of them dying
        // wakes the loop to reconcile (drop the dead one, reconnect with backoff).
        let foreign_disc = foreign.disconnect_signals();

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
                // Foreign sessions share the primary's key provider/notifier
                // slots (cleared by teardown) — drop them too; the rejoin's
                // reconcile brings them back against the fresh slots.
                foreign.disconnect_all().await;
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
            changed = async {
                if members_gen_closed {
                    std::future::pending().await
                } else {
                    members_gen_rx.changed().await
                }
            } => {
                match changed {
                    // A human joined: they may publish on an SFU we aren't on yet.
                    Ok(()) => {
                        if joined {
                            foreign.reconcile(&fctx).await;
                        }
                    }
                    Err(_) => members_gen_closed = true,
                }
            }
            _ = async {
                if foreign_disc.is_empty() {
                    std::future::pending::<()>().await
                } else {
                    let waits = foreign_disc.iter().map(|t| Box::pin(t.cancelled()));
                    futures_util::future::select_all(waits).await;
                }
            } => {
                // A foreign-SFU session died: reconcile drops it (its URL leaves
                // the reaper's observed set immediately) and reconnects with
                // backoff while its members are still in the call.
                tracing::warn!("a foreign-SFU subscriber disconnected — reconciling");
                if joined {
                    foreign.reconcile(&fctx).await;
                }
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
                        // Keep the foreign set current first (members leave, a
                        // failed connect's backoff expires), then judge phantoms
                        // against the union of every roster we actually hold.
                        let snapshot = foreign.reconcile(&fctx).await;
                        let mut live_mxids = live_roster_mxids(&sess.lk_session.room);
                        foreign.extend_live_mxids(&mut live_mxids);
                        let mut observed = foreign.observed_focus_urls();
                        observed.insert(crate::rtc::focus::normalize_service_url(&focus.service_url));
                        crate::matrix::reap_stale_memberships(
                            &client, &room_id, &snapshot.entries, &live_mxids, &observed, &mut phantom_since,
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
    foreign.disconnect_all().await;
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

// ── Multi-SFU fan-in (MSC4195 `multi_sfu`) ────────────────────────────────────

/// Everything a foreign-SFU connect needs, bundled once per `run` so the
/// reconciler can be called from several loop arms. All owned clones.
struct ForeignCtx {
    client: Client,
    http: reqwest::Client,
    room_id: OwnedRoomId,
    bot_user_id: OwnedUserId,
    device_id: String,
    /// Our own SFU (from `.well-known`) — never a "foreign" target.
    primary: LivekitFocusInfo,
    ghost_prefix: Arc<str>,
    media_e2ee: bool,
    shared_provider: Arc<Mutex<Option<KeyProvider>>>,
    key_installed_tx: KeyInstalledTx,
    peer_keys: Arc<keys::PeerKeyCache>,
    router: Arc<dyn PlaybackRouter>,
    bridge: Arc<str>,
    shutdown: CancellationToken,
    call_focus_tx: watch::Sender<Option<LivekitFocusInfo>>,
}

/// Snapshot the memberships and publish the call's elected SFU (falling back to
/// ours when nothing is elected) for the ghosts. Only wakes them on a change.
async fn publish_call_focus(ctx: &ForeignCtx) {
    match crate::matrix::snapshot_call_memberships(
        &ctx.client,
        &ctx.room_id,
        &ctx.bot_user_id,
        &ctx.ghost_prefix,
    )
    .await
    {
        Ok(snap) => publish_elected(ctx, snap.elected),
        Err(e) => tracing::debug!(error = %e, "call-focus election: could not read memberships"),
    }
}

fn publish_elected(ctx: &ForeignCtx, elected: Option<LivekitFocusInfo>) {
    let chosen = elected.unwrap_or_else(|| ctx.primary.clone());
    ctx.call_focus_tx.send_if_modified(|cur| {
        let same = cur.as_ref().is_some_and(|c| {
            focus::normalize_service_url(&c.service_url) == focus::normalize_service_url(&chosen.service_url)
                && c.alias == chosen.alias
        });
        if same {
            false
        } else {
            tracing::info!(
                sfu = %chosen.service_url,
                ours = chosen.service_url == ctx.primary.service_url,
                "call-focus election changed — ghosts publish on this SFU"
            );
            *cur = Some(chosen.clone());
            true
        }
    });
}

/// A live subscribe-only session on somebody else's SFU.
struct ForeignSession {
    focus: LivekitFocusInfo,
    lk_session: session::LiveSession,
}

/// Initial retry delay after a failed foreign connect (doubles up to
/// [`FOREIGN_BACKOFF_MAX`]; reset by a successful connect).
const FOREIGN_BACKOFF_INITIAL: Duration = Duration::from_secs(15);
const FOREIGN_BACKOFF_MAX: Duration = Duration::from_secs(300);

/// The set of foreign-SFU subscriber sessions, keyed by normalized
/// `livekit_service_url`, plus per-URL connect backoff.
#[derive(Default)]
struct ForeignSessions {
    sessions: HashMap<String, ForeignSession>,
    /// URL → (don't retry before, delay to apply after the next failure).
    backoff: HashMap<String, (Instant, Duration)>,
}

impl ForeignSessions {
    /// The disconnect tokens of every live foreign session (snapshot for `select!`).
    fn disconnect_signals(&self) -> Vec<CancellationToken> {
        self.sessions.values().map(|s| s.lk_session.disconnected.clone()).collect()
    }

    /// Normalized service URLs of the foreign SFUs whose roster we currently see
    /// (connected, not dead). Feeds the reaper's observability gate.
    fn observed_focus_urls(&self) -> HashSet<String> {
        self.sessions
            .iter()
            .filter(|(_, s)| !s.lk_session.disconnected.is_cancelled())
            .map(|(url, _)| url.clone())
            .collect()
    }

    /// Add every foreign roster's bare MXIDs to `into` (dead sessions excluded).
    fn extend_live_mxids(&self, into: &mut HashSet<String>) {
        for s in self.sessions.values() {
            if !s.lk_session.disconnected.is_cancelled() {
                into.extend(live_roster_mxids(&s.lk_session.room));
            }
        }
    }

    /// Drop every foreign session (call end, primary rejoin, shutdown).
    async fn disconnect_all(&mut self) {
        for (url, s) in self.sessions.drain() {
            tracing::info!(sfu = %url, alias = %s.focus.alias, "disconnecting foreign-SFU subscriber");
            s.lk_session.disconnect().await;
        }
        self.backoff.clear();
    }

    /// Bring the foreign session set in line with the humans' current SFUs:
    /// drop dead sessions and those whose SFU no longer has a human member;
    /// connect to each SFU that has one and we don't cover yet (honouring
    /// per-URL backoff). Returns the membership snapshot it worked from so the
    /// caller (reaper tick) can reuse it without a second state read. Also
    /// republishes the call's elected SFU for the ghosts.
    async fn reconcile(&mut self, ctx: &ForeignCtx) -> crate::matrix::CallMemberships {
        let snapshot = match crate::matrix::snapshot_call_memberships(
            &ctx.client,
            &ctx.room_id,
            &ctx.bot_user_id,
            &ctx.ghost_prefix,
        )
        .await
        {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "multi-SFU: could not read call memberships — keeping current sessions");
                return crate::matrix::CallMemberships { entries: Vec::new(), elected: None };
            }
        };
        publish_elected(ctx, snapshot.elected.clone());
        let desired = foreign_foci(&snapshot.entries, &ctx.primary);

        // Dead or no longer needed → close.
        let stale: Vec<String> = self
            .sessions
            .iter()
            .filter(|(url, s)| {
                s.lk_session.disconnected.is_cancelled() || !desired.contains_key(url.as_str())
            })
            .map(|(url, _)| url.clone())
            .collect();
        for url in stale {
            if let Some(s) = self.sessions.remove(&url) {
                let dead = s.lk_session.disconnected.is_cancelled();
                tracing::info!(sfu = %url, dead, "multi-SFU: closing foreign subscriber");
                s.lk_session.disconnect().await;
            }
        }

        // Missing → connect (with backoff).
        let now = Instant::now();
        for (url, focus) in desired {
            if self.sessions.contains_key(&url) {
                continue;
            }
            if let Some((not_before, _)) = self.backoff.get(&url) {
                if now < *not_before {
                    continue;
                }
            }
            match connect_foreign(ctx, &focus).await {
                Ok(lk_session) => {
                    tracing::info!(
                        sfu = %focus.service_url,
                        alias = %focus.alias,
                        "multi-SFU: subscribed to a foreign SFU (human members publish there)"
                    );
                    self.backoff.remove(&url);
                    self.sessions.insert(url, ForeignSession { focus, lk_session });
                }
                Err(e) => {
                    let delay = self
                        .backoff
                        .get(&url)
                        .map(|(_, d)| (*d * 2).min(FOREIGN_BACKOFF_MAX))
                        .unwrap_or(FOREIGN_BACKOFF_INITIAL);
                    tracing::warn!(
                        sfu = %focus.service_url,
                        error = %e,
                        retry_in_s = delay.as_secs(),
                        "multi-SFU: failed to connect to a foreign SFU — its members' audio won't reach Discord until this succeeds"
                    );
                    self.backoff.insert(url, (now + delay, delay));
                }
            }
        }
        snapshot
    }
}

/// The distinct foreign SFUs the *human* members may publish on: for each human,
/// both the SFU the election resolves for it AND its own `foci_preferred[0]` —
/// clients don't all follow the election (Commet ignores memberships whose
/// `focus_selection` it doesn't know and publishes on its own homeserver's SFU),
/// and one extra subscribe-only session is far cheaper than a silent user. Keyed
/// by normalized service URL, our own SFU excluded. Bridge-owned memberships
/// (ghosts, the bot) are skipped; members with nothing derivable contribute
/// nothing.
fn foreign_foci(
    entries: &[crate::matrix::MembershipEntry],
    primary: &LivekitFocusInfo,
) -> HashMap<String, LivekitFocusInfo> {
    let ours = focus::normalize_service_url(&primary.service_url);
    let mut out = HashMap::new();
    for e in entries {
        if e.is_bridge_owned {
            continue;
        }
        for f in [&e.focus, &e.preferred].into_iter().flatten() {
            let url = focus::normalize_service_url(&f.service_url);
            if url == ours {
                continue;
            }
            out.entry(url).or_insert_with(|| f.clone());
        }
    }
    out
}


/// Connect a subscribe-only session to a foreign SFU: mint an OpenID token on
/// OUR homeserver, exchange it at THEIR lk-jwt service (`{service_url}/sfu/get`
/// — cross-server OpenID is how every federated call participant gets in), and
/// join their LiveKit room for this call (`livekit_alias`, normally the Matrix
/// room id). Tracks land in the same router as the primary session; E2EE reuses
/// the primary's key provider (keys are per publisher identity, SFU-agnostic)
/// and registers for #73 key-install nudges.
async fn connect_foreign(ctx: &ForeignCtx, focus: &LivekitFocusInfo) -> Result<session::LiveSession> {
    let openid_token = openid::get_openid_token(&ctx.client).await?;
    let room_alias = if focus.alias.is_empty() { ctx.room_id.as_str() } else { focus.alias.as_str() };
    let jwt_resp = lk_jwt::fetch_livekit_token(
        &ctx.http,
        &focus.service_url,
        room_alias,
        &ctx.device_id,
        &openid_token,
    )
    .await?;

    let (key_provider, key_installed_rx) = if ctx.media_e2ee {
        let provider = ctx.shared_provider.lock().expect("provider mutex poisoned").clone();
        let Some(provider) = provider else {
            anyhow::bail!("E2EE key provider not ready (primary session not joined)");
        };
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        ctx.key_installed_tx.lock().expect("key-installed slot mutex poisoned").push(tx);
        (Some(provider), Some(rx))
    } else {
        (None, None)
    };

    session::connect(
        &jwt_resp.url,
        &jwt_resp.jwt,
        key_provider,
        key_installed_rx,
        ctx.peer_keys.clone(),
        ctx.client.clone(),
        ctx.room_id.clone(),
        ctx.router.clone(),
        ctx.bridge.clone(),
        ctx.ghost_prefix.clone(),
        ctx.shutdown.clone(),
    )
    .await
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
    created_ts: matrix_sdk::ruma::MilliSecondsSinceUnixEpoch,
) -> anyhow::Result<()> {
    let room = client
        .get_room(room_id)
        .ok_or_else(|| anyhow::anyhow!("room not in local state — cannot post E2EE membership"))?;
    membership::join(&room, bot_user_id, device_id, focus, created_ts).await
}

/// Keep the bridge bot's `m.call.member` posted until `cancel` fires (call-end /
/// shutdown / disconnect recovery): post it, retrying with backoff until it lands
/// — self-heals a transient failure (e.g. an encrypted room whose
/// `org.matrix.msc3401.call.member` power level is wrong, then fixed) so receive
/// recovers without a reconnect — then re-post every
/// [`membership::MEMBERSHIP_REFRESH`] with a fresh `expires` computed from the
/// fixed `created_ts`. Without the refresh a single encrypted call longer than
/// 4 h silently dropped the bot from the roster, and with it every peer's media
/// key delivery. Same model as the ghosts (`rtc::ghost::keep_membership_posted`).
async fn keep_membership_posted(
    client: Client,
    room_id: OwnedRoomId,
    bot_user_id: OwnedUserId,
    device_id: OwnedDeviceId,
    focus: LivekitFocusInfo,
    created_ts: matrix_sdk::ruma::MilliSecondsSinceUnixEpoch,
    cancel: CancellationToken,
) {
    let mut first = true;
    loop {
        let mut backoff = Duration::from_secs(2);
        let max = Duration::from_secs(30);
        loop {
            match post_membership(&client, &room_id, &bot_user_id, &device_id, &focus, created_ts).await {
                Ok(()) => {
                    if first {
                        tracing::info!("bridge participant posted to call (E2EE key receipt enabled)");
                    } else {
                        tracing::info!("bridge participant membership refreshed (expiry extended)");
                    }
                    first = false;
                    break;
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
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(membership::MEMBERSHIP_REFRESH) => {}
        }
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
        key_installed_tx.lock().expect("key-installed slot mutex poisoned").push(tx);
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
    key_installed_tx.lock().expect("key-installed slot mutex poisoned").clear();
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

#[cfg(test)]
mod foreign_foci_tests {
    use super::*;
    use crate::matrix::MembershipEntry;

    fn lk(url: &str) -> Option<LivekitFocusInfo> {
        Some(LivekitFocusInfo { alias: "!r:x".into(), service_url: url.into() })
    }
    fn human(key: &str, focus: Option<LivekitFocusInfo>, preferred: Option<LivekitFocusInfo>) -> MembershipEntry {
        MembershipEntry { state_key: key.into(), mxid: key.into(), is_bridge_owned: false, legacy_state_key: false, focus, preferred }
    }

    #[test]
    fn subscribes_on_resolved_and_preferred_sfus_but_never_ours() {
        let ours = LivekitFocusInfo { alias: "!r:x".into(), service_url: "https://livekit.nether.im".into() };
        let entries = [
            // Commet: election says ours, but it really publishes on matrix.org.
            human("commet", lk("https://livekit.nether.im"), lk("https://livekit-jwt.call.matrix.org")),
            // A multi_sfu user on a third homeserver: both the same.
            human("ec", lk("https://lk.example.org/"), lk("https://lk.example.org")),
            // Our ghost advertising a foreign SFU (migrated) must not count.
            MembershipEntry { is_bridge_owned: true, ..human("ghost", lk("https://livekit-jwt.call.matrix.org"), lk("https://livekit-jwt.call.matrix.org")) },
            human("nothing", None, None),
        ];
        let got = foreign_foci(&entries, &ours);
        let mut urls: Vec<_> = got.keys().cloned().collect();
        urls.sort();
        assert_eq!(urls, vec!["https://livekit-jwt.call.matrix.org", "https://lk.example.org"]);
    }
}
