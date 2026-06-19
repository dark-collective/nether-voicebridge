use anyhow::{Context, Result};
use futures_util::StreamExt as _;
use matrix_sdk::{
    Client, Room, SessionMeta, SlidingSync,
    authentication::{SessionTokens, matrix::MatrixSession},
    deserialized_responses::SyncOrStrippedState,
    ruma::{
        MilliSecondsSinceUnixEpoch, OwnedUserId, RoomId, UserId,
        api::client::sync::sync_events::v5::request::RoomSubscription,
        events::{
            StateEventType, SyncStateEvent,
            call::member::{CallMemberEventContent, SyncCallMemberEvent},
            room::encryption::SyncRoomEncryptionEvent,
        },
        uint,
    },
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, watch};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use nvb_audio::routing::PlaybackRouter;
use nvb_config::MatrixConfig;

/// Live participants: (sender user_id, state_key) pairs.
///
/// We key on state_key (not device_id) because it is the unique handle for a
/// call-member state event in both the legacy (state_key = user_id) and modern
/// per-device (state_key = `_@user_DEVICE`) format. This lets us remove exactly
/// the right entry when an Empty event arrives for a state_key, without needing
/// to parse the state_key format.
type ParticipantSet = Arc<Mutex<HashSet<(OwnedUserId, String)>>>;

/// Grace period before a membership "leave" takes effect. Element Call memberships
/// spuriously flap active→inactive→active (~2-3s gap, ~every 90s) due to
/// expiry/heartbeat refresh (MSC4140 delayed events) — the member never really
/// leaves. We defer the release this long and cancel it if the member reappears,
/// so a flap no longer tears down the puppet, the capture anchor, or the
/// subscriber. Far below the ~90s reflap period and EC's ~4h hard expiry.
const LEAVE_GRACE: Duration = Duration::from_secs(12);

/// Pending debounced leaves: MXID → cancel handle for its in-flight grace timer.
/// A reappear cancels it; a fresh leave replaces it (one timer per MXID).
type PendingLeaves = Arc<Mutex<HashMap<OwnedUserId, CancellationToken>>>;

/// Default Matrix display name for the bridge bot (the visible roster participant
/// in an E2EE call). Overridable per deployment via `matrix.bot_display_name`.
const DEFAULT_DISPLAY_NAME: &str = "Nether Voice Bridge";
/// Project default avatar (committed, downscaled), embedded so there is no runtime
/// file dependency. Overridable per deployment via `matrix.bot_avatar_path`.
const DEFAULT_AVATAR_PNG: &[u8] = include_bytes!("../assets/default-avatar.png");

fn log_snapshot(set: &HashSet<(OwnedUserId, String)>) {
    let mut users: Vec<&str> = set
        .iter()
        .map(|(u, _)| u.as_str())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    users.sort_unstable();
    tracing::info!(participants = ?users, count = users.len(), "call participants");
}

/// Resolve a Matrix member's display name from room state; falls back to the MXID.
async fn resolve_member_name(room: &Room, user_id: &UserId) -> String {
    match room.get_member(user_id).await {
        Ok(Some(m)) => m
            .display_name()
            .map(str::to_owned)
            .unwrap_or_else(|| user_id.to_string()),
        _ => user_id.to_string(),
    }
}

/// Apply a single call-member event (from sync or initial state scan) to the
/// participant set, raise call-active transitions, and drive the per-participant
/// puppet **presence** off membership (constraint: one Discord bot per Matrix
/// call participant, present even when muted/not transmitting). Echo-excludes the
/// bridge bot itself.
#[allow(clippy::too_many_arguments)]
async fn apply_member_event(
    user_id: OwnedUserId,
    state_key: String,
    content: &CallMemberEventContent,
    ts: MilliSecondsSinceUnixEpoch,
    bot_user_id: &OwnedUserId,
    participants: &ParticipantSet,
    call_active_tx: &watch::Sender<bool>,
    members_gen_tx: &watch::Sender<u64>,
    pending_leaves: &PendingLeaves,
    shutdown: &CancellationToken,
    bridge: &Arc<str>,
    room: &Room,
    router: &Arc<dyn PlaybackRouter>,
) {
    if &user_id == bot_user_id {
        return; // echo-exclusion: bridge bot's own call membership never counted
    }
    // Echo-exclusion (constraint #5): a ghost is a Discord speaker WE publish into
    // this call (Phase 8). Its m.call.member must never be counted as a Matrix
    // participant — otherwise it would trip call-active and, worse, get a Discord
    // puppet (a self-duplicate of the Discord user). Ghosts are driven entirely by
    // the Discord side's voice-state handler, not by membership here.
    // Excludes our current ghosts AND stale legacy `@discord_*` ghosts left in old
    // room state (post-rename) — otherwise a leftover membership is miscounted as a
    // real participant and gets a phantom Discord puppet.
    if crate::rtc::ghost::is_bridge_excluded_identity(user_id.as_str()) {
        return;
    }

    let is_active = match content {
        CallMemberEventContent::Empty(_) => false,
        c => !c.active_memberships(Some(ts)).is_empty(),
    };

    // Gate assign/release on this user's *MXID* presence going 0↔1, not on the
    // per-state-key change: a user present on two devices/sessions has two
    // `(user_id, state_key)` entries, and dropping one must not release the puppet
    // while the other remains. (Periodic m.call.member refreshes are also no-ops.)
    // The raw `participants` set always reflects current truth (we add/remove
    // immediately). The *effects* of a leave (puppet release, `call_active=false`)
    // are what we debounce — see the grace timer below.
    let appeared;
    let gone;
    let became_nonempty;
    {
        let mut set = participants.lock().await;
        let prev_users = set.iter().map(|(u, _)| u).collect::<HashSet<_>>().len();
        let entry = (user_id.clone(), state_key.clone());
        let this_user_before = set.iter().filter(|(u, _)| u == &user_id).count();
        if is_active {
            set.insert(entry);
        } else {
            set.remove(&entry);
        }
        let this_user_after = set.iter().filter(|(u, _)| u == &user_id).count();
        appeared = this_user_before == 0 && this_user_after > 0;
        gone = this_user_before > 0 && this_user_after == 0;
        let new_users = set.iter().map(|(u, _)| u).collect::<HashSet<_>>().len();
        became_nonempty = prev_users == 0 && new_users > 0;
        log_snapshot(&set);
    }

    // Membership owns puppet presence: a member joining gets a (present, possibly
    // silent) named puppet keyed on their bare MXID; leaving releases it. Audio +
    // mute are driven separately by the LiveKit track in `rtc::session`, which
    // keys on the same bare MXID — so the track device need not match the
    // membership state-key device.
    if appeared {
        // Cancel any in-flight grace timer for this MXID: a reappear within the
        // grace window means a spurious flap, not a real leave/rejoin.
        let had_pending = {
            let mut pending = pending_leaves.lock().await;
            pending.remove(&user_id).map(|t| t.cancel()).is_some()
        };
        if had_pending {
            // The puppet/subscriber were never torn down (we deferred the leave),
            // so do NOT re-assign, re-key, or re-raise call-active.
            tracing::info!(mxid = %user_id, "membership re-appeared within grace — cancelled pending release");
            return;
        }
        // Genuinely new (or rejoining after a real, already-applied leave).
        if became_nonempty {
            tracing::info!("call active");
            let _ = call_active_tx.send(true);
        }
        // A new human joined the call → each live ghost must (re-)deliver its media
        // key so this user can decrypt the ghost's E2EE frames. Bump the
        // membership-generation watch the ghost key-distribution loops wait on.
        members_gen_tx.send_modify(|g| *g = g.wrapping_add(1));
        let name = resolve_member_name(room, &user_id).await;
        tracing::info!(mxid = %user_id, display_name = %name, "member joined — assigning puppet");
        router.assign(user_id.as_str(), &name);
    } else if gone {
        // Debounce: defer the release; a reappear within LEAVE_GRACE cancels it.
        // Element Call memberships flap spuriously (expiry/heartbeat) — the member
        // is usually still present, so an immediate release would needlessly tear
        // down the puppet, the capture anchor, and the subscriber.
        tracing::info!(mxid = %user_id, "membership gone — starting leave grace timer");
        let token = CancellationToken::new();
        {
            let mut pending = pending_leaves.lock().await;
            if let Some(old) = pending.insert(user_id.clone(), token.clone()) {
                old.cancel(); // replace any earlier timer for this MXID
            }
        }
        let router = router.clone();
        let participants = participants.clone();
        let pending_leaves = pending_leaves.clone();
        let call_active_tx = call_active_tx.clone();
        let shutdown = shutdown.clone();
        let mxid = user_id.clone();
        let span = tracing::info_span!(parent: None, "bridge", name = %bridge);
        tokio::spawn(
            async move {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => return, // teardown owns final release
                    _ = token.cancelled() => return,    // reappeared within grace
                    _ = tokio::time::sleep(LEAVE_GRACE) => {}
                }
                // Authoritative re-check under the lock: only release if still absent.
                let (still_absent, now_empty) = {
                    let set = participants.lock().await;
                    (
                        set.iter().filter(|(u, _)| u == &mxid).count() == 0,
                        set.is_empty(),
                    )
                };
                pending_leaves.lock().await.remove(&mxid);
                if still_absent {
                    tracing::info!(mxid = %mxid, "leave grace elapsed — releasing puppet");
                    router.release(mxid.as_str());
                    if now_empty {
                        tracing::info!("call ended (after grace)");
                        let _ = call_active_tx.send(false);
                    }
                }
            }
            .instrument(span),
        );
    }
}

/// Retract any stale `m.call.member` the bridge bot itself left in the room.
///
/// The bridge bot is a *hidden subscriber* (Phase 8): it joins LiveKit to receive
/// audio but never posts its own call membership, so it shouldn't appear in
/// Element Call's roster. But a process killed mid-call (no clean leave) leaves
/// the bot's membership live in room state until its ~4h expiry. This clears any
/// such leftover at startup, for every `device_id` it may have used, so the
/// subscriber stays invisible. Ghosts are unaffected (different senders).
pub(crate) async fn retract_self_call_membership(
    client: &Client,
    room_id: &RoomId,
    bot_user_id: &UserId,
) {
    let Some(room) = client.get_room(room_id) else { return };
    let raws = match room.get_state_events_static::<CallMemberEventContent>().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "could not read call memberships for self-cleanup");
            return;
        }
    };
    for raw in raws {
        let Ok(ev) = raw.deserialize() else { continue };
        let orig = match ev {
            SyncOrStrippedState::Sync(SyncStateEvent::Original(o)) => o,
            _ => continue,
        };
        if orig.sender.as_str() != bot_user_id.as_str() {
            continue;
        }
        if matches!(orig.content, CallMemberEventContent::Empty(_)) {
            continue; // already retracted
        }
        match room
            .send_state_event_for_key(&orig.state_key, CallMemberEventContent::new_empty(None))
            .await
        {
            Ok(_) => tracing::info!(
                state_key = %orig.state_key.as_ref(),
                "retracted stale bridge-bot call membership (subscriber stays hidden)"
            ),
            Err(e) => tracing::warn!(error = %e, "failed to retract stale self membership"),
        }
    }
}

/// Build and authenticate a Matrix client from config. Joins the target room,
/// runs one sync round to populate the local state store, and returns the
/// ready client together with the `SlidingSync` instance to reuse for the
/// perpetual sync loop in `run()`.
///
/// Uses Simplified Sliding Sync (MSC3575 / `v5`). On Synapse, the appservice
/// `sender_localpart` user is blocked from ALL sync endpoints — the
/// `bot_user_id` must therefore be a namespace user that is NOT the
/// sender_localpart (Synapse's check is an exact match on `service.sender`).
#[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %bridge))]
pub(crate) async fn build_client(cfg: &MatrixConfig, bridge: &str) -> Result<(Client, SlidingSync)> {
    // Always open the persistent SQLite store at the (auto-derived, validated
    // unique) `store_path`. It must be ready BEFORE the first sync — but whether
    // the room is encrypted can only be read AFTER the sync (see
    // `detect_room_encryption`), so we can't gate the store on E2EE. Persisting it
    // unconditionally is harmless for an unencrypted room (just state + a device
    // identity) and required for an encrypted one (Olm keys/OTKs must survive
    // restarts so the SDK can decrypt the media keys EC/Commet send).
    let builder = Client::builder()
        .homeserver_url(&cfg.homeserver_url)
        .sqlite_store(&cfg.store_path, None);
    let client = builder.build().await?;

    let bot_user_id: OwnedUserId = cfg.bot_user_id.parse()?;

    // Self-provision a device + token via appservice login — no password needed
    // (the homeserver may use SSO), and it yields a clean, un-pinned device
    // identity. `bot_user_id` must be an appservice user. The appservice-login
    // session is GUEST-classified by some homeservers (Tuwunel), which matters
    // for how we join below.
    let access_token = appservice_login(cfg, bot_user_id.localpart()).await?;

    let session = MatrixSession {
        meta: SessionMeta {
            user_id: bot_user_id.clone(),
            device_id: cfg.device_id.as_str().into(),
        },
        tokens: SessionTokens {
            access_token,
            refresh_token: None,
        },
    };
    client.restore_session(session).await?;
    tracing::info!(user_id = %bot_user_id, "Matrix session restored");

    let room_id = RoomId::parse(&cfg.room_id)?;
    // Join the room via an appservice-ASSERTED `POST /join` (the same path ghosts
    // use). The appservice-login session is guest-classified by some homeservers
    // (Tuwunel), so a regular client `/join` hits the `m.room.guest_access` check
    // and 403s on rooms without `guest_access=can_join` — even when the bot is
    // invited. The appservice assertion yields a FULL, non-guest member, so no room
    // needs `guest_access`. The SDK client learns the membership from `sync_once`.
    // Restricted/non-public rooms still need an invite (or allow-space membership)
    // — the assertion respects join rules, it only bypasses the guest check.
    // Bound it: a restricted-room 403 (or a wedged homeserver) otherwise hangs
    // startup silently for minutes.
    match tokio::time::timeout(
        Duration::from_secs(30),
        appservice_join_room(
            &cfg.homeserver_url,
            &cfg.appservice_token,
            &cfg.room_id,
            bot_user_id.as_str(),
        ),
    )
    .await
    {
        Ok(Ok(())) => {
            tracing::info!(room_id = %room_id, "joined Matrix room (appservice-asserted full member)")
        }
        Ok(Err(e)) => {
            let msg = e.to_string();
            if msg.contains("403") || msg.contains("M_FORBIDDEN") {
                tracing::error!(
                    room_id = %room_id, error = %e,
                    "appservice-asserted join forbidden (403) — for a restricted/non-public room the bot still needs an invite (or allow-space membership)"
                );
            } else {
                tracing::error!(room_id = %room_id, error = %e, "appservice-asserted join failed");
            }
            return Err(anyhow::anyhow!("appservice join failed: {e}"));
        }
        Err(_) => {
            tracing::error!(room_id = %room_id, "appservice join timed out after 30 s — homeserver unreachable or wedged");
            return Err(anyhow::anyhow!("appservice join timed out after 30 s"));
        }
    }

    // Build a Simplified Sliding Sync (MSC3575 / v5) instance subscribed to
    // this bridge's room. We request the state events we actually need:
    // encryption state, call memberships, and room members (for display names).
    // timeline_limit=0: no timeline events needed — we only care about state.
    // with_all_extensions: enables to-device + e2ee extensions so Olm key
    // delivery works through the same sync loop.
    let room_id_typed = RoomId::parse(&cfg.room_id)?;
    let sliding_sync = client
        .sliding_sync("nvb-bridge")?
        .with_all_extensions()
        .build()
        .await
        .context(
            "homeserver does not support Simplified Sliding Sync (MSC3575 / org.matrix.simplified_msc3575) \
             — required because Synapse blocks both traditional /v3/sync and MSC3575 for the appservice \
             sender_localpart user; use a namespace user as bot_user_id (not the sender_localpart itself)"
        )?;
    // RoomSubscription is #[non_exhaustive] — construct via Default then set fields.
    let mut room_sub = RoomSubscription::default();
    room_sub.required_state = vec![
        (StateEventType::RoomEncryption, "".to_string()),
        // Wildcard state key to capture all per-device call memberships.
        (StateEventType::CallMember, "*".to_string()),
        (StateEventType::RoomMember, "*".to_string()),
    ];
    room_sub.timeline_limit = uint!(0);
    sliding_sync.subscribe_to_rooms(&[&room_id_typed], Some(room_sub), false);

    // One sync round to populate the local state store — and, with e2e on,
    // upload this device's Olm keys + one-time keys so peers can establish
    // Olm sessions. Bounded for the same reason as the join.
    match tokio::time::timeout(Duration::from_secs(60), sliding_sync.sync_once()).await {
        Ok(Ok(_)) => tracing::info!("initial Matrix sync complete"),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "initial Matrix sync failed");
            return Err(anyhow::anyhow!("initial sync_once failed: {e}"));
        }
        Err(_) => {
            tracing::error!("initial Matrix sync timed out after 60 s");
            return Err(anyhow::anyhow!("initial sync_once timed out after 60 s"));
        }
    }

    Ok((client, sliding_sync))
}

/// Whether the room is end-to-end encrypted, read from local state after the
/// initial sync. Drives the per-bridge `media_e2ee` auto-detection (#14). The
/// `m.room.encryption` state event is a one-way latch (a room never un-encrypts),
/// so this decision is stable for the life of the bridge.
///
/// Reads the raw `m.room.encryption` state event (mirrors the `CallMemberEvent`
/// pattern used elsewhere) rather than a version-specific helper: a present,
/// deserializable original event ⇒ encrypted. A missing room or no event ⇒ false.
pub(crate) fn detect_room_encryption(client: &Client, room_id: &RoomId) -> bool {
    let Some(room) = client.get_room(room_id) else {
        tracing::warn!(%room_id, "encryption detect: room not in local state — assuming unencrypted");
        return false;
    };
    let state = room.encryption_state();
    if state.is_unknown() {
        tracing::warn!(%room_id, "encryption state unknown after sync — assuming unencrypted");
    }
    state.is_encrypted()
}

/// Set the bridge bot's Matrix display name and avatar (best-effort — never blocks
/// startup). Display name = `bot_display_name` or [`DEFAULT_DISPLAY_NAME`]. Avatar
/// is uploaded only when the account has none yet (it then persists as an `mxc://`,
/// so it isn't re-uploaded each restart): from `bot_avatar_path` if set, else the
/// embedded [`DEFAULT_AVATAR_PNG`].
pub(crate) async fn apply_bot_profile(client: &Client, cfg: &MatrixConfig) {
    let name = cfg.bot_display_name.as_deref().unwrap_or(DEFAULT_DISPLAY_NAME);
    if let Err(e) = client.account().set_display_name(Some(name)).await {
        tracing::warn!(error = %e, "failed to set bridge-bot display name");
    } else {
        tracing::info!(display_name = name, "bridge-bot display name set");
    }

    match client.account().get_avatar_url().await {
        Ok(Some(_)) => return, // already has one — don't re-upload on every restart
        Ok(None) => {}
        Err(e) => {
            tracing::warn!(error = %e, "could not read bridge-bot avatar; skipping avatar set");
            return;
        }
    }

    let (mime, bytes) = match cfg.bot_avatar_path.as_deref() {
        Some(path) => match std::fs::read(path) {
            Ok(bytes) => (guess_image_mime(path), bytes),
            Err(e) => {
                tracing::warn!(path, error = %e, "could not read configured bot_avatar_path; using default avatar");
                (mime::IMAGE_PNG, DEFAULT_AVATAR_PNG.to_vec())
            }
        },
        None => (mime::IMAGE_PNG, DEFAULT_AVATAR_PNG.to_vec()),
    };
    match client.account().upload_avatar(&mime, bytes).await {
        Ok(mxc) => tracing::info!(mxc = %mxc, "bridge-bot avatar uploaded"),
        Err(e) => tracing::warn!(error = %e, "failed to upload bridge-bot avatar"),
    }
}

/// Best-effort image MIME from a file extension (defaults to PNG).
fn guess_image_mime(path: &str) -> mime::Mime {
    match path.rsplit('.').next().map(str::to_ascii_lowercase).as_deref() {
        Some("jpg") | Some("jpeg") => mime::IMAGE_JPEG,
        Some("gif") => mime::IMAGE_GIF,
        _ => mime::IMAGE_PNG,
    }
}

/// Mint a real device + access token for an appservice-namespace user via
/// `m.login.application_service`, authenticated with the appservice token (no
/// password — the homeserver may use SSO). A stable `device_id` is asserted so
/// the persistent crypto store stays valid across restarts. Mirrors the
/// `as_token` HTTP pattern the ghosts use in `rtc::ghost`.
async fn appservice_login(cfg: &MatrixConfig, localpart: &str) -> Result<String> {
    appservice_login_as(
        &cfg.homeserver_url,
        &cfg.appservice_token,
        localpart,
        &cfg.device_id,
    )
    .await
}

/// Parameterized appservice login: register (idempotent) + mint a device/token for
/// `localpart`, asserting `device_id`. The bot path calls this via
/// [`appservice_login`]; ghosts call it directly to become crypto-capable clients
/// (each with its own stable device id), reusing the exact, proven flow rather
/// than reimplementing it. Authenticated with the appservice `as_token`.
pub(crate) async fn appservice_login_as(
    homeserver_url: &str,
    appservice_token: &str,
    localpart: &str,
    device_id: &str,
) -> Result<String> {
    let http = reqwest::Client::builder().use_rustls_tls().build()?;
    let base = homeserver_url.trim_end_matches('/');

    // Idempotent register — tolerate an already-registered user (M_USER_IN_USE).
    let reg = http
        .post(format!("{base}/_matrix/client/v3/register"))
        .bearer_auth(appservice_token)
        .json(&serde_json::json!({
            "type": "m.login.application_service",
            "username": localpart,
        }))
        .send()
        .await
        .context("appservice register request")?;
    if !reg.status().is_success() {
        let status = reg.status();
        let body = reg.text().await.unwrap_or_default();
        if !body.contains("M_USER_IN_USE") {
            tracing::warn!(%status, "appservice register non-success (continuing to login)");
        }
    }

    // Mint device + token, asserting our stable device_id (MSC4326).
    let resp = http
        .post(format!("{base}/_matrix/client/v3/login"))
        .bearer_auth(appservice_token)
        .json(&serde_json::json!({
            "type": "m.login.application_service",
            "identifier": { "type": "m.id.user", "user": localpart },
            "device_id": device_id,
        }))
        .send()
        .await
        .context("appservice login request")?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        tracing::error!(
            %status, body = %body,
            "appservice login failed — does this homeserver accept m.login.application_service for the appservice user?"
        );
        return Err(anyhow::anyhow!("appservice login failed: {status}: {body}"));
    }
    let json: serde_json::Value =
        serde_json::from_str(&body).context("parsing appservice login response")?;
    let access_token = json["access_token"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("appservice login response missing access_token"))?
        .to_owned();
    if let Some(returned) = json["device_id"].as_str() {
        if returned != device_id {
            tracing::warn!(
                asserted = %device_id, returned = %returned,
                "homeserver returned a different device_id than asserted — crypto store may not match"
            );
        }
    }
    tracing::info!(user = %localpart, device_id = %device_id, "appservice login succeeded (self-provisioned device)");
    Ok(access_token)
}

/// Join `room_id` as `user_id` via an appservice-asserted `POST /join`, using the
/// `as_token`. Unlike a client `/join` with the bot's own (guest-classified)
/// login token, an appservice assertion produces a FULL (non-guest) member — the
/// same path ghosts use (`rtc::ghost::join_room`) — so the room does not need
/// `m.room.guest_access=can_join`. `user_id` must be appservice-controlled (the
/// `sender_localpart` or a namespaced user); on the appservice-auth path the bot
/// always is. Join rules are still enforced (a restricted/non-public room needs an
/// invite or allow-space membership) — only the guest check is bypassed.
async fn appservice_join_room(
    homeserver_url: &str,
    appservice_token: &str,
    room_id: &str,
    user_id: &str,
) -> Result<()> {
    let http = reqwest::Client::builder().use_rustls_tls().build()?;
    // Reuse the ghost path's URL builder (percent-encodes the room_id path segment
    // and appends the `?user_id=` masquerade).
    let url = crate::rtc::ghost::cs_url(homeserver_url, &["join", room_id], Some(user_id))?;
    let resp = http
        .post(url)
        .bearer_auth(appservice_token)
        .json(&serde_json::json!({}))
        .send()
        .await
        .context("appservice join request")?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("{status}: {body}"));
    }
    Ok(())
}

/// Run the Matrix sync loop. Must be called with a client and `SlidingSync`
/// produced by `build_client`. Drives the `SyncCallMemberEvent` handler and
/// sends call-active signals through `call_active_tx` when the first human
/// joins or the last human leaves.
#[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %bridge))]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run(
    client: Client,
    sliding_sync: SlidingSync,
    cfg: &MatrixConfig,
    media_e2ee: bool,
    call_active_tx: watch::Sender<bool>,
    members_gen_tx: watch::Sender<u64>,
    router: Arc<dyn PlaybackRouter>,
    bridge: Arc<str>,
    shutdown: CancellationToken,
) -> Result<()> {
    let bot_user_id: OwnedUserId = cfg.bot_user_id.parse()?;
    let room_id = RoomId::parse(&cfg.room_id)?;
    let participants: ParticipantSet = Arc::new(Mutex::new(HashSet::new()));
    let pending_leaves: PendingLeaves = Arc::new(Mutex::new(HashMap::new()));

    // Register handler BEFORE sync so events during the initial round-trip are captured.
    // Scope it to THIS bridge's room: `add_event_handler` is global across every
    // joined room, so with multiple bridges sharing one Matrix account (the bot is
    // a member of all their rooms) an unfiltered handler would react to other
    // bridges' call membership and double-assign puppets. `add_room_event_handler`
    // makes the SDK dispatch only this room's events.
    {
        let participants = participants.clone();
        let pending_leaves = pending_leaves.clone();
        let bot_user_id = bot_user_id.clone();
        let call_active_tx = call_active_tx.clone();
        let members_gen_tx = members_gen_tx.clone();
        let shutdown = shutdown.clone();
        // matrix-sdk dispatches this handler from its own sync task, which does
        // not inherit the bridge span — re-establish it for per-bridge logs.
        let bridge = bridge.clone();
        let router = router.clone();
        client.add_room_event_handler(&room_id, move |ev: SyncCallMemberEvent, room: Room| {
            let participants = participants.clone();
            let pending_leaves = pending_leaves.clone();
            let bot_user_id = bot_user_id.clone();
            let call_active_tx = call_active_tx.clone();
            let members_gen_tx = members_gen_tx.clone();
            let shutdown = shutdown.clone();
            let bridge = bridge.clone();
            let router = router.clone();
            let span = tracing::info_span!(parent: None, "bridge", name = %bridge);
            async move {
                let Some(orig) = ev.as_original() else { return };
                let user_id = orig.sender.clone();
                let state_key = orig.state_key.as_ref().to_owned();
                let ts = orig.origin_server_ts;
                apply_member_event(
                    user_id,
                    state_key,
                    &orig.content,
                    ts,
                    &bot_user_id,
                    &participants,
                    &call_active_tx,
                    &members_gen_tx,
                    &pending_leaves,
                    &shutdown,
                    &bridge,
                    &room,
                    &router,
                )
                .await;
            }
            .instrument(span)
        });
    }

    // Seed participant set from state loaded by the initial sync_once in build_client.
    if let Some(room) = client.get_room(&room_id) {
        let raws = room
            .get_state_events_static::<CallMemberEventContent>()
            .await?;
        for raw in raws {
            let Ok(ev) = raw.deserialize() else { continue };
            let orig = match ev {
                SyncOrStrippedState::Sync(SyncStateEvent::Original(o)) => o,
                _ => continue,
            };
            let user_id = orig.sender.clone();
            let state_key = orig.state_key.as_ref().to_owned();
            let ts = orig.origin_server_ts;
            apply_member_event(
                user_id,
                state_key,
                &orig.content,
                ts,
                &bot_user_id,
                &participants,
                &call_active_tx,
                &members_gen_tx,
                &pending_leaves,
                &shutdown,
                &bridge,
                &room,
                &router,
            )
            .await;
        }
        tracing::info!("initial call snapshot loaded");
    } else {
        tracing::warn!(room_id = %room_id, "room not in local state after sync_once");
    }

    // Live encryption-latch watcher (#14). `m.room.encryption` is one-way: a room
    // can only go unencrypted→encrypted. If THIS bridge started unencrypted
    // (`media_e2ee == false`) and the room becomes encrypted at runtime, we must
    // not keep publishing plaintext frames into a now-E2EE call (peers would hear
    // static) — tear the bridge down cleanly and log loudly; a restart re-detects
    // and comes back E2EE-on. When we already started encrypted, there's nothing to
    // react to. (Auto-rebuild / hot-switch is future work.)
    if !media_e2ee {
        let shutdown = shutdown.clone();
        let bridge = bridge.clone();
        client.add_room_event_handler(&room_id, move |_ev: SyncRoomEncryptionEvent, room: Room| {
            let shutdown = shutdown.clone();
            let bridge = bridge.clone();
            let span = tracing::info_span!(parent: None, "bridge", name = %bridge);
            async move {
                tracing::error!(
                    room_id = %room.room_id(),
                    "room became ENCRYPTED while the bridge is running unencrypted — \
                     tearing down to avoid pumping plaintext audio into an E2EE call; \
                     restart the bridge to re-detect and enable media E2EE"
                );
                shutdown.cancel();
            }
            .instrument(span)
        });
    }

    // Run the Simplified Sliding Sync stream forever — drives the event
    // handlers registered above and processes to-device messages for E2EE.
    // The SlidingSync stream continues its internal loop after yielding an
    // error, so we don't restart it; we just back off before the next poll.
    let sync_stream = sliding_sync.sync();
    tokio::pin!(sync_stream);

    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                tracing::info!("Matrix sync stopping (shutdown)");
                return Ok(());
            }
            item = sync_stream.next() => match item {
                None => {
                    // Only happens if stop_sync() was called externally.
                    tracing::info!("Matrix sync stream ended");
                    return Ok(());
                }
                Some(Ok(_)) => {} // UpdateSummary — event handlers already fired
                Some(Err(e)) => {
                    let msg = e.to_string();
                    if msg.contains("401") || msg.contains("403") || msg.contains("M_UNKNOWN_TOKEN") {
                        return Err(anyhow::anyhow!("Matrix sync fatal auth error: {e}"));
                    }
                    tracing::warn!(error = %e, "Matrix sync error — retrying in 5 s");
                    tokio::select! {
                        _ = shutdown.cancelled() => return Ok(()),
                        _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                    }
                }
            }
        }
    }
}
