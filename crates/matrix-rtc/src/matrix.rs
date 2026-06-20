use anyhow::{Context, Result};
use matrix_sdk::{
    Client, Room, SessionMeta,
    authentication::{SessionTokens, matrix::MatrixSession},
    config::SyncSettings,
    deserialized_responses::SyncOrStrippedState,
    ruma::{
        MilliSecondsSinceUnixEpoch, OwnedUserId, RoomId, UserId,
        events::{
            SyncStateEvent,
            call::member::{CallMemberEventContent, SyncCallMemberEvent},
            room::encryption::SyncRoomEncryptionEvent,
        },
    },
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
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

/// Mint a unique device id from the configured base: `{configured}-{8 lowercase-alnum}`.
/// A minted id is always distinct from the bare `configured`, which is how the caller
/// distinguishes a properly-provisioned marker from a "plain poison" one.
fn mint_fresh_device_id(configured: &str) -> String {
    use rand::Rng as _;
    let mut rng = rand::thread_rng();
    let suffix: String = (0..8)
        .map(|_| {
            let n: u8 = rng.gen_range(0..36);
            if n < 10 { (b'0' + n) as char } else { (b'a' + n - 10) as char }
        })
        .collect();
    format!("{configured}-{suffix}")
}

/// Best-effort delete the bot's crypto store files so a newly-minted device id gets a clean
/// Olm account. matrix-sdk-crypto is single-account-per-store: opening it with a new device id
/// would otherwise restore the OLD account, defeating the re-provision. Only the crypto sqlite
/// is removed — state/event-cache/media stores are keyed by room/event/user (device-agnostic)
/// and the ghosts' independent crypto stores under `ghosts/` are untouched.
///
/// SAFETY: callers MUST invoke this only from `resolve_store_device_id`, which runs *before*
/// `build_client` opens the sqlite store (see `lib.rs::run`). Deleting an open DB would corrupt
/// it; do not move this past the store-open point.
fn reset_bot_crypto_store(dir: &std::path::Path) {
    tracing::warn!(
        path = %dir.display(),
        "bot crypto store reset for device re-provision (one-time; peers see a brand-new device, not a key change)"
    );
    for name in [
        "matrix-sdk-crypto.sqlite3",
        "matrix-sdk-crypto.sqlite3-shm",
        "matrix-sdk-crypto.sqlite3-wal",
    ] {
        let p = dir.join(name);
        if let Err(e) = std::fs::remove_file(&p) {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(error = %e, path = %p.display(), "failed to remove bot crypto store file during reset");
            }
        }
    }
}

/// Resolve the Matrix `device_id` to assert for the bot, **pairing the device identity
/// with the crypto store** so identity and Olm keys always travel together (fixes #30).
///
/// The bot's configured `device_id` is derived from the bridge *name*, not the store — so the
/// SAME plain id can exist in two different stores (e.g. a laptop and a VPS) with DIFFERENT
/// Olm signing keys. Peers that cached one reject the other as `SigningKeyChanged` and never
/// open a fresh Olm session, so they never deliver the call's media key to the subscriber →
/// the bot can't decrypt the humans' audio (Matrix→Discord goes silent). Making the id a
/// per-store-unique property avoids the collision entirely:
/// - **minted/custom marker present** (`<store_path>/device_id`, value `!= configured`) → reuse
///   it (warm restart; crypto stays valid).
/// - **plain-poison marker** (value `== configured`, persisted by the old adopt path) → re-mint
///   a unique id, reset the crypto store, rewrite the marker. One-time, idempotent self-heal:
///   next start the marker `!= configured` and is reused unchanged.
/// - **no marker but the store already held an SDK crypto db** (a pre-#30 store's first upgrade)
///   → mint a unique id and reset the crypto store (the existing crypto belongs to the old plain
///   identity), then write the marker.
/// - **genuinely fresh store** → mint a unique id and write the marker.
///
/// Accepted one-time cost: a deployment that deliberately set a *plain* `matrix.device_id` on a
/// single machine gets re-keyed once on upgrade (peers just see one new device); idempotent after.
///
/// Appservice-auth only — the caller must NOT use this on the regular-account (`access_token`)
/// path, whose device is pinned to the token.
pub(crate) fn resolve_store_device_id(store_path: &str, configured: &str) -> String {
    if store_path.is_empty() {
        return configured.to_owned();
    }
    let dir = std::path::Path::new(store_path);
    let marker = dir.join("device_id");

    // A previously-persisted id. Reuse it UNLESS it is the bare configured id — that "plain
    // poison" was written by the old adopt path and collides across stores, so heal it.
    if let Ok(s) = std::fs::read_to_string(&marker) {
        let s = s.trim();
        if !s.is_empty() && s != configured {
            tracing::info!(device_id = %s, "device identity: reusing persisted id from store");
            return s.to_owned();
        }
        if s == configured {
            let id = mint_fresh_device_id(configured);
            tracing::warn!(old = %s, device_id = %id, "device identity: migrating plain-poison marker to a unique id");
            reset_bot_crypto_store(dir);
            persist_device_marker(&marker, dir, &id);
            return id;
        }
    }

    // No marker yet.
    let crypto_pre_existing = dir.join("matrix-sdk-crypto.sqlite3").exists();
    let id = mint_fresh_device_id(configured);
    if crypto_pre_existing {
        // A pre-#30 store's first upgrade: its crypto belongs to the old plain identity, so the
        // freshly-minted id needs a clean crypto store (mint → reset → persist marker).
        tracing::warn!(device_id = %id, "device identity: minted unique id for pre-existing store (re-provisioning crypto)");
        reset_bot_crypto_store(dir);
    } else {
        tracing::info!(device_id = %id, "device identity: minted fresh unique id for new store");
    }
    persist_device_marker(&marker, dir, &id);
    id
}

/// Best-effort persist the resolved device id to the marker (shared by all resolve branches).
fn persist_device_marker(marker: &std::path::Path, dir: &std::path::Path, id: &str) {
    if let Err(e) = std::fs::create_dir_all(dir) {
        tracing::warn!(error = %e, path = %dir.display(), "could not create store dir for device_id marker");
    }
    if let Err(e) = std::fs::write(marker, id) {
        tracing::warn!(error = %e, path = %marker.display(), "failed to persist device_id marker (will re-resolve next start)");
    }
}

/// Resolve a per-store random salt that makes every appservice ghost's otherwise
/// deterministic device id (`D{key}` → `D{key}-{salt}`) unique to THIS crypto store.
///
/// Background: ghosts mint their device id from the Discord-side key alone, so `D{key}`
/// is byte-identical on every machine. Each store mints its own Olm signing keys, so a
/// second store presenting the same `D{key}` with different keys trips a peer's
/// `SigningKeyChanged` and breaks Olm — the #30 trap, here extended from the bot
/// (`resolve_store_device_id`) to the ghosts.
///
/// - empty `store_path` → return `""`; the caller falls back to bare `D{key}` (no store,
///   so no persistence and no E2EE — nothing to collide).
/// - marker `<store_path>/ghost_device_salt` present & non-empty → reuse (warm restart →
///   stable ghost ids → the persistent ghost crypto stores keep matching, no re-key).
/// - otherwise → mint a fresh salt and persist it.
///
/// Unlike `resolve_store_device_id` there is deliberately **no** "adopt for a pre-existing
/// store" branch: a warm store with no salt marker is a pre-fix store whose `D{key}` ids
/// are poisoned, so we *want* fresh ghost identities (the new salted ids open fresh ghost
/// crypto stores; see `build_ghost_crypto_client`).
pub(crate) fn resolve_ghost_device_salt(store_path: &str) -> String {
    if store_path.is_empty() {
        return String::new();
    }
    let dir = std::path::Path::new(store_path);
    let marker = dir.join("ghost_device_salt");

    if let Ok(s) = std::fs::read_to_string(&marker) {
        let s = s.trim();
        if !s.is_empty() {
            tracing::info!(salt = %s, "ghost device identity: reusing persisted ghost_device_salt from store");
            return s.to_owned();
        }
    }

    use rand::Rng as _;
    let mut rng = rand::thread_rng();
    let salt: String = (0..8)
        .map(|_| {
            let n: u8 = rng.gen_range(0..36);
            if n < 10 { (b'0' + n) as char } else { (b'a' + n - 10) as char }
        })
        .collect();
    tracing::info!(salt = %salt, "ghost device identity: minted fresh ghost_device_salt for store");

    if let Err(e) = std::fs::create_dir_all(dir) {
        tracing::warn!(error = %e, path = %dir.display(), "could not create store dir for ghost_device_salt marker");
    }
    if let Err(e) = std::fs::write(&marker, &salt) {
        tracing::warn!(error = %e, path = %marker.display(), "failed to persist ghost_device_salt marker (will re-resolve next start)");
    }
    salt
}

/// Build and authenticate a Matrix client from config. Joins the target room,
/// runs one sync round to populate the local state store, and returns the
/// ready client.
///
/// This is kept separate from `run()` so the crate's `run()` can clone the
/// client into the RTC task before starting the perpetual sync loop.
#[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %bridge))]
pub(crate) async fn build_client(cfg: &MatrixConfig, bridge: &str) -> Result<Client> {
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

    // Two auth modes. With a config `access_token`, use it (regular account).
    // Without one, self-provision a device + token via appservice login — no
    // password needed (the homeserver may use SSO), and it yields a clean,
    // un-pinned device identity. `bot_user_id` must then be an appservice user.
    // The appservice-login session is GUEST-classified by some homeservers
    // (Tuwunel), which matters for how we join below.
    let (access_token, used_appservice_auth) = match cfg.access_token.as_deref() {
        Some(tok) if !tok.is_empty() => (tok.to_owned(), false),
        _ => (appservice_login(cfg, bot_user_id.localpart()).await?, true),
    };

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
    // Join the room (idempotent if already a member). Bound it: a restricted-room
    // 403 (or a wedged homeserver) otherwise hangs startup silently for minutes.
    if used_appservice_auth {
        // The appservice-login session is guest-classified by some homeservers
        // (Tuwunel), so a client `/join` with it hits the `m.room.guest_access`
        // check and 403s ("Guests are not allowed to join this room") on any room
        // without `guest_access=can_join` — even when the bot is invited. Join via
        // an appservice-ASSERTED `POST /join` instead (the exact path ghosts use):
        // an appservice assertion yields a FULL, non-guest member, so no room needs
        // `guest_access`. The SDK client learns the membership from the `sync_once`
        // below. Restricted/non-public rooms still need an invite (or allow-space
        // membership) — the assertion respects join rules, it only bypasses the
        // guest check.
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
    } else {
        // Regular account: its login session is a full (non-guest) user, so the
        // SDK join is fine.
        match tokio::time::timeout(Duration::from_secs(30), client.join_room_by_id(&room_id)).await {
            Ok(Ok(_)) => tracing::info!(room_id = %room_id, "joined Matrix room"),
            Ok(Err(e)) => {
                let msg = e.to_string();
                if msg.contains("403") || msg.contains("M_FORBIDDEN") {
                    tracing::error!(
                        room_id = %room_id, error = %e,
                        "Matrix join forbidden (403) — is the bot invited/joined to the room (and its space, if restricted)?"
                    );
                } else {
                    tracing::error!(room_id = %room_id, error = %e, "Matrix join failed");
                }
                return Err(anyhow::anyhow!("join_room_by_id failed: {e}"));
            }
            Err(_) => {
                tracing::error!(room_id = %room_id, "Matrix join timed out after 30 s — homeserver unreachable or wedged");
                return Err(anyhow::anyhow!("join_room_by_id timed out after 30 s"));
            }
        }
    }

    // One sync round to populate the local state store — and, with e2e on, upload
    // this device's Olm keys + one-time keys so peers can establish Olm sessions.
    // Bounded for the same reason as the join.
    match tokio::time::timeout(Duration::from_secs(60), client.sync_once(SyncSettings::default())).await {
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

    Ok(client)
}

/// Whether the room is end-to-end encrypted. Drives the per-bridge `media_e2ee`
/// auto-detection (#14). The `m.room.encryption` state event is a one-way latch (a
/// room never un-encrypts), so this decision is stable for the life of the bridge.
///
/// Uses [`Room::latest_encryption_state`], which fetches `m.room.encryption` from
/// the server when the local cache is `Unknown`, then returns the state. This is
/// **authoritative regardless of store freshness** — critical on a fresh store
/// (first deploy), where the initial `sync_once` often hasn't loaded the encryption
/// state yet, so the cache-only `encryption_state()` returns `Unknown` and
/// mis-detects an encrypted room as plaintext. That false-negative is silent and
/// catastrophic: the bridge then publishes unencrypted frames into an E2EE call
/// (peers hear nothing) and can't decrypt incoming frames → no audio either way.
///
/// A missing `m.room.encryption` (HTTP 404) ⇒ not encrypted (`Ok(false)`). A real
/// transport error propagates rather than defaulting to a guess — guessing either
/// way breaks audio, so a determinable answer is required.
pub(crate) async fn detect_room_encryption(client: &Client, room_id: &RoomId) -> Result<bool> {
    let room = client
        .get_room(room_id)
        .with_context(|| format!("encryption detect: room {room_id} not in local state after sync"))?;
    let state = room
        .latest_encryption_state()
        .await
        .with_context(|| format!("fetching m.room.encryption for room {room_id}"))?;
    Ok(state.is_encrypted())
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

/// Run the Matrix sync loop. Must be called with a client produced by
/// `build_client`. Drives the `SyncCallMemberEvent` handler and sends
/// call-active signals through `call_active_tx` when the first human
/// joins or the last human leaves.
#[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %bridge))]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run(
    client: Client,
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

    // Run sync forever — drives the event handler registered above and feeds
    // to-device decryption in Stage 2 (when e2e-encryption is enabled).
    // Retry on transient network errors (connection resets, timeouts) with a
    // short back-off so the bridge survives long idles.
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                tracing::info!("Matrix sync stopping (shutdown)");
                return Ok(());
            }
            res = client.sync(SyncSettings::default()) => match res {
                Ok(()) => return Ok(()),
                Err(e) => {
                    let msg = e.to_string();
                    // 401/403 are auth failures — propagate immediately.
                    if msg.contains("401") || msg.contains("403") || msg.contains("M_UNKNOWN_TOKEN") {
                        return Err(anyhow::anyhow!("Matrix sync fatal auth error: {e}"));
                    }
                    tracing::warn!(error = %e, "Matrix sync error — retrying in 5 s");
                    // Make the back-off interruptible by shutdown too.
                    tokio::select! {
                        _ = shutdown.cancelled() => return Ok(()),
                        _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
                    }
                }
            }
        }
    }
}

/// How long a non-ghost `m.call.member` must be continuously absent from the
/// LiveKit roster before the reaper empties it. Long enough to never reap a user
/// still establishing their RTC connection; short enough to clear a failed-to-
/// connect phantom within ~a minute of a call becoming active. See
/// [`reap_stale_memberships`].
pub(crate) const REAP_GRACE: Duration = Duration::from_secs(45);

/// One `m.call.member` entry, reduced to just the fields the reaper decision
/// needs. Keeping the decision pure (this + a live-MXID set + a clock) makes it
/// unit-testable without a live SDK or LiveKit room.
struct MembershipEntry {
    /// The state event's state_key (its unique handle; what we'd empty).
    state_key: String,
    /// The membership owner's bare MXID (the event sender).
    mxid: String,
    /// A bridge-owned identity (our ghost, or the bridge bot itself). Never reaped
    /// — those are managed by their own lifecycles (constraint #5 / self-cleanup).
    is_bridge_owned: bool,
    /// The state_key starts with `@` (legacy format). Matrix auth only lets that
    /// user set it, so the bot cannot empty it — we skip (and never track) it.
    legacy_state_key: bool,
}

/// Pure reaper decision. Returns the state_keys to empty **now**, and mutates
/// `phantom_since` in place: a newly-absent membership starts its grace clock; a
/// present (or bridge-owned, or reaped) one is cleared; entries whose membership
/// has vanished from room state are pruned.
///
/// "Present" is matched on the **bare MXID**, not mxid+device: a genuinely
/// connected user is never reaped even if their membership's device disagrees
/// with their LiveKit-track device (the keying hazard fixed in `8bf4a90`). The
/// tradeoff — a duplicate stale membership for a present multi-device user is left
/// alone — is acceptable.
fn classify_stale_memberships(
    live_mxids: &HashSet<String>,
    entries: &[MembershipEntry],
    phantom_since: &mut HashMap<String, Instant>,
    now: Instant,
    grace: Duration,
) -> Vec<String> {
    let mut reap = Vec::new();
    let mut present_keys: HashSet<&str> = HashSet::new();
    for e in entries {
        present_keys.insert(e.state_key.as_str());
        if e.is_bridge_owned || live_mxids.contains(&e.mxid) {
            phantom_since.remove(&e.state_key); // present / not ours to reap
            continue;
        }
        if e.legacy_state_key {
            continue; // can't overwrite an @-state-key as the bot
        }
        let since = *phantom_since.entry(e.state_key.clone()).or_insert(now);
        if now.duration_since(since) >= grace {
            reap.push(e.state_key.clone());
            phantom_since.remove(&e.state_key);
        }
    }
    // Drop timers for memberships no longer present in room state.
    phantom_since.retain(|k, _| present_keys.contains(k.as_str()));
    reap
}

/// Reap stale `m.call.member` phantoms: real users whose membership lingers in
/// room state but who have **no live LiveKit participant** (failed-to-connect,
/// or a client that died without a clean leave — common when the homeserver
/// lacks MSC4140 delayed events, e.g. Tuwunel). Forgejo issue #25.
///
/// Empties each confirmed phantom as the bridge bot via
/// `send_state_event_for_key` — permitted because Element Call uses the per-device
/// `_@…` state-key (not `@`-prefixed, so not owner-restricted) and the room sets
/// `m.call.member` at PL 0. `live_mxids` is the set of bare MXIDs currently in the
/// LiveKit room (built by the caller from `remote_participants()`); `phantom_since`
/// carries each entry's grace clock across calls. Best-effort: read/write failures
/// are logged, never fatal.
pub(crate) async fn reap_stale_memberships(
    client: &Client,
    room_id: &RoomId,
    bot_user_id: &UserId,
    live_mxids: &HashSet<String>,
    phantom_since: &mut HashMap<String, Instant>,
) {
    let Some(room) = client.get_room(room_id) else { return };
    let raws = match room.get_state_events_static::<CallMemberEventContent>().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "reaper: could not read call memberships");
            return;
        }
    };

    // Keep the originals so we can write the retraction with the typed state_key.
    let mut originals = Vec::new();
    for raw in raws {
        let Ok(ev) = raw.deserialize() else { continue };
        if let SyncOrStrippedState::Sync(SyncStateEvent::Original(o)) = ev {
            if matches!(o.content, CallMemberEventContent::Empty(_)) {
                continue; // already retracted
            }
            originals.push(o);
        }
    }

    let entries: Vec<MembershipEntry> = originals
        .iter()
        .map(|o| {
            let mxid = o.sender.as_str().to_owned();
            let is_bridge_owned = o.sender.as_str() == bot_user_id.as_str()
                || crate::rtc::ghost::is_bridge_excluded_identity(o.sender.as_str());
            let state_key = o.state_key.as_ref().to_owned();
            let legacy_state_key = state_key.starts_with('@');
            MembershipEntry { state_key, mxid, is_bridge_owned, legacy_state_key }
        })
        .collect();

    let to_reap: HashSet<String> =
        classify_stale_memberships(live_mxids, &entries, phantom_since, Instant::now(), REAP_GRACE)
            .into_iter()
            .collect();
    if to_reap.is_empty() {
        return;
    }

    for o in &originals {
        if !to_reap.contains(o.state_key.as_ref()) {
            continue;
        }
        match room
            .send_state_event_for_key(&o.state_key, CallMemberEventContent::new_empty(None))
            .await
        {
            Ok(_) => tracing::info!(
                sender = %o.sender,
                state_key = %o.state_key.as_ref(),
                "reaped phantom m.call.member (no live LiveKit participant)"
            ),
            Err(e) => tracing::warn!(
                sender = %o.sender,
                state_key = %o.state_key.as_ref(),
                error = %e,
                "failed to reap phantom membership (room PL for m.call.member may be > 0)"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{resolve_ghost_device_salt, resolve_store_device_id};
    use std::fs;
    use std::path::PathBuf;

    /// A unique throwaway directory under the system temp dir, removed on drop.
    struct TmpDir(PathBuf);
    impl TmpDir {
        fn new() -> Self {
            use rand::Rng as _;
            let n: u64 = rand::thread_rng().gen();
            let dir = std::env::temp_dir().join(format!("nvb-devid-test-{n:016x}"));
            fs::create_dir_all(&dir).unwrap();
            TmpDir(dir)
        }
        fn path(&self) -> &str {
            self.0.to_str().unwrap()
        }
    }
    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn fresh_store_mints_unique_id() {
        let tmp = TmpDir::new();
        let id = resolve_store_device_id(tmp.path(), "dark-voice");
        assert!(id.starts_with("dark-voice-"), "expected a minted id, got {id}");
        assert!(id.len() > "dark-voice-".len(), "expected a random suffix, got {id}");
        // The minted id is persisted so the next restart reuses it.
        let marker = fs::read_to_string(tmp.0.join("device_id")).unwrap();
        assert_eq!(marker.trim(), id);
    }

    #[test]
    fn pre_existing_store_mints_and_resets_crypto() {
        let tmp = TmpDir::new();
        // A pre-#30 store's first upgrade: SDK crypto db present, no marker. The old adopt
        // behavior kept the plain `dark-voice` (collision-prone); now we mint a unique id and
        // reset the crypto store so the new device gets a clean Olm account.
        fs::write(tmp.0.join("matrix-sdk-crypto.sqlite3"), b"x").unwrap();
        fs::write(tmp.0.join("matrix-sdk-crypto.sqlite3-wal"), b"x").unwrap();
        let id = resolve_store_device_id(tmp.path(), "dark-voice");
        assert!(id.starts_with("dark-voice-"), "expected a minted id, got {id}");
        assert_ne!(id, "dark-voice");
        // Crypto files removed for re-provision; marker holds the new id.
        assert!(!tmp.0.join("matrix-sdk-crypto.sqlite3").exists());
        assert!(!tmp.0.join("matrix-sdk-crypto.sqlite3-wal").exists());
        assert_eq!(fs::read_to_string(tmp.0.join("device_id")).unwrap().trim(), id);
    }

    #[test]
    fn plain_marker_migrates_and_resets_then_is_idempotent() {
        let tmp = TmpDir::new();
        // The "plain poison" written by the old adopt path: marker == configured.
        fs::write(tmp.0.join("device_id"), "dark-voice").unwrap();
        fs::write(tmp.0.join("matrix-sdk-crypto.sqlite3"), b"x").unwrap();
        let id = resolve_store_device_id(tmp.path(), "dark-voice");
        assert!(id.starts_with("dark-voice-") && id != "dark-voice", "expected migration, got {id}");
        assert!(!tmp.0.join("matrix-sdk-crypto.sqlite3").exists(), "crypto store must be reset on migrate");
        assert_eq!(fs::read_to_string(tmp.0.join("device_id")).unwrap().trim(), id);

        // Idempotent: a second start (marker now != configured) reuses the healed id and does
        // NOT touch a freshly-recreated crypto store.
        fs::write(tmp.0.join("matrix-sdk-crypto.sqlite3"), b"y").unwrap();
        let id2 = resolve_store_device_id(tmp.path(), "dark-voice");
        assert_eq!(id2, id, "healed id must be stable across restarts");
        assert!(tmp.0.join("matrix-sdk-crypto.sqlite3").exists(), "must not reset crypto on a healed store");
    }

    #[test]
    fn existing_minted_marker_is_reused_verbatim() {
        let tmp = TmpDir::new();
        // A properly-minted/custom marker (!= configured) is reused, crypto untouched.
        fs::write(tmp.0.join("device_id"), "persisted-xyz").unwrap();
        fs::write(tmp.0.join("matrix-sdk-crypto.sqlite3"), b"x").unwrap();
        let id = resolve_store_device_id(tmp.path(), "dark-voice");
        assert_eq!(id, "persisted-xyz");
        assert!(tmp.0.join("matrix-sdk-crypto.sqlite3").exists(), "minted/custom marker must not trigger reset");
    }

    #[test]
    fn empty_store_path_returns_configured() {
        // Defensive: no store path → just the configured id, no filesystem writes.
        assert_eq!(resolve_store_device_id("", "dark-voice"), "dark-voice");
    }

    #[test]
    fn fresh_store_mints_ghost_salt() {
        let tmp = TmpDir::new();
        let salt = resolve_ghost_device_salt(tmp.path());
        assert_eq!(salt.len(), 8, "expected an 8-char salt, got {salt:?}");
        assert!(
            salt.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()),
            "expected lowercase-alnum salt, got {salt:?}"
        );
        let marker = fs::read_to_string(tmp.0.join("ghost_device_salt")).unwrap();
        assert_eq!(marker.trim(), salt);
    }

    #[test]
    fn ghost_salt_reused_across_calls() {
        let tmp = TmpDir::new();
        let a = resolve_ghost_device_salt(tmp.path());
        let b = resolve_ghost_device_salt(tmp.path());
        assert_eq!(a, b, "warm restart must reuse the persisted salt");
    }

    #[test]
    fn ghost_salt_independent_of_crypto_db() {
        let tmp = TmpDir::new();
        // A pre-fix warm store (crypto db present, no salt marker) must still MINT a fresh
        // salt — the pre-fix ghost ids are poisoned and we want new identities. (Contrast
        // `pre_existing_store_adopts_configured_id`, which is the opposite for the bot.)
        fs::write(tmp.0.join("matrix-sdk-crypto.sqlite3"), b"").unwrap();
        let salt = resolve_ghost_device_salt(tmp.path());
        assert_eq!(salt.len(), 8, "must mint despite an existing crypto db, got {salt:?}");
        let marker = fs::read_to_string(tmp.0.join("ghost_device_salt")).unwrap();
        assert_eq!(marker.trim(), salt);
    }

    #[test]
    fn empty_store_path_returns_empty_ghost_salt() {
        assert_eq!(resolve_ghost_device_salt(""), "");
    }

    // ── Stale-membership reaper (classify_stale_memberships) ───────────────────
    use super::{classify_stale_memberships, MembershipEntry};
    use std::collections::{HashMap, HashSet};
    use std::time::{Duration, Instant};

    const GRACE: Duration = Duration::from_secs(45);

    fn entry(state_key: &str, mxid: &str, owned: bool, legacy: bool) -> MembershipEntry {
        MembershipEntry {
            state_key: state_key.to_owned(),
            mxid: mxid.to_owned(),
            is_bridge_owned: owned,
            legacy_state_key: legacy,
        }
    }

    fn live(mxids: &[&str]) -> HashSet<String> {
        mxids.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn present_user_is_never_reaped_and_clears_timer() {
        let t0 = Instant::now();
        let mut since = HashMap::new();
        let entries = [entry("_@alice:x_DEV_m.call", "@alice:x", false, false)];
        // Even with a pre-existing (stale) timer, a user back in LiveKit is kept.
        since.insert("_@alice:x_DEV_m.call".to_owned(), t0);
        let reap = classify_stale_memberships(&live(&["@alice:x"]), &entries, &mut since, t0 + GRACE * 2, GRACE);
        assert!(reap.is_empty());
        assert!(since.is_empty(), "present user's timer is cleared");
    }

    #[test]
    fn absent_user_reaped_only_after_grace() {
        let t0 = Instant::now();
        let mut since = HashMap::new();
        let entries = [entry("_@ghosty:x_DEV_m.call", "@ghosty:x", false, false)];
        let empty = live(&[]);

        // First sight: starts the clock, nothing reaped yet.
        let reap = classify_stale_memberships(&empty, &entries, &mut since, t0, GRACE);
        assert!(reap.is_empty());
        // Still within grace.
        let reap = classify_stale_memberships(&empty, &entries, &mut since, t0 + Duration::from_secs(30), GRACE);
        assert!(reap.is_empty());
        // Past grace → reaped, timer dropped.
        let reap = classify_stale_memberships(&empty, &entries, &mut since, t0 + GRACE, GRACE);
        assert_eq!(reap, vec!["_@ghosty:x_DEV_m.call".to_string()]);
        assert!(since.is_empty());
    }

    #[test]
    fn bridge_owned_and_legacy_are_never_reaped() {
        let t0 = Instant::now();
        let mut since = HashMap::new();
        let entries = [
            entry("_@nether-voicebridge_1:x_D1_m.call", "@nether-voicebridge_1:x", true, false), // ghost
            entry("@legacy:x", "@legacy:x", false, true),                                         // @-state-key
        ];
        let empty = live(&[]);
        // Even well past grace, neither is reaped.
        let reap = classify_stale_memberships(&empty, &entries, &mut since, t0 + GRACE * 3, GRACE);
        assert!(reap.is_empty());
        assert!(since.is_empty(), "neither owned nor legacy entries are tracked");
    }

    #[test]
    fn vanished_membership_is_pruned_from_timers() {
        let t0 = Instant::now();
        let mut since = HashMap::new();
        since.insert("_@gone:x_DEV_m.call".to_owned(), t0);
        // The membership is no longer in room state (no entries) → its timer is pruned.
        let reap = classify_stale_memberships(&live(&[]), &[], &mut since, t0 + Duration::from_secs(5), GRACE);
        assert!(reap.is_empty());
        assert!(since.is_empty(), "timer for a vanished membership is dropped");
    }
}
