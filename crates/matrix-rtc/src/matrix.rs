use anyhow::{Context, Result};
use matrix_sdk::{
    Client, Room, SessionMeta,
    authentication::{SessionTokens, matrix::MatrixSession},
    config::SyncSettings,
    deserialized_responses::SyncOrStrippedState,
    ruma::{
        MilliSecondsSinceUnixEpoch, OwnedRoomId, OwnedUserId, RoomId, UserId,
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
use nvb_config::links::{LinkBoard, LinkEvent};
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

// The leave-grace debounce (12 s default — far below EC's ~90 s reflap period
// and ~4 h hard expiry) was a hardcoded const here until #38; it now comes
// from the resolved `[timing]` config (`TimingConfig::leave_grace`) and is
// threaded into `apply_member_event` as the `leave_grace` parameter.

/// Pending debounced leaves: MXID → cancel handle for its in-flight grace timer.
/// A reappear cancels it; a fresh leave replaces it (one timer per MXID).
type PendingLeaves = Arc<Mutex<HashMap<OwnedUserId, CancellationToken>>>;

/// Pending paired-arrival graces (ADR 0003, Consequences): MXID -> cancel handle
/// for its in-flight timer. The *outcome* is owned by whoever wins
/// [`LinkBoard::end_arrival_grace`]; the handle only stops an idle timer early.
type PendingArrivals = Arc<Mutex<HashMap<OwnedUserId, CancellationToken>>>;

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

/// Which user does a call-member event affect? An *active* membership belongs to
/// its sender. A *retraction* (Empty content / all memberships expired) may be
/// posted by someone other than the member it clears — the #25 reaper empties
/// phantoms as the bridge bot (#54) — so resolve the owner from the participant
/// set by state_key (its unique handle in both the legacy and per-device
/// formats). An unknown state_key falls back to the sender: the normal
/// clean-leave case, or a no-op retraction for an entry we never held.
fn resolve_affected_user(
    set: &HashSet<(OwnedUserId, String)>,
    sender: OwnedUserId,
    state_key: &str,
    is_active: bool,
) -> OwnedUserId {
    if is_active {
        return sender;
    }
    set.iter()
        .find(|(_, key)| key.as_str() == state_key)
        .map(|(owner, _)| owner.clone())
        .unwrap_or(sender)
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
    ghost_prefix: &str,
    participants: &ParticipantSet,
    call_active_tx: &watch::Sender<bool>,
    members_gen_tx: &watch::Sender<u64>,
    pending_leaves: &PendingLeaves,
    leave_grace: Duration,
    pending_arrivals: &PendingArrivals,
    arrival_grace: Duration,
    shutdown: &CancellationToken,
    bridge: &Arc<str>,
    room: &Room,
    router: &Arc<dyn PlaybackRouter>,
    links: &Arc<LinkBoard>,
) {
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
    let user_id = {
        let mut set = participants.lock().await;
        // A retraction may be posted by someone other than the member it clears —
        // the #25 reaper empties phantoms as the bridge bot (#54) — so attribute
        // it to the membership's owner, looked up by state_key.
        let user_id = resolve_affected_user(&set, user_id, &state_key, is_active);
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
        if crate::rtc::ghost::is_bridge_excluded_identity(user_id.as_str(), ghost_prefix) {
            return;
        }
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
        user_id
    };

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
        // Linked identities (ADR 0001): record the arrival; if it flips a paired
        // Discord member's status the Discord side retires that ghost; and a
        // member who is bridge-owned (paired half present / service account /
        // config-excluded) gets no puppet. ADR 0003: "call active" counts
        // LISTENERS — the Matrix half of a present pairing still listens.
        let listeners_before = links.matrix_listeners();
        links.dispatch_all(links.set_matrix_present(user_id.as_str(), true));
        let excluded = links.matrix_excluded(user_id.as_str());
        let listeners_after = links.matrix_listeners();
        // Genuinely new (or rejoining after a real, already-applied leave).
        let _ = became_nonempty; // superseded by the listener count below
        if listeners_before == 0 && listeners_after > 0 {
            tracing::info!("call active");
            let _ = call_active_tx.send(true);
        }
        // A new human joined the call → each live ghost must (re-)deliver its media
        // key so this user can decrypt the ghost's E2EE frames. Bump the
        // membership-generation watch the ghost key-distribution loops wait on.
        members_gen_tx.send_modify(|g| *g = g.wrapping_add(1));
        if excluded {
            if links.matrix_listener_only(user_id.as_str()) {
                // ADR 0003: no puppet, but they listen — keep Discord capture for them.
                tracing::info!(mxid = %user_id, "member joined — paired Discord account present — no puppet, listening");
                router.listen(user_id.as_str());
            } else {
                tracing::info!(mxid = %user_id, "member joined — bridge-owned (service account or excluded) — no puppet");
            }
            return;
        }
        // Paired arrival grace (ADR 0003, Consequences): a paired member whose Discord
        // half is not in the channel yet usually has it arriving in seconds (a
        // companion client / second device). Assigning a puppet now means a bot
        // joining Discord under their name, then renaming to standby and lingering
        // when the half arrives — visible churn. Instead they only LISTEN (the
        // anchor joins in standby, never under their name) until the half arrives
        // (MatrixExcluded ends the grace), the timer expires, or they speak
        // (`rtc::session` playback consumer) — the latter two assign the puppet.
        if !arrival_grace.is_zero() && links.paired_half_absent(user_id.as_str()) {
            tracing::info!(
                mxid = %user_id,
                grace_ms = arrival_grace.as_millis() as u64,
                "member joined — paired Discord account not in the channel yet — listening, arrival grace started"
            );
            links.begin_arrival_grace(user_id.as_str());
            router.listen(user_id.as_str());
            let token = CancellationToken::new();
            if let Some(old) = pending_arrivals.lock().await.insert(user_id.clone(), token.clone()) {
                old.cancel();
            }
            let router = router.clone();
            let pending_arrivals = pending_arrivals.clone();
            let shutdown = shutdown.clone();
            let links = links.clone();
            let room = room.clone();
            let mxid = user_id.clone();
            let span = tracing::info_span!(parent: None, "bridge", name = %bridge);
            tokio::spawn(
                async move {
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => return, // teardown owns final release
                        _ = token.cancelled() => return,    // half arrived / leave took effect
                        _ = tokio::time::sleep(arrival_grace) => {}
                    }
                    pending_arrivals.lock().await.remove(&mxid);
                    if !links.end_arrival_grace(mxid.as_str()) {
                        return; // already ended (first voiced frame, half arrived, leave)
                    }
                    // Authoritative re-check: still in the call (a debounced leave only
                    // clears board presence once effective) and still not bridge-owned.
                    if !links.matrix_present(mxid.as_str()) || links.matrix_excluded(mxid.as_str()) {
                        tracing::info!(mxid = %mxid, "arrival grace elapsed — member gone or paired half present — no puppet");
                        return;
                    }
                    let name = resolve_member_name(&room, &mxid).await;
                    tracing::info!(mxid = %mxid, display_name = %name, "arrival grace elapsed — paired Discord account never came — assigning puppet");
                    router.assign(mxid.as_str(), &name);
                    router.unlisten(mxid.as_str()); // after the assign: the anchor never lapses
                }
                .instrument(span),
            );
            return;
        }
        let name = resolve_member_name(room, &user_id).await;
        tracing::info!(mxid = %user_id, display_name = %name, "member joined — assigning puppet");
        router.assign(user_id.as_str(), &name);
    } else if gone {
        // Debounce: defer the release; a reappear within the leave grace cancels
        // it. Element Call memberships flap spuriously (expiry/heartbeat) — the
        // member is usually still present, so an immediate release would
        // needlessly tear down the puppet, the capture anchor, and the subscriber.
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
        let links = links.clone();
        let pending_arrivals = pending_arrivals.clone();
        let mxid = user_id.clone();
        let span = tracing::info_span!(parent: None, "bridge", name = %bridge);
        tokio::spawn(
            async move {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => return, // teardown owns final release
                    _ = token.cancelled() => return,    // reappeared within grace
                    _ = tokio::time::sleep(leave_grace) => {}
                }
                // Authoritative re-check under the lock: only release if still absent.
                let still_absent = {
                    let set = participants.lock().await;
                    set.iter().filter(|(u, _)| u == &mxid).count() == 0
                };
                pending_leaves.lock().await.remove(&mxid);
                if still_absent {
                    tracing::info!(mxid = %mxid, "leave grace elapsed — releasing puppet");
                    router.release(mxid.as_str());
                    router.unlisten(mxid.as_str());
                    // A pending arrival grace ends with the leave (the board clears its
                    // claim in `set_matrix_present(false)`; this just stops the timer).
                    if let Some(t) = pending_arrivals.lock().await.remove(&mxid) {
                        t.cancel();
                    }
                    // Linked identities: the leave is now effective — a paired Discord
                    // member still in the channel gets its ghost back (Discord side).
                    links.dispatch_all(links.set_matrix_present(mxid.as_str(), false));
                    if links.matrix_listeners() == 0 {
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
/// Applies to every bridge: all Matrix auth is now appservice login (the bridge
/// self-provisions an un-pinned device), so the device id is always store-paired here.
///
/// Every branch that abandons an old id records it in the store's retired-id history
/// (#53) so the prune pass can delete it server-side later; `reused` tells the caller
/// whether the id came from a persisted marker (only then can it have been deleted
/// server-side while we were away — the deletion self-heal is gated on it).
pub(crate) fn resolve_store_device_id(store_path: &str, configured: &str) -> ResolvedDeviceId {
    if store_path.is_empty() {
        return ResolvedDeviceId { id: configured.to_owned(), reused: false };
    }
    let dir = std::path::Path::new(store_path);
    let marker = dir.join("device_id");

    // A previously-persisted id. Reuse it UNLESS it is the bare configured id — that "plain
    // poison" was written by the old adopt path and collides across stores, so heal it.
    if let Ok(s) = std::fs::read_to_string(&marker) {
        let s = s.trim();
        if !s.is_empty() && s != configured {
            tracing::info!(device_id = %s, "device identity: reusing persisted id from store");
            return ResolvedDeviceId { id: s.to_owned(), reused: true };
        }
        if s == configured {
            let id = mint_fresh_device_id(configured);
            tracing::warn!(old = %s, device_id = %id, "device identity: migrating plain-poison marker to a unique id");
            // The plain id may still be live under ANOTHER pre-migration deployment; if so,
            // pruning it is self-healing there too (that deployment migrates off it on its
            // next start regardless, and its self-heal covers an early prune).
            crate::device_prune::append_retired(store_path, s);
            reset_bot_crypto_store(dir);
            persist_device_marker(&marker, dir, &id);
            return ResolvedDeviceId { id, reused: false };
        }
    }

    // No marker yet.
    let crypto_pre_existing = dir.join("matrix-sdk-crypto.sqlite3").exists();
    let id = mint_fresh_device_id(configured);
    if crypto_pre_existing {
        // A pre-#30 store's first upgrade: its crypto belongs to the old plain identity, so the
        // freshly-minted id needs a clean crypto store (mint → reset → persist marker). The old
        // identity was the bare configured id — retire it.
        tracing::warn!(device_id = %id, "device identity: minted unique id for pre-existing store (re-provisioning crypto)");
        crate::device_prune::append_retired(store_path, configured);
        reset_bot_crypto_store(dir);
    } else {
        tracing::info!(device_id = %id, "device identity: minted fresh unique id for new store");
    }
    persist_device_marker(&marker, dir, &id);
    ResolvedDeviceId { id, reused: false }
}

/// Result of [`resolve_store_device_id`]: the effective device id, and whether it was
/// reused from a persisted marker (vs freshly minted this start).
pub(crate) struct ResolvedDeviceId {
    pub(crate) id: String,
    pub(crate) reused: bool,
}

/// Deletion self-heal (#53): the store wants to reuse `dead_id` but its device keys are
/// gone server-side (deleted by an operator or another deployment's mistake). Resurrecting
/// it would wedge every client that saw the deletion — they drop the device and never
/// re-learn a re-upload with unchanged keys, so they silently withhold the call media key
/// (the 2026-07-03 outage). Instead: retire the dead id (queued for prune), reset the
/// crypto store, and mint a fresh device — one-time and peer-safe, exactly like a store wipe.
///
/// SAFETY: like [`resolve_store_device_id`], this must run before `build_client` opens the
/// sqlite store (see `reset_bot_crypto_store`).
pub(crate) fn remint_bot_device(store_path: &str, configured: &str, dead_id: &str) -> String {
    let dir = std::path::Path::new(store_path);
    let id = mint_fresh_device_id(configured);
    tracing::warn!(
        old = %dead_id,
        device_id = %id,
        "bot device was deleted server-side while offline — re-provisioning a fresh device (one-time re-key; peers see a new device)"
    );
    crate::device_prune::append_retired(store_path, dead_id);
    reset_bot_crypto_store(dir);
    persist_device_marker(&dir.join("device_id"), dir, &id);
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
///
/// On Synapse, the appservice `sender_localpart` user is blocked from ALL sync
/// endpoints (`get_app_service_by_user_id` exact-matches it). Use a namespace
/// user as `bot_user_id` (not the sender_localpart itself) so sync proceeds
/// normally — the `_bot`-suffixed auto-derivation in `resolve_bot_user_ids`
/// handles this automatically when `bot_user_id` is not set explicitly.
#[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %bridge))]
pub(crate) async fn build_client(cfg: &MatrixConfig, bridge: &str) -> Result<Client> {
    // Always open the persistent SQLite store at the (auto-derived, validated
    // unique) `store_path`. It must be ready BEFORE the first sync — but whether
    // the room is encrypted can only be read AFTER the sync (see
    // `detect_room_encryption`), so we can't gate the store on E2EE. Persisting it
    // unconditionally is harmless for an unencrypted room (just state + a device
    // identity) and required for an encrypted one (Olm keys/OTKs must survive
    // restarts so the SDK can decrypt the media keys EC/Commet send).
    // History sharing on invite (MSC4268, default-on in matrix-sdk 0.18) is
    // turned off: it runs before every `invite_user_by_id` once the bot has a
    // cross-signing identity (0.4.13), fails for ghosts (no identity of their
    // own), and its error aborts the invite itself — so a new ghost could never
    // enter an invite-only/restricted room. Ghosts don't need message history.
    let builder = Client::builder()
        .homeserver_url(&cfg.homeserver_url)
        .sqlite_store(&cfg.store_path, None)
        .with_enable_share_history_on_invite(false);
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
            None,
        ),
    )
    .await
    {
        Ok(Ok(())) => {
            tracing::info!(room_id = %room_id, "joined Matrix room (appservice-asserted full member)")
        }
        Ok(Err(e)) => {
            // Branch on the response's status, not on the error text — a room id
            // or URL containing "403" must not fake a permission problem (#130).
            let http = e.downcast_ref::<AppserviceHttpError>();
            let forbidden = http.is_some_and(|h| h.status == reqwest::StatusCode::FORBIDDEN);
            // A banned room (homeserver room ban, or the bot banned from it) won't
            // come back on a retry: hand the supervisor a typed error so it parks
            // the bridge instead of restarting it forever.
            if let Some(reason) = http.and_then(|h| room_ban_reason(h.status, &h.body)) {
                tracing::error!(
                    room_id = %room_id, reason,
                    "Matrix room is banned — the bridge cannot join it; unlink the bridge (or lift the ban, then reload)"
                );
                return Err(RoomUnavailable { room_id: room_id.to_string(), reason }.into());
            }
            if forbidden {
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

    // #150: the device keys are uploaded now — sign this device with the bot
    // account's cross-signing identity (creating it on first run).
    crate::cross_signing::ensure_device_cross_signed(&client).await;

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
        // Typed, so callers can branch on retryability structurally (#126) —
        // the rendered text is unchanged ("appservice login failed: {status}: {body}").
        return Err(anyhow::Error::new(AppserviceHttpError { status, body })
            .context("appservice login failed"));
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
pub(crate) async fn appservice_join_room(
    homeserver_url: &str,
    appservice_token: &str,
    room_id: &str,
    user_id: &str,
    via: Option<&str>,
) -> Result<()> {
    let http = reqwest::Client::builder().use_rustls_tls().build()?;
    // Reuse the ghost path's URL builder (percent-encodes the room_id path segment
    // and appends the `?user_id=` masquerade).
    let mut url = crate::rtc::ghost::cs_url(homeserver_url, &["join", room_id], Some(user_id))?;
    // v12+ room ids carry no domain (#103), so a first-contact federated join
    // needs an explicit resolution hint. Harmless extra for legacy ids and
    // already-known rooms. `server_name` is the deprecated spelling, sent too
    // for older homeservers.
    if let Some(server) = via {
        url.query_pairs_mut()
            .append_pair("via", server)
            .append_pair("server_name", server);
    }
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
        return Err(AppserviceHttpError { status, body }.into());
    }
    Ok(())
}

/// A non-2xx response from a raw appservice-asserted CS-API call, carrying the
/// status as a *value* rather than only as text.
///
/// Callers used to classify these with `e.to_string().contains("403")`, the
/// same class of bug as #130 — a room id or URL containing "403" would match.
/// `Display` is deliberately unchanged (`"403 Forbidden: {body}"`), because the
/// management bot surfaces it verbatim in operator DMs; branch on `.status` via
/// `downcast_ref` instead of on the text.
#[derive(Debug)]
pub(crate) struct AppserviceHttpError {
    pub(crate) status: reqwest::StatusCode,
    pub(crate) body: String,
}

impl std::fmt::Display for AppserviceHttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.status, self.body)
    }
}

impl std::error::Error for AppserviceHttpError {}

/// The bridged room can't be joined and retrying won't change that: the room
/// is banned on the homeserver (e.g. an admin `rooms moderation ban-room`
/// after deleting it) or the bot is banned from it. The supervisor parks a
/// bridge that fails with this instead of restarting it with backoff.
#[derive(Debug)]
pub struct RoomUnavailable {
    pub room_id: String,
    /// The homeserver's own `error` text.
    pub reason: String,
}

impl std::fmt::Display for RoomUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "room {} is unavailable: {}", self.room_id, self.reason)
    }
}

impl std::error::Error for RoomUnavailable {}

/// Classify a failed join: `Some(reason)` when the response is a 403
/// `M_FORBIDDEN` whose message says the room (or the bot) is banned. Tuwunel
/// says "This room is banned on this homeserver."; Synapse "You are banned
/// from this room". Any other 403 (an uninvited restricted room) stays
/// retryable — an invite can still fix it.
pub(crate) fn room_ban_reason(status: reqwest::StatusCode, body: &str) -> Option<String> {
    if status != reqwest::StatusCode::FORBIDDEN {
        return None;
    }
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    if v.get("errcode")?.as_str()? != "M_FORBIDDEN" {
        return None;
    }
    let msg = v.get("error")?.as_str()?;
    msg.to_ascii_lowercase().contains("banned").then(|| msg.to_string())
}

/// Open a **plaintext** direct room with `target` as the bot and post `text`
/// into it — over the raw appservice-asserted client API, never through the
/// SDK `Client`.
///
/// This exists for the one outbound notice that must not touch the SDK's
/// crypto store: the #101 "I only work in DMs" explanation that follows a
/// rejected non-direct invite. Sent via the SDK (`create_dm` + encrypted
/// `send`), that message races the management bot's own sync loop — the
/// encrypted send does a `/keys/claim` for the bot's sibling devices and then
/// sits inside the crypto store transaction, while the sync loop blocks on the
/// same store lock at the end of its iteration. Live-traced 2026-09-27: the
/// sync loop went silent for ~31 s, which is exactly how a DM invite accepted
/// a moment earlier "never reached Joined" and was never greeted (nightly red
/// 09-24 → 09-27). A plaintext room needs no key claim, no Olm session, no
/// store transaction — so the explanation can't stall anything.
///
/// The room is `is_direct` for the invitee (their client files it as a DM) and
/// otherwise a throwaway: if they never accept, the #89b empty-room sweep
/// leaves it once the pending invite is gone. Returns the new room id.
pub(crate) async fn appservice_dm_plaintext(
    http: &reqwest::Client,
    homeserver_url: &str,
    appservice_token: &str,
    own_user: &str,
    target: &str,
    text: &str,
) -> Result<String> {
    let create_url = crate::rtc::ghost::cs_url(homeserver_url, &["createRoom"], Some(own_user))?;
    let resp = http
        .post(create_url)
        .bearer_auth(appservice_token)
        .json(&serde_json::json!({
            "preset": "trusted_private_chat",
            "is_direct": true,
            "invite": [target],
        }))
        .send()
        .await
        .context("appservice createRoom request")?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(AppserviceHttpError { status, body }.into());
    }
    let created: serde_json::Value = resp.json().await.context("createRoom response JSON")?;
    let room_id = created["room_id"]
        .as_str()
        .map(str::to_owned)
        .context("createRoom response missing room_id")?;

    let txn = plaintext_txn_id();
    let send_url = crate::rtc::ghost::cs_url(
        homeserver_url,
        &["rooms", &room_id, "send", "m.room.message", &txn],
        Some(own_user),
    )?;
    let resp = http
        .put(send_url)
        .bearer_auth(appservice_token)
        .json(&serde_json::json!({ "msgtype": "m.text", "body": text }))
        .send()
        .await
        .context("appservice send request")?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(AppserviceHttpError { status, body }.into());
    }
    Ok(room_id)
}

/// A transaction id for raw client-API sends. Wall-clock nanoseconds plus pid
/// and a process-local counter: the homeserver de-duplicates `txnId`s per
/// access token **forever** (Tuwunel), and the appservice token is shared by
/// every bridge process, so a replayed id would silently return an OLD event
/// id and create nothing. Nanos make ids unique across processes and
/// restarts; pid + counter cover same-instant sends within one process.
fn plaintext_txn_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("nvb-{nanos}-{}-{}", std::process::id(), COUNTER.fetch_add(1, Ordering::Relaxed))
}

/// Leave a room via an appservice-asserted `POST /rooms/{id}/leave` — the mirror
/// of [`appservice_join_room`]. Used to undo a `link` preflight join when the
/// link does not proceed AND the probe was what created the membership (#83):
/// the probe must join to read a room's state, so an unauthorized/abandoned
/// `link` naming a room the bot was not already in would otherwise pin the
/// shared bridge bot there permanently. Best-effort at the call site; the caller
/// must NOT call this for a room a running bridge already keeps the bot in (all
/// bridges share one appservice MXID).
pub(crate) async fn appservice_leave_room(
    homeserver_url: &str,
    appservice_token: &str,
    room_id: &str,
    user_id: &str,
) -> Result<()> {
    let http = reqwest::Client::builder().use_rustls_tls().build()?;
    let url =
        crate::rtc::ghost::cs_url(homeserver_url, &["rooms", room_id, "leave"], Some(user_id))?;
    let resp = http
        .post(url)
        .bearer_auth(appservice_token)
        .json(&serde_json::json!({}))
        .send()
        .await
        .context("appservice leave request")?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(AppserviceHttpError { status, body }.into());
    }
    Ok(())
}

/// Is `user_id` currently joined to `room_id`? Reads the caller's OWN
/// `/joined_rooms` list (always readable, unlike a state GET which 403s for a
/// non-member), masqueraded via the appservice `as_token`. Used by the `link`
/// probe to decide whether a join it is about to make is new — so a failed probe
/// only reaps a membership it created, never one a running bridge holds (#83).
pub(crate) async fn appservice_is_room_joined(
    http: &reqwest::Client,
    homeserver_url: &str,
    appservice_token: &str,
    room_id: &str,
    user_id: &str,
) -> Result<bool> {
    let url = crate::rtc::ghost::cs_url(homeserver_url, &["joined_rooms"], Some(user_id))?;
    let resp = http
        .get(url)
        .bearer_auth(appservice_token)
        .send()
        .await
        .context("GET joined_rooms")?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(AppserviceHttpError { status, body }.into());
    }
    let body: serde_json::Value = resp.json().await.context("parse joined_rooms")?;
    Ok(body
        .get("joined_rooms")
        .and_then(|a| a.as_array())
        .is_some_and(|rooms| rooms.iter().any(|r| r.as_str() == Some(room_id))))
}

/// GET a room state event via the CS API, authenticated with the appservice
/// `as_token` and masqueraded as `user_id` (must be appservice-controlled and
/// joined to the room). `state_key` is `""` for keyless events.
/// `Ok(Some(content))` on 200, `Ok(None)` on 404 (event absent), `Err`
/// otherwise (auth, not in room, network).
/// SERVER-side count of pending (`membership=invite`) members in `room_id`
/// (#97 residual, #100 mode 1): the SDK's `invited_members_count()` reads the
/// local store summary, which lags a freshly created room by seconds — long
/// enough for the #89b sweep to reap a DM whose invite was pending
/// server-side the whole time. `GET /members?membership=invite` cannot lag.
pub(crate) async fn appservice_pending_invites(
    http: &reqwest::Client,
    homeserver_url: &str,
    appservice_token: &str,
    room_id: &str,
    user_id: &str,
) -> Result<usize> {
    let mut url =
        crate::rtc::ghost::cs_url(homeserver_url, &["rooms", room_id, "members"], Some(user_id))?;
    url.query_pairs_mut().append_pair("membership", "invite");
    let resp = http
        .get(url)
        .bearer_auth(appservice_token)
        .send()
        .await
        .context("GET room members (invite)")?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(AppserviceHttpError { status, body }.into());
    }
    let json: serde_json::Value = resp.json().await.context("members response JSON")?;
    Ok(json["chunk"].as_array().map(|c| c.len()).unwrap_or(0))
}

/// GET one FULL event (incl. `sender`) via the CS API, appservice-asserted.
/// `Ok(Some(event))` on 200, `Ok(None)` on 404, `Err` otherwise. First use:
/// resolving a v12 room's creator — the create event's sender — since state
/// reads return content only and v11+ removed `content.creator` (#112).
pub(crate) async fn get_room_event(
    http: &reqwest::Client,
    homeserver_url: &str,
    appservice_token: &str,
    room_id: &str,
    event_id: &str,
    user_id: &str,
) -> Result<Option<serde_json::Value>> {
    let url = crate::rtc::ghost::cs_url(
        homeserver_url,
        &["rooms", room_id, "event", event_id],
        Some(user_id),
    )?;
    let resp = http
        .get(url)
        .bearer_auth(appservice_token)
        .send()
        .await
        .context("GET room event")?;
    let status = resp.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(AppserviceHttpError { status, body }.into());
    }
    let event = resp.json().await.context("parsing room event")?;
    Ok(Some(event))
}

pub(crate) async fn get_room_state(
    http: &reqwest::Client,
    homeserver_url: &str,
    appservice_token: &str,
    room_id: &str,
    event_type: &str,
    state_key: &str,
    user_id: &str,
) -> Result<Option<serde_json::Value>> {
    let url = crate::rtc::ghost::cs_url(
        homeserver_url,
        &["rooms", room_id, "state", event_type, state_key],
        Some(user_id),
    )?;
    let resp = http
        .get(url)
        .bearer_auth(appservice_token)
        .send()
        .await
        .with_context(|| format!("GET room state {event_type}"))?;
    let status = resp.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(AppserviceHttpError { status, body }.into());
    }
    let content = resp
        .json()
        .await
        .with_context(|| format!("parsing {event_type} state content"))?;
    Ok(Some(content))
}

/// Run the Matrix sync loop (classic `/v3/sync` via `client.sync`). Must be called
/// with a client produced by `build_client`. Drives the `SyncCallMemberEvent`
/// handler and sends call-active signals through `call_active_tx` when the first
/// human joins or the last human leaves.
/// A departure grace in flight (see the `MatrixIncluded` handler in [`run`]).
struct DepartureCtx {
    mxid: OwnedUserId,
    /// Cancelled when the half comes back or the member's leave takes effect.
    token: CancellationToken,
    grace: Duration,
    router: Arc<dyn PlaybackRouter>,
    participants: ParticipantSet,
    pending_leaves: PendingLeaves,
    pending_arrivals: PendingArrivals,
    links: Arc<LinkBoard>,
    client: Client,
    room_id: OwnedRoomId,
    shutdown: CancellationToken,
}

/// How often a departure grace re-checks a pending leave after it elapsed.
const DEPARTURE_LEAVE_POLL: Duration = Duration::from_millis(250);

/// Ends a departure grace: after `grace`, and once no leave of the member is
/// pending, a member still present and still not bridge-owned gets its puppet
/// (and the "your Discord session dropped" notice).
async fn departure_grace_elapsed(d: DepartureCtx) {
    let mut wait = d.grace;
    loop {
        tokio::select! {
            biased;
            _ = d.shutdown.cancelled() => return, // teardown owns final release
            _ = d.token.cancelled() => return,    // half back / leave took effect
            _ = tokio::time::sleep(wait) => {}
        }
        if !d.pending_leaves.lock().await.contains_key(&d.mxid) {
            break;
        }
        wait = DEPARTURE_LEAVE_POLL; // their Matrix leave is being debounced — wait it out
    }
    {
        // A newer timer for this member cancels ours on insert, so an uncancelled
        // token means the entry is still ours.
        let mut arrivals = d.pending_arrivals.lock().await;
        if d.token.is_cancelled() {
            return;
        }
        arrivals.remove(&d.mxid);
    }
    let user = d.mxid.as_str();
    if !d.links.end_arrival_grace(user) {
        return; // already ended (first voiced frame assigned, half back, leave)
    }
    let present = d.participants.lock().await.iter().any(|(u, _)| u == &d.mxid);
    if !present || d.links.matrix_excluded(user) {
        tracing::info!(mxid = %user, "departure grace elapsed — member gone or paired half back — no puppet");
        return;
    }
    let name = match d.client.get_room(&d.room_id) {
        Some(room) => resolve_member_name(&room, &d.mxid).await,
        None => user.to_owned(),
    };
    tracing::info!(mxid = %user, display_name = %name, "departure grace elapsed — member stayed in the call — assigning puppet");
    d.router.assign(user, &name);
    d.router.unlisten(user); // after the assign: the anchor never lapses
    crate::links_notice::emit(&d.links, user);
}

#[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %bridge))]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run(
    client: Client,
    cfg: &MatrixConfig,
    media_e2ee: bool,
    leave_grace: Duration,
    arrival_grace: Duration,
    departure_grace: Duration,
    call_active_tx: watch::Sender<bool>,
    members_gen_tx: watch::Sender<u64>,
    router: Arc<dyn PlaybackRouter>,
    bridge: Arc<str>,
    shutdown: CancellationToken,
    links: Arc<LinkBoard>,
    mut link_rx: tokio::sync::mpsc::UnboundedReceiver<LinkEvent>,
) -> Result<()> {
    let bot_user_id: OwnedUserId = cfg.bot_user_id.parse()?;
    let room_id = RoomId::parse(&cfg.room_id)?;
    let participants: ParticipantSet = Arc::new(Mutex::new(HashSet::new()));
    let pending_leaves: PendingLeaves = Arc::new(Mutex::new(HashMap::new()));
    let pending_arrivals: PendingArrivals = Arc::new(Mutex::new(HashMap::new()));

    // Linked identities (ADR 0001): Matrix-side transitions caused by Discord
    // presence changes. A paired Discord account arriving makes its Matrix half
    // bridge-owned (release the puppet); it leaving hands the puppet back — the
    // "your Discord session dropped" case, which is also where the notice goes.
    {
        let router = router.clone();
        let participants = participants.clone();
        let shutdown = shutdown.clone();
        let client = client.clone();
        let room_id = room_id.clone();
        let links = links.clone();
        let pending_arrivals = pending_arrivals.clone();
        let pending_leaves = pending_leaves.clone();
        let span = tracing::info_span!(parent: None, "bridge", name = %bridge);
        tokio::spawn(
            async move {
                loop {
                    let ev = tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => return,
                        ev = link_rx.recv() => match ev { Some(e) => e, None => return },
                    };
                    match ev {
                        LinkEvent::MatrixExcluded(mxid) => {
                            // The paired half arrived during the arrival grace: the grace did
                            // its job — no puppet was ever assigned; it just keeps listening.
                            if links.end_arrival_grace(&mxid) {
                                if let Ok(uid) = mxid.parse::<OwnedUserId>() {
                                    if let Some(t) = pending_arrivals.lock().await.remove(&uid) {
                                        t.cancel();
                                    }
                                }
                                tracing::info!(mxid = %mxid, "paired Discord account arrived within the arrival grace — no puppet, still listening");
                                router.listen(&mxid); // idempotent; already listening
                                // A no-op normally; covers a playback consumer that assigned
                                // before the membership started the grace.
                                router.release(&mxid);
                                continue;
                            }
                            tracing::info!(mxid = %mxid, "member is now bridge-owned (its paired Discord account joined the channel) — releasing puppet, still listening");
                            // Listen FIRST so the pool keeps the capture anchor through the
                            // puppet's release (ADR 0003).
                            router.listen(&mxid);
                            router.release(&mxid);
                        }
                        LinkEvent::MatrixIncluded { user, discord_half_vanished } => {
                            let present = participants.lock().await.iter().any(|(u, _)| u.as_str() == user);
                            if !present || links.matrix_excluded(&user) {
                                continue;
                            }
                            // Departure grace (ADR 0005): the Discord half usually vanishes
                            // because the person is leaving altogether, and their Matrix
                            // membership goes a moment later. Assigning a puppet now would
                            // flash a bot under their name on the way out. Keep them
                            // listen-only instead (the arrival-grace claim: their first
                            // voiced frame still assigns at once) until the grace elapses
                            // and no leave of theirs is pending.
                            if discord_half_vanished && !departure_grace.is_zero() && !links.in_arrival_grace(&user) {
                                if let Ok(uid) = user.parse::<OwnedUserId>() {
                                    tracing::info!(mxid = %user, grace_ms = departure_grace.as_millis() as u64, "paired Discord account left — listening, departure grace started");
                                    links.begin_arrival_grace(&user);
                                    router.listen(&user);
                                    let token = CancellationToken::new();
                                    if let Some(old) = pending_arrivals.lock().await.insert(uid.clone(), token.clone()) {
                                        old.cancel();
                                    }
                                    tokio::spawn(
                                        departure_grace_elapsed(DepartureCtx {
                                            mxid: uid,
                                            token,
                                            grace: departure_grace,
                                            router: router.clone(),
                                            participants: participants.clone(),
                                            pending_leaves: pending_leaves.clone(),
                                            pending_arrivals: pending_arrivals.clone(),
                                            links: links.clone(),
                                            client: client.clone(),
                                            room_id: room_id.clone(),
                                            shutdown: shutdown.clone(),
                                        })
                                        .in_current_span(),
                                    );
                                    continue;
                                }
                            }
                            // A pairing removed mid-grace: ordinary now, the grace is moot.
                            links.end_arrival_grace(&user);
                            let name = match (client.get_room(&room_id), user.parse::<OwnedUserId>()) {
                                (Some(room), Ok(uid)) => resolve_member_name(&room, &uid).await,
                                _ => user.clone(),
                            };
                            tracing::info!(mxid = %user, display_name = %name, discord_half_vanished, "member is no longer bridge-owned — assigning puppet");
                            router.assign(&user, &name);
                            router.unlisten(&user); // after the assign: the anchor never lapses

                            if discord_half_vanished {
                                // Notice to the person (and their delegate) — delivered by the
                                // management bot when one is running (see `LinkNotice`).
                                crate::links_notice::emit(&links, &user);
                            }
                        }
                        LinkEvent::DiscordExcluded(_) | LinkEvent::DiscordIncluded(_) => {} // Discord side's job
                    }
                }
            }
            .instrument(span),
        );
    }
    let ghost_prefix: Arc<str> = Arc::from(
        crate::rtc::ghost::ghost_localpart_prefix(cfg.sender_localpart.as_deref()).as_str(),
    );

    // Register handler BEFORE sync so events during the initial round-trip are captured.
    // Scope it to THIS bridge's room: `add_event_handler` is global across every
    // joined room, so with multiple bridges sharing one Matrix account (the bot is
    // a member of all their rooms) an unfiltered handler would react to other
    // bridges' call membership and double-assign puppets. `add_room_event_handler`
    // makes the SDK dispatch only this room's events.
    {
        let participants = participants.clone();
        let pending_leaves = pending_leaves.clone();
        let pending_arrivals = pending_arrivals.clone();
        let bot_user_id = bot_user_id.clone();
        let call_active_tx = call_active_tx.clone();
        let members_gen_tx = members_gen_tx.clone();
        let shutdown = shutdown.clone();
        // matrix-sdk dispatches this handler from its own sync task, which does
        // not inherit the bridge span — re-establish it for per-bridge logs.
        let bridge = bridge.clone();
        let router = router.clone();
        let ghost_prefix = ghost_prefix.clone();
        let links = links.clone();
        client.add_room_event_handler(&room_id, move |ev: SyncCallMemberEvent, room: Room| {
            let participants = participants.clone();
            let pending_leaves = pending_leaves.clone();
            let pending_arrivals = pending_arrivals.clone();
            let bot_user_id = bot_user_id.clone();
            let call_active_tx = call_active_tx.clone();
            let members_gen_tx = members_gen_tx.clone();
            let shutdown = shutdown.clone();
            let bridge = bridge.clone();
            let router = router.clone();
            let ghost_prefix = ghost_prefix.clone();
            let links = links.clone();
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
                    &ghost_prefix,
                    &participants,
                    &call_active_tx,
                    &members_gen_tx,
                    &pending_leaves,
                    leave_grace,
                    &pending_arrivals,
                    arrival_grace,
                    &shutdown,
                    &bridge,
                    &room,
                    &router,
                    &links,
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
                &ghost_prefix,
                &participants,
                &call_active_tx,
                &members_gen_tx,
                &pending_leaves,
                leave_grace,
                &pending_arrivals,
                arrival_grace,
                &shutdown,
                &bridge,
                &room,
                &router,
                &links,
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
                    // Decide on the error's STRUCTURE (status + errcode), never
                    // its text: the stringified error embeds the sync URL, whose
                    // `since=` token can itself contain "401"/"403" (#130).
                    if crate::auth_error::sdk_error_is_fatal_auth(&e) {
                        return Err(anyhow::anyhow!("Matrix sync fatal auth error: {e}"));
                    }
                    tracing::warn!(error = %e, "Matrix sync error — retrying in 5 s");
                    // Make the back-off interruptible by shutdown too.
                    tokio::select! {
                        _ = shutdown.cancelled() => return Ok(()),
                        _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                    }
                }
            }
        }
    }
}

// The reap grace (45 s default — long enough to never reap a user still
// establishing their RTC connection, short enough to clear a failed-to-connect
// phantom within ~a minute of a call becoming active) was a hardcoded const
// here until #38; it now comes from the resolved `[timing]` config
// (`TimingConfig::reap_grace`) and reaches `reap_stale_memberships` as its
// `grace` parameter.

/// One non-empty `m.call.member` entry from room state, reduced to the fields
/// the reaper decision and the multi-SFU reconciler need. Keeping the decision
/// pure (this + a live-MXID set + a clock) makes it unit-testable without a live
/// SDK or LiveKit room.
#[derive(Debug, Clone)]
pub(crate) struct MembershipEntry {
    /// The state event's state_key (its unique handle; what we'd empty).
    pub(crate) state_key: String,
    /// The membership owner's bare MXID (the event sender).
    pub(crate) mxid: String,
    /// A bridge-owned identity (our ghost, or the bridge bot itself). Never reaped
    /// — those are managed by their own lifecycles (constraint #5 / self-cleanup).
    pub(crate) is_bridge_owned: bool,
    /// The state_key starts with `@` (legacy format). Matrix auth only lets that
    /// user set it, so the bot cannot empty it — we skip (and never track) it.
    pub(crate) legacy_state_key: bool,
    /// The SFU this member publishes its media on, resolved per MSC4195 focus
    /// selection (`rtc::focus`). `None` = no transport could be derived.
    pub(crate) focus: Option<crate::rtc::well_known::LivekitFocusInfo>,
    /// The member's own `foci_preferred[0]`. Clients don't all follow the
    /// election (Commet ignores memberships with a `focus_selection` it doesn't
    /// know and falls back to its own homeserver's SFU), so a member may really
    /// be publishing HERE rather than on `focus`. The RTC loop subscribes on
    /// both, and the reaper only judges a member once both are watched.
    pub(crate) preferred: Option<crate::rtc::well_known::LivekitFocusInfo>,
    /// The device the (first) membership is for — what a member's own
    /// listen-only device is told about (ADR 0005). `None` = no membership body.
    pub(crate) device_id: Option<String>,
    /// The membership has not expired.
    pub(crate) active: bool,
}

/// The call's memberships as read from room state, focus-resolved.
pub(crate) struct CallMemberships {
    /// Every non-empty membership (bridge-owned included), each with the SFU it
    /// publishes on.
    pub(crate) entries: Vec<MembershipEntry>,
    /// The call's elected SFU — the oldest active membership's transport
    /// (`rtc::focus::elected_focus`); where legacy clients listen and ghosts must
    /// publish. `None` when there is no active membership or its transport is
    /// unknown (callers fall back to the bridge's own SFU).
    pub(crate) elected: Option<crate::rtc::well_known::LivekitFocusInfo>,
}

/// Read every non-empty `m.call.member` in room state and resolve each member's
/// effective SFU plus the call's elected one. Shared by the reaper, the RTC
/// loop's multi-SFU reconciler, and each ghost's publish-target choice.
pub(crate) async fn snapshot_call_memberships(
    client: &Client,
    room_id: &RoomId,
    bot_user_id: &UserId,
    ghost_prefix: &str,
) -> Result<CallMemberships> {
    use crate::rtc::focus::{FocusEntry, Selection, elected_focus, first_livekit_focus, resolve_effective_foci};
    let room = client
        .get_room(room_id)
        .ok_or_else(|| anyhow::anyhow!("room not in local state"))?;
    let raws = room
        .get_state_events_static::<CallMemberEventContent>()
        .await
        .context("reading call memberships from room state")?;

    let mut entries = Vec::new();
    let mut focus_inputs = Vec::new();
    for raw in raws {
        let Ok(ev) = raw.deserialize() else { continue };
        let SyncOrStrippedState::Sync(SyncStateEvent::Original(o)) = ev else { continue };
        if matches!(o.content, CallMemberEventContent::Empty(_)) {
            continue; // already retracted
        }
        let state_key = o.state_key.as_ref().to_owned();
        // Focus fields from the (first) membership carried by this event: session
        // events hold exactly one; legacy events list one per device, all sharing
        // the sender's foci. Expired-but-unretracted entries are kept — those are
        // exactly the phantoms the reaper exists to clear.
        let mems = o.content.memberships();
        let (selection, preferred, created, device_id) = match mems.first() {
            Some(m) => (
                Selection::from_active_focus(m.focus_active()),
                first_livekit_focus(m.foci_preferred()),
                m.created_ts().unwrap_or(o.origin_server_ts),
                Some(m.device_id().to_string()),
            ),
            None => (Selection::Unknown, None, o.origin_server_ts, None),
        };
        let active = !o.content.active_memberships(Some(o.origin_server_ts)).is_empty();
        let mxid = o.sender.as_str().to_owned();
        let is_bridge_owned = o.sender.as_str() == bot_user_id.as_str()
            || crate::rtc::ghost::is_bridge_excluded_identity(o.sender.as_str(), ghost_prefix);
        focus_inputs.push(FocusEntry {
            key: state_key.clone(),
            created_ts_ms: u64::from(created.get()),
            active,
            bridge_owned: is_bridge_owned,
            selection,
            preferred: preferred.clone(),
        });
        let legacy_state_key = state_key.starts_with('@');
        entries.push(MembershipEntry {
            state_key,
            mxid,
            is_bridge_owned,
            legacy_state_key,
            focus: None,
            preferred,
            device_id,
            active,
        });
    }
    let mut resolved = resolve_effective_foci(&focus_inputs);
    for e in &mut entries {
        e.focus = resolved.remove(&e.state_key).flatten();
    }
    let elected = elected_focus(&focus_inputs);
    Ok(CallMemberships { entries, elected })
}

/// Pure reaper decision. Returns the state_keys to empty **now**, and mutates
/// `phantom_since` in place: a newly-absent membership starts its grace clock; a
/// present (or bridge-owned, or unobservable, or reaped) one is cleared; entries
/// whose membership has vanished from room state are pruned.
///
/// "Present" is matched on the **bare MXID**, not mxid+device: a genuinely
/// connected user is never reaped even if their membership's device disagrees
/// with their LiveKit-track device (the keying hazard fixed in `8bf4a90`). The
/// tradeoff — a duplicate stale membership for a present multi-device user is left
/// alone — is acceptable.
///
/// "Observable" (multi-SFU, MSC4195): `live_mxids` is only ground truth for the
/// SFUs we hold a live connection to — `observed_focus_urls`, normalized. A member
/// is judged only when BOTH the SFU the election says it publishes on (`focus`)
/// AND its own `foci_preferred[0]` (`preferred`) are watched — clients don't all
/// follow the election (Commet, 2026-09-29), so either may be the truth. A member
/// with neither derivable is never judged. Without this the reaper emptied a
/// matrix.org user's *live* membership 84× in one evening because they were on
/// matrix.org's SFU while we only watched nether.im's.
fn classify_stale_memberships(
    live_mxids: &HashSet<String>,
    observed_focus_urls: &HashSet<String>,
    entries: &[MembershipEntry],
    phantom_since: &mut HashMap<String, Instant>,
    now: Instant,
    grace: Duration,
) -> Vec<String> {
    use crate::rtc::focus::normalize_service_url;
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
        let watched = |f: &Option<crate::rtc::well_known::LivekitFocusInfo>| {
            f.as_ref()
                .is_some_and(|f| observed_focus_urls.contains(&normalize_service_url(&f.service_url)))
        };
        // Every SFU this member might really be on must be watched; at least one
        // must be known at all.
        let candidates = [&e.focus, &e.preferred];
        let known: Vec<_> = candidates.iter().filter(|f| f.is_some()).collect();
        let observable = !known.is_empty() && known.iter().all(|f| watched(f));
        if !observable {
            phantom_since.remove(&e.state_key); // on an SFU we don't watch — no evidence
            continue;
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
/// Empties each confirmed phantom as the bridge bot via a raw state PUT —
/// permitted because Element Call uses the per-device `_@…` state-key (not
/// `@`-prefixed, so not owner-restricted) and the room sets `m.call.member` at
/// PL 0. `entries` is the current [`snapshot_call_memberships`]; `live_mxids` is
/// the set of bare MXIDs currently present across every LiveKit room we are
/// connected to, and `observed_focus_urls` names those SFUs (normalized
/// `livekit_service_url`s) so members on any other SFU are never judged;
/// `phantom_since` carries each entry's grace clock across calls; `grace` is the
/// resolved `[timing]` reap grace. Best-effort: write failures are logged, never
/// fatal.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn reap_stale_memberships(
    client: &Client,
    room_id: &RoomId,
    entries: &[MembershipEntry],
    live_mxids: &HashSet<String>,
    observed_focus_urls: &HashSet<String>,
    phantom_since: &mut HashMap<String, Instant>,
    grace: Duration,
) {
    let Some(room) = client.get_room(room_id) else { return };
    let to_reap: HashSet<String> = classify_stale_memberships(
        live_mxids,
        observed_focus_urls,
        entries,
        phantom_since,
        Instant::now(),
        grace,
    )
    .into_iter()
    .collect();
    if to_reap.is_empty() {
        return;
    }

    let empty = match serde_json::to_value(CallMemberEventContent::new_empty(None)) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "reaper: could not serialize empty m.call.member");
            return;
        }
    };
    for e in entries {
        if !to_reap.contains(&e.state_key) {
            continue;
        }
        match room
            .send_state_event_raw(CALL_MEMBER_EVENT_TYPE, &e.state_key, empty.clone())
            .await
        {
            Ok(_) => tracing::info!(
                sender = %e.mxid,
                state_key = %e.state_key,
                "reaped phantom m.call.member (no live LiveKit participant on its SFU)"
            ),
            Err(err) => tracing::warn!(
                sender = %e.mxid,
                state_key = %e.state_key,
                error = %err,
                "failed to reap phantom membership (room PL for m.call.member may be > 0)"
            ),
        }
    }
}

/// Wire type of the MatrixRTC call-membership state event.
const CALL_MEMBER_EVENT_TYPE: &str = "org.matrix.msc3401.call.member";

#[cfg(test)]
mod tests {
    #[test]
    fn plaintext_txn_ids_are_unique_and_prefixed() {
        let a = super::plaintext_txn_id();
        let b = super::plaintext_txn_id();
        assert!(a.starts_with("nvb-") && b.starts_with("nvb-"));
        assert_ne!(a, b, "same-instant sends must still get distinct txn ids");
    }

    use super::{
        resolve_affected_user, resolve_ghost_device_salt, resolve_store_device_id, room_ban_reason,
    };
    use matrix_sdk::ruma::OwnedUserId;
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

    fn uid(s: &str) -> OwnedUserId {
        OwnedUserId::try_from(s).unwrap()
    }

    #[test]
    fn room_ban_reason_only_matches_banned_403s() {
        use reqwest::StatusCode;
        let tuwunel = r#"{"errcode":"M_FORBIDDEN","error":"M_FORBIDDEN: This room is banned on this homeserver."}"#;
        let synapse = r#"{"errcode":"M_FORBIDDEN","error":"You are banned from this room"}"#;
        assert_eq!(
            room_ban_reason(StatusCode::FORBIDDEN, tuwunel).as_deref(),
            Some("M_FORBIDDEN: This room is banned on this homeserver.")
        );
        assert!(room_ban_reason(StatusCode::FORBIDDEN, synapse).is_some());
        // An uninvited restricted room is still retryable (an invite fixes it).
        let restricted = r#"{"errcode":"M_FORBIDDEN","error":"You are not invited to this room."}"#;
        assert_eq!(room_ban_reason(StatusCode::FORBIDDEN, restricted), None);
        // Only a 403 M_FORBIDDEN counts; a proxy error page never does.
        assert_eq!(room_ban_reason(StatusCode::BAD_GATEWAY, tuwunel), None);
        assert_eq!(room_ban_reason(StatusCode::FORBIDDEN, "<html>banned</html>"), None);
        let other = r#"{"errcode":"M_UNKNOWN","error":"banned"}"#;
        assert_eq!(room_ban_reason(StatusCode::FORBIDDEN, other), None);
    }

    /// Participant set with one modern per-device entry and one legacy entry.
    fn participant_set() -> HashSet<(OwnedUserId, String)> {
        HashSet::from([
            (uid("@dark:nether.im"), "_@dark:nether.im_ABCDEF_m.call".to_owned()),
            (uid("@legacy:nether.im"), "@legacy:nether.im".to_owned()),
        ])
    }

    #[test]
    fn active_event_keys_on_sender() {
        // An active membership always belongs to its sender, even if the
        // state_key matches someone else's entry.
        let set = participant_set();
        let got = resolve_affected_user(&set, uid("@dark:nether.im"), "_@dark:nether.im_ABCDEF_m.call", true);
        assert_eq!(got, uid("@dark:nether.im"));
        let got = resolve_affected_user(&set, uid("@other:nether.im"), "_@dark:nether.im_ABCDEF_m.call", true);
        assert_eq!(got, uid("@other:nether.im"));
    }

    #[test]
    fn foreign_retraction_resolves_to_owner() {
        // The #25 reaper empties a phantom as the bridge bot (#54): the
        // retraction must be attributed to the phantom's owner, not the bot.
        let set = participant_set();
        let got = resolve_affected_user(&set, uid("@bridgebot:nether.im"), "_@dark:nether.im_ABCDEF_m.call", false);
        assert_eq!(got, uid("@dark:nether.im"));
        // Same for a legacy-format entry.
        let got = resolve_affected_user(&set, uid("@bridgebot:nether.im"), "@legacy:nether.im", false);
        assert_eq!(got, uid("@legacy:nether.im"));
    }

    #[test]
    fn unknown_state_key_retraction_falls_back_to_sender() {
        // A retraction for an entry we never held (bot retracting its own stale
        // self-membership, duplicate leave) stays keyed on the sender.
        let set = participant_set();
        let got = resolve_affected_user(&set, uid("@bridgebot:nether.im"), "_@bridgebot:nether.im_XYZ_m.call", false);
        assert_eq!(got, uid("@bridgebot:nether.im"));
        let empty: HashSet<(OwnedUserId, String)> = HashSet::new();
        let got = resolve_affected_user(&empty, uid("@dark:nether.im"), "_@dark:nether.im_ABCDEF_m.call", false);
        assert_eq!(got, uid("@dark:nether.im"));
    }

    #[test]
    fn fresh_store_mints_unique_id() {
        let tmp = TmpDir::new();
        let r = resolve_store_device_id(tmp.path(), "dark-voice");
        let id = &r.id;
        assert!(id.starts_with("dark-voice-"), "expected a minted id, got {id}");
        assert!(id.len() > "dark-voice-".len(), "expected a random suffix, got {id}");
        assert!(!r.reused, "a fresh mint must not report reused");
        // The minted id is persisted so the next restart reuses it.
        let marker = fs::read_to_string(tmp.0.join("device_id")).unwrap();
        assert_eq!(marker.trim(), *id);
        // Nothing was retired: a fresh store has no prior identity.
        assert!(!tmp.0.join("retired_device_ids").exists());
    }

    #[test]
    fn pre_existing_store_mints_and_resets_crypto() {
        let tmp = TmpDir::new();
        // A pre-#30 store's first upgrade: SDK crypto db present, no marker. The old adopt
        // behavior kept the plain `dark-voice` (collision-prone); now we mint a unique id and
        // reset the crypto store so the new device gets a clean Olm account.
        fs::write(tmp.0.join("matrix-sdk-crypto.sqlite3"), b"x").unwrap();
        fs::write(tmp.0.join("matrix-sdk-crypto.sqlite3-wal"), b"x").unwrap();
        let r = resolve_store_device_id(tmp.path(), "dark-voice");
        let id = &r.id;
        assert!(id.starts_with("dark-voice-"), "expected a minted id, got {id}");
        assert_ne!(*id, "dark-voice");
        assert!(!r.reused);
        // Crypto files removed for re-provision; marker holds the new id.
        assert!(!tmp.0.join("matrix-sdk-crypto.sqlite3").exists());
        assert!(!tmp.0.join("matrix-sdk-crypto.sqlite3-wal").exists());
        assert_eq!(fs::read_to_string(tmp.0.join("device_id")).unwrap().trim(), *id);
        // The abandoned plain identity is queued for prune (#53).
        assert_eq!(crate::device_prune::read_retired(tmp.path()), vec!["dark-voice"]);
    }

    #[test]
    fn plain_marker_migrates_and_resets_then_is_idempotent() {
        let tmp = TmpDir::new();
        // The "plain poison" written by the old adopt path: marker == configured.
        fs::write(tmp.0.join("device_id"), "dark-voice").unwrap();
        fs::write(tmp.0.join("matrix-sdk-crypto.sqlite3"), b"x").unwrap();
        let r = resolve_store_device_id(tmp.path(), "dark-voice");
        let id = r.id;
        assert!(id.starts_with("dark-voice-") && id != "dark-voice", "expected migration, got {id}");
        assert!(!r.reused, "a migration mints, it does not reuse");
        assert!(!tmp.0.join("matrix-sdk-crypto.sqlite3").exists(), "crypto store must be reset on migrate");
        assert_eq!(fs::read_to_string(tmp.0.join("device_id")).unwrap().trim(), id);
        assert_eq!(crate::device_prune::read_retired(tmp.path()), vec!["dark-voice"]);

        // Idempotent: a second start (marker now != configured) reuses the healed id and does
        // NOT touch a freshly-recreated crypto store.
        fs::write(tmp.0.join("matrix-sdk-crypto.sqlite3"), b"y").unwrap();
        let r2 = resolve_store_device_id(tmp.path(), "dark-voice");
        assert_eq!(r2.id, id, "healed id must be stable across restarts");
        assert!(r2.reused, "a persisted minted id must report reused");
        assert!(tmp.0.join("matrix-sdk-crypto.sqlite3").exists(), "must not reset crypto on a healed store");
    }

    #[test]
    fn existing_minted_marker_is_reused_verbatim() {
        let tmp = TmpDir::new();
        // A properly-minted/custom marker (!= configured) is reused, crypto untouched.
        fs::write(tmp.0.join("device_id"), "persisted-xyz").unwrap();
        fs::write(tmp.0.join("matrix-sdk-crypto.sqlite3"), b"x").unwrap();
        let r = resolve_store_device_id(tmp.path(), "dark-voice");
        assert_eq!(r.id, "persisted-xyz");
        assert!(r.reused);
        assert!(tmp.0.join("matrix-sdk-crypto.sqlite3").exists(), "minted/custom marker must not trigger reset");
    }

    #[test]
    fn empty_store_path_returns_configured() {
        // Defensive: no store path → just the configured id, no filesystem writes.
        let r = resolve_store_device_id("", "dark-voice");
        assert_eq!(r.id, "dark-voice");
        assert!(!r.reused);
    }

    #[test]
    fn remint_retires_dead_id_and_provisions_fresh() {
        let tmp = TmpDir::new();
        // A store whose persisted device was deleted server-side (#53 self-heal).
        fs::write(tmp.0.join("device_id"), "dark-voice-dead1234").unwrap();
        fs::write(tmp.0.join("matrix-sdk-crypto.sqlite3"), b"x").unwrap();
        let id = super::remint_bot_device(tmp.path(), "dark-voice", "dark-voice-dead1234");
        assert!(id.starts_with("dark-voice-") && id != "dark-voice-dead1234");
        // Dead id queued for prune; crypto reset; marker rewritten to the fresh id.
        assert_eq!(crate::device_prune::read_retired(tmp.path()), vec!["dark-voice-dead1234"]);
        assert!(!tmp.0.join("matrix-sdk-crypto.sqlite3").exists());
        assert_eq!(fs::read_to_string(tmp.0.join("device_id")).unwrap().trim(), id);
        // The next resolve reuses the fresh id.
        let r = resolve_store_device_id(tmp.path(), "dark-voice");
        assert_eq!(r.id, id);
        assert!(r.reused);
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
    use crate::rtc::well_known::LivekitFocusInfo;
    use std::collections::{HashMap, HashSet};
    use std::time::{Duration, Instant};

    const GRACE: Duration = Duration::from_secs(45);
    /// The bridge's own SFU — the one the reaper is always connected to.
    const OUR_SFU: &str = "https://livekit.nether.im";
    /// Someone else's SFU (a multi-SFU matrix.org member publishes here).
    const THEIR_SFU: &str = "https://livekit-jwt.call.matrix.org";

    fn focus(url: &str) -> Option<LivekitFocusInfo> {
        Some(LivekitFocusInfo { alias: "!r:x".into(), service_url: url.into() })
    }

    fn entry(state_key: &str, mxid: &str, owned: bool, legacy: bool) -> MembershipEntry {
        MembershipEntry {
            state_key: state_key.to_owned(),
            mxid: mxid.to_owned(),
            is_bridge_owned: owned,
            legacy_state_key: legacy,
            focus: focus(OUR_SFU),
            preferred: focus(OUR_SFU),
            device_id: None,
            active: true,
        }
    }

    fn entry_on(state_key: &str, mxid: &str, focus: Option<LivekitFocusInfo>) -> MembershipEntry {
        MembershipEntry { preferred: focus.clone(), focus, ..entry(state_key, mxid, false, false) }
    }

    /// Commet (2026-09-29): the election said "nether.im" (it followed the oldest
    /// ghost on paper) but the client really published on its own matrix.org SFU.
    /// A member whose resolved and preferred SFUs differ is judged only when BOTH
    /// are watched — otherwise it was reaped alive.
    #[test]
    fn member_is_judged_only_when_resolved_and_preferred_sfus_are_both_watched() {
        let key = "_@commet:matrix.org_DEV_m.call";
        let t0 = Instant::now();
        let mut since = HashMap::new();
        let entries = [MembershipEntry { focus: focus(OUR_SFU), preferred: focus(THEIR_SFU), ..entry(key, "@commet:matrix.org", false, false) }];
        // Only our SFU watched → not judged, no clock.
        assert!(classify_stale_memberships(&live(&[]), &ours(), &entries, &mut since, t0 + GRACE * 3, GRACE).is_empty());
        assert!(since.is_empty());
        // Both watched and absent from both → a genuine phantom, reaped after grace.
        let both: HashSet<String> = HashSet::from([OUR_SFU.to_owned(), THEIR_SFU.to_owned()]);
        assert!(classify_stale_memberships(&live(&[]), &both, &entries, &mut since, t0, GRACE).is_empty());
        assert_eq!(classify_stale_memberships(&live(&[]), &both, &entries, &mut since, t0 + GRACE, GRACE), vec![key.to_string()]);
        // A member with no derivable SFU at all is never judged.
        let unknown = [MembershipEntry { focus: None, preferred: None, ..entry("_@x:y_D_m.call", "@x:y", false, false) }];
        assert!(classify_stale_memberships(&live(&[]), &both, &unknown, &mut since, t0 + GRACE * 3, GRACE).is_empty());
    }

    /// ADR 0005: an enrolled listen-only device stays connected after its owner's
    /// client dies, so it must not keep the dead membership alive; the owner's
    /// live client (legacy, Commet-style without `member.id`, or hashed) does.
    #[test]
    fn listen_only_devices_are_no_evidence_but_hashed_and_commet_members_are() {
        use crate::rtc::roster_mxids;
        use nvb_companion::selfcall::{
            hashed_identity_with_device, legacy_identity, set_listen_only_devices, PeerKey,
        };
        let owner = "@reaper-owner:example.org";
        let with_dev = |key: &str, mxid: &str, dev: &str| MembershipEntry {
            device_id: Some(dev.to_owned()),
            ..entry(key, mxid, false, false)
        };
        set_listen_only_devices([(owner.to_owned(), "LISTEN".to_owned())]);
        let entries = [with_dev("_@reaper-owner:example.org_PHONE_m.call", owner, "PHONE")];
        let t0 = Instant::now();

        // Crashed client: only the listen-only device (both identity forms) is
        // left → the membership is a phantom and is reaped after grace.
        let only_app: HashSet<String> = [
            legacy_identity(owner, "LISTEN"),
            hashed_identity_with_device(owner, "LISTEN", "LISTEN"),
        ]
        .into();
        let mxids = roster_mxids(&only_app, &entries, &[]);
        assert!(mxids.is_empty());
        let mut since = HashMap::new();
        assert!(classify_stale_memberships(&mxids, &ours(), &entries, &mut since, t0, GRACE).is_empty());
        assert_eq!(
            classify_stale_memberships(&mxids, &ours(), &entries, &mut since, t0 + GRACE, GRACE),
            vec![entries[0].state_key.clone()]
        );

        // Live client next to the app → kept.
        let mut both = only_app.clone();
        both.insert(legacy_identity(owner, "PHONE"));
        let mxids = roster_mxids(&both, &entries, &[]);
        let mut since = HashMap::new();
        assert!(classify_stale_memberships(&mxids, &ours(), &entries, &mut since, t0 + GRACE * 3, GRACE).is_empty());

        // A hashed member is recognised once a key named its member.id …
        let hashed = HashSet::from([hashed_identity_with_device(owner, "PHONE", "mem-1")]);
        assert!(!roster_mxids(&hashed, &entries, &[]).contains(owner), "unknown until its member.id is seen");
        let key = PeerKey {
            sender: owner.into(),
            device_id: "PHONE".into(),
            member_id: Some("mem-1".into()),
            index: 0,
            key_b64: "k".into(),
        };
        assert_eq!(roster_mxids(&hashed, &entries, &[key]), live(&[owner]));

        // … and a Commet-style member (legacy identity, no member.id) counts as is.
        let commet = [with_dev("_@c:matrix.org_CDEV_m.call", "@c:matrix.org", "CDEV")];
        let ids = HashSet::from([legacy_identity("@c:matrix.org", "CDEV")]);
        assert_eq!(roster_mxids(&ids, &commet, &[]), live(&["@c:matrix.org"]));
        set_listen_only_devices([]);
    }

    fn live(mxids: &[&str]) -> HashSet<String> {
        mxids.iter().map(|s| s.to_string()).collect()
    }

    /// The SFUs we watch: by default just our own.
    fn ours() -> HashSet<String> {
        HashSet::from([OUR_SFU.to_owned()])
    }

    #[test]
    fn present_user_is_never_reaped_and_clears_timer() {
        let t0 = Instant::now();
        let mut since = HashMap::new();
        let entries = [entry("_@alice:x_DEV_m.call", "@alice:x", false, false)];
        // Even with a pre-existing (stale) timer, a user back in LiveKit is kept.
        since.insert("_@alice:x_DEV_m.call".to_owned(), t0);
        let reap = classify_stale_memberships(&live(&["@alice:x"]), &ours(), &entries, &mut since, t0 + GRACE * 2, GRACE);
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
        let reap = classify_stale_memberships(&empty, &ours(), &entries, &mut since, t0, GRACE);
        assert!(reap.is_empty());
        // Still within grace.
        let reap = classify_stale_memberships(&empty, &ours(), &entries, &mut since, t0 + Duration::from_secs(30), GRACE);
        assert!(reap.is_empty());
        // Past grace → reaped, timer dropped.
        let reap = classify_stale_memberships(&empty, &ours(), &entries, &mut since, t0 + GRACE, GRACE);
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
        let reap = classify_stale_memberships(&empty, &ours(), &entries, &mut since, t0 + GRACE * 3, GRACE);
        assert!(reap.is_empty());
        assert!(since.is_empty(), "neither owned nor legacy entries are tracked");
    }

    #[test]
    fn vanished_membership_is_pruned_from_timers() {
        let t0 = Instant::now();
        let mut since = HashMap::new();
        since.insert("_@gone:x_DEV_m.call".to_owned(), t0);
        // The membership is no longer in room state (no entries) → its timer is pruned.
        let reap = classify_stale_memberships(&live(&[]), &ours(), &[], &mut since, t0 + Duration::from_secs(5), GRACE);
        assert!(reap.is_empty());
        assert!(since.is_empty(), "timer for a vanished membership is dropped");
    }

    /// Multi-SFU (MSC4195): a member publishing on an SFU we are NOT connected to
    /// is invisible, not absent. The 2026-09-28 Limited-VC incident: a matrix.org
    /// user in `multi_sfu` mode was emptied 84× by this reaper while live on
    /// matrix.org's SFU. Never judge what we can't observe.
    #[test]
    fn member_on_unwatched_sfu_is_never_reaped() {
        let key = "_@fame:matrix.org_DEV_m.call";
        let t0 = Instant::now();
        let mut since = HashMap::new();
        let entries = [entry_on(key, "@fame:matrix.org", focus(THEIR_SFU))];
        let absent = live(&[]);
        // Absent from OUR roster for many graces — still kept, and no clock runs.
        assert!(classify_stale_memberships(&absent, &ours(), &entries, &mut since, t0, GRACE).is_empty());
        assert!(classify_stale_memberships(&absent, &ours(), &entries, &mut since, t0 + GRACE * 5, GRACE).is_empty());
        assert!(since.is_empty(), "unobservable members never start a phantom clock");
    }

    /// Once the RTC loop holds a subscriber on that foreign SFU, its roster IS
    /// evidence: the member is judged like anyone else (absent there ⇒ phantom).
    #[test]
    fn member_on_watched_foreign_sfu_is_judged_normally() {
        let key = "_@fame:matrix.org_DEV_m.call";
        let t0 = Instant::now();
        let mut since = HashMap::new();
        let entries = [entry_on(key, "@fame:matrix.org", focus(THEIR_SFU))];
        let watched: HashSet<String> = HashSet::from([OUR_SFU.to_owned(), THEIR_SFU.to_owned()]);
        // Present on the foreign SFU → kept.
        assert!(classify_stale_memberships(&live(&["@fame:matrix.org"]), &watched, &entries, &mut since, t0, GRACE).is_empty());
        // Absent from every watched SFU for a full grace → reaped.
        assert!(classify_stale_memberships(&live(&[]), &watched, &entries, &mut since, t0, GRACE).is_empty());
        let reap = classify_stale_memberships(&live(&[]), &watched, &entries, &mut since, t0 + GRACE, GRACE);
        assert_eq!(reap, vec![key.to_string()]);
    }

    /// Losing the foreign subscriber mid-grace (its URL drops out of the watched
    /// set) must clear the clock, not let a stale absence run to a reap.
    #[test]
    fn losing_the_foreign_sfu_clears_a_running_clock() {
        let key = "_@fame:matrix.org_DEV_m.call";
        let t0 = Instant::now();
        let mut since = HashMap::new();
        let entries = [entry_on(key, "@fame:matrix.org", focus(THEIR_SFU))];
        let watched: HashSet<String> = HashSet::from([OUR_SFU.to_owned(), THEIR_SFU.to_owned()]);
        assert!(classify_stale_memberships(&live(&[]), &watched, &entries, &mut since, t0, GRACE).is_empty());
        assert!(since.contains_key(key), "clock started while observable");
        // Foreign session died → only our SFU is watched now.
        assert!(classify_stale_memberships(&live(&[]), &ours(), &entries, &mut since, t0 + GRACE, GRACE).is_empty());
        assert!(since.is_empty(), "clock cleared once the member became unobservable");
    }

    /// A membership whose transport can't be derived (unknown `focus_selection`,
    /// empty `foci_preferred`) is never reaped — no evidence either way.
    #[test]
    fn member_with_unknown_focus_is_never_reaped() {
        let t0 = Instant::now();
        let mut since = HashMap::new();
        let entries = [entry_on("_@odd:x_DEV_m.call", "@odd:x", None)];
        assert!(classify_stale_memberships(&live(&[]), &ours(), &entries, &mut since, t0 + GRACE * 2, GRACE).is_empty());
        assert!(since.is_empty());
    }

    /// Focus URLs compare in normalized form (trailing slash / case).
    #[test]
    fn observed_focus_match_is_normalized() {
        let key = "_@bob:x_DEV_m.call";
        let t0 = Instant::now();
        let mut since = HashMap::new();
        let entries = [entry_on(key, "@bob:x", focus("HTTPS://LiveKit.nether.im/"))];
        assert!(classify_stale_memberships(&live(&[]), &ours(), &entries, &mut since, t0, GRACE).is_empty());
        let reap = classify_stale_memberships(&live(&[]), &ours(), &entries, &mut since, t0 + GRACE, GRACE);
        assert_eq!(reap, vec![key.to_string()], "our own SFU spelled differently is still ours");
    }

    /// Tier-2 named scenario (#38 Phase C2): a user flapping absent→present→
    /// absent must survive the reaper. Presence within the grace clears the
    /// clock, and the SECOND absence starts a fresh one — only a full grace of
    /// *continuous* absence reaps (the #54 class needs a genuine phantom, not
    /// an EC heartbeat flap or a reconnecting client).
    #[test]
    fn scenario_flap_within_grace_survives_reaper() {
        let key = "_@flappy:x_DEV_m.call";
        let t0 = Instant::now();
        let mut since = HashMap::new();
        let entries = [entry(key, "@flappy:x", false, false)];
        let absent = live(&[]);
        let present = live(&["@flappy:x"]);

        // Absent: the clock starts; nothing reaped inside the grace.
        assert!(classify_stale_memberships(&absent, &ours(), &entries, &mut since, t0, GRACE).is_empty());
        let mid = t0 + GRACE / 2;
        assert!(classify_stale_memberships(&absent, &ours(), &entries, &mut since, mid, GRACE).is_empty());

        // Reappears within the grace → the clock is cleared.
        assert!(classify_stale_memberships(&present, &ours(), &entries, &mut since, mid, GRACE).is_empty());
        assert!(since.is_empty(), "flap-back clears the phantom clock");

        // Absent again: a FRESH clock — the earlier absence must not count.
        let t1 = t0 + GRACE; // a full grace after first absence, but just re-absent
        assert!(classify_stale_memberships(&absent, &ours(), &entries, &mut since, t1, GRACE).is_empty());
        let almost = t1 + GRACE - Duration::from_millis(1);
        assert!(
            classify_stale_memberships(&absent, &ours(), &entries, &mut since, almost, GRACE).is_empty(),
            "continuous absence not yet a full grace — kept"
        );

        // Only now has it been continuously absent for the full grace.
        let reap = classify_stale_memberships(&absent, &ours(), &entries, &mut since, t1 + GRACE, GRACE);
        assert_eq!(reap, vec![key.to_string()]);
        assert!(since.is_empty());
    }
}
