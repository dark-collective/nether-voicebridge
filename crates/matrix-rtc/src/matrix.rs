use anyhow::Result;
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
        },
    },
};
use std::collections::HashSet;
use std::sync::Arc;
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
    if crate::rtc::ghost::is_ghost_identity(user_id.as_str()) {
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
    let appeared;
    let gone;
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

        if prev_users == 0 && new_users > 0 {
            tracing::info!("call active");
            let _ = call_active_tx.send(true);
        } else if prev_users > 0 && new_users == 0 {
            tracing::info!("call ended");
            let _ = call_active_tx.send(false);
        }
        log_snapshot(&set);
    }

    // Membership owns puppet presence: a member joining gets a (present, possibly
    // silent) named puppet keyed on their bare MXID; leaving releases it. Audio +
    // mute are driven separately by the LiveKit track in `rtc::session`, which
    // keys on the same bare MXID — so the track device need not match the
    // membership state-key device.
    if appeared {
        let name = resolve_member_name(room, &user_id).await;
        tracing::info!(mxid = %user_id, display_name = %name, "member joined — assigning puppet");
        router.assign(user_id.as_str(), &name);
    } else if gone {
        tracing::info!(mxid = %user_id, "member left — releasing puppet");
        router.release(user_id.as_str());
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
/// ready client.
///
/// This is kept separate from `run()` so the crate's `run()` can clone the
/// client into the RTC task before starting the perpetual sync loop.
#[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %bridge))]
pub(crate) async fn build_client(cfg: &MatrixConfig, bridge: &str) -> Result<Client> {
    let client = Client::builder()
        .homeserver_url(&cfg.homeserver_url)
        .build()
        .await?;

    let bot_user_id: OwnedUserId = cfg.bot_user_id.parse()?;

    let session = MatrixSession {
        meta: SessionMeta {
            user_id: bot_user_id.clone(),
            device_id: cfg.device_id.as_str().into(),
        },
        tokens: SessionTokens {
            access_token: cfg.access_token.clone(),
            refresh_token: None,
        },
    };
    client.restore_session(session).await?;
    tracing::info!(user_id = %bot_user_id, "Matrix session restored");

    let room_id = RoomId::parse(&cfg.room_id)?;
    // Join the room (idempotent if already a member).
    client.join_room_by_id(&room_id).await?;
    tracing::info!(room_id = %room_id, "joined Matrix room");

    // One sync round to populate the local state store.
    client.sync_once(SyncSettings::default()).await?;
    tracing::info!("initial Matrix sync complete");

    Ok(client)
}

/// Run the Matrix sync loop. Must be called with a client produced by
/// `build_client`. Drives the `SyncCallMemberEvent` handler and sends
/// call-active signals through `call_active_tx` when the first human
/// joins or the last human leaves.
#[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %bridge))]
pub(crate) async fn run(
    client: Client,
    cfg: &MatrixConfig,
    call_active_tx: watch::Sender<bool>,
    router: Arc<dyn PlaybackRouter>,
    bridge: Arc<str>,
    shutdown: CancellationToken,
) -> Result<()> {
    let bot_user_id: OwnedUserId = cfg.bot_user_id.parse()?;
    let room_id = RoomId::parse(&cfg.room_id)?;
    let participants: ParticipantSet = Arc::new(Mutex::new(HashSet::new()));

    // Register handler BEFORE sync so events during the initial round-trip are captured.
    // Scope it to THIS bridge's room: `add_event_handler` is global across every
    // joined room, so with multiple bridges sharing one Matrix account (the bot is
    // a member of all their rooms) an unfiltered handler would react to other
    // bridges' call membership and double-assign puppets. `add_room_event_handler`
    // makes the SDK dispatch only this room's events.
    {
        let participants = participants.clone();
        let bot_user_id = bot_user_id.clone();
        let call_active_tx = call_active_tx.clone();
        // matrix-sdk dispatches this handler from its own sync task, which does
        // not inherit the bridge span — re-establish it for per-bridge logs.
        let bridge = bridge.clone();
        let router = router.clone();
        client.add_room_event_handler(&room_id, move |ev: SyncCallMemberEvent, room: Room| {
            let participants = participants.clone();
            let bot_user_id = bot_user_id.clone();
            let call_active_tx = call_active_tx.clone();
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
                &room,
                &router,
            )
            .await;
        }
        tracing::info!("initial call snapshot loaded");
    } else {
        tracing::warn!(room_id = %room_id, "room not in local state after sync_once");
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
