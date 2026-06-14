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

/// Derive the LiveKit participant identity `{mxid}:{device}` from a call-member
/// `(user_id, state_key)`. Modern Element Call state keys are
/// `_{mxid}_{device}_m.call`; the device is the final `_`-delimited segment (the
/// mxid may itself contain `_`, but the device never does, so `rsplit_once`
/// recovers it). Returns `None` for legacy `{mxid}` keys (no device → cannot
/// correlate to an RTC participant, so no dedicated puppet).
fn member_identity(user_id: &UserId, state_key: &str) -> Option<String> {
    let inner = state_key.strip_prefix('_')?.strip_suffix("_m.call")?;
    let device = inner.rsplit_once('_').map(|(_, d)| d)?;
    Some(format!("{}:{}", user_id.as_str(), device))
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

    let is_active = match content {
        CallMemberEventContent::Empty(_) => false,
        c => !c.active_memberships(Some(ts)).is_empty(),
    };

    // Only act on a real membership transition (insert/remove returning true), so
    // periodic m.call.member refreshes don't re-resolve names or re-assign.
    let changed;
    {
        let mut set = participants.lock().await;
        let prev_users = set.iter().map(|(u, _)| u).collect::<HashSet<_>>().len();
        let entry = (user_id.clone(), state_key.clone());
        changed = if is_active {
            set.insert(entry)
        } else {
            set.remove(&entry)
        };
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
    // silent) named puppet; leaving releases it. Audio + mute are driven
    // separately by the LiveKit track in `rtc::session`.
    if changed {
        if let Some(key) = member_identity(&user_id, &state_key) {
            if is_active {
                let name = resolve_member_name(room, &user_id).await;
                tracing::info!(identity = %key, display_name = %name, "member joined — assigning puppet");
                router.assign(&key, &name);
            } else {
                tracing::info!(identity = %key, "member left — releasing puppet");
                router.release(&key);
            }
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
) -> Result<()> {
    let bot_user_id: OwnedUserId = cfg.bot_user_id.parse()?;
    let room_id = RoomId::parse(&cfg.room_id)?;
    let participants: ParticipantSet = Arc::new(Mutex::new(HashSet::new()));

    // Register handler BEFORE sync so events during the initial round-trip are captured.
    {
        let participants = participants.clone();
        let bot_user_id = bot_user_id.clone();
        let call_active_tx = call_active_tx.clone();
        // matrix-sdk dispatches this handler from its own sync task, which does
        // not inherit the bridge span — re-establish it for per-bridge logs.
        let bridge = bridge.clone();
        let router = router.clone();
        client.add_event_handler(move |ev: SyncCallMemberEvent, room: Room| {
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
        match client.sync(SyncSettings::default()).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                let msg = e.to_string();
                // 401/403 are auth failures — propagate immediately.
                if msg.contains("401") || msg.contains("403") || msg.contains("M_UNKNOWN_TOKEN") {
                    return Err(anyhow::anyhow!("Matrix sync fatal auth error: {e}"));
                }
                tracing::warn!(error = %e, "Matrix sync error — retrying in 5 s");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    }
}
