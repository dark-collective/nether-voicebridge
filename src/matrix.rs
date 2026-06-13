use anyhow::Result;
use matrix_sdk::{
    Client, Room, SessionMeta,
    authentication::{SessionTokens, matrix::MatrixSession},
    config::SyncSettings,
    deserialized_responses::SyncOrStrippedState,
    ruma::{
        MilliSecondsSinceUnixEpoch, OwnedUserId, RoomId,
        events::{
            SyncStateEvent,
            call::member::{CallMemberEventContent, SyncCallMemberEvent},
        },
    },
};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{Mutex, watch};

use crate::config::MatrixConfig;

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

/// Apply a single call-member event (from sync or initial state scan) to the
/// participant set, then log. Handles echo-exclusion for the bridge bot.
/// Returns `true` if the call transitioned from no-humans to some-humans
/// (call became active), or `false` if nothing changed regarding call activity.
async fn apply_member_event(
    user_id: OwnedUserId,
    state_key: String,
    content: &CallMemberEventContent,
    ts: MilliSecondsSinceUnixEpoch,
    bot_user_id: &OwnedUserId,
    participants: &ParticipantSet,
    call_active_tx: &watch::Sender<bool>,
) {
    if &user_id == bot_user_id {
        return; // echo-exclusion: bridge bot's own call membership never counted
    }

    let is_active = match content {
        CallMemberEventContent::Empty(_) => false,
        c => !c.active_memberships(Some(ts)).is_empty(),
    };

    let mut set = participants.lock().await;
    let prev_users = set
        .iter()
        .map(|(u, _)| u)
        .collect::<HashSet<_>>()
        .len();

    if is_active {
        set.insert((user_id, state_key));
    } else {
        set.remove(&(user_id, state_key));
    }

    let new_users = set
        .iter()
        .map(|(u, _)| u)
        .collect::<HashSet<_>>()
        .len();

    if prev_users == 0 && new_users > 0 {
        tracing::info!("call active");
        let _ = call_active_tx.send(true);
    } else if prev_users > 0 && new_users == 0 {
        tracing::info!("call ended");
        let _ = call_active_tx.send(false);
    }
    log_snapshot(&set);
}

/// Build and authenticate a Matrix client from config. Joins the target room,
/// runs one sync round to populate the local state store, and returns the
/// ready client.
///
/// This is kept separate from `run()` so `main` can clone the client into the
/// RTC task before starting the perpetual sync loop.
pub async fn build_client(cfg: &MatrixConfig) -> Result<Client> {
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
pub async fn run(
    client: Client,
    cfg: &MatrixConfig,
    call_active_tx: watch::Sender<bool>,
) -> Result<()> {
    let bot_user_id: OwnedUserId = cfg.bot_user_id.parse()?;
    let room_id = RoomId::parse(&cfg.room_id)?;
    let participants: ParticipantSet = Arc::new(Mutex::new(HashSet::new()));

    // Register handler BEFORE sync so events during the initial round-trip are captured.
    {
        let participants = participants.clone();
        let bot_user_id = bot_user_id.clone();
        let call_active_tx = call_active_tx.clone();
        client.add_event_handler(move |ev: SyncCallMemberEvent, _room: Room| {
            let participants = participants.clone();
            let bot_user_id = bot_user_id.clone();
            let call_active_tx = call_active_tx.clone();
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
                )
                .await;
            }
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
            )
            .await;
        }
        tracing::info!("initial call snapshot loaded");
    } else {
        tracing::warn!(room_id = %room_id, "room not in local state after sync_once");
    }

    // Run sync forever — drives the event handler registered above and feeds
    // to-device decryption in Stage 2 (when e2e-encryption is enabled).
    client
        .sync(SyncSettings::default())
        .await
        .map_err(|e| anyhow::anyhow!("Matrix sync error: {e}"))
}
