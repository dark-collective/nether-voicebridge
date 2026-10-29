//! What the companion hub learns about this bridge's room for a member's own
//! listen-only device (ADR 0005): membership checks against the bridge's synced
//! room state, and a live [`RoomView`] of the call — memberships (focus-resolved)
//! plus the media keys the bridge holds. The hub cuts that view down to the
//! requester's own slice; nothing here is sent anywhere by itself.

use std::sync::Arc;
use std::time::Duration;

use matrix_sdk::ruma::{OwnedRoomId, OwnedUserId, UserId};
use matrix_sdk::{Client, RoomMemberships};
use nvb_companion::selfcall::{CallMember, RoomFacts, RoomView, Transport};
use tokio::sync::{oneshot, watch};
use tokio_util::sync::CancellationToken;

use super::keys::{self, PeerKeyCache};
use super::well_known::LivekitFocusInfo;

/// How often each bridge re-checks the discovery it advertises in its room.
const DISCOVERY_RECHECK: Duration = Duration::from_secs(600);

/// How often the view re-reads call memberships from the local state store
/// (cheap: no network). Key changes refresh it immediately.
const REFRESH: Duration = Duration::from_secs(2);

/// The bridge's [`RoomFacts`]. The view's sender lives in the maintainer task, so
/// when the bridge's RTC loop ends the hub's watchers see the channel close.
pub(crate) struct SelfView {
    client: Client,
    room_id: OwnedRoomId,
    view: watch::Receiver<Arc<RoomView>>,
}

/// Everything the maintainer task needs.
pub(crate) struct ViewInputs {
    pub(crate) client: Client,
    pub(crate) room_id: OwnedRoomId,
    pub(crate) bot_user_id: OwnedUserId,
    pub(crate) ghost_prefix: Arc<str>,
    pub(crate) media_e2ee: bool,
    pub(crate) peer_keys: Arc<PeerKeyCache>,
}

impl SelfView {
    /// Start maintaining the view until `shutdown`; returns the facts to register.
    pub(crate) fn start(inputs: ViewInputs, shutdown: CancellationToken) -> Arc<Self> {
        let (tx, rx) = watch::channel(Arc::new(RoomView {
            encrypted: inputs.media_e2ee,
            key_params: inputs.media_e2ee.then(keys::subscriber_key_params),
            ..RoomView::default()
        }));
        let facts = Arc::new(Self { client: inputs.client.clone(), room_id: inputs.room_id.clone(), view: rx });
        tokio::spawn(maintain(inputs, tx, shutdown));
        facts
    }
}

async fn maintain(inputs: ViewInputs, tx: watch::Sender<Arc<RoomView>>, shutdown: CancellationToken) {
    let mut key_changes = inputs.peer_keys.changes();
    let mut tick = tokio::time::interval(REFRESH);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            _ = tick.tick() => {}
            r = key_changes.changed() => if r.is_err() { return },
        }
        let next = Arc::new(build_view(&inputs).await);
        tx.send_if_modified(|cur| {
            if **cur == *next {
                false
            } else {
                *cur = next;
                true
            }
        });
    }
}

fn transport(f: Option<LivekitFocusInfo>) -> Option<Transport> {
    f.map(|f| Transport { livekit_service_url: f.service_url, livekit_alias: f.alias })
}

async fn build_view(i: &ViewInputs) -> RoomView {
    let members = match crate::matrix::snapshot_call_memberships(&i.client, &i.room_id, &i.bot_user_id, &i.ghost_prefix).await {
        // One member per membership: a legacy event (state key = MXID) lists every device, each with its own expiry.
        Ok(snap) => snap
            .entries
            .into_iter()
            .flat_map(|e| {
                let (focus, preferred) = (transport(e.focus), transport(e.preferred));
                e.devices.into_iter().map(move |d| CallMember {
                    device_id: d.device_id,
                    mxid: e.mxid.clone(),
                    active: d.active,
                    bridge_owned: e.is_bridge_owned,
                    transport: focus.clone(),
                    preferred: preferred.clone(),
                })
            })
            .collect(),
        Err(e) => {
            tracing::debug!(error = %e, "self view: could not read call memberships");
            Vec::new()
        }
    };
    RoomView {
        encrypted: i.media_e2ee,
        members,
        // Keys are only ever handed out for an encrypted room.
        keys: if i.media_e2ee { i.peer_keys.snapshot() } else { Vec::new() },
        key_params: i.media_e2ee.then(keys::subscriber_key_params),
    }
}

impl RoomFacts for SelfView {
    fn is_joined(&self, mxid: &str) -> oneshot::Receiver<bool> {
        let (tx, rx) = oneshot::channel();
        let (client, room_id) = (self.client.clone(), self.room_id.clone());
        let mxid = mxid.to_owned();
        tokio::spawn(async move {
            let joined = match (client.get_room(&room_id), UserId::parse(&mxid)) {
                (Some(room), Ok(uid)) => matches!(
                    room.get_member_no_sync(&uid).await,
                    Ok(Some(m)) if *m.membership() == matrix_sdk::ruma::events::room::member::MembershipState::Join
                ),
                _ => false,
            };
            let _ = tx.send(joined);
        });
        rx
    }

    fn has_joined_server(&self, server_name: &str) -> oneshot::Receiver<bool> {
        let (tx, rx) = oneshot::channel();
        let (client, room_id) = (self.client.clone(), self.room_id.clone());
        let server = server_name.to_ascii_lowercase();
        tokio::spawn(async move {
            let found = match client.get_room(&room_id) {
                Some(room) => room
                    .members_no_sync(RoomMemberships::JOIN)
                    .await
                    .map(|ms| ms.iter().any(|m| m.user_id().server_name().as_str().eq_ignore_ascii_case(&server)))
                    .unwrap_or(false),
                None => false,
            };
            let _ = tx.send(found);
        });
        rx
    }

    fn view(&self) -> watch::Receiver<Arc<RoomView>> {
        self.view.clone()
    }
}

// ── discovery: advertise the companion API in the room ───────────────────────

/// After writing the member-content fallback, how long before the bridge writes it again if the field is missing:
/// a homeserver may strip unknown member-content keys, and rewriting `m.room.member` every check would spam the room.
const MEMBER_REWRITE_BACKOFF: Duration = Duration::from_secs(6 * 3600);

/// What one discovery check did.
#[derive(Debug, PartialEq, Eq)]
enum Advertised {
    /// The room already says what we want.
    Current,
    /// Wrote the state event.
    State,
    /// Wrote the member-content fallback.
    Member,
    /// The member field is missing again soon after we wrote it: held off (see [`MEMBER_REWRITE_BACKOFF`]).
    MemberHeld,
}

/// Keep the room advertising the companion API (ADR 0005) while `[companions]
/// public_url` is set: as the `im.nether.voicebridge.api` state event (state key
/// = the bot) when the bot may send it, else as a field of the bot's own
/// `m.room.member`. Re-checked every [`DISCOVERY_RECHECK`]; only written when the
/// room's copy differs, and the member fallback at most once per
/// [`MEMBER_REWRITE_BACKOFF`]. With `public_url` unset, an earlier advertisement
/// is withdrawn once. Best-effort: failures are logged and retried next time.
pub(crate) async fn advertise_api(client: Client, room_id: OwnedRoomId, bot_user_id: OwnedUserId, shutdown: CancellationToken) {
    let mut tick = tokio::time::interval(DISCOVERY_RECHECK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut member_written: Option<tokio::time::Instant> = None;
    let mut held_logged = false;
    let mut withdrawn = false;
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            _ = tick.tick() => {}
        }
        let Some(room) = client.get_room(&room_id) else { continue };
        let Some(want) = nvb_companion::selfcall::discovery_content() else {
            if !withdrawn {
                match withdraw_once(&room, &bot_user_id).await {
                    Ok(()) => withdrawn = true,
                    Err(e) => tracing::warn!(error = %e, "could not withdraw the companion API advertisement"),
                }
            }
            continue;
        };
        withdrawn = false;
        let may_write_member = member_written.is_none_or(|t| t.elapsed() >= MEMBER_REWRITE_BACKOFF);
        match advertise_once(&room, &bot_user_id, &want, may_write_member).await {
            Ok(Advertised::Member) => member_written = Some(tokio::time::Instant::now()),
            Ok(Advertised::MemberHeld) => {
                if !held_logged {
                    held_logged = true;
                    tracing::warn!(
                        "the companion API field written to the bot's room membership did not stick (the homeserver \
                         may drop unknown member keys) — not rewriting it for a while; allow the bot to send the \
                         im.nether.voicebridge.api state event instead"
                    );
                }
            }
            Ok(Advertised::Current | Advertised::State) => held_logged = false,
            Err(e) => tracing::warn!(error = %e, "could not advertise the companion API in the room"),
        }
    }
}

async fn state_content(room: &matrix_sdk::Room, event_type: &str, state_key: &str) -> Option<serde_json::Value> {
    use matrix_sdk::deserialized_responses::RawAnySyncOrStrippedState;
    let raw = room.get_state_event(event_type.into(), state_key).await.ok().flatten()?;
    let RawAnySyncOrStrippedState::Sync(raw) = raw else { return None };
    let ev: serde_json::Value = serde_json::from_str(raw.json().get()).ok()?;
    ev.get("content").cloned()
}

async fn advertise_once(room: &matrix_sdk::Room, bot: &UserId, want: &serde_json::Value, may_write_member: bool) -> anyhow::Result<Advertised> {
    use nvb_companion::selfcall::{API_MEMBER_FIELD, API_STATE_EVENT};
    if room.power_levels().await?.user_can_send_state(bot, API_STATE_EVENT.into()) {
        if state_content(room, API_STATE_EVENT, bot.as_str()).await.as_ref() == Some(want) {
            return Ok(Advertised::Current);
        }
        room.send_state_event_raw(API_STATE_EVENT, bot.as_str(), want.clone()).await?;
        tracing::info!("advertised the companion API in room state");
        return Ok(Advertised::State);
    }
    // Fallback: the bot's own membership (always writable by the bot itself).
    let Some(mut member) = state_content(room, "m.room.member", bot.as_str()).await else {
        anyhow::bail!("bot membership not in local state");
    };
    if member.get(API_MEMBER_FIELD) == Some(want) {
        return Ok(Advertised::Current);
    }
    if !may_write_member {
        return Ok(Advertised::MemberHeld);
    }
    let Some(obj) = member.as_object_mut() else { anyhow::bail!("bot membership content is not an object") };
    obj.insert(API_MEMBER_FIELD.to_owned(), want.clone());
    room.send_state_event_raw("m.room.member", bot.as_str(), member).await?;
    tracing::info!("advertised the companion API in the bot's room membership (no power to send its state event)");
    Ok(Advertised::Member)
}

/// `public_url` unset: withdraw an advertisement an earlier configuration left in the room (empty state event
/// content; the member field removed), so devices stop finding an API that is no longer served.
async fn withdraw_once(room: &matrix_sdk::Room, bot: &UserId) -> anyhow::Result<()> {
    use nvb_companion::selfcall::{API_MEMBER_FIELD, API_STATE_EVENT};
    let advertised = state_content(room, API_STATE_EVENT, bot.as_str()).await.is_some_and(|c| c.as_object().is_some_and(|o| !o.is_empty()));
    if advertised {
        if room.power_levels().await?.user_can_send_state(bot, API_STATE_EVENT.into()) {
            room.send_state_event_raw(API_STATE_EVENT, bot.as_str(), serde_json::json!({})).await?;
            tracing::info!("withdrew the companion API advertisement from room state (public_url is unset)");
        } else {
            tracing::warn!("the room still advertises the companion API, but the bot may no longer send that state event");
        }
    }
    if let Some(mut member) = state_content(room, "m.room.member", bot.as_str()).await {
        if member.as_object_mut().is_some_and(|o| o.remove(API_MEMBER_FIELD).is_some()) {
            room.send_state_event_raw("m.room.member", bot.as_str(), member).await?;
            tracing::info!("withdrew the companion API field from the bot's room membership (public_url is unset)");
        }
    }
    Ok(())
}
