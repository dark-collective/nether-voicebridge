//! Post and retract the bridge bot's `m.call.member` state event.
//!
//! MatrixRTC call membership is signalled via the `org.matrix.msc3401.call.member`
//! state event. Element Call reads this to decide who is in the call.
//!
//! State-key format: `_{user_id}_{device_id}_m.call`
//!   Example: `_@voicebridge:nether.im_ABCDEFGHIJ_m.call`
//!   The leading underscore avoids Matrix auth-rule issues on @-prefixed state keys.

use anyhow::{Context, Result};
use matrix_sdk::Room;
use matrix_sdk::ruma::{
    OwnedDeviceId, OwnedUserId,
    events::call::member::{
        ActiveFocus, ActiveLivekitFocus, Application, CallApplicationContent, CallMemberEventContent,
        CallMemberStateKey, CallScope, Focus, LivekitFocus,
    },
};

use crate::rtc::well_known::LivekitFocusInfo;

/// Post the bridge bot's join membership state event.
///
/// `room`   — the Matrix room (must be joined)
/// `user_id` — the bridge bot's MXID
/// `device_id` — the bridge bot's Matrix device id
/// `focus`  — the LiveKit SFU focus discovered from `.well-known`
///
/// Currently UNUSED: the bridge bot is a subscribe-only LiveKit participant and
/// deliberately posts no `m.call.member`, so it stays out of Element Call's
/// roster (see `rtc::run`). Kept compiling as the easy revert if the SFU ever
/// requires membership to forward tracks. Ghosts post membership via the raw
/// CS-API in `rtc::ghost`, not this.
#[allow(dead_code)]
pub async fn join(
    room: &Room,
    user_id: &OwnedUserId,
    device_id: &OwnedDeviceId,
    focus: &LivekitFocusInfo,
) -> Result<()> {
    // State-key member_id: "{device_id}_m.call"
    // This is the element-call-expected per-device-per-application format (ruma 0.34).
    let member_id = format!("{device_id}_m.call");
    let state_key = CallMemberStateKey::new(user_id.clone(), Some(member_id), true);

    let foci_preferred = vec![Focus::Livekit(LivekitFocus::new(
        focus.alias.clone(),
        focus.service_url.clone(),
    ))];

    let content = CallMemberEventContent::new(
        Application::Call(CallApplicationContent::new(
            String::new(), // call_id: empty string = legacy "unnamed" m.call session
            CallScope::Room,
        )),
        device_id.clone(),
        ActiveFocus::Livekit(ActiveLivekitFocus::new()),
        foci_preferred,
        None,  // created_ts: None on initial join
        None,  // expires: None → defaults to 4 hours
    );

    tracing::info!(
        state_key = %state_key.as_ref(),
        "posting m.call.member join"
    );

    room.send_state_event_for_key(&state_key, content)
        .await
        .context("posting m.call.member join event")?;

    tracing::info!("bridge participant posted to call");
    Ok(())
}

/// Post an empty membership (leave) for the bridge bot.
///
/// Element Call removes the participant from the call view when it receives
/// an Empty (or expired) membership for their state key. UNUSED for the same
/// reason as [`join`] — the subscriber posts no membership.
#[allow(dead_code)]
pub async fn leave(
    room: &Room,
    user_id: &OwnedUserId,
    device_id: &OwnedDeviceId,
) -> Result<()> {
    let member_id = format!("{device_id}_m.call");
    let state_key = CallMemberStateKey::new(user_id.clone(), Some(member_id), true);
    let content = CallMemberEventContent::new_empty(None);

    tracing::info!(state_key = %state_key.as_ref(), "posting m.call.member leave");

    room.send_state_event_for_key(&state_key, content)
        .await
        .context("posting m.call.member leave event")?;

    tracing::info!("bridge participant removed from call");
    Ok(())
}
