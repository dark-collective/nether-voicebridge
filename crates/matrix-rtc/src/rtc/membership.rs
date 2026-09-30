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
    MilliSecondsSinceUnixEpoch, OwnedDeviceId, OwnedUserId,
    events::call::member::{
        Application, CallApplicationContent, CallMemberEventContent, CallMemberStateKey, CallScope,
        Focus, LivekitFocus,
    },
};
use std::time::Duration;

use crate::rtc::focus::bridge_active_focus;
use crate::rtc::well_known::LivekitFocusInfo;

/// How long a posted membership is valid for (MSC4143 `expires`; Element Call's
/// default). Clients treat a membership as gone once `created_ts + expires` passes.
pub(crate) const MEMBERSHIP_EXPIRES: Duration = Duration::from_secs(4 * 3600);
/// How often a live membership is re-posted to push its expiry out. One hour
/// leaves three hours of slack for a homeserver hiccup.
pub(crate) const MEMBERSHIP_REFRESH: Duration = Duration::from_secs(3600);

/// The `expires` to post *now* so the membership stays valid for
/// [`MEMBERSHIP_EXPIRES`] from now while `created_ts` stays put (the age is what
/// the call-focus election is decided on, so it must never move).
///
/// Why: `expires` is relative to `created_ts`, not to the post. Pinning
/// `created_ts` across re-posts (0.3.8) meant a re-post kept the ORIGINAL expiry
/// — a Discord user who had been in the channel > 4 h stayed invisible to every
/// Matrix client even after the ghost re-posted (live: huxley, Light Voice,
/// 2026-09-30). Element Call does exactly this: fixed `created_ts`, growing
/// `expires`, periodic refresh. Shared by the ghosts and the bridge bot's own
/// E2EE roster membership.
pub(crate) fn membership_expires(created_ts_ms: u64, now_ms: u64) -> Duration {
    Duration::from_millis(now_ms.saturating_sub(created_ts_ms)) + MEMBERSHIP_EXPIRES
}

/// The bridge bot's join content: legacy `oldest_membership` (see
/// `rtc::focus::bridge_active_focus`) naming `focus`, with the given age and
/// validity. Pure, so the wire shape is unit-testable.
fn build_join_content(
    device_id: &OwnedDeviceId,
    focus: &LivekitFocusInfo,
    created_ts: Option<MilliSecondsSinceUnixEpoch>,
    expires: Option<Duration>,
) -> CallMemberEventContent {
    let foci_preferred = vec![Focus::Livekit(LivekitFocus::new(
        focus.alias.clone(),
        focus.service_url.clone(),
    ))];
    CallMemberEventContent::new(
        Application::Call(CallApplicationContent::new(
            String::new(), // call_id: empty string = legacy "unnamed" m.call session
            CallScope::Room,
        )),
        device_id.clone(),
        // `oldest_membership`: Commet skips any selection it doesn't know; a
        // multi-SFU client resolves us to the oldest member's SFU (`rtc::focus`).
        bridge_active_focus(),
        foci_preferred,
        created_ts,
        expires,
    )
}

/// Post the bridge bot's join membership state event.
///
/// `room`   — the Matrix room (must be joined)
/// `user_id` — the bridge bot's MXID
/// `device_id` — the bridge bot's Matrix device id
/// `focus`  — the LiveKit SFU focus discovered from `.well-known`
/// `created_ts` — the membership's fixed age for this activation; every
///   (re-)post computes `expires` from it so the roster entry stays valid for
///   [`MEMBERSHIP_EXPIRES`] from now (a call longer than 4 h used to drop the bot
///   from the roster — and with it the peers' key delivery).
///
/// Used only when `media_e2ee = true`: in an encrypted room Element Call / Commet
/// distribute per-sender media keys via to-device **only to roster members**, so
/// the otherwise-hidden subscriber must appear in the roster (as a silent,
/// non-publishing participant) to receive them. In the unencrypted case the
/// subscriber posts no membership and stays out of the call UI (see `rtc::run`).
/// Ghosts post membership via the raw CS-API in `rtc::ghost`, not this.
pub async fn join(
    room: &Room,
    user_id: &OwnedUserId,
    device_id: &OwnedDeviceId,
    focus: &LivekitFocusInfo,
    created_ts: MilliSecondsSinceUnixEpoch,
) -> Result<()> {
    // State-key member_id: "{device_id}_m.call"
    // This is the element-call-expected per-device-per-application format (ruma 0.34).
    let member_id = format!("{device_id}_m.call");
    let state_key = CallMemberStateKey::new(user_id.clone(), Some(member_id), true);

    let now_ms = u64::from(MilliSecondsSinceUnixEpoch::now().get());
    let expires = membership_expires(u64::from(created_ts.get()), now_ms);
    let content = build_join_content(device_id, focus, Some(created_ts), Some(expires));

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
/// an Empty (or expired) membership for their state key. Used in the
/// `media_e2ee = true` path to retract the membership posted by [`join`].
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expires_grows_with_age_so_created_ts_can_stay_fixed() {
        assert_eq!(membership_expires(1_000, 1_000), MEMBERSHIP_EXPIRES);
        let ten_h = 10 * 3600 * 1000;
        assert_eq!(membership_expires(1_000, 1_000 + ten_h), Duration::from_millis(ten_h) + MEMBERSHIP_EXPIRES);
        assert_eq!(membership_expires(5_000, 1_000), MEMBERSHIP_EXPIRES, "clock skew never underflows");
    }

    #[test]
    fn join_content_carries_fixed_created_ts_and_growing_expires() {
        let focus = LivekitFocusInfo { alias: "!r:x".into(), service_url: "https://lk".into() };
        let created = MilliSecondsSinceUnixEpoch(1_700_000_000_000u64.try_into().unwrap());
        let c = build_join_content(&"BRIDGE01".into(), &focus, Some(created), Some(Duration::from_millis(36_000_000)));
        let v = serde_json::to_value(&c).unwrap();
        assert_eq!(v["created_ts"], 1_700_000_000_000u64);
        assert_eq!(v["expires"], 36_000_000u64);
        assert_eq!(v["device_id"], "BRIDGE01");
        assert_eq!(v["focus_active"]["focus_selection"], "oldest_membership");
        assert_eq!(v["foci_preferred"][0]["livekit_service_url"], "https://lk");
    }
}

