//! The room's bridge record (MSC2346 `m.bridge`, plus the older
//! `uk.half-shot.bridge` that Element still reads). Clients use it to list the
//! bridge under room settings and to tell the bridge's people apart from
//! ordinary Matrix users: Vommet shows a Discord logo after their names and
//! trusts the mark because the room's own record names our bot.
//!
//! Content stays minimal on purpose: no guild or channel ids (the record is
//! readable by anyone who can read the room's state).

use matrix_sdk::ruma::UserId;
use serde_json::{json, Value};

/// Both event types carry the same content.
const EVENT_TYPES: [&str; 2] = ["m.bridge", "uk.half-shot.bridge"];

/// One record per bridge, keyed by the bridge, not by the Discord channel.
pub(crate) const STATE_KEY: &str = "nether-voicebridge";

/// The protocol id clients map to "Discord, through the voice bridge".
pub(crate) const PROTOCOL_ID: &str = "discord-voice";

pub(crate) fn content(bot: &UserId) -> Value {
    json!({
        "bridgebot": bot.as_str(),
        "protocol": { "id": PROTOCOL_ID, "displayname": "Discord voice" },
        "channel": { "id": "voice", "displayname": "Discord voice channel" },
    })
}

/// Post the record if it's missing or different. Best-effort: a room where the
/// bot lacks the power level only gets one log line saying what to change.
pub(crate) async fn publish(room: &matrix_sdk::Room, bot: &UserId) {
    let want = content(bot);
    let pls = match room.power_levels().await {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, "could not read power levels; not posting the room's bridge record");
            return;
        }
    };
    for ev in EVENT_TYPES {
        if !pls.user_can_send_state(bot, ev.into()) {
            tracing::info!(
                event_type = ev,
                "the bot may not post the room's bridge record; give it the power level for this event type \
                 (or set it to 0) so clients can show that voice-bridge people are from Discord"
            );
            continue;
        }
        if state_content(room, ev).await.as_ref() == Some(&want) {
            continue;
        }
        match room.send_state_event_raw(ev, STATE_KEY, want.clone()).await {
            Ok(_) => tracing::info!(event_type = ev, "posted the room's bridge record"),
            Err(e) => tracing::warn!(event_type = ev, error = %e, "failed to post the room's bridge record"),
        }
    }
}

async fn state_content(room: &matrix_sdk::Room, event_type: &str) -> Option<Value> {
    use matrix_sdk::deserialized_responses::RawAnySyncOrStrippedState;
    let raw = room.get_state_event(event_type.into(), STATE_KEY).await.ok().flatten()?;
    let RawAnySyncOrStrippedState::Sync(raw) = raw else { return None };
    let ev: Value = serde_json::from_str(raw.json().get()).ok()?;
    ev.get("content").cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_names_the_bot_and_protocol_only() {
        let bot = UserId::parse("@voicebridge:example.org").unwrap();
        let c = content(&bot);
        assert_eq!(c["bridgebot"], "@voicebridge:example.org");
        assert_eq!(c["protocol"]["id"], PROTOCOL_ID);
        assert_eq!(c["channel"]["id"], "voice");
        // Nothing beyond these three keys: no guild/channel ids leak into room state.
        assert_eq!(c.as_object().unwrap().len(), 3);
    }
}
