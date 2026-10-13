//! The `observe` lease's view of a room (ADR 0004 §6): represented members, call
//! activity, listener count, and whether the key's issuer is present as
//! themselves. Nothing else — no audio, no messages, nothing about the Discord
//! guild beyond the bridged channel.

use nvb_config::links::LinkRegistry;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Participant {
    pub discord_user_id: String,
    pub ghost_mxid: Option<String>,
    pub display_name: String,
    pub streaming: bool,
    /// Whether a `stream:attach` lease is attached to this ghost.
    pub attached: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub participants: Vec<Participant>,
    pub call_active: bool,
    pub listeners: usize,
    /// `Some` only when the issuer is a Matrix user (not a config-issued key).
    pub pairing_present: Option<bool>,
}

impl Snapshot {
    /// The room as the bridge sees it now. A room whose bridge is not running
    /// reads as empty and inactive.
    pub fn take(registry: &LinkRegistry, room_id: &str, issuer: &str, attached: &std::collections::HashSet<u64>) -> Self {
        let Some(board) = registry.board_for_room(room_id) else {
            return Self {
                participants: Vec::new(),
                call_active: false,
                listeners: 0,
                pairing_present: issuer.starts_with('@').then_some(false),
            };
        };
        let listeners = board.matrix_listeners();
        Self {
            participants: board
                .represented()
                .into_iter()
                .map(|m| Participant {
                    discord_user_id: m.discord_user_id.to_string(),
                    ghost_mxid: m.ghost_mxid,
                    display_name: m.display_name,
                    streaming: m.streaming,
                    attached: attached.contains(&m.discord_user_id),
                })
                .collect(),
            call_active: listeners > 0,
            listeners,
            pairing_present: issuer.starts_with('@').then(|| board.paired_present(issuer)),
        }
    }

    /// SSE events turning `prev` (None = nothing sent yet) into `self`, as
    /// `(event name, JSON data)`.
    pub fn events_since(&self, prev: Option<&Snapshot>) -> Vec<(&'static str, serde_json::Value)> {
        let mut out = Vec::new();
        if prev.is_none_or(|p| p.participants != self.participants) {
            out.push(("participants", serde_json::json!({ "participants": self.participants })));
        }
        if prev.is_none_or(|p| p.call_active != self.call_active) {
            out.push(("call_active", serde_json::json!({ "active": self.call_active })));
        }
        if prev.is_none_or(|p| p.listeners != self.listeners) {
            out.push(("listeners", serde_json::json!({ "count": self.listeners })));
        }
        if let Some(present) = self.pairing_present {
            if prev.is_none_or(|p| p.pairing_present != self.pairing_present) {
                out.push(("pairing_present", serde_json::json!({ "present": present })));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nvb_config::links::LinkShared;

    const ROOM: &str = "!room:hs";

    #[test]
    fn snapshot_follows_the_board_and_diffs_minimally() {
        let shared = LinkShared::in_memory();
        shared.pairings.add("@me:hs", 1, 0).unwrap();
        let board = shared.board_for(ROOM, &[], &[]);
        shared.registry.register("b", board.clone());
        board.set_ghost_naming("nvb_", "hs");

        let s0 = Snapshot::take(&shared.registry, ROOM, "@me:hs", &Default::default());
        assert!(!s0.call_active);
        let first = s0.events_since(None);
        assert_eq!(
            first.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
            vec!["participants", "call_active", "listeners", "pairing_present"]
        );
        assert!(s0.events_since(Some(&s0)).is_empty());

        board.set_discord_present(7, true);
        board.note_ghost(7, Some("Seven"));
        board.set_discord_streaming(7, true);
        let s1 = Snapshot::take(&shared.registry, ROOM, "@me:hs", &Default::default());
        let ev = s1.events_since(Some(&s0));
        assert_eq!(ev.len(), 1);
        assert_eq!(ev[0].0, "participants");
        assert_eq!(ev[0].1["participants"][0]["ghost_mxid"], "@nvb_7:hs");
        assert_eq!(ev[0].1["participants"][0]["discord_user_id"], "7");
        assert_eq!(ev[0].1["participants"][0]["streaming"], true);

        board.set_matrix_present("@me:hs", true);
        board.set_discord_present(1, true);
        let s2 = Snapshot::take(&shared.registry, ROOM, "@me:hs", &Default::default());
        let names: Vec<_> = s2.events_since(Some(&s1)).into_iter().map(|(n, _)| n).collect();
        assert_eq!(names, vec!["call_active", "listeners", "pairing_present"]);
        assert_eq!(s2.listeners, 1);

        // A config-issued key gets no pairing_present; an unknown room is empty.
        assert!(Snapshot::take(&shared.registry, ROOM, "config", &Default::default()).pairing_present.is_none());
        let none = Snapshot::take(&shared.registry, "!gone:hs", "@me:hs", &Default::default());
        assert!(none.participants.is_empty() && !none.call_active);
    }
}
