//! Failover notices (ADR 0001 §5): when a paired person's Discord half vanishes
//! while they are still in the call, they are bridged through a bot again and
//! should be told. The bridge side only *emits*; the management bot (when one
//! runs) drains the channel and sends the DMs, rate-limited per person.

use std::sync::OnceLock;

use nvb_config::links::LinkBoard;
use tokio::sync::mpsc;

/// One notice: who to tell (the person + their delegates) and about which room.
#[derive(Debug, Clone)]
pub struct LinkNotice {
    pub user: String,
    pub delegates: Vec<String>,
    pub room_id: String,
}

static SINK: OnceLock<mpsc::UnboundedSender<LinkNotice>> = OnceLock::new();

/// Install the drain (called once by whoever runs the management bot).
pub fn install_sink(tx: mpsc::UnboundedSender<LinkNotice>) {
    let _ = SINK.set(tx);
}

/// Emit a notice for `user` in the board's room. Logged and dropped without a sink.
pub fn emit(board: &LinkBoard, user: &str) {
    let notice = LinkNotice {
        user: user.to_string(),
        delegates: board.delegates_for(user),
        room_id: board.room_id().to_string(),
    };
    match SINK.get() {
        Some(tx) => {
            let _ = tx.send(notice);
        }
        None => tracing::info!(mxid = %user, room = %board.room_id(), "linked-identity failover (no management bot to notify): the member's Discord session dropped; bridged through a bot until it returns"),
    }
}
