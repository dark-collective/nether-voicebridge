//! Scenario registry. Each scenario is a static [`ScenarioDef`]; with the #7 and
//! #15 live variants this reaches the full 16-entry catalogue in the design doc
//! (the remaining unlisted row, #6 `steal_release_repromotion`, is hermetic-only
//! and lives in `puppet.rs`'s unit tests).

pub mod audio;
pub mod lifecycle;
pub mod mgmt_self_serve;
pub mod moderation;
pub mod multi_bridge_isolation;
pub mod overflow_fcfs;
pub mod smoke;

use crate::scenario::ScenarioDef;

pub fn all() -> Vec<&'static ScenarioDef> {
    vec![
        &smoke::SMOKE_OPEN_CLOSE,
        &mgmt_self_serve::MGMT_SELF_SERVE_LINK,
        &audio::AUDIO_M2D_UNENC,
        &audio::AUDIO_D2M_UNENC,
        &audio::AUDIO_M2D_ENC,
        &audio::AUDIO_D2M_ENC,
        &audio::AUDIO_M2D_ENC_FRESHJOIN,
        &audio::SHUTDOWN_DRAIN,
        &audio::LONG_GLITCH_SWEEP,
        &audio::FRESH_STORE_REKEY,
        &audio::GHOST_SESSION_ROTATION,
        &moderation::EXTERNAL_DISCONNECT_RECOVERY,
        &moderation::FORCE_MOVE_RECOVERY,
        &lifecycle::REAP_RELEASE,
        &lifecycle::LEAVE_GRACE_DEBOUNCE,
        &overflow_fcfs::OVERFLOW_FCFS_PROMOTION,
        &multi_bridge_isolation::MULTI_BRIDGE_ISOLATION,
    ]
}

pub fn by_name(name: &str) -> Option<&'static ScenarioDef> {
    all().into_iter().find(|d| d.name == name)
}
