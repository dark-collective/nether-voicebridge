//! Scenario registry (23 scenarios). Each scenario is a static
//! [`ScenarioDef`]; with the #6 live variant (`steal_release_repromotion`,
//! which also has a hermetic twin in `puppet.rs`'s unit tests) every row of
//! the design doc's catalogue runs live. The #99 invite-lifecycle additions
//! (`mgmt_dm_e2ee`, `mgmt_invite_offline`, and the `KnownFail{#97}`
//! `mgmt_dm_pending_reap`) cover the DM surface the `@dau:tinier.town`
//! incident showed the suite was blind to.

pub mod audio;
pub mod invite_lifecycle;
pub mod lifecycle;
pub mod mgmt_dm_e2ee;
pub mod mgmt_self_serve;
pub mod room_misconfig;
pub mod moderation;
pub mod multi_bridge_isolation;
pub mod overflow_fcfs;
pub mod smoke;
pub mod steal_repromotion;

use crate::scenario::ScenarioDef;

pub fn all() -> Vec<&'static ScenarioDef> {
    vec![
        &smoke::SMOKE_OPEN_CLOSE,
        &mgmt_self_serve::MGMT_SELF_SERVE_LINK,
        &mgmt_dm_e2ee::MGMT_DM_E2EE,
        &invite_lifecycle::MGMT_INVITE_OFFLINE,
        &invite_lifecycle::MGMT_DM_PENDING_REAP,
        &room_misconfig::ROOM_MISCONFIG_NOTICE,
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
        &moderation::RECOVERY_NO_SPARE,
        &lifecycle::REAP_RELEASE,
        &lifecycle::LEAVE_GRACE_DEBOUNCE,
        &overflow_fcfs::OVERFLOW_FCFS_PROMOTION,
        &multi_bridge_isolation::MULTI_BRIDGE_ISOLATION,
        &steal_repromotion::STEAL_RELEASE_REPROMOTION,
    ]
}

pub fn by_name(name: &str) -> Option<&'static ScenarioDef> {
    all().into_iter().find(|d| d.name == name)
}
