//! Scenario registry. Each scenario is a static [`ScenarioDef`]; the D3 slice
//! grows this toward the full 16-entry catalogue in the design doc.

pub mod audio;
pub mod moderation;
pub mod smoke;

use crate::scenario::ScenarioDef;

pub fn all() -> Vec<&'static ScenarioDef> {
    vec![
        &smoke::SMOKE_OPEN_CLOSE,
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
    ]
}

pub fn by_name(name: &str) -> Option<&'static ScenarioDef> {
    all().into_iter().find(|d| d.name == name)
}
