//! Per-user bridge opt-out (#63, ToS follow-ups).
//!
//! A flat list of Discord user ids (numeric snowflakes, stored as strings like
//! every other id in the config) persisted at `<data_dir>/optout.toml` and
//! edited by the `/bridge optout` / `/bridge optin` slash commands. An
//! opted-out user is never forwarded Discord→Matrix and never gets a Matrix
//! ghost — enforced pool-side at the capture demux and at ghost minting.
//! Global per user (not per guild): opting out means "never bridge my voice",
//! everywhere this instance reaches. Writes are atomic (tmp + rename), same
//! crash-safety contract as `tenants.toml`.
//!
//! Also holds [`ApprovalOutcome`], the payload of the `/bridge approve` reply
//! oneshot (#64): it crosses the `nvb-discord` ↔ `nvb-matrix-rtc` boundary
//! through the binary's adapter untouched, so it must live in the one crate
//! both depend on.

use serde::{Deserialize, Serialize};

use crate::ConfigError;
use crate::tenants_edit::save_toml_list;

/// The persisted opt-out list. Default-empty; a missing file is an empty list.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct OptoutList {
    #[serde(default)]
    pub users: Vec<String>,
}

impl OptoutList {
    pub fn contains(&self, user_id: u64) -> bool {
        let s = user_id.to_string();
        self.users.iter().any(|v| v == &s)
    }
}

/// Canonical location of the opt-out list: `<data_dir>/optout.toml`.
pub fn optout_path(data_dir: &str) -> String {
    format!("{}/optout.toml", data_dir.trim_end_matches('/'))
}

/// Load the opt-out list; a missing file is an empty list (nobody opted out).
pub fn load_optout(path: &str) -> Result<OptoutList, ConfigError> {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(OptoutList::default()),
        Err(source) => {
            return Err(ConfigError::Io { path: path.to_string(), source });
        }
    };
    toml::from_str(&content)
        .map_err(|e| ConfigError::Validation(format!("opt-out list is not valid TOML: {e}")))
}

/// Add `user_id` to the opt-out list. `Ok(false)` = already opted out (file
/// untouched); `Ok(true)` = written.
pub fn add_optout(path: &str, user_id: u64) -> Result<bool, ConfigError> {
    let mut list = load_optout(path)?;
    if list.contains(user_id) {
        return Ok(false);
    }
    list.users.push(user_id.to_string());
    save(path, &list)?;
    Ok(true)
}

/// Remove `user_id` from the opt-out list. `Ok(false)` = was not opted out
/// (file untouched); `Ok(true)` = written.
pub fn remove_optout(path: &str, user_id: u64) -> Result<bool, ConfigError> {
    let mut list = load_optout(path)?;
    let s = user_id.to_string();
    let before = list.users.len();
    list.users.retain(|v| v != &s);
    if list.users.len() == before {
        return Ok(false);
    }
    save(path, &list)?;
    Ok(true)
}

fn save(path: &str, list: &OptoutList) -> Result<(), ConfigError> {
    save_toml_list(path, list, "opt-out list")
}

/// The `/bridge approve` reply payload (#64): the outcome text for the
/// interaction follow-up, plus an optional public disclosure notice for the
/// bridged voice channel's text chat. Plain data — the oneshot carrying it
/// passes through the binary's adapter untouched.
#[derive(Debug)]
pub struct ApprovalOutcome {
    /// Human-readable outcome, shown as the interaction response.
    pub text: String,
    /// Discord channel id to post `notice` into (the bridged voice channel),
    /// when the approval succeeded and the id parsed.
    pub notice_channel_id: Option<u64>,
    /// The public disclosure message.
    pub notice: Option<String>,
}

impl ApprovalOutcome {
    /// An outcome with no disclosure notice (refusals, errors).
    pub fn text_only(text: impl Into<String>) -> Self {
        Self { text: text.into(), notice_channel_id: None, notice: None }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TmpDir(std::path::PathBuf);
    impl TmpDir {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!(
                "nvb-optout-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
        fn file(&self) -> String {
            self.0.join("optout.toml").to_string_lossy().into_owned()
        }
    }
    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn missing_file_is_empty_and_round_trips() {
        let dir = TmpDir::new();
        let path = dir.file();
        assert!(!load_optout(&path).unwrap().contains(111));

        assert!(add_optout(&path, 111).unwrap());
        assert!(add_optout(&path, 222).unwrap());
        // Duplicate add is a no-op.
        assert!(!add_optout(&path, 111).unwrap());

        let list = load_optout(&path).unwrap();
        assert!(list.contains(111));
        assert!(list.contains(222));
        assert!(!list.contains(333));

        assert!(remove_optout(&path, 111).unwrap());
        assert!(!remove_optout(&path, 111).unwrap());
        assert!(!load_optout(&path).unwrap().contains(111));
        assert!(load_optout(&path).unwrap().contains(222));
    }
}
