//! Self-serve allowlist (#82): an invite-only middle tier for open registration.
//!
//! Persisted at `<data_dir>/self_serve_allow.toml` (`mxids = [...]`). The file's
//! EXISTENCE means restriction is ON — only the listed MXIDs (plus operators) may
//! use the self-serve tier when `open_registration` is on. Its ABSENCE means
//! unrestricted — every Matrix user may self-serve (the pre-#82 behavior). Seeded
//! once from `[management] self_serve_allow`, then edited live by the management
//! bot's `serve-allow` / `serve-remove` / `serve-open` commands. Writes are atomic
//! (tmp + rename), the same crash-safety contract as `tenants.toml`/`banlist.toml`.

use serde::{Deserialize, Serialize};

use crate::ConfigError;
use crate::tenants_edit::write_atomic;

/// The persisted allowlist. A missing file is *not* this (an empty struct) — it
/// is `None` from [`load_self_serve_allow`], meaning unrestricted.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct SelfServeAllow {
    #[serde(default)]
    pub mxids: Vec<String>,
}

/// Canonical location: `<data_dir>/self_serve_allow.toml`.
pub fn self_serve_allow_path(data_dir: &str) -> String {
    format!("{}/self_serve_allow.toml", data_dir.trim_end_matches('/'))
}

/// Load the allowlist. `Ok(None)` = file absent ⇒ **unrestricted**; `Ok(Some(list))`
/// = restriction ON, only these MXIDs may self-serve (a present-but-empty file ⇒
/// `Some(vec![])` ⇒ only operators).
pub fn load_self_serve_allow(path: &str) -> Result<Option<Vec<String>>, ConfigError> {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(ConfigError::Io { path: path.to_string(), source }),
    };
    let parsed: SelfServeAllow = toml::from_str(&content).map_err(|e| {
        ConfigError::Validation(format!("self-serve allowlist is not valid TOML: {e}"))
    })?;
    Ok(Some(parsed.mxids))
}

/// Write the allowlist file (turns restriction ON). Used for the config seed and
/// by the edit commands.
pub fn save_self_serve_allow(path: &str, mxids: &[String]) -> Result<(), ConfigError> {
    let list = SelfServeAllow { mxids: mxids.to_vec() };
    let content = toml::to_string_pretty(&list)
        .map_err(|e| ConfigError::Validation(format!("serializing self-serve allowlist: {e}")))?;
    write_atomic(path, &content)
}

/// Add `mxid`, creating the file (⇒ restriction ON) if it was absent. `Ok(true)`
/// = the list changed (added, and/or restriction newly enabled); `Ok(false)` =
/// already present in an existing file.
pub fn add_self_serve(path: &str, mxid: &str) -> Result<bool, ConfigError> {
    let mut list = load_self_serve_allow(path)?.unwrap_or_default();
    if list.iter().any(|m| m == mxid) {
        return Ok(false);
    }
    list.push(mxid.to_string());
    save_self_serve_allow(path, &list)?;
    Ok(true)
}

/// Remove `mxid`. The file PERSISTS when it existed (restriction stays ON even if
/// the list is now empty — removing the last entry does not silently re-open the
/// surface). `Ok(false)` when unrestricted (no file) or `mxid` was not listed.
pub fn remove_self_serve(path: &str, mxid: &str) -> Result<bool, ConfigError> {
    let Some(mut list) = load_self_serve_allow(path)? else {
        return Ok(false);
    };
    let before = list.len();
    list.retain(|m| m != mxid);
    if list.len() == before {
        return Ok(false);
    }
    save_self_serve_allow(path, &list)?;
    Ok(true)
}

/// Delete the allowlist file ⇒ unrestricted. `Ok(true)` if a file existed.
pub fn open_self_serve(path: &str) -> Result<bool, ConfigError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(ConfigError::Io { path: path.to_string(), source }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TmpDir(std::path::PathBuf);
    impl TmpDir {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!(
                "nvb-selfserve-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
        fn file(&self) -> String {
            self.0.join("self_serve_allow.toml").to_string_lossy().into_owned()
        }
    }
    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn absent_is_unrestricted_add_enables_restriction() {
        let dir = TmpDir::new();
        let p = dir.file();
        // Absent ⇒ None ⇒ unrestricted.
        assert_eq!(load_self_serve_allow(&p).unwrap(), None);
        // First add creates the file (restriction ON) and returns changed.
        assert!(add_self_serve(&p, "@a:x").unwrap());
        assert_eq!(load_self_serve_allow(&p).unwrap(), Some(vec!["@a:x".to_string()]));
        // Duplicate add is a no-op.
        assert!(!add_self_serve(&p, "@a:x").unwrap());
        assert!(add_self_serve(&p, "@b:x").unwrap());
    }

    #[test]
    fn remove_keeps_the_file_and_open_deletes_it() {
        let dir = TmpDir::new();
        let p = dir.file();
        add_self_serve(&p, "@a:x").unwrap();
        // Removing the LAST entry keeps the file (⇒ Some([]) ⇒ only operators),
        // it does not silently re-open to everyone.
        assert!(remove_self_serve(&p, "@a:x").unwrap());
        assert_eq!(load_self_serve_allow(&p).unwrap(), Some(vec![]));
        // Removing a non-listed / absent entry is a no-op.
        assert!(!remove_self_serve(&p, "@a:x").unwrap());
        // `serve-open` deletes the file ⇒ back to unrestricted.
        assert!(open_self_serve(&p).unwrap());
        assert_eq!(load_self_serve_allow(&p).unwrap(), None);
        assert!(!open_self_serve(&p).unwrap()); // already open
        // Remove on an unrestricted (absent) list is a no-op, not an error.
        assert!(!remove_self_serve(&p, "@a:x").unwrap());
    }
}
