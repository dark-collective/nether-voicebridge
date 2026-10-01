//! Service accounts (ADR 0001 §3): a sidecar's own Matrix account sits in a room
//! call but represents nobody on Discord. The room's bridge authority approves it
//! *per room* (`relay allow @sidecar:hs` in the management DM); the bridge then
//! gives it no puppet and does not count it as a human. Persisted at
//! `<data_dir>/relays.toml`, mirrored in memory by [`ServiceAccounts`].

use std::sync::{PoisonError, RwLock};

use serde::{Deserialize, Serialize};

use crate::ConfigError;
use crate::tenants_edit::write_atomic;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ServiceApproval {
    pub room_id: String,
    pub user_id: String,
    /// Who approved it (room authority MXID, or "config" for hand-written entries).
    #[serde(default)]
    pub approved_by: String,
    #[serde(default)]
    pub created: i64,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct RelaysFile {
    #[serde(default, rename = "approval")]
    pub approvals: Vec<ServiceApproval>,
}

/// Canonical location: `<data_dir>/relays.toml`.
pub fn relays_path(data_dir: &str) -> String {
    format!("{}/relays.toml", data_dir.trim_end_matches('/'))
}

pub fn load_relays(path: &str) -> Result<RelaysFile, ConfigError> {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(RelaysFile::default()),
        Err(source) => return Err(ConfigError::Io { path: path.to_string(), source }),
    };
    toml::from_str(&content)
        .map_err(|e| ConfigError::Validation(format!("relays file is not valid TOML: {e}")))
}

#[derive(Debug)]
pub struct ServiceAccounts {
    path: Option<String>,
    inner: RwLock<Vec<ServiceApproval>>,
}

impl ServiceAccounts {
    pub fn open(path: String) -> Result<Self, ConfigError> {
        let file = load_relays(&path)?;
        Ok(Self { path: Some(path), inner: RwLock::new(file.approvals) })
    }

    pub fn in_memory() -> Self {
        Self { path: None, inner: RwLock::new(Vec::new()) }
    }

    pub fn is_service(&self, room_id: &str, user_id: &str) -> bool {
        self.inner
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(|a| a.room_id == room_id && a.user_id == user_id)
    }

    pub fn list(&self, room_id: &str) -> Vec<ServiceApproval> {
        self.inner
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|a| a.room_id == room_id)
            .cloned()
            .collect()
    }

    /// `Ok(false)` = already approved.
    pub fn approve(&self, room_id: &str, user_id: &str, approved_by: &str, created: i64) -> Result<bool, ConfigError> {
        let mut guard = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        if guard.iter().any(|a| a.room_id == room_id && a.user_id == user_id) {
            return Ok(false);
        }
        let mut next = guard.clone();
        next.push(ServiceApproval {
            room_id: room_id.to_string(),
            user_id: user_id.to_string(),
            approved_by: approved_by.to_string(),
            created,
        });
        self.persist(&next)?;
        *guard = next;
        Ok(true)
    }

    /// `Ok(false)` = was not approved.
    pub fn revoke(&self, room_id: &str, user_id: &str) -> Result<bool, ConfigError> {
        let mut guard = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        let mut next = guard.clone();
        let before = next.len();
        next.retain(|a| !(a.room_id == room_id && a.user_id == user_id));
        if next.len() == before {
            return Ok(false);
        }
        self.persist(&next)?;
        *guard = next;
        Ok(true)
    }

    fn persist(&self, approvals: &[ServiceApproval]) -> Result<(), ConfigError> {
        let Some(path) = &self.path else { return Ok(()) };
        let content = toml::to_string_pretty(&RelaysFile { approvals: approvals.to_vec() })
            .map_err(|e| ConfigError::Validation(format!("serializing relays: {e}")))?;
        write_atomic(path, &content)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn approve_is_per_room_and_revocable() {
        let s = ServiceAccounts::in_memory();
        assert!(s.approve("!a:hs", "@side:hs", "@owner:hs", 1).unwrap());
        assert!(!s.approve("!a:hs", "@side:hs", "@owner:hs", 2).unwrap());
        assert!(s.is_service("!a:hs", "@side:hs"));
        assert!(!s.is_service("!b:hs", "@side:hs"));
        assert_eq!(s.list("!a:hs").len(), 1);
        assert!(s.revoke("!a:hs", "@side:hs").unwrap());
        assert!(!s.revoke("!a:hs", "@side:hs").unwrap());
        assert!(!s.is_service("!a:hs", "@side:hs"));
    }

    #[test]
    fn persists_and_reloads() {
        let dir = std::env::temp_dir().join(format!("nvb-relays-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let path = relays_path(dir.to_str().unwrap());
        ServiceAccounts::open(path.clone()).unwrap().approve("!a:hs", "@side:hs", "config", 1).unwrap();
        assert!(ServiceAccounts::open(path).unwrap().is_service("!a:hs", "@side:hs"));
    }
}
