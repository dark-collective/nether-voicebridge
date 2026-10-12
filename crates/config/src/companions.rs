//! Companion keys (ADR 0004 §3): bearer credentials the bridge issues to
//! companion tools. A key has an issuer, a scope (one bridged room, or `self` —
//! the issuer's own pairings) and the lease kinds it may request. Only the
//! SHA-256 of the secret is stored; the token is shown once when created.
//! Persisted at `<data_dir>/companions.toml` (atomic writes, same contract as
//! `pairings.toml`), mirrored in memory by [`CompanionKeys`].
//!
//! Hashing, token minting and the HTTP side live in `nvb-companion`; this module
//! is only the store.

use std::sync::{PoisonError, RwLock};

use serde::{Deserialize, Serialize};

use crate::ConfigError;
use crate::tenants_edit::write_atomic;

/// Lease kinds a key may be created with (ADR 0004 §2). Phase 1 serves only
/// `observe`; the others can already be issued so keys need not be re-created
/// when their leases land.
pub const KINDS: &[&str] = &["observe", "stream:attach", "stream:standalone", "listen:self"];

/// Kinds a room-scoped key may carry.
pub const ROOM_KINDS: &[&str] = &["observe", "stream:attach", "stream:standalone"];

/// Kinds a self-scoped key may carry (the issuer's own pairings).
pub const SELF_KINDS: &[&str] = &["stream:attach", "listen:self"];

/// A key's scope.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "type", content = "room_id", rename_all = "lowercase")]
pub enum KeyScope {
    /// One bridged Matrix room.
    Room(String),
    /// The issuer's own pairings.
    #[serde(rename = "self")]
    SelfScope,
}

impl KeyScope {
    pub fn room_id(&self) -> Option<&str> {
        match self {
            KeyScope::Room(r) => Some(r),
            KeyScope::SelfScope => None,
        }
    }

    pub fn describe(&self) -> String {
        match self {
            KeyScope::Room(r) => r.clone(),
            KeyScope::SelfScope => "self".into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CompanionKey {
    /// Public id (the part of the token before the dot).
    pub id: String,
    /// Lowercase hex SHA-256 of the secret part of the token.
    pub secret_sha256: String,
    /// MXID that created the key, or "config".
    pub issuer: String,
    pub kinds: Vec<String>,
    #[serde(default)]
    pub label: String,
    /// Unix epoch seconds.
    #[serde(default)]
    pub created: i64,
    /// Last, so it serializes as a sub-table after the plain values.
    pub scope: KeyScope,
}

impl CompanionKey {
    pub fn allows(&self, kind: &str) -> bool {
        self.kinds.iter().any(|k| k == kind)
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct CompanionsFile {
    #[serde(default, rename = "key")]
    pub keys: Vec<CompanionKey>,
}

/// Canonical location: `<data_dir>/companions.toml`.
pub fn companions_path(data_dir: &str) -> String {
    format!("{}/companions.toml", data_dir.trim_end_matches('/'))
}

pub fn load_companions(path: &str) -> Result<CompanionsFile, ConfigError> {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(CompanionsFile::default()),
        Err(source) => return Err(ConfigError::Io { path: path.to_string(), source }),
    };
    toml::from_str(&content)
        .map_err(|e| ConfigError::Validation(format!("companions file is not valid TOML: {e}")))
}

/// Check a requested kind list against a scope: every kind known, allowed for
/// the scope, at least one. Returns the de-duplicated list in [`KINDS`] order.
pub fn validate_kinds(scope: &KeyScope, kinds: &[String]) -> Result<Vec<String>, String> {
    if kinds.is_empty() {
        return Err(format!("name at least one kind: {}", KINDS.join(", ")));
    }
    let allowed = match scope {
        KeyScope::Room(_) => ROOM_KINDS,
        KeyScope::SelfScope => SELF_KINDS,
    };
    for k in kinds {
        if !KINDS.contains(&k.as_str()) {
            return Err(format!("unknown kind '{k}' (known: {})", KINDS.join(", ")));
        }
        if !allowed.contains(&k.as_str()) {
            return Err(format!(
                "kind '{k}' is not available for a {} key (allowed: {})",
                if scope.room_id().is_some() { "room" } else { "self" },
                allowed.join(", ")
            ));
        }
    }
    Ok(KINDS.iter().filter(|k| kinds.iter().any(|r| r == *k)).map(|k| k.to_string()).collect())
}

/// In-memory mirror of the companions file. Writes go through the file first so
/// a crash cannot fork the two.
#[derive(Debug)]
pub struct CompanionKeys {
    path: Option<String>,
    inner: RwLock<Vec<CompanionKey>>,
}

impl CompanionKeys {
    pub fn open(path: String) -> Result<Self, ConfigError> {
        let file = load_companions(&path)?;
        Ok(Self { path: Some(path), inner: RwLock::new(file.keys) })
    }

    pub fn in_memory() -> Self {
        Self { path: None, inner: RwLock::new(Vec::new()) }
    }

    fn persist(&self, keys: &[CompanionKey]) -> Result<(), ConfigError> {
        let Some(path) = &self.path else { return Ok(()) };
        let file = CompanionsFile { keys: keys.to_vec() };
        let content = toml::to_string_pretty(&file)
            .map_err(|e| ConfigError::Validation(format!("serializing companions: {e}")))?;
        write_atomic(path, &content)
    }

    /// Mutate a copy, persist it, then publish it — under the write lock so two
    /// writers cannot interleave.
    fn edit<R>(&self, f: impl FnOnce(&mut Vec<CompanionKey>) -> R) -> Result<R, ConfigError> {
        let mut g = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        let mut next = g.clone();
        let r = f(&mut next);
        if next != *g {
            self.persist(&next)?;
            *g = next;
        }
        Ok(r)
    }

    pub fn add(&self, key: CompanionKey) -> Result<(), ConfigError> {
        self.edit(|v| {
            v.retain(|k| k.id != key.id);
            v.push(key);
        })
    }

    /// Remove one key; returns it if it existed.
    pub fn remove(&self, id: &str) -> Result<Option<CompanionKey>, ConfigError> {
        self.edit(|v| {
            let pos = v.iter().position(|k| k.id == id)?;
            Some(v.remove(pos))
        })
    }

    /// Remove every key scoped to `room_id` (its bridge was unlinked); returns them.
    pub fn remove_room(&self, room_id: &str) -> Result<Vec<CompanionKey>, ConfigError> {
        self.edit(|v| {
            let (gone, keep): (Vec<_>, Vec<_>) =
                v.drain(..).partition(|k| k.scope.room_id() == Some(room_id));
            *v = keep;
            gone
        })
    }

    pub fn get(&self, id: &str) -> Option<CompanionKey> {
        self.inner.read().unwrap_or_else(PoisonError::into_inner).iter().find(|k| k.id == id).cloned()
    }

    pub fn list(&self) -> Vec<CompanionKey> {
        self.inner.read().unwrap_or_else(PoisonError::into_inner).clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(id: &str, scope: KeyScope) -> CompanionKey {
        CompanionKey {
            id: id.into(),
            secret_sha256: "00".into(),
            issuer: "@a:hs".into(),
            scope,
            kinds: vec!["observe".into()],
            label: String::new(),
            created: 1,
        }
    }

    #[test]
    fn kinds_are_checked_against_scope() {
        let room = KeyScope::Room("!r:hs".into());
        assert_eq!(validate_kinds(&room, &["observe".into()]).unwrap(), vec!["observe"]);
        assert!(validate_kinds(&room, &["listen:self".into()]).is_err());
        assert!(validate_kinds(&KeyScope::SelfScope, &["observe".into()]).is_err());
        assert!(validate_kinds(&room, &["bogus".into()]).is_err());
        assert!(validate_kinds(&room, &[]).is_err());
        // De-duplicated, canonical order.
        assert_eq!(
            validate_kinds(&room, &["stream:attach".into(), "observe".into(), "observe".into()]).unwrap(),
            vec!["observe", "stream:attach"]
        );
    }

    #[test]
    fn store_round_trips_through_the_file() {
        let dir = std::env::temp_dir().join(format!("nvb-companions-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = companions_path(dir.to_str().unwrap());
        let _ = std::fs::remove_file(&path);
        let s = CompanionKeys::open(path.clone()).unwrap();
        s.add(key("k1", KeyScope::Room("!r:hs".into()))).unwrap();
        s.add(key("k2", KeyScope::SelfScope)).unwrap();
        s.add(key("k3", KeyScope::Room("!other:hs".into()))).unwrap();
        let again = CompanionKeys::open(path.clone()).unwrap();
        assert_eq!(again.list().len(), 3);
        assert_eq!(again.get("k2").unwrap().scope, KeyScope::SelfScope);
        let gone = again.remove_room("!r:hs").unwrap();
        assert_eq!(gone.len(), 1);
        assert!(again.remove("k3").unwrap().is_some());
        assert!(again.remove("k3").unwrap().is_none());
        let last = CompanionKeys::open(path.clone()).unwrap();
        assert_eq!(last.list().iter().map(|k| k.id.as_str()).collect::<Vec<_>>(), vec!["k2"]);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
