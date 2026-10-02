//! Linked identities (ADR 0001): a *pairing* is a proven link between one Matrix
//! account and one Discord account — "these are the same person". Persisted at
//! `<data_dir>/pairings.toml` (atomic writes, same contract as `optout.toml`),
//! edited by the management bot's `pair`/`unpair` DM commands + the `/bridge pair`
//! slash command, and consulted live by both sides through [`PairingStore`].
//!
//! Cardinality: a Matrix account may pair several Discord accounts; a Discord
//! account pairs with at most one Matrix account. Pairings are per instance and
//! describe a person, not a room. A pairing on its own changes nothing — the
//! presence rule in [`crate::links`] is what acts on it.

use std::sync::{PoisonError, RwLock};

use serde::{Deserialize, Serialize};

use crate::ConfigError;
use crate::tenants_edit::write_atomic;

/// One proven Matrix ↔ Discord link.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Pairing {
    /// Full Matrix user id (`@alice:example.org`).
    pub matrix_user: String,
    /// Discord user id, stored as a string like every other id in the config.
    pub discord_user: String,
    /// Optional delegate account that acts for this person (receives notices on
    /// their behalf). Grants nothing else.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegate: Option<String>,
    /// Unix epoch seconds when the pairing was completed.
    #[serde(default)]
    pub created: i64,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct PairingsFile {
    #[serde(default, rename = "pairing")]
    pub pairings: Vec<Pairing>,
}

/// Canonical location: `<data_dir>/pairings.toml`.
pub fn pairings_path(data_dir: &str) -> String {
    format!("{}/pairings.toml", data_dir.trim_end_matches('/'))
}

pub fn load_pairings(path: &str) -> Result<PairingsFile, ConfigError> {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(PairingsFile::default()),
        Err(source) => return Err(ConfigError::Io { path: path.to_string(), source }),
    };
    toml::from_str(&content)
        .map_err(|e| ConfigError::Validation(format!("pairings file is not valid TOML: {e}")))
}

fn save(path: &str, file: &PairingsFile) -> Result<(), ConfigError> {
    let content = toml::to_string_pretty(file)
        .map_err(|e| ConfigError::Validation(format!("serializing pairings: {e}")))?;
    write_atomic(path, &content)
}

/// Why a pairing could not be added.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PairError {
    /// This Discord account is already paired with a *different* Matrix user.
    DiscordTakenBy(String),
    /// Exactly this pairing already exists.
    AlreadyPaired,
}

/// In-memory mirror of the pairings file, shared by every bridge in the process
/// (`Arc<PairingStore>`). Reads are lock-cheap and happen on every presence
/// change; writes go through the file first so a crash can't fork the two.
#[derive(Debug)]
pub struct PairingStore {
    path: Option<String>,
    inner: RwLock<Vec<Pairing>>,
}

impl PairingStore {
    /// Load from `path`; a missing file is an empty store.
    pub fn open(path: String) -> Result<Self, ConfigError> {
        let file = load_pairings(&path)?;
        Ok(Self { path: Some(path), inner: RwLock::new(file.pairings) })
    }

    /// A store that never touches disk (tests, instances without a data dir).
    pub fn in_memory() -> Self {
        Self { path: None, inner: RwLock::new(Vec::new()) }
    }

    pub fn len(&self) -> usize {
        self.inner.read().unwrap_or_else(PoisonError::into_inner).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The Matrix user a Discord account is paired with, if any.
    pub fn matrix_for_discord(&self, discord_user: u64) -> Option<String> {
        let s = discord_user.to_string();
        self.inner
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .find(|p| p.discord_user == s)
            .map(|p| p.matrix_user.clone())
    }

    /// Every Discord account paired with a Matrix user.
    pub fn discords_for_matrix(&self, matrix_user: &str) -> Vec<u64> {
        self.inner
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|p| p.matrix_user == matrix_user)
            .filter_map(|p| p.discord_user.parse().ok())
            .collect()
    }

    pub fn pairings_for_matrix(&self, matrix_user: &str) -> Vec<Pairing> {
        self.inner
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|p| p.matrix_user == matrix_user)
            .cloned()
            .collect()
    }

    /// Delegates named by any of this person's pairings (deduplicated).
    pub fn delegates_for_matrix(&self, matrix_user: &str) -> Vec<String> {
        let mut out: Vec<String> = self
            .inner
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|p| p.matrix_user == matrix_user)
            .filter_map(|p| p.delegate.clone())
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// Add a pairing (persisting first). Enforces the cardinality rule.
    pub fn add(&self, matrix_user: &str, discord_user: u64, created: i64) -> Result<(), PairErrorOrIo> {
        let s = discord_user.to_string();
        let mut guard = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        if let Some(existing) = guard.iter().find(|p| p.discord_user == s) {
            return Err(if existing.matrix_user == matrix_user {
                PairErrorOrIo::Pair(PairError::AlreadyPaired)
            } else {
                PairErrorOrIo::Pair(PairError::DiscordTakenBy(existing.matrix_user.clone()))
            });
        }
        let mut next = guard.clone();
        next.push(Pairing { matrix_user: matrix_user.to_string(), discord_user: s, delegate: None, created });
        self.persist(&next).map_err(PairErrorOrIo::Io)?;
        *guard = next;
        Ok(())
    }

    /// Remove one pairing. `Ok(false)` = it did not exist.
    pub fn remove(&self, matrix_user: &str, discord_user: u64) -> Result<bool, ConfigError> {
        let s = discord_user.to_string();
        let mut guard = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        let mut next = guard.clone();
        let before = next.len();
        next.retain(|p| !(p.matrix_user == matrix_user && p.discord_user == s));
        if next.len() == before {
            return Ok(false);
        }
        self.persist(&next)?;
        *guard = next;
        Ok(true)
    }

    /// Remove every pairing of a Matrix user. Returns how many were removed.
    pub fn remove_all_for_matrix(&self, matrix_user: &str) -> Result<usize, ConfigError> {
        let mut guard = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        let mut next = guard.clone();
        let before = next.len();
        next.retain(|p| p.matrix_user != matrix_user);
        let removed = before - next.len();
        if removed > 0 {
            self.persist(&next)?;
            *guard = next;
        }
        Ok(removed)
    }

    /// Set (or clear with `None`) the delegate on every pairing of a Matrix user.
    /// Returns how many pairings were updated (0 = the user has no pairings).
    pub fn set_delegate(&self, matrix_user: &str, delegate: Option<&str>) -> Result<usize, ConfigError> {
        let mut guard = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        let mut next = guard.clone();
        let mut n = 0;
        for p in next.iter_mut().filter(|p| p.matrix_user == matrix_user) {
            p.delegate = delegate.map(str::to_string);
            n += 1;
        }
        if n > 0 {
            self.persist(&next)?;
            *guard = next;
        }
        Ok(n)
    }

    fn persist(&self, pairings: &[Pairing]) -> Result<(), ConfigError> {
        match &self.path {
            Some(p) => save(p, &PairingsFile { pairings: pairings.to_vec() }),
            None => Ok(()),
        }
    }
}

/// `add` can fail on the rule or on disk.
#[derive(Debug)]
pub enum PairErrorOrIo {
    Pair(PairError),
    Io(ConfigError),
}

impl std::fmt::Display for PairErrorOrIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PairErrorOrIo::Pair(PairError::AlreadyPaired) => write!(f, "already paired"),
            PairErrorOrIo::Pair(PairError::DiscordTakenBy(m)) => {
                write!(f, "that Discord account is already paired with {m}")
            }
            PairErrorOrIo::Io(e) => write!(f, "{e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_path(name: &str) -> String {
        let dir = std::env::temp_dir().join(format!("nvb-pairings-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_dir_all(&dir);
        pairings_path(dir.to_str().unwrap())
    }

    #[test]
    fn add_lookup_remove_roundtrip_through_file() {
        let path = tmp_path("roundtrip");
        let store = PairingStore::open(path.clone()).unwrap();
        store.add("@alice:hs", 11, 1).unwrap();
        store.add("@alice:hs", 12, 2).unwrap();
        assert_eq!(store.matrix_for_discord(11).as_deref(), Some("@alice:hs"));
        assert_eq!(store.discords_for_matrix("@alice:hs"), vec![11, 12]);
        // Reopen from disk: same content.
        let again = PairingStore::open(path.clone()).unwrap();
        assert_eq!(again.discords_for_matrix("@alice:hs"), vec![11, 12]);
        assert!(again.remove("@alice:hs", 11).unwrap());
        assert!(!again.remove("@alice:hs", 11).unwrap());
        assert_eq!(PairingStore::open(path).unwrap().discords_for_matrix("@alice:hs"), vec![12]);
    }

    #[test]
    fn one_discord_account_pairs_with_at_most_one_matrix_user() {
        let store = PairingStore::in_memory();
        store.add("@alice:hs", 11, 1).unwrap();
        match store.add("@bob:hs", 11, 2) {
            Err(PairErrorOrIo::Pair(PairError::DiscordTakenBy(m))) => assert_eq!(m, "@alice:hs"),
            other => panic!("expected DiscordTakenBy, got {other:?}"),
        }
        match store.add("@alice:hs", 11, 3) {
            Err(PairErrorOrIo::Pair(PairError::AlreadyPaired)) => {}
            other => panic!("expected AlreadyPaired, got {other:?}"),
        }
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn delegate_applies_to_all_of_a_users_pairings() {
        let store = PairingStore::in_memory();
        store.add("@alice:hs", 11, 1).unwrap();
        store.add("@alice:hs", 12, 1).unwrap();
        assert_eq!(store.set_delegate("@alice:hs", Some("@host:hs")).unwrap(), 2);
        assert_eq!(store.delegates_for_matrix("@alice:hs"), vec!["@host:hs".to_string()]);
        assert_eq!(store.set_delegate("@nobody:hs", Some("@host:hs")).unwrap(), 0);
        assert_eq!(store.remove_all_for_matrix("@alice:hs").unwrap(), 2);
        assert!(store.is_empty());
    }

    #[test]
    fn missing_file_is_empty_store() {
        let store = PairingStore::open(tmp_path("missing")).unwrap();
        assert!(store.is_empty());
        assert_eq!(store.matrix_for_discord(1), None);
    }
}
