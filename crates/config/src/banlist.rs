//! Ban list (#47 abuse controls).
//!
//! Three flat deny-lists — owner MXIDs, Discord guild ids, Matrix room ids —
//! persisted at `<data_dir>/banlist.toml` and edited by the management bot's
//! `ban`/`unban` commands. Enforcement is at admission (`link` rejects a
//! banned owner/guild/room before writing anything) plus a teardown sweep at
//! ban time (`remove_tenant_entries_matching` + reconcile), so the running set
//! never carries a banned tenant. Writes are atomic (tmp + rename), same
//! crash-safety contract as `tenants.toml`.
//!
//! Owner bans only match tenants that carry an `owner` record (bot-created
//! entries); hand-written entries are operator-authored and have no owner.

use serde::{Deserialize, Serialize};

use crate::ConfigError;
use crate::tenants_edit::write_atomic;

/// What a ban matches against. The string forms are the management-bot
/// command arguments (`ban owner @x:y`, `ban guild 123`, `ban room !r:y`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BanKind {
    /// An owner MXID (`owner` record on bot-created tenants + the `link` sender).
    Owner,
    /// A Discord guild id (numeric snowflake).
    Guild,
    /// A Matrix room id (`!localpart:domain`).
    Room,
}

impl BanKind {
    pub fn as_str(self) -> &'static str {
        match self {
            BanKind::Owner => "owner",
            BanKind::Guild => "guild",
            BanKind::Room => "room",
        }
    }

    /// Parse a command argument. `None` for anything else (caller replies usage).
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "owner" => Some(BanKind::Owner),
            "guild" => Some(BanKind::Guild),
            "room" => Some(BanKind::Room),
            _ => None,
        }
    }
}

/// The persisted deny-lists. All default-empty; a missing file is an empty list.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct BanList {
    #[serde(default)]
    pub owners: Vec<String>,
    #[serde(default)]
    pub guilds: Vec<String>,
    #[serde(default)]
    pub rooms: Vec<String>,
}

impl BanList {
    fn list(&self, kind: BanKind) -> &Vec<String> {
        match kind {
            BanKind::Owner => &self.owners,
            BanKind::Guild => &self.guilds,
            BanKind::Room => &self.rooms,
        }
    }

    fn list_mut(&mut self, kind: BanKind) -> &mut Vec<String> {
        match kind {
            BanKind::Owner => &mut self.owners,
            BanKind::Guild => &mut self.guilds,
            BanKind::Room => &mut self.rooms,
        }
    }

    pub fn contains(&self, kind: BanKind, value: &str) -> bool {
        self.list(kind).iter().any(|v| v == value)
    }

    pub fn is_empty(&self) -> bool {
        self.owners.is_empty() && self.guilds.is_empty() && self.rooms.is_empty()
    }
}

/// Canonical location of the ban list: `<data_dir>/banlist.toml`.
pub fn banlist_path(data_dir: &str) -> String {
    format!("{}/banlist.toml", data_dir.trim_end_matches('/'))
}

/// Load the ban list; a missing file is an empty list (nothing banned).
pub fn load_banlist(path: &str) -> Result<BanList, ConfigError> {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(BanList::default()),
        Err(source) => {
            return Err(ConfigError::Io { path: path.to_string(), source });
        }
    };
    toml::from_str(&content)
        .map_err(|e| ConfigError::Validation(format!("ban list is not valid TOML: {e}")))
}

/// Add `value` to the `kind` deny-list. `Ok(false)` = already banned (file
/// untouched); `Ok(true)` = written.
pub fn add_ban(path: &str, kind: BanKind, value: &str) -> Result<bool, ConfigError> {
    let mut bans = load_banlist(path)?;
    if bans.contains(kind, value) {
        return Ok(false);
    }
    bans.list_mut(kind).push(value.to_string());
    save(path, &bans)?;
    Ok(true)
}

/// Remove `value` from the `kind` deny-list. `Ok(false)` = was not banned
/// (file untouched); `Ok(true)` = written.
pub fn remove_ban(path: &str, kind: BanKind, value: &str) -> Result<bool, ConfigError> {
    let mut bans = load_banlist(path)?;
    let list = bans.list_mut(kind);
    let before = list.len();
    list.retain(|v| v != value);
    if list.len() == before {
        return Ok(false);
    }
    save(path, &bans)?;
    Ok(true)
}

fn save(path: &str, bans: &BanList) -> Result<(), ConfigError> {
    let content = toml::to_string_pretty(bans)
        .map_err(|e| ConfigError::Validation(format!("serializing ban list: {e}")))?;
    write_atomic(path, &content)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TmpDir(std::path::PathBuf);
    impl TmpDir {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!(
                "nvb-banlist-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
        fn file(&self) -> String {
            self.0.join("banlist.toml").to_string_lossy().into_owned()
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
        let bans = load_banlist(&path).unwrap();
        assert!(bans.is_empty());

        assert!(add_ban(&path, BanKind::Owner, "@evil:x").unwrap());
        assert!(add_ban(&path, BanKind::Guild, "123").unwrap());
        assert!(add_ban(&path, BanKind::Room, "!bad:x").unwrap());
        // Duplicate add is a no-op.
        assert!(!add_ban(&path, BanKind::Owner, "@evil:x").unwrap());

        let bans = load_banlist(&path).unwrap();
        assert!(bans.contains(BanKind::Owner, "@evil:x"));
        assert!(bans.contains(BanKind::Guild, "123"));
        assert!(bans.contains(BanKind::Room, "!bad:x"));
        assert!(!bans.contains(BanKind::Owner, "@fine:x"));
        // Kinds are separate namespaces.
        assert!(!bans.contains(BanKind::Guild, "@evil:x"));

        assert!(remove_ban(&path, BanKind::Guild, "123").unwrap());
        assert!(!remove_ban(&path, BanKind::Guild, "123").unwrap());
        assert!(!load_banlist(&path).unwrap().contains(BanKind::Guild, "123"));
    }

    #[test]
    fn ban_kind_parses_command_arguments() {
        assert_eq!(BanKind::parse("owner"), Some(BanKind::Owner));
        assert_eq!(BanKind::parse("GUILD"), Some(BanKind::Guild));
        assert_eq!(BanKind::parse("room"), Some(BanKind::Room));
        assert_eq!(BanKind::parse("channel"), None);
    }
}
