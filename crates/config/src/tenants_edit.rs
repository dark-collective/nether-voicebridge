//! Programmatic edits to `tenants.toml` (#4).
//!
//! The management bot mutates the tenant set by rewriting the desired-state
//! file and asking the supervisor to reconcile — the file stays the single
//! source of truth (`crate::tenants`). Edits go through `toml_edit` so operator
//! comments and hand-formatting survive a bot edit, and every write is atomic
//! (tmp + rename) so a crash mid-write can never leave a half file for the
//! next reload to reject.
//!
//! Each mutation returns the file's PRIOR content so the caller can roll the
//! file back if the supervisor rejects the reconcile (all-or-nothing): without
//! the rollback, a rejected edit would leave `tenants.toml` lying about the
//! running set and poison every later SIGHUP.

use std::path::Path;
use toml_edit::{DocumentMut, Item, Table, value};

use crate::ConfigError;

/// The fields of one bot-created tenant entry. Deliberately minimal — the
/// entry inherits everything else from its `[server.<name>]` profile, exactly
/// like a hand-written one.
#[derive(Debug, Clone)]
pub struct NewTenant {
    pub name: String,
    pub server: String,
    pub room_id: String,
    pub guild_id: String,
    pub channel_id: String,
    /// Owner record (#47): the MXID that issued `link`. Written as `owner` on
    /// the entry so `ban owner` can match it later.
    pub owner: Option<String>,
    /// Creation time, unix epoch seconds. Written as `created` on the entry.
    pub created: Option<i64>,
}

/// Prior state of the tenants file, for rollback: `None` = the file did not
/// exist.
pub type PriorContent = Option<String>;

/// A tenant removed by [`remove_tenant_entries_matching`]: `(name, discord
/// channel_id)` — the channel id feeds the unlink disclosure notice (#64).
pub type RemovedTenant = (String, Option<String>);

/// Append a `[[bridge]]` entry for `t` to the tenants file (creating the file
/// if absent). Fails without touching the file when an entry named `t.name`
/// already exists. Returns the prior content for [`restore_tenants_file`].
pub fn add_tenant_entry(path: &str, t: &NewTenant) -> Result<PriorContent, ConfigError> {
    let prior = read_prior(path)?;
    let mut doc = parse_doc(prior.as_deref())?;

    if find_entry(&doc, &t.name).is_some() {
        return Err(ConfigError::Validation(format!(
            "tenants file already has a bridge named '{}'",
            t.name
        )));
    }

    let mut entry = Table::new();
    entry["name"] = value(&t.name);
    entry["server"] = value(&t.server);
    entry["room_id"] = value(&t.room_id);
    if let Some(owner) = &t.owner {
        entry["owner"] = value(owner);
    }
    if let Some(created) = t.created {
        entry["created"] = value(created);
    }
    let mut discord = Table::new();
    discord["guild_id"] = value(&t.guild_id);
    discord["channel_id"] = value(&t.channel_id);
    entry["discord"] = Item::Table(discord);

    // `[[bridge]]` array-of-tables; created on first use.
    let bridges = doc
        .entry("bridge")
        .or_insert(Item::ArrayOfTables(toml_edit::ArrayOfTables::new()));
    let Some(arr) = bridges.as_array_of_tables_mut() else {
        return Err(ConfigError::Validation(
            "tenants file has a non-array `bridge` key — fix it by hand".to_string(),
        ));
    };
    arr.push(entry);

    write_atomic(path, &doc.to_string())?;
    Ok(prior)
}

/// Remove the `[[bridge]]` entry named `name`. Fails without touching the file
/// when no such entry exists. Returns the prior content for
/// [`restore_tenants_file`].
pub fn remove_tenant_entry(path: &str, name: &str) -> Result<PriorContent, ConfigError> {
    let prior = read_prior(path)?;
    let mut doc = parse_doc(prior.as_deref())?;

    let Some(idx) = find_entry(&doc, name) else {
        return Err(ConfigError::Validation(format!(
            "tenants file has no bridge named '{name}'"
        )));
    };
    // find_entry only returns Some when `bridge` is an array-of-tables.
    doc["bridge"].as_array_of_tables_mut().unwrap().remove(idx);

    write_atomic(path, &doc.to_string())?;
    Ok(prior)
}

/// Remove every `[[bridge]]` entry matching `pred` (#47 ban teardown: ban a
/// guild/room/owner and every tenant it matches is removed in one atomic
/// write, then reconciled away). `pred` sees each entry's `(owner, guild_id,
/// room_id)`, any of which may be absent on hand-written entries. Returns the
/// removed entries as `(name, discord channel_id)` — the channel id feeds the
/// unlink disclosure notice (#64) — plus the prior content for
/// [`restore_tenants_file`]. The file is not rewritten when nothing matches.
pub fn remove_tenant_entries_matching(
    path: &str,
    pred: impl Fn(Option<&str>, Option<&str>, Option<&str>) -> bool,
) -> Result<(Vec<RemovedTenant>, PriorContent), ConfigError> {
    let prior = read_prior(path)?;
    let mut doc = parse_doc(prior.as_deref())?;

    let mut removed = Vec::new();
    if let Some(arr) = doc.get_mut("bridge").and_then(|b| b.as_array_of_tables_mut()) {
        let mut idx = 0;
        while idx < arr.len() {
            let t = arr.get(idx).expect("idx < len");
            let owner = t.get("owner").and_then(|v| v.as_str());
            let guild = t
                .get("discord")
                .and_then(|d| d.as_table_like())
                .and_then(|d| d.get("guild_id"))
                .and_then(|v| v.as_str());
            let room = t.get("room_id").and_then(|v| v.as_str());
            if pred(owner, guild, room) {
                let chan = t
                    .get("discord")
                    .and_then(|d| d.as_table_like())
                    .and_then(|d| d.get("channel_id"))
                    .and_then(|v| v.as_str())
                    .map(str::to_owned);
                removed.push((
                    t.get("name").and_then(|v| v.as_str()).unwrap_or("<unnamed>").to_string(),
                    chan,
                ));
                arr.remove(idx);
            } else {
                idx += 1;
            }
        }
    }

    if !removed.is_empty() {
        write_atomic(path, &doc.to_string())?;
    }
    Ok((removed, prior))
}

/// Owner records for every `[[bridge]]` entry, `(name, owner)` in file order.
/// `owner` is `None` when the entry predates owner records or was hand-written
/// without one. Read-only; a missing file is an empty list. Open registration
/// (#43) keys `unlink`/`list` scoping and the per-owner cap off this.
pub fn tenant_owners(path: &str) -> Result<Vec<(String, Option<String>)>, ConfigError> {
    let prior = read_prior(path)?;
    let doc = parse_doc(prior.as_deref())?;
    let mut out = Vec::new();
    if let Some(arr) = doc.get("bridge").and_then(|b| b.as_array_of_tables()) {
        for t in arr {
            let name =
                t.get("name").and_then(|v| v.as_str()).unwrap_or("<unnamed>").to_string();
            let owner = t.get("owner").and_then(|v| v.as_str()).map(str::to_owned);
            out.push((name, owner));
        }
    }
    Ok(out)
}

/// The Discord guild id recorded on every `[[bridge]]` entry, `(name, guild)`
/// in file order. `guild` is `None` on a hand-written entry lacking a
/// `[bridge.discord]` guild_id. Read-only; a missing file is an empty list. The
/// per-guild tenant cap (#77) counts off this.
pub fn tenant_guilds(path: &str) -> Result<Vec<(String, Option<String>)>, ConfigError> {
    let prior = read_prior(path)?;
    let doc = parse_doc(prior.as_deref())?;
    let mut out = Vec::new();
    if let Some(arr) = doc.get("bridge").and_then(|b| b.as_array_of_tables()) {
        for t in arr {
            let name =
                t.get("name").and_then(|v| v.as_str()).unwrap_or("<unnamed>").to_string();
            let guild = t
                .get("discord")
                .and_then(|d| d.as_table_like())
                .and_then(|d| d.get("guild_id"))
                .and_then(|v| v.as_str())
                .map(str::to_owned);
            out.push((name, guild));
        }
    }
    Ok(out)
}

/// The Matrix `room_id` of every `[[bridge]]` tenant entry, in file order (#87).
/// Read-only; a missing file is an empty list. The management-bot maintenance
/// sweep unions this with the running-bridge set so it never leaves a configured
/// tenant's room even when that bridge is momentarily not running.
pub fn tenant_rooms(path: &str) -> Result<Vec<String>, ConfigError> {
    let prior = read_prior(path)?;
    let doc = parse_doc(prior.as_deref())?;
    let mut out = Vec::new();
    if let Some(arr) = doc.get("bridge").and_then(|b| b.as_array_of_tables()) {
        for t in arr {
            if let Some(room) = t.get("room_id").and_then(|v| v.as_str()) {
                out.push(room.to_string());
            }
        }
    }
    Ok(out)
}

/// The Discord channel id recorded on the `[[bridge]]` entry named `name`
/// (`None` = no such entry, or a hand-written entry without one). Read-only;
/// the unlink disclosure notice (#64) targets this channel.
pub fn tenant_discord_channel(path: &str, name: &str) -> Result<Option<String>, ConfigError> {
    let prior = read_prior(path)?;
    let doc = parse_doc(prior.as_deref())?;
    let Some(idx) = find_entry(&doc, name) else { return Ok(None) };
    let chan = doc["bridge"]
        .as_array_of_tables()
        .and_then(|arr| arr.get(idx))
        .and_then(|t| t.get("discord"))
        .and_then(|d| d.as_table_like())
        .and_then(|d| d.get("channel_id"))
        .and_then(|v| v.as_str())
        .map(str::to_owned);
    Ok(chan)
}

/// Roll the tenants file back to `prior` (the return value of a mutation):
/// rewrite the old content, or delete the file if it did not exist before.
pub fn restore_tenants_file(path: &str, prior: PriorContent) -> Result<(), ConfigError> {
    match prior {
        Some(content) => write_atomic(path, &content),
        None => match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(ConfigError::Io {
                path: path.to_string(),
                source,
            }),
        },
    }
}

fn read_prior(path: &str) -> Result<PriorContent, ConfigError> {
    match std::fs::read_to_string(path) {
        Ok(c) => Ok(Some(c)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(ConfigError::Io {
            path: path.to_string(),
            source,
        }),
    }
}

fn parse_doc(content: Option<&str>) -> Result<DocumentMut, ConfigError> {
    content
        .unwrap_or_default()
        .parse::<DocumentMut>()
        .map_err(|e| ConfigError::Validation(format!("tenants file is not valid TOML: {e}")))
}

/// Index of the `[[bridge]]` entry whose `name` equals `name`, if any.
fn find_entry(doc: &DocumentMut, name: &str) -> Option<usize> {
    doc.get("bridge")?
        .as_array_of_tables()?
        .iter()
        .position(|t| t.get("name").and_then(|v| v.as_str()) == Some(name))
}

/// Write `content` to `path` atomically: tmp file in the same directory, then
/// rename over the target. Creates the parent directory if missing (first-ever
/// tenant on a fresh data_dir). Shared with the ban list (#47), which has the
/// same crash-mid-write concern.
pub(crate) fn write_atomic(path: &str, content: &str) -> Result<(), ConfigError> {
    let io_err = |source: std::io::Error| ConfigError::Io {
        path: path.to_string(),
        source,
    };
    let target = Path::new(path);
    if let Some(parent) = target.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(io_err)?;
        }
    }
    let tmp = target.with_extension("toml.tmp");
    std::fs::write(&tmp, content).map_err(io_err)?;
    std::fs::rename(&tmp, target).map_err(io_err)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TmpDir(std::path::PathBuf);
    impl TmpDir {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!(
                "nvb-tenants-edit-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
        fn file(&self) -> String {
            self.0.join("tenants.toml").to_string_lossy().into_owned()
        }
    }
    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn tenant(name: &str) -> NewTenant {
        NewTenant {
            name: name.to_string(),
            server: "nether".to_string(),
            room_id: format!("!{name}:nether.im"),
            guild_id: "111".to_string(),
            channel_id: "222".to_string(),
            owner: None,
            created: None,
        }
    }

    #[test]
    fn add_creates_file_and_loads_back() {
        let dir = TmpDir::new();
        let path = dir.file();
        let prior = add_tenant_entry(&path, &tenant("Guest Voice")).unwrap();
        assert!(prior.is_none(), "file should not have existed");

        let content = std::fs::read_to_string(&path).unwrap();
        // Round-trips through the real loader shape (name/server/room_id + discord).
        let parsed: crate::TenantsFile = toml::from_str(&content).unwrap();
        assert_eq!(parsed.bridges.len(), 1);
        assert_eq!(parsed.bridges[0].name.as_deref(), Some("Guest Voice"));
        assert_eq!(parsed.bridges[0].server.as_deref(), Some("nether"));
        assert_eq!(parsed.bridges[0].room_id, "!Guest Voice:nether.im");
        assert_eq!(parsed.bridges[0].discord.guild_id, "111");
    }

    #[test]
    fn add_preserves_comments_and_appends() {
        let dir = TmpDir::new();
        let path = dir.file();
        let hand_written = "# operator notes survive bot edits\n\n[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!a:x\"\n\n[bridge.discord]\nguild_id = \"1\"\nchannel_id = \"2\"\n";
        std::fs::write(&path, hand_written).unwrap();

        let prior = add_tenant_entry(&path, &tenant("b")).unwrap();
        assert_eq!(prior.as_deref(), Some(hand_written));

        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("# operator notes survive bot edits"));
        let parsed: crate::TenantsFile = toml::from_str(&content).unwrap();
        assert_eq!(parsed.bridges.len(), 2);
        assert_eq!(parsed.bridges[1].name.as_deref(), Some("b"));
    }

    #[test]
    fn add_rejects_duplicate_name_without_touching_file() {
        let dir = TmpDir::new();
        let path = dir.file();
        add_tenant_entry(&path, &tenant("a")).unwrap();
        let before = std::fs::read_to_string(&path).unwrap();
        let err = add_tenant_entry(&path, &tenant("a")).unwrap_err();
        assert!(err.to_string().contains("already has a bridge named 'a'"));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), before);
    }

    #[test]
    fn remove_deletes_only_the_named_entry() {
        let dir = TmpDir::new();
        let path = dir.file();
        add_tenant_entry(&path, &tenant("a")).unwrap();
        add_tenant_entry(&path, &tenant("b")).unwrap();
        remove_tenant_entry(&path, "a").unwrap();
        let parsed: crate::TenantsFile =
            toml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(parsed.bridges.len(), 1);
        assert_eq!(parsed.bridges[0].name.as_deref(), Some("b"));
    }

    #[test]
    fn remove_unknown_name_errors_without_touching_file() {
        let dir = TmpDir::new();
        let path = dir.file();
        add_tenant_entry(&path, &tenant("a")).unwrap();
        let before = std::fs::read_to_string(&path).unwrap();
        let err = remove_tenant_entry(&path, "ghost").unwrap_err();
        assert!(err.to_string().contains("no bridge named 'ghost'"));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), before);
    }

    #[test]
    fn restore_rewrites_prior_content() {
        let dir = TmpDir::new();
        let path = dir.file();
        add_tenant_entry(&path, &tenant("a")).unwrap();
        let before = std::fs::read_to_string(&path).unwrap();
        let prior = add_tenant_entry(&path, &tenant("b")).unwrap();
        restore_tenants_file(&path, prior).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), before);
    }

    #[test]
    fn restore_none_deletes_the_created_file() {
        let dir = TmpDir::new();
        let path = dir.file();
        let prior = add_tenant_entry(&path, &tenant("a")).unwrap();
        restore_tenants_file(&path, prior).unwrap();
        assert!(!std::path::Path::new(&path).exists());
    }

    #[test]
    fn owner_record_round_trips() {
        let dir = TmpDir::new();
        let path = dir.file();
        let mut t = tenant("owned");
        t.owner = Some("@dark:nether.im".to_string());
        t.created = Some(1_780_000_000);
        add_tenant_entry(&path, &t).unwrap();

        let parsed: crate::TenantsFile =
            toml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(parsed.bridges[0].owner.as_deref(), Some("@dark:nether.im"));
        assert_eq!(parsed.bridges[0].created, Some(1_780_000_000));
        // Hand-written entries (no owner) still parse with None.
        add_tenant_entry(&path, &tenant("plain")).unwrap();
        let parsed: crate::TenantsFile =
            toml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(parsed.bridges[1].owner, None);
    }

    #[test]
    fn remove_matching_by_owner_guild_and_room() {
        let dir = TmpDir::new();
        let path = dir.file();
        let mut a = tenant("a");
        a.owner = Some("@evil:x".to_string());
        let mut b = tenant("b");
        b.owner = Some("@fine:x".to_string());
        b.guild_id = "999".to_string();
        let c = tenant("c"); // no owner (hand-written shape)
        add_tenant_entry(&path, &a).unwrap();
        add_tenant_entry(&path, &b).unwrap();
        add_tenant_entry(&path, &c).unwrap();

        // Owner match removes only a; entries without an owner never match.
        let (removed, _prior) =
            remove_tenant_entries_matching(&path, |o, _, _| o == Some("@evil:x")).unwrap();
        assert_eq!(removed, vec![("a".to_string(), Some("222".to_string()))]);

        // Guild match removes b; c (guild 111) stays.
        let (removed, _prior) =
            remove_tenant_entries_matching(&path, |_, g, _| g == Some("999")).unwrap();
        assert_eq!(removed, vec![("b".to_string(), Some("222".to_string()))]);

        // No match: file untouched, empty result, prior still returned.
        let before = std::fs::read_to_string(&path).unwrap();
        let (removed, prior) =
            remove_tenant_entries_matching(&path, |_, _, r| r == Some("!nope:x")).unwrap();
        assert!(removed.is_empty());
        assert_eq!(prior.as_deref(), Some(before.as_str()));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), before);

        // Room match removes the rest.
        let (removed, _prior) =
            remove_tenant_entries_matching(&path, |_, _, r| r == Some("!c:nether.im")).unwrap();
        assert_eq!(removed, vec![("c".to_string(), Some("222".to_string()))]);
    }

    #[test]
    fn bot_written_file_passes_the_real_loader() {
        // End-to-end shape check: an entry written here resolves through
        // load_tenants with a profile, like any hand-written tenant.
        let dir = TmpDir::new();
        let path = dir.file();
        add_tenant_entry(&path, &tenant("Guest Voice")).unwrap();

        let profile: crate::ServerProfile = toml::from_str(
            "homeserver_url = \"https://nether.im\"\nsender_localpart = \"voicebridge\"\nappservice_id = \"voicebridge\"\nappservice_token = \"tok\"\n",
        )
        .unwrap();
        let servers = std::collections::HashMap::from([("nether".to_string(), profile)]);
        let got = crate::load_tenants(&path, &servers, "/data").unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].matrix.store_path, "/data/guest-voice");
        assert_eq!(got[0].matrix.bot_user_id, "@voicebridge_bot:nether.im");
    }

    #[test]
    fn tenant_owners_reads_records_and_tolerates_absences() {
        let dir = TmpDir::new();
        let path = dir.file();

        // Missing file ⇒ empty list, not an error.
        assert_eq!(tenant_owners(&path).unwrap(), vec![]);

        let mut owned = tenant("mine");
        owned.owner = Some("@me:nether.im".to_string());
        add_tenant_entry(&path, &owned).unwrap();
        // Hand-written / pre-owner-records entry: no owner field.
        add_tenant_entry(&path, &tenant("legacy")).unwrap();

        let got = tenant_owners(&path).unwrap();
        assert_eq!(
            got,
            vec![
                ("mine".to_string(), Some("@me:nether.im".to_string())),
                ("legacy".to_string(), None),
            ]
        );

        // The unlink-notice lookup (#64): channel id by name, None for unknown.
        assert_eq!(tenant_discord_channel(&path, "mine").unwrap(), Some("222".to_string()));
        assert_eq!(tenant_discord_channel(&path, "nope").unwrap(), None);
    }

    #[test]
    fn tenant_guilds_reads_guild_ids_for_the_per_guild_cap() {
        let dir = TmpDir::new();
        let path = dir.file();

        // Missing file ⇒ empty list, not an error (#77).
        assert_eq!(tenant_guilds(&path).unwrap(), vec![]);

        add_tenant_entry(&path, &tenant("a")).unwrap(); // guild 111
        let mut other = tenant("b");
        other.guild_id = "999".to_string();
        add_tenant_entry(&path, &other).unwrap();

        let got = tenant_guilds(&path).unwrap();
        assert_eq!(
            got,
            vec![
                ("a".to_string(), Some("111".to_string())),
                ("b".to_string(), Some("999".to_string())),
            ]
        );
        // The cap counts entries matching one guild.
        assert_eq!(got.iter().filter(|(_, g)| g.as_deref() == Some("111")).count(), 1);
    }
}
