//! The runtime-provisioned bridge set (#45).
//!
//! `tenants.toml` lives under `data_dir` and holds `[[bridge]]` entries in the
//! exact same schema as the static config — it is the declarative desired state
//! for runtime-added bridges, reconciled by the supervisor on SIGHUP and loaded
//! once at boot. The file itself is the tenant store: all mutations happen by
//! (atomically) rewriting it, so there is no write-back path in the bridge.
//!
//! Differences from the static config, by design:
//! - every entry MUST carry an explicit `name` — the auto-derive leaves
//!   (`device_id`, `store_path`) and the supervisor's registry key on it, and
//!   positional `bridge-{i}` fallbacks renumber across add/remove;
//! - no `[server.*]` / `[puppets]` blocks — profile references resolve against
//!   the static config's `[server.<name>]` profiles (unknown top-level keys are
//!   rejected so a misplaced block errors instead of being silently ignored);
//! - cross-set uniqueness (against the static bridges and the live running set)
//!   is deliberately NOT checked here — that is the supervisor's job via
//!   [`validate_bridge_against`](crate::validate_bridge_against), which owns the
//!   live view.

use serde::Deserialize;
use std::collections::{HashMap, HashSet};

use crate::{
    BridgeConfig, ConfigError, ServerProfile, resolve_bridge_bot_user_id,
    resolve_bridge_device_id, resolve_bridge_server, resolve_bridge_store_path, slugify,
};

/// The parsed shape of `tenants.toml`: just `[[bridge]]` entries. Unknown
/// top-level keys (a stray `[server.x]` or `[puppets]`) are hard parse errors.
#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TenantsFile {
    #[serde(rename = "bridge", default)]
    pub bridges: Vec<BridgeConfig>,
}

/// Canonical location of the tenants file: `<data_dir>/tenants.toml`.
pub fn tenants_path(data_dir: &str) -> String {
    format!("{}/tenants.toml", data_dir.trim_end_matches('/'))
}

/// Load + resolve the tenants file. A missing file is an empty tenant set (the
/// normal state for non-hosting deployments); any other IO/parse/shape error
/// propagates — the caller decides whether that is fatal (boot) or a rejected
/// reload (SIGHUP).
pub fn load_tenants(
    path: &str,
    servers: &HashMap<String, ServerProfile>,
    data_dir: &str,
) -> Result<Vec<BridgeConfig>, ConfigError> {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(ConfigError::Io {
                path: path.to_string(),
                source,
            });
        }
    };
    parse_tenants(&content, path, servers, data_dir)
}

/// The tenant names currently in the file, WITHOUT profile resolution — for
/// display (the management bot's `list`) where the caller only needs to know
/// which entries exist, not their resolved identities. Missing file = empty.
pub fn list_tenant_names(path: &str) -> Result<Vec<String>, ConfigError> {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(ConfigError::Io {
                path: path.to_string(),
                source,
            });
        }
    };
    let file: TenantsFile = toml::from_str(&content).map_err(ConfigError::Parse)?;
    Ok(file
        .bridges
        .into_iter()
        .filter_map(|b| b.name)
        .collect())
}

/// Parse + resolve tenants from TOML content (separated from file IO for tests).
fn parse_tenants(
    content: &str,
    path: &str,
    servers: &HashMap<String, ServerProfile>,
    data_dir: &str,
) -> Result<Vec<BridgeConfig>, ConfigError> {
    let file: TenantsFile = toml::from_str(content).map_err(ConfigError::Parse)?;
    let mut names: HashSet<String> = HashSet::new();
    let mut out = Vec::with_capacity(file.bridges.len());
    for mut b in file.bridges {
        let Some(name) = b.name.clone().filter(|n| !n.is_empty()) else {
            return Err(ConfigError::Validation(format!(
                "tenants file '{path}': every [[bridge]] must set an explicit name \
                 (runtime-added bridges cannot use positional fallbacks)"
            )));
        };
        if !names.insert(name.clone()) {
            return Err(ConfigError::Validation(format!(
                "tenants file '{path}': duplicate bridge name '{name}'"
            )));
        }
        // Same resolution pipeline as `Config::load`, with the leaf always derived
        // from the (mandatory) name.
        let leaf = slugify(&name);
        resolve_bridge_server(&mut b, servers);
        resolve_bridge_bot_user_id(&mut b);
        resolve_bridge_device_id(&mut b, &leaf);
        resolve_bridge_store_path(&mut b, data_dir, &leaf);
        out.push(b);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile() -> HashMap<String, ServerProfile> {
        let toml = r#"
homeserver_url = "https://nether.im"
sender_localpart = "voicebridge"
appservice_id = "voicebridge"
appservice_token = "hs_tok"
"#;
        let p: ServerProfile = toml::from_str(toml).unwrap();
        HashMap::from([("nether".to_string(), p)])
    }

    const TENANT: &str = r#"
[[bridge]]
name = "Guest Voice"
server = "nether"
room_id = "!guest:nether.im"

[bridge.discord]
guild_id = "222"
channel_id = "333"
"#;

    #[test]
    fn resolves_profile_and_derives_identity_from_name() {
        let got = parse_tenants(TENANT, "t.toml", &profile(), "/data").unwrap();
        assert_eq!(got.len(), 1);
        let b = &got[0];
        assert_eq!(b.matrix.homeserver_url, "https://nether.im");
        assert_eq!(b.matrix.room_id, "!guest:nether.im");
        assert_eq!(b.matrix.bot_user_id, "@voicebridge_bot:nether.im");
        assert_eq!(b.matrix.device_id, "guest-voice");
        assert_eq!(b.matrix.store_path, "/data/guest-voice");
    }

    #[test]
    fn nameless_entry_is_rejected() {
        let toml = TENANT.replace("name = \"Guest Voice\"\n", "");
        let err = parse_tenants(&toml, "t.toml", &profile(), "/data").unwrap_err();
        assert!(
            err.to_string().contains("must set an explicit name"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn duplicate_names_are_rejected() {
        let toml = format!("{TENANT}\n{TENANT}");
        let err = parse_tenants(&toml, "t.toml", &profile(), "/data").unwrap_err();
        assert!(
            err.to_string().contains("duplicate bridge name 'Guest Voice'"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn missing_file_is_an_empty_tenant_set() {
        let got = load_tenants(
            "/nonexistent/nvb-test/tenants.toml",
            &profile(),
            "/data",
        )
        .unwrap();
        assert!(got.is_empty());
    }

    #[test]
    fn stray_server_block_is_a_parse_error() {
        let toml = format!("{TENANT}\n[server.rogue]\nhomeserver_url = \"https://x\"\n");
        let err = parse_tenants(&toml, "t.toml", &profile(), "/data").unwrap_err();
        assert!(matches!(err, ConfigError::Parse(_)), "unexpected error: {err}");
    }

    #[test]
    fn toml_syntax_error_surfaces_as_parse() {
        let err = parse_tenants("[[bridge", "t.toml", &profile(), "/data").unwrap_err();
        assert!(matches!(err, ConfigError::Parse(_)));
    }
}
