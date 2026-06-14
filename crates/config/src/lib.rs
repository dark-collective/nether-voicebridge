#![allow(dead_code)] // secret fields are read in later phases

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file '{path}': {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("config validation error: {0}")]
    Validation(String),
}

#[derive(Debug, Deserialize)]
pub struct Config {
    /// One entry per bridged channel-pair. TOML `[[bridge]]` blocks map here.
    /// Each bridge is a fully independent pipeline (its own Discord/Matrix
    /// clients, tokens, room, and audio plumbing). Defaults to empty so a
    /// bridge-less config surfaces the friendly `validate()` error rather than a
    /// raw serde "missing field" parse failure.
    #[serde(rename = "bridge", default)]
    pub bridges: Vec<BridgeConfig>,
}

#[derive(Debug, Deserialize)]
pub struct BridgeConfig {
    /// Optional human label, used in logs/spans. Falls back to the index.
    #[serde(default)]
    pub name: Option<String>,
    pub discord: DiscordConfig,
    pub matrix: MatrixConfig,
    pub livekit: LiveKitConfig,
}

#[derive(Debug, Deserialize)]
pub struct DiscordConfig {
    pub guild_id: String,
    pub channel_id: String,
    pub listener_token: String,
    pub puppet_tokens: Vec<String>,
    /// When true, also write per-SSRC `capture_<ssrc>.wav` files (the Phase 3
    /// debugging artifact) alongside forwarding audio to Matrix. Off in
    /// production to avoid unbounded file growth.
    #[serde(default)]
    pub capture_wav: bool,
}

fn default_store_path() -> String {
    "./nether-store".to_string()
}

#[derive(Debug, Deserialize)]
pub struct MatrixConfig {
    pub homeserver_url: String,
    pub room_id: String,
    pub bot_user_id: String,
    /// The device ID associated with the access_token (from /_matrix/client/v3/account/whoami
    /// or recorded when the token was minted). Required by matrix-sdk to restore a session.
    pub device_id: String,
    pub access_token: String,
    pub appservice_id: String,
    pub appservice_token: String,
    /// Path to the matrix-sdk SQLite store (Olm crypto + state). Defaults to ./nether-store.
    #[serde(default = "default_store_path")]
    pub store_path: String,
}

fn default_media_e2ee() -> bool {
    false
}

#[derive(Debug, Deserialize)]
pub struct LiveKitConfig {
    pub lk_jwt_url: String,
    pub url: String,
    pub api_key: String,
    pub api_secret: String,
    /// Enable Element Call media E2EE (HKDF key exchange via to-device).
    /// Set to true only when the EC deployment uses frame encryption.
    /// Default: false (most deployments run without media E2EE).
    #[serde(default = "default_media_e2ee")]
    pub media_e2ee: bool,
}

impl Config {
    pub fn load(path: &str) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_string(),
            source,
        })?;
        toml::from_str(&content).map_err(ConfigError::Parse)
    }

    /// Validate cross-bridge invariants. Hard errors abort startup; softer
    /// likely-mistakes are logged as warnings.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.bridges.is_empty() {
            return Err(ConfigError::Validation(
                "config must define at least one [[bridge]]".to_string(),
            ));
        }

        // Hard: distinct store_path per bridge. NOTE: build_client currently uses
        // an in-memory matrix-sdk store and does not read store_path, so this is
        // forward-looking — but the moment the SQLite store is wired up, two
        // clients sharing one path would lock/corrupt, and the `./nether-store`
        // default makes that the collision a multi-bridge config falls into by
        // omission. Enforce uniqueness now so the config is correct ahead of time.
        let mut seen_stores = std::collections::HashSet::new();
        for b in &self.bridges {
            if !seen_stores.insert(b.matrix.store_path.as_str()) {
                return Err(ConfigError::Validation(format!(
                    "duplicate matrix store_path '{}' across bridges — each bridge \
                     needs its own store",
                    b.matrix.store_path
                )));
            }
        }

        // Soft: a room_id reused across bridges is almost always a copy-paste slip.
        let mut seen_rooms = std::collections::HashMap::new();
        for (i, b) in self.bridges.iter().enumerate() {
            if let Some(j) = seen_rooms.insert(b.matrix.room_id.as_str(), i) {
                tracing::warn!(
                    room_id = %b.matrix.room_id,
                    first = j,
                    second = i,
                    "duplicate Matrix room_id across bridges"
                );
            }
        }

        // Soft: two bridges sharing a Discord bot token would fight over one
        // gateway connection.
        let mut seen_tokens = std::collections::HashMap::new();
        for (i, b) in self.bridges.iter().enumerate() {
            for tok in std::iter::once(&b.discord.listener_token)
                .chain(b.discord.puppet_tokens.iter())
            {
                if let Some(j) = seen_tokens.insert(tok.clone(), i) {
                    tracing::warn!(
                        bridge = i,
                        other = j,
                        "duplicate Discord token reused across bridges"
                    );
                }
            }
        }

        Ok(())
    }

    pub fn log_redacted_summary(&self) {
        tracing::info!(bridge_count = self.bridges.len(), "config loaded");
        for b in &self.bridges {
            b.log_redacted_summary();
        }
    }
}

impl BridgeConfig {
    /// Display label for logs/spans: the configured `name`, else "unnamed".
    pub fn label(&self) -> String {
        self.name.clone().unwrap_or_else(|| "unnamed".to_string())
    }

    pub fn log_redacted_summary(&self) {
        tracing::info!(
            bridge = %self.label(),
            discord_guild_id = %self.discord.guild_id,
            discord_channel_id = %self.discord.channel_id,
            discord_listener_token = "[redacted]",
            discord_puppet_count = self.discord.puppet_tokens.len(),
            matrix_homeserver_url = %self.matrix.homeserver_url,
            matrix_room_id = %self.matrix.room_id,
            matrix_bot_user_id = %self.matrix.bot_user_id,
            matrix_device_id = %self.matrix.device_id,
            matrix_store_path = %self.matrix.store_path,
            matrix_access_token = "[redacted]",
            matrix_appservice_id = %self.matrix.appservice_id,
            matrix_appservice_token = "[redacted]",
            livekit_lk_jwt_url = %self.livekit.lk_jwt_url,
            livekit_url = %self.livekit.url,
            livekit_api_key = %self.livekit.api_key,
            livekit_api_secret = "[redacted]",
            "bridge config loaded"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One `[[bridge]]` block. `store` lets each test pick a distinct store_path
    /// so multi-bridge tests pass validation; `name` is emitted only when `Some`.
    fn bridge_block(name: Option<&str>, room: &str, store: &str, listener: &str) -> String {
        let name_line = match name {
            Some(n) => format!("name = \"{n}\"\n"),
            None => String::new(),
        };
        format!(
            r#"[[bridge]]
{name_line}[bridge.discord]
guild_id = "111"
channel_id = "222"
listener_token = "{listener}"
puppet_tokens = ["Bot def", "Bot ghi"]

[bridge.matrix]
homeserver_url = "https://nether.im"
room_id = "{room}"
bot_user_id = "@bridge:nether.im"
device_id = "BRIDGE01"
access_token = "syt_tok"
appservice_id = "voicebridge"
appservice_token = "hs_tok"
store_path = "{store}"

[bridge.livekit]
lk_jwt_url = "https://lk-jwt.nether.im"
url = "wss://livekit.nether.im"
api_key = "key"
api_secret = "secret"
"#
        )
    }

    #[test]
    fn parses_single_bridge_array() {
        let raw = bridge_block(Some("general"), "!room:nether.im", "./s1", "Bot abc");
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        assert_eq!(cfg.bridges.len(), 1);
        let b = &cfg.bridges[0];
        assert_eq!(b.name.as_deref(), Some("general"));
        assert_eq!(b.discord.puppet_tokens.len(), 2);
        assert_eq!(b.matrix.homeserver_url, "https://nether.im");
        assert_eq!(b.livekit.api_key, "key");
        cfg.validate().expect("should validate");
    }

    #[test]
    fn parses_two_bridges() {
        let raw = format!(
            "{}\n{}",
            bridge_block(Some("a"), "!a:nether.im", "./sa", "Bot a"),
            bridge_block(Some("b"), "!b:nether.im", "./sb", "Bot b"),
        );
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        assert_eq!(cfg.bridges.len(), 2);
        assert_eq!(cfg.bridges[0].matrix.room_id, "!a:nether.im");
        assert_eq!(cfg.bridges[1].matrix.room_id, "!b:nether.im");
        cfg.validate().expect("should validate");
    }

    #[test]
    fn name_is_optional() {
        let raw = bridge_block(None, "!room:nether.im", "./s1", "Bot abc");
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        assert!(cfg.bridges[0].name.is_none());
        assert_eq!(cfg.bridges[0].label(), "unnamed");
    }

    #[test]
    fn rejects_empty_bridges() {
        // No `[[bridge]]` at all → empty Vec → validation error.
        let cfg: Config = toml::from_str("").expect("empty parses to zero bridges");
        assert!(matches!(cfg.validate(), Err(ConfigError::Validation(_))));
    }

    #[test]
    fn rejects_duplicate_store_path() {
        let raw = format!(
            "{}\n{}",
            bridge_block(Some("a"), "!a:nether.im", "./dup", "Bot a"),
            bridge_block(Some("b"), "!b:nether.im", "./dup", "Bot b"),
        );
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        assert!(matches!(cfg.validate(), Err(ConfigError::Validation(_))));
    }

    #[test]
    fn duplicate_room_and_token_are_warnings_not_errors() {
        // Same room_id and same listener token across bridges → Ok (warn only).
        let raw = format!(
            "{}\n{}",
            bridge_block(Some("a"), "!same:nether.im", "./sa", "Bot same"),
            bridge_block(Some("b"), "!same:nether.im", "./sb", "Bot same"),
        );
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        cfg.validate().expect("dupes are warnings, not errors");
    }

    #[test]
    fn rejects_missing_section() {
        // A `[[bridge]]` missing its `[bridge.matrix]`/`[bridge.livekit]` sections.
        let raw = r#"
[[bridge]]
[bridge.discord]
guild_id = "111"
channel_id = "222"
listener_token = "Bot abc"
puppet_tokens = []
"#;
        assert!(toml::from_str::<Config>(raw).is_err());
    }
}
