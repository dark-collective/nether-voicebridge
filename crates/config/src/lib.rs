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
    /// The process-wide pool of Discord puppet bots, shared across all bridges.
    /// Bots are dynamically allocated to channels on demand (one mandatory
    /// "anchor" per active channel + dedicated puppets for individual Matrix
    /// speakers). TOML `[puppets]` maps here. Defaults to empty so a
    /// puppet-less config surfaces the friendly `validate()` error rather than a
    /// raw serde "missing field" parse failure.
    #[serde(default)]
    pub puppets: PuppetsConfig,
    /// One entry per bridged channel-pair. TOML `[[bridge]]` blocks map here.
    /// Each bridge is an independent Matrix/RTC pipeline (its own Matrix client,
    /// room, ghosts, and audio plumbing); the Discord puppet bots are shared
    /// from the `[puppets]` pool above. Defaults to empty so a bridge-less
    /// config surfaces the friendly `validate()` error rather than a raw serde
    /// "missing field" parse failure.
    #[serde(rename = "bridge", default)]
    pub bridges: Vec<BridgeConfig>,
}

/// The shared Discord puppet bot pool (process-wide, not per-bridge).
#[derive(Debug, Deserialize, Default)]
pub struct PuppetsConfig {
    /// One entry per puppet bot — each a SEPARATE Discord application with its
    /// own bot token. Pool size = max simultaneous dedicated Matrix speakers
    /// (across all channels); one bot per active channel is reserved as its
    /// capture anchor. Each bot must be invited to every guild it may serve.
    #[serde(default)]
    pub tokens: Vec<String>,
    /// Prefix prepended to puppet nicknames so Discord users can tell which
    /// participants are bridged from Matrix. Default `"[Matrix] "`; set to `""`
    /// to disable. Prefix + name is truncated to Discord's 32-char nickname cap.
    #[serde(default = "default_nickname_prefix")]
    pub nickname_prefix: String,
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
    /// When true, also write per-SSRC `capture_<ssrc>.wav` files (the Phase 3
    /// debugging artifact) alongside forwarding audio to Matrix. Off in
    /// production to avoid unbounded file growth.
    #[serde(default)]
    pub capture_wav: bool,
}

fn default_nickname_prefix() -> String {
    "[Matrix] ".to_string()
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
    /// Optional override for the lk-jwt-service endpoint. When absent (the
    /// default), it is auto-derived from the homeserver's `.well-known`
    /// MSC4143 `rtc_foci` (`focus.service_url`). Set this only to override that
    /// — e.g. a homeserver without `rtc_foci` or a non-Nether deploy.
    #[serde(default)]
    pub lk_jwt_url: Option<String>,
    /// Optional override for the LiveKit SFU WebSocket URL. When absent (the
    /// default), it is taken from the lk-jwt response (`jwt_resp.url`). Set this
    /// only to override that.
    #[serde(default)]
    pub url: Option<String>,
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

        // Hard: the shared pool must have at least one bot — every active
        // channel needs an anchor, so a pool of zero can bridge nothing.
        if self.puppets.tokens.is_empty() {
            return Err(ConfigError::Validation(
                "config must define at least one puppet token in [puppets]".to_string(),
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

        // Soft: a Discord bot token listed twice in the shared pool would build
        // two gateway connections for one bot identity, which Discord rejects.
        let mut seen_tokens = std::collections::HashMap::new();
        for (i, tok) in self.puppets.tokens.iter().enumerate() {
            if let Some(j) = seen_tokens.insert(tok.as_str(), i) {
                tracing::warn!(
                    index = i,
                    other = j,
                    "duplicate Discord token in [puppets] pool"
                );
            }
        }

        // Soft: same Discord voice channel bridged twice (a bot can only be in
        // one VC per guild — two bridges on one channel would fight over bots).
        let mut seen_channels = std::collections::HashMap::new();
        for (i, b) in self.bridges.iter().enumerate() {
            let key = (b.discord.guild_id.as_str(), b.discord.channel_id.as_str());
            if let Some(j) = seen_channels.insert(key, i) {
                tracing::warn!(
                    bridge = i,
                    other = j,
                    "duplicate Discord (guild, channel) across bridges"
                );
            }
        }

        Ok(())
    }

    pub fn log_redacted_summary(&self) {
        tracing::info!(
            bridge_count = self.bridges.len(),
            puppet_pool_size = self.puppets.tokens.len(),
            nickname_prefix = %self.puppets.nickname_prefix,
            "config loaded"
        );
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
            matrix_homeserver_url = %self.matrix.homeserver_url,
            matrix_room_id = %self.matrix.room_id,
            matrix_bot_user_id = %self.matrix.bot_user_id,
            matrix_device_id = %self.matrix.device_id,
            matrix_store_path = %self.matrix.store_path,
            matrix_access_token = "[redacted]",
            matrix_appservice_id = %self.matrix.appservice_id,
            matrix_appservice_token = "[redacted]",
            livekit_lk_jwt_url = ?self.livekit.lk_jwt_url,
            livekit_url = ?self.livekit.url,
            "bridge config loaded"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A shared `[puppets]` pool block. `tokens` are emitted verbatim.
    fn puppets_block(tokens: &[&str]) -> String {
        let toks = tokens
            .iter()
            .map(|t| format!("\"{t}\""))
            .collect::<Vec<_>>()
            .join(", ");
        format!("[puppets]\ntokens = [{toks}]\n\n")
    }

    /// One `[[bridge]]` block. `store`/`chan` let each test pick distinct values
    /// so multi-bridge tests pass validation; `name` is emitted only when `Some`.
    /// `[bridge.livekit]` deliberately omits the optional `lk_jwt_url`/`url`
    /// overrides so the default derive (`None`) path is exercised. Puppet tokens
    /// live in the shared `[puppets]` block, not here.
    fn bridge_block(name: Option<&str>, room: &str, store: &str, chan: &str) -> String {
        let name_line = match name {
            Some(n) => format!("name = \"{n}\"\n"),
            None => String::new(),
        };
        format!(
            r#"[[bridge]]
{name_line}[bridge.discord]
guild_id = "111"
channel_id = "{chan}"

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
media_e2ee = false
"#
        )
    }

    #[test]
    fn parses_pool_and_single_bridge() {
        let raw = format!(
            "{}{}",
            puppets_block(&["Bot abc", "Bot ghi"]),
            bridge_block(Some("general"), "!room:nether.im", "./s1", "222"),
        );
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        assert_eq!(cfg.puppets.tokens.len(), 2);
        assert_eq!(cfg.puppets.nickname_prefix, "[Matrix] ");
        assert_eq!(cfg.bridges.len(), 1);
        let b = &cfg.bridges[0];
        assert_eq!(b.name.as_deref(), Some("general"));
        assert_eq!(b.matrix.homeserver_url, "https://nether.im");
        // Optional override URLs absent ⇒ derive-by-default path.
        assert_eq!(b.livekit.lk_jwt_url, None);
        assert_eq!(b.livekit.url, None);
        cfg.validate().expect("should validate");
    }

    #[test]
    fn parses_two_bridges() {
        let raw = format!(
            "{}{}\n{}",
            puppets_block(&["Bot a", "Bot b"]),
            bridge_block(Some("a"), "!a:nether.im", "./sa", "222"),
            bridge_block(Some("b"), "!b:nether.im", "./sb", "333"),
        );
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        assert_eq!(cfg.bridges.len(), 2);
        assert_eq!(cfg.bridges[0].matrix.room_id, "!a:nether.im");
        assert_eq!(cfg.bridges[1].matrix.room_id, "!b:nether.im");
        cfg.validate().expect("should validate");
    }

    #[test]
    fn name_is_optional() {
        let raw = format!(
            "{}{}",
            puppets_block(&["Bot a"]),
            bridge_block(None, "!room:nether.im", "./s1", "222"),
        );
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        assert!(cfg.bridges[0].name.is_none());
        assert_eq!(cfg.bridges[0].label(), "unnamed");
    }

    #[test]
    fn rejects_empty_bridges() {
        // A pool but no `[[bridge]]` → empty Vec → validation error.
        let cfg: Config = toml::from_str(&puppets_block(&["Bot a"])).expect("parses");
        assert!(matches!(cfg.validate(), Err(ConfigError::Validation(_))));
    }

    #[test]
    fn rejects_empty_puppet_pool() {
        // Bridges but no puppet tokens → validation error (no anchors possible).
        let raw = bridge_block(Some("a"), "!a:nether.im", "./sa", "222");
        let cfg: Config = toml::from_str(&raw).expect("parses");
        assert!(matches!(cfg.validate(), Err(ConfigError::Validation(_))));
    }

    #[test]
    fn rejects_duplicate_store_path() {
        let raw = format!(
            "{}{}\n{}",
            puppets_block(&["Bot a"]),
            bridge_block(Some("a"), "!a:nether.im", "./dup", "222"),
            bridge_block(Some("b"), "!b:nether.im", "./dup", "333"),
        );
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        assert!(matches!(cfg.validate(), Err(ConfigError::Validation(_))));
    }

    #[test]
    fn duplicate_room_and_token_are_warnings_not_errors() {
        // Same room_id across bridges + a duplicate pool token → Ok (warn only).
        let raw = format!(
            "{}{}\n{}",
            puppets_block(&["Bot dup", "Bot dup"]),
            bridge_block(Some("a"), "!same:nether.im", "./sa", "222"),
            bridge_block(Some("b"), "!same:nether.im", "./sb", "333"),
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
"#;
        assert!(toml::from_str::<Config>(raw).is_err());
    }

    #[test]
    fn livekit_override_urls_parse_when_present() {
        // When set, the optional override URLs are honored (Some).
        let raw = r#"[puppets]
tokens = ["Bot abc"]

[[bridge]]
[bridge.discord]
guild_id = "111"
channel_id = "222"

[bridge.matrix]
homeserver_url = "https://nether.im"
room_id = "!r:nether.im"
bot_user_id = "@bridge:nether.im"
device_id = "BRIDGE01"
access_token = "syt_tok"
appservice_id = "voicebridge"
appservice_token = "hs_tok"
store_path = "./s1"

[bridge.livekit]
lk_jwt_url = "https://lk-jwt.example.org"
url = "wss://livekit.example.org"
"#;
        let cfg: Config = toml::from_str(raw).expect("parse failed");
        let lk = &cfg.bridges[0].livekit;
        assert_eq!(lk.lk_jwt_url.as_deref(), Some("https://lk-jwt.example.org"));
        assert_eq!(lk.url.as_deref(), Some("wss://livekit.example.org"));
    }
}
