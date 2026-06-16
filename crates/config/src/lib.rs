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
    /// Base directory under which each bridge's matrix-sdk store is auto-derived
    /// (`<data_dir>/<bridge>`) when its `matrix.store_path` is omitted. Defaults
    /// to `./nether-store`. Overridden at load time by the `NVB_DATA_DIR` env var
    /// — the Docker image sets `NVB_DATA_DIR=/app/data` so stores land on the
    /// persistent volume with no config. An explicit `matrix.store_path` always
    /// wins over the derived value.
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
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

fn default_data_dir() -> String {
    "./nether-store".to_string()
}

/// Lowercase a bridge name into a filesystem-safe leaf: alphanumerics kept,
/// every other run collapsed to a single `-`, and leading/trailing `-` trimmed.
/// Empty result (e.g. a name of only punctuation) falls back to `"bridge"`.
fn slugify(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut prev_dash = false;
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            prev_dash = false;
        } else if !prev_dash {
            out.push('-');
            prev_dash = true;
        }
    }
    let trimmed = out.trim_matches('-');
    if trimmed.is_empty() {
        "bridge".to_string()
    } else {
        trimmed.to_string()
    }
}

#[derive(Debug, Deserialize)]
pub struct MatrixConfig {
    pub homeserver_url: String,
    pub room_id: String,
    pub bot_user_id: String,
    /// The device ID for the bot's matrix-sdk session. In access_token mode this
    /// must match the device the token was minted for; in appservice-auth mode
    /// (no `access_token`) it is a stable device id we assert via appservice
    /// login, reused every start so the persistent crypto store stays valid.
    pub device_id: String,
    /// Regular-account access token. When omitted, the bridge self-provisions a
    /// device + token via appservice login (`m.login.application_service`,
    /// authenticated with `appservice_token`) — `bot_user_id` must then be an
    /// appservice-namespace user (e.g. the registration's `sender_localpart`).
    #[serde(default)]
    pub access_token: Option<String>,
    pub appservice_id: String,
    pub appservice_token: String,
    /// Path to the matrix-sdk SQLite store (Olm crypto + state). Optional: when
    /// omitted it is auto-derived at load time as `<data_dir>/<bridge>` (see
    /// `Config::resolve_store_paths`), so most configs never set it. An explicit
    /// value here always wins. The empty-string serde default is the "omitted"
    /// sentinel that `resolve_store_paths` fills in.
    #[serde(default)]
    pub store_path: String,
    /// Override the bridge bot's Matrix display name (shown in the Element Call
    /// roster when `media_e2ee` makes the subscriber a visible member). Defaults to
    /// "Nether Voice Bridge".
    #[serde(default)]
    pub bot_display_name: Option<String>,
    /// Override the bridge bot's avatar with a local image file. When absent, the
    /// project's committed default avatar is used. Uploaded once (persists as an
    /// `mxc://`), only if the bot has no avatar yet.
    #[serde(default)]
    pub bot_avatar_path: Option<String>,
}

/// How a ghost delivers its LiveKit frame key to the call's human members.
/// Only meaningful when `media_e2ee` resolves to true.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KeyTransport {
    /// Path 2 — an unencrypted `io.element.call.encryption_keys` to-device event
    /// masqueraded as the ghost. The homeserver can read the key; fine for an
    /// unencrypted room (Element Call has no media-E2EE security model there).
    Plaintext,
    /// Path 3 — the same content, Olm-encrypted per recipient device via the SDK's
    /// `encrypt_and_send_raw_to_device`. True end-to-end encryption: the homeserver
    /// cannot read the key. Each ghost becomes a full crypto-capable Matrix client
    /// (its own device + persistent crypto store). Use this for an encrypted room.
    Olm,
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
    /// Element Call media E2EE (HKDF frame encryption). **Optional override.**
    /// When omitted (the default), it is auto-detected from the room's
    /// `m.room.encryption` state at startup (`encrypted ⇒ true`). Set it
    /// explicitly only to force a value — e.g. `false` to keep a bridge plaintext
    /// even in an encrypted room. See `resolved_media_e2ee`.
    #[serde(default)]
    pub media_e2ee: Option<bool>,
    /// How ghosts deliver their media key when media E2EE is on. **Optional
    /// override.** When omitted it is auto-selected from the resolved
    /// `media_e2ee`: an encrypted room defaults to `"olm"` (true per-ghost E2EE,
    /// Path 3); otherwise `"plaintext"` (homeserver-readable Path 2). Set it to
    /// pin a transport. Ignored when media E2EE resolves to false. See
    /// `resolved_key_transport`.
    #[serde(default)]
    pub key_transport: Option<KeyTransport>,
}

impl LiveKitConfig {
    /// Resolve the effective media-E2EE setting: the explicit config override if
    /// present, else the value auto-detected from the room's `m.room.encryption`
    /// state (`detected`).
    pub fn resolved_media_e2ee(&self, detected: bool) -> bool {
        self.media_e2ee.unwrap_or(detected)
    }

    /// Resolve the effective key transport given the already-resolved
    /// `media_e2ee`: the explicit override if present, else auto — an encrypted
    /// (media-E2EE) room wants its key delivered E2EE too, so default to `Olm`;
    /// otherwise the homeserver-readable `Plaintext` (moot when E2EE is off).
    pub fn resolved_key_transport(&self, media_e2ee: bool) -> KeyTransport {
        self.key_transport.unwrap_or(if media_e2ee {
            KeyTransport::Olm
        } else {
            KeyTransport::Plaintext
        })
    }
}

impl Config {
    pub fn load(path: &str) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_string(),
            source,
        })?;
        let mut cfg: Config = toml::from_str(&content).map_err(ConfigError::Parse)?;
        // `NVB_DATA_DIR` (set to `/app/data` by the Docker image) overrides the
        // configured `data_dir`; fall back to it otherwise.
        let data_dir = std::env::var("NVB_DATA_DIR").unwrap_or_else(|_| cfg.data_dir.clone());
        cfg.resolve_store_paths(&data_dir);
        Ok(cfg)
    }

    /// Fill each bridge's omitted (empty) `matrix.store_path` with a value derived
    /// from `data_dir` and the bridge's identity, so a multi-bridge config never
    /// has to spell out per-bridge store paths (and never collides on the old
    /// shared `./nether-store` default). An explicit `store_path` is left as-is.
    ///
    /// The derived leaf is the slug of the bridge `name` when set, else
    /// `bridge-<index>` — both stable and unique per bridge. Two bridges sharing a
    /// `name` derive the same path; `validate()`'s duplicate-store_path check then
    /// turns that into a clear error pointing at the real mistake.
    pub fn resolve_store_paths(&mut self, data_dir: &str) {
        let base = data_dir.trim_end_matches('/');
        for (i, b) in self.bridges.iter_mut().enumerate() {
            if !b.matrix.store_path.is_empty() {
                continue;
            }
            let leaf = match &b.name {
                Some(name) => slugify(name),
                None => format!("bridge-{i}"),
            };
            b.matrix.store_path = format!("{base}/{leaf}");
        }
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

        // Hard: distinct store_path per bridge. `build_client` always opens a
        // persistent SQLite store at `store_path`, and with Olm key transport each
        // bridge's ghosts nest their own stores under it
        // (`<store_path>/ghosts/<localpart>`); two bridges sharing one path would
        // lock/corrupt. Auto-derivation (`resolve_store_paths`) already gives each
        // bridge a distinct `<data_dir>/<bridge>` path, so this normally only fires
        // for explicit collisions — e.g. two bridges sharing a `name` (identical
        // derived path) or hand-set duplicate paths. Enforce uniqueness either way.
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

        // Hard: every bridge's bot needs a way to authenticate — either a
        // regular-account access_token, or (when absent) appservice login, which
        // requires a non-empty appservice_token.
        for b in &self.bridges {
            let has_access_token = b
                .matrix
                .access_token
                .as_deref()
                .is_some_and(|t| !t.is_empty());
            if !has_access_token && b.matrix.appservice_token.is_empty() {
                return Err(ConfigError::Validation(format!(
                    "bridge bot '{}': no access_token and no appservice_token — \
                     cannot authenticate (provide one or the other)",
                    b.matrix.bot_user_id
                )));
            }
        }

        // Soft: explicitly pinning `key_transport = "olm"` while explicitly
        // forcing `media_e2ee = false` is contradictory — Olm transport is ignored
        // with E2EE off. (Both are now auto by default; only flag the deliberate
        // both-set-and-conflicting case, not the auto path.)
        for b in &self.bridges {
            if b.livekit.key_transport == Some(KeyTransport::Olm)
                && b.livekit.media_e2ee == Some(false)
            {
                tracing::warn!(
                    room_id = %b.matrix.room_id,
                    "key_transport = \"olm\" set but media_e2ee = false — Olm key transport \
                     is ignored; remove the media_e2ee override (auto-detect) or set it true"
                );
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
            matrix_bot_display_name = ?self.matrix.bot_display_name,
            matrix_bot_avatar_path = ?self.matrix.bot_avatar_path,
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

    /// A `[[bridge]]` block that OMITS `store_path` so the auto-derive path runs.
    /// Mirrors `bridge_block` minus the `store_path = ...` line.
    fn bridge_block_no_store(name: Option<&str>, room: &str, chan: &str) -> String {
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

[bridge.livekit]
media_e2ee = false
"#
        )
    }

    #[test]
    fn store_path_defaults_empty_and_data_dir_defaults() {
        // Omitted store_path parses to the empty sentinel; data_dir gets its default.
        let raw = format!(
            "{}{}",
            puppets_block(&["Bot a"]),
            bridge_block_no_store(Some("general"), "!r:nether.im", "222"),
        );
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        assert_eq!(cfg.data_dir, "./nether-store");
        assert_eq!(cfg.bridges[0].matrix.store_path, "");
    }

    #[test]
    fn resolve_derives_store_path_from_name() {
        let raw = format!(
            "{}{}",
            puppets_block(&["Bot a"]),
            bridge_block_no_store(Some("General Voice"), "!r:nether.im", "222"),
        );
        let mut cfg: Config = toml::from_str(&raw).expect("parse failed");
        cfg.resolve_store_paths("./nether-store");
        // Slugged + lowercased; spaces collapse to a single dash.
        assert_eq!(cfg.bridges[0].matrix.store_path, "./nether-store/general-voice");
        cfg.validate().expect("should validate");
    }

    #[test]
    fn resolve_respects_explicit_store_path() {
        // An explicit path is never overwritten by derivation.
        let raw = format!(
            "{}{}",
            puppets_block(&["Bot a"]),
            bridge_block(Some("general"), "!r:nether.im", "/custom/store", "222"),
        );
        let mut cfg: Config = toml::from_str(&raw).expect("parse failed");
        cfg.resolve_store_paths("/app/data");
        assert_eq!(cfg.bridges[0].matrix.store_path, "/custom/store");
    }

    #[test]
    fn resolve_uses_data_dir_override_and_trims_slash() {
        // The Docker case: NVB_DATA_DIR=/app/data (passed here as the arg). A
        // trailing slash on the base is normalised away.
        let raw = format!(
            "{}{}",
            puppets_block(&["Bot a"]),
            bridge_block_no_store(Some("gaming"), "!r:nether.im", "222"),
        );
        let mut cfg: Config = toml::from_str(&raw).expect("parse failed");
        cfg.resolve_store_paths("/app/data/");
        assert_eq!(cfg.bridges[0].matrix.store_path, "/app/data/gaming");
    }

    #[test]
    fn resolve_unnamed_bridges_get_distinct_indexed_paths() {
        let raw = format!(
            "{}{}\n{}",
            puppets_block(&["Bot a"]),
            bridge_block_no_store(None, "!a:nether.im", "222"),
            bridge_block_no_store(None, "!b:nether.im", "333"),
        );
        let mut cfg: Config = toml::from_str(&raw).expect("parse failed");
        cfg.resolve_store_paths("./nether-store");
        assert_eq!(cfg.bridges[0].matrix.store_path, "./nether-store/bridge-0");
        assert_eq!(cfg.bridges[1].matrix.store_path, "./nether-store/bridge-1");
        // Distinct → passes the uniqueness check.
        cfg.validate().expect("should validate");
    }

    #[test]
    fn resolve_same_named_bridges_collide_and_fail_validation() {
        // Two bridges sharing a name derive the same path → duplicate-store error.
        let raw = format!(
            "{}{}\n{}",
            puppets_block(&["Bot a"]),
            bridge_block_no_store(Some("dup"), "!a:nether.im", "222"),
            bridge_block_no_store(Some("dup"), "!b:nether.im", "333"),
        );
        let mut cfg: Config = toml::from_str(&raw).expect("parse failed");
        cfg.resolve_store_paths("./nether-store");
        assert_eq!(cfg.bridges[0].matrix.store_path, "./nether-store/dup");
        assert_eq!(cfg.bridges[1].matrix.store_path, "./nether-store/dup");
        assert!(matches!(cfg.validate(), Err(ConfigError::Validation(_))));
    }

    fn livekit(media_e2ee: Option<bool>, key_transport: Option<KeyTransport>) -> LiveKitConfig {
        LiveKitConfig {
            lk_jwt_url: None,
            url: None,
            media_e2ee,
            key_transport,
        }
    }

    #[test]
    fn resolved_media_e2ee_override_beats_detection() {
        // None → follow detection; Some → force regardless of detection.
        assert!(livekit(None, None).resolved_media_e2ee(true));
        assert!(!livekit(None, None).resolved_media_e2ee(false));
        assert!(!livekit(Some(false), None).resolved_media_e2ee(true)); // force off in enc room
        assert!(livekit(Some(true), None).resolved_media_e2ee(false)); // force on in unenc room
    }

    #[test]
    fn resolved_key_transport_auto_and_override() {
        // Unset: auto — Olm when E2EE on, Plaintext when off.
        assert_eq!(livekit(None, None).resolved_key_transport(true), KeyTransport::Olm);
        assert_eq!(
            livekit(None, None).resolved_key_transport(false),
            KeyTransport::Plaintext
        );
        // Explicit override is honored regardless of media_e2ee.
        assert_eq!(
            livekit(None, Some(KeyTransport::Plaintext)).resolved_key_transport(true),
            KeyTransport::Plaintext
        );
        assert_eq!(
            livekit(None, Some(KeyTransport::Olm)).resolved_key_transport(true),
            KeyTransport::Olm
        );
    }

    #[test]
    fn media_e2ee_and_key_transport_are_optional() {
        // An EMPTY [bridge.livekit] (no override lines) → None/None (auto path).
        let raw = format!(
            r#"{}[[bridge]]
name = "g"
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
store_path = "./s"

[bridge.livekit]
"#,
            puppets_block(&["Bot a"]),
        );
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        assert_eq!(cfg.bridges[0].livekit.media_e2ee, None);
        assert_eq!(cfg.bridges[0].livekit.key_transport, None);
    }

    #[test]
    fn explicit_media_e2ee_parses_to_some() {
        // The fixtures set `media_e2ee = false` → Some(false), an explicit override.
        let raw = format!(
            "{}{}",
            puppets_block(&["Bot a"]),
            bridge_block(Some("g"), "!r:nether.im", "./s", "222"),
        );
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        assert_eq!(cfg.bridges[0].livekit.media_e2ee, Some(false));
    }

    #[test]
    fn slugify_handles_punctuation_and_empties() {
        assert_eq!(slugify("General Voice"), "general-voice");
        assert_eq!(slugify("  weird___name!! "), "weird-name");
        assert_eq!(slugify("a/b\\c"), "a-b-c");
        // All-punctuation → safe fallback rather than an empty leaf.
        assert_eq!(slugify("!!!"), "bridge");
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
