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
    #[error("missing required environment variables: {0}")]
    MissingEnv(String),
    #[error("invalid environment variable value: {0}")]
    InvalidEnv(String),
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
    /// Named homeserver/appservice profiles, keyed by label. TOML `[server.<name>]`
    /// blocks map here. A bridge sets `server = "<name>"` to inherit this profile's
    /// `homeserver_url` / `bot_user_id` / `appservice_id` / `appservice_token`
    /// instead of repeating them — any field a bridge sets explicitly still wins.
    /// Defaults to empty (bridges may instead spell everything out inline in
    /// `[bridge.matrix]`).
    #[serde(rename = "server", default)]
    pub servers: std::collections::HashMap<String, ServerProfile>,
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

/// A reusable homeserver + appservice identity, referenced by bridges via
/// `server = "<name>"`. Bundles the fields that are normally identical across every
/// bridge on one homeserver so they're written once. Each field can still be
/// overridden per bridge in `[bridge.matrix]`.
#[derive(Debug, Deserialize)]
pub struct ServerProfile {
    pub homeserver_url: String,
    /// The public Matrix server domain (e.g. `nether.im`), used when constructing MXIDs
    /// from `sender_localpart`. Optional: when omitted, the server domain is extracted
    /// from `homeserver_url`. Needed in Docker deployments where `homeserver_url` is
    /// an internal proxy address (e.g. `http://matrix-traefik:8008`) whose hostname
    /// differs from the public server domain. Inherited by bridges that reference this
    /// profile; an explicit `matrix.matrix_domain` on the bridge always wins.
    #[serde(default)]
    pub matrix_domain: Option<String>,
    /// The bot's full MXID (e.g. `@voicebridge:example.com`). Optional: when omitted,
    /// it is derived from `sender_localpart` + the resolved server domain via
    /// `resolve_bot_user_ids`. An explicit value always wins. Separately configurable
    /// from `sender_localpart` so the namespace anchor and the bot account can differ.
    #[serde(default)]
    pub bot_user_id: String,
    /// The appservice `sender_localpart` from the registration YAML
    /// (e.g. `voicebridge-bot`). When `bot_user_id` is absent, the bridge derives the
    /// bot MXID as `@{sender_localpart}:{server_name}` via `resolve_bot_user_ids`.
    #[serde(default)]
    pub sender_localpart: Option<String>,
    pub appservice_id: String,
    pub appservice_token: String,
}

#[derive(Debug, Deserialize)]
pub struct BridgeConfig {
    /// Optional human label, used in logs/spans. Falls back to the index. Also the
    /// source for the auto-derived `matrix.device_id` and `store_path` leaf (slug).
    #[serde(default)]
    pub name: Option<String>,
    /// Name of the `[server.<name>]` profile to inherit Matrix/appservice settings
    /// from. Omit to spell everything out inline in `[bridge.matrix]`.
    #[serde(default)]
    pub server: Option<String>,
    /// The bridged Matrix room — genuinely per-bridge, so it lives at the bridge
    /// level. Accepted here (preferred) or inside `[bridge.matrix]` (back-compat);
    /// this bridge-level value wins and is copied into the resolved `matrix.room_id`.
    #[serde(default)]
    pub room_id: String,
    pub discord: DiscordConfig,
    /// Per-bridge Matrix settings: anything not set here is filled from the `server`
    /// profile (`resolve_servers`), then `device_id`/`store_path` auto-derive. May be
    /// omitted entirely when the profile + bridge-level `room_id` supply everything.
    #[serde(default)]
    pub matrix: MatrixConfig,
    #[serde(default)]
    pub livekit: LiveKitConfig,
}

#[derive(Debug, Deserialize)]
pub struct DiscordConfig {
    pub guild_id: String,
    pub channel_id: String,
    /// When true, write the Discord→Matrix audio-bisection WAV taps for
    /// debugging (issue #32): per-SSRC `capture_<ssrc>.wav` at Discord ingress
    /// ("Tap A", what arrived) **and** per-ghost `publish_<key>.wav` at the
    /// LiveKit publish boundary ("Tap B", the mono PCM we send). Comparing the
    /// pair localizes corruption to our audio core vs downstream (Opus/E2EE).
    /// Off in production to avoid unbounded file growth. (Cheap diagnostics —
    /// drop counters, per-ghost health logging, frame-shape warnings — are always
    /// on and need no flag.)
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

/// Extract the Matrix server name from a homeserver URL: the host (and non-standard
/// port), without scheme or path. `https://matrix.example.com` → `matrix.example.com`;
/// `https://matrix.example.com:8448/` → `matrix.example.com:8448`.
fn server_name_from_url(homeserver_url: &str) -> Option<String> {
    let rest = homeserver_url
        .strip_prefix("https://")
        .or_else(|| homeserver_url.strip_prefix("http://"))?;
    let host_port = rest.split('/').next()?;
    if host_port.is_empty() { None } else { Some(host_port.to_owned()) }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct MatrixConfig {
    /// Inherited from the `[server.<name>]` profile when omitted. Empty string is the
    /// "not set" sentinel that `resolve_servers` fills.
    #[serde(default)]
    pub homeserver_url: String,
    /// The public Matrix server domain (e.g. `nether.im`), used when constructing MXIDs
    /// from `sender_localpart`. Optional: when omitted, the server domain is extracted
    /// from `homeserver_url`. Needed in Docker deployments where `homeserver_url` is
    /// an internal proxy address (e.g. `http://matrix-traefik:8008`) whose hostname
    /// differs from the public server domain. Inherited from the `[server.<name>]`
    /// profile when omitted; an explicit value here always wins.
    #[serde(default)]
    pub matrix_domain: Option<String>,
    /// Genuinely per-bridge; normally set at the bridge level (copied here by
    /// `resolve_servers`). Empty = "not set".
    #[serde(default)]
    pub room_id: String,
    /// The bot's full MXID. Inherited from the `[server.<name>]` profile or derived
    /// from `sender_localpart` + resolved server domain via `resolve_bot_user_ids` when
    /// omitted. An explicit value always wins. Empty = "not set".
    #[serde(default)]
    pub bot_user_id: String,
    /// Appservice `sender_localpart` (from the registration YAML). When `bot_user_id`
    /// is absent, the bridge derives it as `@{sender_localpart}:{server_name}` via
    /// `resolve_bot_user_ids`. Inherited from the `[server.<name>]` profile when
    /// omitted. Separately configurable so the namespace anchor and bot account can differ.
    #[serde(default)]
    pub sender_localpart: Option<String>,
    /// The device ID for the bot's matrix-sdk session. A stable id we assert via
    /// appservice login, reused every start so the persistent crypto store stays
    /// valid. Optional: when omitted it auto-derives from the bridge `name` (slug,
    /// same leaf as `store_path`) via `resolve_device_ids` — stable per bridge and
    /// distinct across bridges. Empty = "not set".
    #[serde(default)]
    pub device_id: String,
    /// Inherited from the `[server.<name>]` profile when omitted. Empty = "not set".
    #[serde(default)]
    pub appservice_id: String,
    /// Inherited from the `[server.<name>]` profile when omitted. Empty = "not set".
    #[serde(default)]
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

impl MatrixConfig {
    /// Base URL to use for `.well-known/matrix/client` discovery.
    ///
    /// When `matrix_domain` is set, returns `https://<matrix_domain>` — the public
    /// address that actually serves `.well-known`. Falls back to `homeserver_url`
    /// when `matrix_domain` is absent (non-proxy deployments where the two are the
    /// same host).
    pub fn well_known_base_url(&self) -> String {
        match &self.matrix_domain {
            Some(domain) => format!("https://{domain}"),
            None => self.homeserver_url.trim_end_matches('/').to_owned(),
        }
    }
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

#[derive(Debug, Deserialize, Default)]
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
        // Merge each bridge's referenced `[server.<name>]` profile + bridge-level
        // room_id into its `matrix` block BEFORE the per-bridge auto-derivations,
        // so inherited fields are in place when device_id/store_path resolve.
        cfg.resolve_servers();
        cfg.resolve_bot_user_ids();
        cfg.resolve_device_ids();
        // `NVB_DATA_DIR` (set to `/app/data` by the Docker image) overrides the
        // configured `data_dir`; fall back to it otherwise.
        let data_dir = std::env::var("NVB_DATA_DIR").unwrap_or_else(|_| cfg.data_dir.clone());
        cfg.resolve_store_paths(&data_dir);
        Ok(cfg)
    }

    /// Fill each bridge's empty `matrix` fields from its referenced `[server.<name>]`
    /// profile, and copy the bridge-level `room_id` into `matrix.room_id`. An explicit
    /// per-bridge value (non-empty) always wins over the profile. A bridge with no
    /// `server` (or an unknown one — caught later by `validate`) is left as-is, so a
    /// fully-inline `[bridge.matrix]` still works. Runs before the auto-derivations.
    pub fn resolve_servers(&mut self) {
        // Disjoint borrows: `servers` (read) and `bridges` (write) are distinct fields.
        let servers = &self.servers;
        for b in self.bridges.iter_mut() {
            let m = &mut b.matrix;
            // Bridge-level room_id is the preferred spelling; it wins over any
            // `[bridge.matrix].room_id` only when the latter wasn't set.
            if m.room_id.is_empty() && !b.room_id.is_empty() {
                m.room_id = b.room_id.clone();
            }
            let Some(profile) = b.server.as_ref().and_then(|name| servers.get(name)) else {
                continue;
            };
            if m.homeserver_url.is_empty() {
                m.homeserver_url = profile.homeserver_url.clone();
            }
            if m.matrix_domain.is_none() {
                m.matrix_domain = profile.matrix_domain.clone();
            }
            if m.bot_user_id.is_empty() {
                m.bot_user_id = profile.bot_user_id.clone();
            }
            if m.sender_localpart.is_none() {
                m.sender_localpart = profile.sender_localpart.clone();
            }
            if m.appservice_id.is_empty() {
                m.appservice_id = profile.appservice_id.clone();
            }
            if m.appservice_token.is_empty() {
                m.appservice_token = profile.appservice_token.clone();
            }
        }
    }

    /// Fill each bridge's empty `matrix.bot_user_id` by deriving it from
    /// `matrix.sender_localpart` and the resolved server domain. Runs after
    /// `resolve_servers` so inherited fields are in place. An explicit `bot_user_id`
    /// is left as-is. Silently skips a bridge when derivation can't complete (no
    /// `sender_localpart`, and neither `server_name` nor a parseable `homeserver_url`
    /// is available) — `validate()` will surface the remaining empty `bot_user_id` as
    /// a clear error.
    ///
    /// server domain resolution order:
    ///   1. Explicit `matrix.matrix_domain` (or inherited from `[server.<name>]`) — use as-is.
    ///   2. Extract host from `matrix.homeserver_url` (the original behaviour).
    /// The explicit field is needed when `homeserver_url` is an internal proxy address
    /// (e.g. `http://matrix-traefik:8008` in Docker) whose hostname is not the public
    /// Matrix server name.
    ///
    /// The derived localpart is `{sender_localpart}_bot`, NOT `{sender_localpart}` itself.
    /// On Synapse, the exact sender_localpart user is blocked from all sync endpoints
    /// (`get_app_service_by_user_id` exact-matches it). A `_bot`-suffixed user in the
    /// appservice namespace (which must cover `{sender_localpart}_.*` per the standard
    /// registration template) is treated as a regular Matrix user and can sync normally.
    pub fn resolve_bot_user_ids(&mut self) {
        for b in self.bridges.iter_mut() {
            let m = &mut b.matrix;
            if !m.bot_user_id.is_empty() {
                continue;
            }
            let Some(localpart) = &m.sender_localpart else { continue };
            let Some(server_name) = m.matrix_domain.clone()
                .or_else(|| server_name_from_url(&m.homeserver_url))
            else {
                continue;
            };
            m.bot_user_id = format!("@{localpart}_bot:{server_name}");
        }
    }

    /// Fill each bridge's omitted (empty) `matrix.device_id` from the bridge `name`
    /// (slug, `bridge-<index>` when unnamed) — the SAME leaf as `store_path`, so a
    /// bridge's device and store stay consistent and unique-by-name. The device id is
    /// stable across restarts (tied to the name), which the crypto store + Element
    /// Call identity mapping require. An explicit `device_id` is left as-is.
    pub fn resolve_device_ids(&mut self) {
        for (i, b) in self.bridges.iter_mut().enumerate() {
            if !b.matrix.device_id.is_empty() {
                continue;
            }
            b.matrix.device_id = match &b.name {
                Some(name) => slugify(name),
                None => format!("bridge-{i}"),
            };
        }
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

    /// Build a single-bridge config entirely from environment variables.
    ///
    /// Required vars:
    ///   NETHER_PUPPET_TOKENS, NETHER_DISCORD_GUILD_ID, NETHER_DISCORD_CHANNEL_ID,
    ///   NETHER_MATRIX_HOMESERVER_URL, NETHER_MATRIX_ROOM_ID,
    ///   NETHER_MATRIX_APPSERVICE_ID, NETHER_MATRIX_APPSERVICE_TOKEN
    ///   At least one of: NETHER_MATRIX_BOT_USER_ID, NETHER_MATRIX_SENDER_LOCALPART
    ///
    /// Optional vars:
    ///   NETHER_PUPPET_NICKNAME_PREFIX (default "[Matrix] ")
    ///   NETHER_BRIDGE_NAME
    ///   NETHER_DISCORD_CAPTURE_WAV (default false)
    ///   NETHER_MATRIX_DEVICE_ID (auto-derived from bridge name when absent)
    ///   NETHER_MATRIX_DOMAIN — public Matrix domain name (e.g. `nether.im`); derived
    ///     from HOMESERVER_URL when absent
    ///   NETHER_MATRIX_BOT_USER_ID — full bot MXID; derived from SENDER_LOCALPART when absent
    ///   NETHER_MATRIX_SENDER_LOCALPART — bot localpart; derives bot MXID when BOT_USER_ID absent
    ///   NETHER_MATRIX_STORE_PATH (auto-derived from bridge name when absent)
    ///   NETHER_MATRIX_BOT_DISPLAY_NAME, NETHER_MATRIX_BOT_AVATAR_PATH
    ///   NETHER_LIVEKIT_MEDIA_E2EE (auto-detected from room encryption when absent)
    ///   NETHER_LIVEKIT_KEY_TRANSPORT ("plaintext" | "olm", auto-selected when absent)
    ///   NETHER_LIVEKIT_LK_JWT_URL, NETHER_LIVEKIT_URL
    pub fn from_env() -> Result<Self, ConfigError> {
        let mut missing: Vec<&'static str> = Vec::new();

        // Returns Some(value) for a non-empty env var, records the name as missing otherwise.
        macro_rules! require {
            ($var:literal) => {
                match std::env::var($var) {
                    Ok(v) if !v.is_empty() => Some(v),
                    _ => {
                        missing.push($var);
                        None
                    }
                }
            };
        }

        // Returns Some(value) for a set, non-empty env var; None when absent or empty.
        macro_rules! optional {
            ($var:literal) => {
                std::env::var($var).ok().filter(|v| !v.is_empty())
            };
        }

        let tokens_raw = match std::env::var("NETHER_PUPPET_TOKENS") {
            Ok(v) if !v.is_empty() => v,
            _ => {
                missing.push("NETHER_PUPPET_TOKENS");
                String::new()
            }
        };

        let nickname_prefix = optional!("NETHER_PUPPET_NICKNAME_PREFIX")
            .unwrap_or_else(default_nickname_prefix);

        let discord_guild_id = require!("NETHER_DISCORD_GUILD_ID");
        let discord_channel_id = require!("NETHER_DISCORD_CHANNEL_ID");
        let capture_wav = optional!("NETHER_DISCORD_CAPTURE_WAV")
            .map(|v| matches!(v.to_lowercase().as_str(), "true" | "1"))
            .unwrap_or(false);

        let homeserver_url = require!("NETHER_MATRIX_HOMESERVER_URL");
        let matrix_domain = optional!("NETHER_MATRIX_DOMAIN");
        let room_id = require!("NETHER_MATRIX_ROOM_ID");
        // bot_user_id and sender_localpart are independently optional: at least one must
        // be present. When bot_user_id is absent, resolve_bot_user_ids derives it from
        // sender_localpart + resolved server name (matrix_domain field, else homeserver_url host).
        let bot_user_id = optional!("NETHER_MATRIX_BOT_USER_ID");
        let sender_localpart = optional!("NETHER_MATRIX_SENDER_LOCALPART");
        if bot_user_id.is_none() && sender_localpart.is_none() {
            missing.push("NETHER_MATRIX_BOT_USER_ID (or NETHER_MATRIX_SENDER_LOCALPART)");
        }
        // Optional like file mode: when absent, `resolve_device_ids` derives a stable
        // id from the bridge name (the same leaf as `store_path`).
        let device_id = optional!("NETHER_MATRIX_DEVICE_ID");
        let appservice_id = require!("NETHER_MATRIX_APPSERVICE_ID");
        let appservice_token = require!("NETHER_MATRIX_APPSERVICE_TOKEN");
        let store_path = optional!("NETHER_MATRIX_STORE_PATH").unwrap_or_default();
        let bot_display_name = optional!("NETHER_MATRIX_BOT_DISPLAY_NAME");
        let bot_avatar_path = optional!("NETHER_MATRIX_BOT_AVATAR_PATH");

        let media_e2ee = optional!("NETHER_LIVEKIT_MEDIA_E2EE")
            .map(|v| matches!(v.to_lowercase().as_str(), "true" | "1"));
        let key_transport = optional!("NETHER_LIVEKIT_KEY_TRANSPORT")
            .map(|v| match v.to_lowercase().as_str() {
                "plaintext" => Ok(KeyTransport::Plaintext),
                "olm" => Ok(KeyTransport::Olm),
                other => Err(ConfigError::InvalidEnv(format!(
                    "NETHER_LIVEKIT_KEY_TRANSPORT: unknown value '{other}' \
                     (expected 'plaintext' or 'olm')"
                ))),
            })
            .transpose()?;
        let lk_jwt_url = optional!("NETHER_LIVEKIT_LK_JWT_URL");
        let url = optional!("NETHER_LIVEKIT_URL");

        if !missing.is_empty() {
            return Err(ConfigError::MissingEnv(missing.join(", ")));
        }

        let name = optional!("NETHER_BRIDGE_NAME");

        let mut cfg = Config {
            puppets: PuppetsConfig {
                tokens: tokens_raw
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect(),
                nickname_prefix,
            },
            servers: std::collections::HashMap::new(),
            data_dir: default_data_dir(),
            bridges: vec![BridgeConfig {
                name,
                server: None,
                room_id: String::new(),
                discord: DiscordConfig {
                    guild_id: discord_guild_id.unwrap(),
                    channel_id: discord_channel_id.unwrap(),
                    capture_wav,
                },
                matrix: MatrixConfig {
                    homeserver_url: homeserver_url.unwrap(),
                    matrix_domain,
                    room_id: room_id.unwrap(),
                    bot_user_id: bot_user_id.unwrap_or_default(),
                    sender_localpart,
                    device_id: device_id.unwrap_or_default(),
                    appservice_id: appservice_id.unwrap(),
                    appservice_token: appservice_token.unwrap(),
                    store_path,
                    bot_display_name,
                    bot_avatar_path,
                },
                livekit: LiveKitConfig {
                    lk_jwt_url,
                    url,
                    media_e2ee,
                    key_transport,
                },
            }],
        };
        cfg.resolve_bot_user_ids();
        // Omitted `NETHER_MATRIX_DEVICE_ID` derives from the bridge name, as in file mode.
        cfg.resolve_device_ids();
        // Mirror `load()`'s store-path resolution: an omitted `NETHER_MATRIX_STORE_PATH`
        // leaves `store_path` empty, which must be filled from `data_dir` (honoring the
        // `NVB_DATA_DIR` override the Docker image sets to `/app/data`) — otherwise the
        // sqlite store + ghost Olm stores land off the persistent volume.
        let data_dir = std::env::var("NVB_DATA_DIR").unwrap_or_else(|_| cfg.data_dir.clone());
        cfg.resolve_store_paths(&data_dir);
        Ok(cfg)
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

        // Hard: a `server = "<name>"` reference must resolve to a defined
        // `[server.<name>]` profile (otherwise the merge silently filled nothing).
        for b in &self.bridges {
            if let Some(name) = &b.server {
                if !self.servers.contains_key(name) {
                    return Err(ConfigError::Validation(format!(
                        "bridge '{}': references unknown server profile '{name}' — \
                         define a [server.{name}] block (or set the fields inline in \
                         [bridge.matrix])",
                        b.label()
                    )));
                }
            }
        }

        // Hard: after profile-merge + auto-derive, every required Matrix field must be
        // present. Empty here means the bridge neither inherited it (no/empty profile)
        // nor set it inline.
        for b in &self.bridges {
            let m = &b.matrix;
            let missing: Vec<&str> = [
                ("homeserver_url", m.homeserver_url.is_empty()),
                ("bot_user_id (or sender_localpart)", m.bot_user_id.is_empty()),
                ("room_id", m.room_id.is_empty()),
                ("device_id", m.device_id.is_empty()),
                ("appservice_id", m.appservice_id.is_empty()),
                ("appservice_token", m.appservice_token.is_empty()),
            ]
            .into_iter()
            .filter_map(|(field, empty)| empty.then_some(field))
            .collect();
            if !missing.is_empty() {
                return Err(ConfigError::Validation(format!(
                    "bridge '{}': missing required matrix field(s): {} — set them in a \
                     [server.<name>] profile (and reference it via server = \"…\") or \
                     inline in [bridge.matrix]",
                    b.label(),
                    missing.join(", ")
                )));
            }
        }

        // Hard: distinct (homeserver_url, bot_user_id, device_id) per bridge — two
        // matrix-sdk sessions on one device steal each other's to-device messages
        // (the encrypted media key arrives once per device), silently breaking E2EE.
        // Auto-derived device_ids are distinct-by-name; this catches explicit
        // collisions (e.g. two bridges hand-set the same device_id on one bot).
        let mut seen_devices = std::collections::HashSet::new();
        for b in &self.bridges {
            let m = &b.matrix;
            if !seen_devices.insert((
                m.homeserver_url.as_str(),
                m.bot_user_id.as_str(),
                m.device_id.as_str(),
            )) {
                return Err(ConfigError::Validation(format!(
                    "duplicate (homeserver_url, bot_user_id, device_id) across bridges \
                     for bot '{}' device '{}' — each bridge needs its own device_id \
                     (omit it to auto-derive a distinct one from the bridge name)",
                    m.bot_user_id, m.device_id
                )));
            }
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
            server_profiles = self.servers.len(),
            puppet_pool_size = self.puppets.tokens.len(),
            nickname_prefix = %self.puppets.nickname_prefix,
            "config loaded"
        );
        for (name, p) in &self.servers {
            tracing::info!(
                server = %name,
                homeserver_url = %p.homeserver_url,
                matrix_domain = ?p.matrix_domain,
                bot_user_id = %p.bot_user_id,
                sender_localpart = ?p.sender_localpart,
                appservice_id = %p.appservice_id,
                appservice_token = "[redacted]",
                "server profile loaded"
            );
        }
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
            matrix_domain = ?self.matrix.matrix_domain,
            matrix_room_id = %self.matrix.room_id,
            matrix_bot_user_id = %self.matrix.bot_user_id,
            matrix_sender_localpart = ?self.matrix.sender_localpart,
            matrix_device_id = %self.matrix.device_id,
            matrix_store_path = %self.matrix.store_path,
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
device_id = "DEV{chan}"
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
    fn rejects_missing_matrix_fields() {
        // `[bridge.matrix]`/`[bridge.livekit]` are now optional sections (a bridge can
        // inherit everything from a `[server.<name>]` profile), so this PARSES — but
        // with no profile and no inline fields, `validate()` flags the missing
        // required Matrix fields rather than serde failing at parse time.
        let raw = format!(
            "{}{}",
            puppets_block(&["Bot a"]),
            r#"[[bridge]]
[bridge.discord]
guild_id = "111"
channel_id = "222"
"#,
        );
        let cfg: Config = toml::from_str(&raw).expect("parses (sections optional)");
        assert!(matches!(cfg.validate(), Err(ConfigError::Validation(_))));
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
device_id = "DEV{chan}"
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

    // ---- server/appservice profiles (named, referenced) ----

    /// A `[server.<name>]` profile block.
    fn server_block(name: &str) -> String {
        format!(
            r#"[server.{name}]
homeserver_url = "https://nether.im"
bot_user_id = "@voicebridge_as:nether.im"
appservice_id = "voicebridge"
appservice_token = "hs_tok"

"#
        )
    }

    /// Parse + run the same resolution chain `Config::load` does (server-merge,
    /// bot_user_id derive, device_id derive, store_path derive) so tests exercise
    /// the real pipeline.
    fn loaded(raw: &str) -> Config {
        let mut cfg: Config = toml::from_str(raw).expect("parse failed");
        cfg.resolve_servers();
        cfg.resolve_bot_user_ids();
        cfg.resolve_device_ids();
        cfg.resolve_store_paths("./nether-store");
        cfg
    }

    #[test]
    fn server_profile_inherits_and_autoderives() {
        // A bridge that references a profile and only sets room_id inherits every
        // server field, auto-derives device_id (slug of name) + store_path, validates.
        let raw = format!(
            "{}{}[[bridge]]\nname = \"Dark Voice\"\nserver = \"nether\"\nroom_id = \"!d:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
            server_block("nether"),
        );
        let cfg = loaded(&raw);
        let m = &cfg.bridges[0].matrix;
        assert_eq!(m.homeserver_url, "https://nether.im");
        assert_eq!(m.bot_user_id, "@voicebridge_as:nether.im");
        assert_eq!(m.appservice_id, "voicebridge");
        assert_eq!(m.appservice_token, "hs_tok");
        assert_eq!(m.room_id, "!d:nether.im"); // bridge-level room_id → matrix.room_id
        assert_eq!(m.device_id, "dark-voice"); // auto-derived from name
        assert_eq!(m.store_path, "./nether-store/dark-voice");
        cfg.validate().expect("should validate");
    }

    #[test]
    fn bridge_override_beats_profile() {
        // Inline [bridge.matrix] values win over the referenced profile.
        let raw = format!(
            "{}{}[[bridge]]\nname = \"b\"\nserver = \"nether\"\nroom_id = \"!b:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n[bridge.matrix]\nbot_user_id = \"@special:nether.im\"\ndevice_id = \"PINNED\"\n",
            puppets_block(&["Bot a"]),
            server_block("nether"),
        );
        let cfg = loaded(&raw);
        let m = &cfg.bridges[0].matrix;
        assert_eq!(m.bot_user_id, "@special:nether.im"); // override
        assert_eq!(m.device_id, "PINNED"); // explicit, not auto-derived
        assert_eq!(m.appservice_token, "hs_tok"); // still inherited
        cfg.validate().expect("should validate");
    }

    #[test]
    fn unknown_server_reference_errors() {
        let raw = format!(
            "{}[[bridge]]\nname = \"b\"\nserver = \"ghost\"\nroom_id = \"!b:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
        );
        let cfg = loaded(&raw);
        assert!(matches!(cfg.validate(), Err(ConfigError::Validation(_))));
    }

    #[test]
    fn missing_room_id_after_merge_errors() {
        // Profile supplies everything except the per-bridge room_id → validate flags it.
        let raw = format!(
            "{}{}[[bridge]]\nname = \"b\"\nserver = \"nether\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
            server_block("nether"),
        );
        let cfg = loaded(&raw);
        assert!(matches!(cfg.validate(), Err(ConfigError::Validation(_))));
    }

    #[test]
    fn duplicate_explicit_device_id_errors() {
        // Two bridges on one profile (same bot) hand-set the same device_id → reject.
        let raw = format!(
            "{}{}[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!a:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n[bridge.matrix]\ndevice_id = \"SAME\"\n\n[[bridge]]\nname = \"b\"\nserver = \"nether\"\nroom_id = \"!b:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"333\"\n[bridge.matrix]\ndevice_id = \"SAME\"\n",
            puppets_block(&["Bot a"]),
            server_block("nether"),
        );
        let cfg = loaded(&raw);
        assert!(matches!(cfg.validate(), Err(ConfigError::Validation(_))));
    }

    #[test]
    fn two_profile_bridges_get_distinct_derived_devices() {
        // The real config shape: two bridges, one profile, device_ids auto-derive
        // distinct from their names → no duplicate-device error.
        let raw = format!(
            "{}{}[[bridge]]\nname = \"Light Voice\"\nserver = \"nether\"\nroom_id = \"!l:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n\n[[bridge]]\nname = \"Dark Voice\"\nserver = \"nether\"\nroom_id = \"!d:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"333\"\n",
            puppets_block(&["Bot a"]),
            server_block("nether"),
        );
        let cfg = loaded(&raw);
        assert_eq!(cfg.bridges[0].matrix.device_id, "light-voice");
        assert_eq!(cfg.bridges[1].matrix.device_id, "dark-voice");
        cfg.validate().expect("distinct derived devices → valid");
    }

    // ---- sender_localpart → bot_user_id derivation ----

    /// A `[server.<name>]` profile that uses sender_localpart instead of bot_user_id.
    fn server_block_localpart(name: &str, localpart: &str) -> String {
        format!(
            r#"[server.{name}]
homeserver_url = "https://nether.im"
sender_localpart = "{localpart}"
appservice_id = "voicebridge"
appservice_token = "hs_tok"

"#
        )
    }

    #[test]
    fn server_name_from_url_parses_correctly() {
        assert_eq!(
            server_name_from_url("https://matrix.example.com"),
            Some("matrix.example.com".to_owned())
        );
        assert_eq!(
            server_name_from_url("https://matrix.example.com:8448/"),
            Some("matrix.example.com:8448".to_owned())
        );
        assert_eq!(
            server_name_from_url("http://localhost:8008"),
            Some("localhost:8008".to_owned())
        );
        assert_eq!(server_name_from_url("not-a-url"), None);
        assert_eq!(server_name_from_url("https://"), None);
    }

    #[test]
    fn sender_localpart_inline_derives_bot_user_id() {
        // sender_localpart in [bridge.matrix] → bot_user_id derived as localpart_bot.
        let raw = format!(
            "{}[[bridge]]\nname = \"a\"\nroom_id = \"!r:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n[bridge.matrix]\nhomeserver_url = \"https://nether.im\"\nsender_localpart = \"voicebridge-bot\"\ndevice_id = \"DEV\"\nappservice_id = \"voicebridge\"\nappservice_token = \"hs_tok\"\nstore_path = \"./s\"\n",
            puppets_block(&["Bot a"]),
        );
        let cfg = loaded(&raw);
        let m = &cfg.bridges[0].matrix;
        assert_eq!(m.bot_user_id, "@voicebridge-bot_bot:nether.im");
        assert_eq!(m.sender_localpart.as_deref(), Some("voicebridge-bot"));
        cfg.validate().expect("should validate");
    }

    #[test]
    fn sender_localpart_in_profile_derives_bot_user_id() {
        // Profile with only sender_localpart (no bot_user_id) → bot_user_id derived as localpart_bot.
        let raw = format!(
            "{}{}[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!r:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
            server_block_localpart("nether", "voicebridge-bot"),
        );
        let cfg = loaded(&raw);
        let m = &cfg.bridges[0].matrix;
        assert_eq!(m.bot_user_id, "@voicebridge-bot_bot:nether.im");
        assert_eq!(m.sender_localpart.as_deref(), Some("voicebridge-bot"));
        cfg.validate().expect("should validate");
    }

    #[test]
    fn explicit_bot_user_id_beats_sender_localpart() {
        // Both set in profile: bot_user_id wins; sender_localpart is still propagated.
        let raw = format!(
            "{}[server.nether]\nhomeserver_url = \"https://nether.im\"\nbot_user_id = \"@custom-bot:nether.im\"\nsender_localpart = \"voicebridge-bot\"\nappservice_id = \"voicebridge\"\nappservice_token = \"hs_tok\"\n\n{}[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!r:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
            "",
        );
        let cfg = loaded(&raw);
        let m = &cfg.bridges[0].matrix;
        assert_eq!(m.bot_user_id, "@custom-bot:nether.im");
        assert_eq!(m.sender_localpart.as_deref(), Some("voicebridge-bot"));
        cfg.validate().expect("should validate");
    }

    #[test]
    fn missing_both_bot_user_id_and_sender_localpart_errors() {
        // Neither bot_user_id nor sender_localpart → validate catches the empty bot_user_id.
        let raw = format!(
            "{}[[bridge]]\nname = \"a\"\nroom_id = \"!r:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n[bridge.matrix]\nhomeserver_url = \"https://nether.im\"\ndevice_id = \"DEV\"\nappservice_id = \"voicebridge\"\nappservice_token = \"hs_tok\"\nstore_path = \"./s\"\n",
            puppets_block(&["Bot a"]),
        );
        let cfg = loaded(&raw);
        assert!(matches!(cfg.validate(), Err(ConfigError::Validation(_))));
    }

    #[test]
    fn sender_localpart_independent_of_bot_user_id_in_profile() {
        // bot_user_id and sender_localpart can differ — both are preserved as-is.
        let raw = format!(
            "{}[server.nether]\nhomeserver_url = \"https://nether.im\"\nbot_user_id = \"@bot-account:nether.im\"\nsender_localpart = \"namespace-anchor\"\nappservice_id = \"voicebridge\"\nappservice_token = \"hs_tok\"\n\n{}[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!r:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
            "",
        );
        let cfg = loaded(&raw);
        let m = &cfg.bridges[0].matrix;
        assert_eq!(m.bot_user_id, "@bot-account:nether.im");
        assert_eq!(m.sender_localpart.as_deref(), Some("namespace-anchor"));
        cfg.validate().expect("should validate");
    }

    // ---- matrix_domain overrides homeserver_url host for MXID derivation ----

    #[test]
    fn matrix_domain_inline_overrides_url_host() {
        // homeserver_url is an internal proxy; matrix_domain gives the real server name.
        let raw = format!(
            "{}[[bridge]]\nname = \"a\"\nroom_id = \"!r:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n[bridge.matrix]\nhomeserver_url = \"http://matrix-traefik:8008\"\nmatrix_domain = \"nether.im\"\nsender_localpart = \"voicebridge-bot\"\ndevice_id = \"DEV\"\nappservice_id = \"voicebridge\"\nappservice_token = \"hs_tok\"\nstore_path = \"./s\"\n",
            puppets_block(&["Bot a"]),
        );
        let cfg = loaded(&raw);
        let m = &cfg.bridges[0].matrix;
        assert_eq!(m.matrix_domain.as_deref(), Some("nether.im"));
        assert_eq!(m.bot_user_id, "@voicebridge-bot_bot:nether.im");
        cfg.validate().expect("should validate");
    }

    #[test]
    fn matrix_domain_in_profile_overrides_url_host() {
        // Same but matrix_domain is in the server profile and inherited by the bridge.
        let raw = format!(
            "{}[server.nether]\nhomeserver_url = \"http://matrix-traefik:8008\"\nmatrix_domain = \"nether.im\"\nsender_localpart = \"voicebridge-bot\"\nappservice_id = \"voicebridge\"\nappservice_token = \"hs_tok\"\n\n{}[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!r:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
            "",
        );
        let cfg = loaded(&raw);
        let m = &cfg.bridges[0].matrix;
        assert_eq!(m.matrix_domain.as_deref(), Some("nether.im"));
        assert_eq!(m.bot_user_id, "@voicebridge-bot_bot:nether.im");
        cfg.validate().expect("should validate");
    }

    // ---- from_env() ----

    /// `from_env()` must run the same store-path resolution `load()` does: an omitted
    /// `NETHER_MATRIX_STORE_PATH` is derived from the bridge name, never left empty
    /// (an empty path sends the sqlite + ghost Olm stores off the persistent volume).
    /// Sole reader/writer of `NETHER_*`/`NVB_DATA_DIR` in the suite, so the env
    /// mutation can't race another test.
    #[test]
    fn from_env_derives_store_path_when_unset() {
        let vars = [
            ("NETHER_PUPPET_TOKENS", "tok-a,tok-b"),
            ("NETHER_DISCORD_GUILD_ID", "111"),
            ("NETHER_DISCORD_CHANNEL_ID", "222"),
            ("NETHER_MATRIX_HOMESERVER_URL", "https://nether.im"),
            ("NETHER_MATRIX_ROOM_ID", "!r:nether.im"),
            ("NETHER_MATRIX_SENDER_LOCALPART", "voicebridge-bot"),
            ("NETHER_MATRIX_APPSERVICE_ID", "voicebridge"),
            ("NETHER_MATRIX_APPSERVICE_TOKEN", "hs_tok"),
            ("NETHER_BRIDGE_NAME", "envtest"),
        ];
        for (k, v) in vars {
            std::env::set_var(k, v);
        }
        std::env::set_var("NVB_DATA_DIR", "/app/data");

        let cfg = Config::from_env().expect("from_env should succeed");
        let m = &cfg.bridges[0].matrix;
        // store_path derived under NVB_DATA_DIR, NOT left empty.
        assert_eq!(m.store_path, "/app/data/envtest");
        // device_id auto-derives from the bridge name when unset (file-mode parity).
        assert_eq!(m.device_id, "envtest");
        // sender_localpart still derives the bot MXID (as localpart_bot).
        assert_eq!(m.bot_user_id, "@voicebridge-bot_bot:nether.im");
        cfg.validate().expect("env config should validate");

        for (k, _) in vars {
            std::env::remove_var(k);
        }
        std::env::remove_var("NVB_DATA_DIR");
    }
}
