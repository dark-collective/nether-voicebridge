use crate::*;

/// Per-bridge body of [`Config::resolve_servers`]: merge the referenced
/// `[server.<name>]` profile + bridge-level `room_id` into `matrix`. An explicit
/// per-bridge value (non-empty) always wins over the profile.
pub fn resolve_bridge_server(
    b: &mut BridgeConfig,
    servers: &std::collections::HashMap<String, ServerProfile>,
) {
    let m = &mut b.matrix;
    // Bridge-level room_id is the preferred spelling; it wins over any
    // `[bridge.matrix].room_id` only when the latter wasn't set.
    if m.room_id.is_empty() && !b.room_id.is_empty() {
        m.room_id = b.room_id.clone();
    }
    let Some(profile) = b.server.as_ref().and_then(|name| servers.get(name)) else {
        return;
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
    if m.prune_stale_devices.is_none() {
        m.prune_stale_devices = profile.prune_stale_devices;
    }
    if m.rotate_ghost_sessions.is_none() {
        m.rotate_ghost_sessions = profile.rotate_ghost_sessions;
    }
}

/// Per-bridge body of [`Config::resolve_bot_user_ids`]: derive an omitted
/// `bot_user_id` from `sender_localpart` + the resolved server domain
/// (`{sender_localpart}_bot` localpart — see the method docs for why `_bot`).
pub fn resolve_bridge_bot_user_id(b: &mut BridgeConfig) {
    let m = &mut b.matrix;
    if !m.bot_user_id.is_empty() {
        return;
    }
    let Some(localpart) = &m.sender_localpart else { return };
    let Some(server_name) = m
        .matrix_domain
        .clone()
        .or_else(|| server_name_from_url(&m.homeserver_url))
    else {
        return;
    };
    m.bot_user_id = format!("@{localpart}_bot:{server_name}");
}

/// Per-bridge body of [`Config::resolve_device_ids`]: fill an omitted `device_id`
/// with the given leaf (the same leaf as `store_path`, so device + store stay
/// consistent). An explicit `device_id` is left as-is.
pub fn resolve_bridge_device_id(b: &mut BridgeConfig, leaf: &str) {
    if b.matrix.device_id.is_empty() {
        b.matrix.device_id = leaf.to_string();
    }
}

/// Per-bridge body of [`Config::resolve_store_paths`]: fill an omitted
/// `store_path` with `<data_dir>/<leaf>`. An explicit `store_path` is left as-is.
pub fn resolve_bridge_store_path(b: &mut BridgeConfig, data_dir: &str, leaf: &str) {
    if b.matrix.store_path.is_empty() {
        let base = data_dir.trim_end_matches('/');
        b.matrix.store_path = format!("{base}/{leaf}");
    }
}

/// Resolve the `[management]` block against the server profiles + `data_dir`:
/// fill identity fields from the referenced profile (explicit values win),
/// derive `bot_user_id` from `sender_localpart` the same way bridges do, and
/// default `device_id`/`store_path` to the `management` leaf. When `server` is
/// omitted and exactly one profile exists, that profile is implied (and the
/// reference is written back so validation and logs see it).
pub fn resolve_management(
    m: &mut ManagementConfig,
    servers: &std::collections::HashMap<String, ServerProfile>,
    data_dir: &str,
) {
    if m.server.is_none() && servers.len() == 1 {
        m.server = servers.keys().next().cloned();
    }
    if let Some(profile) = m.server.as_ref().and_then(|name| servers.get(name)) {
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
        if m.appservice_token.is_empty() {
            m.appservice_token = profile.appservice_token.clone();
        }
    }
    // Same `{localpart}_bot` derivation as bridges (see resolve_bot_user_ids).
    if m.bot_user_id.is_empty() {
        if let (Some(localpart), Some(server_name)) = (
            m.sender_localpart.as_ref(),
            m.matrix_domain
                .clone()
                .or_else(|| server_name_from_url(&m.homeserver_url)),
        ) {
            m.bot_user_id = format!("@{localpart}_bot:{server_name}");
        }
    }
    if m.device_id.is_empty() {
        m.device_id = "management".to_string();
    }
    if m.store_path.is_empty() {
        let base = data_dir.trim_end_matches('/');
        m.store_path = format!("{base}/management");
    }
}

/// The `NVB_TIMING` env preset, normalized: unset or empty ⇒ `None` (default
/// profile). Read at load time only, alongside `NVB_DATA_DIR`.
fn timing_preset_from_env() -> Option<String> {
    std::env::var("NVB_TIMING").ok().filter(|v| !v.is_empty())
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
        // configured `data_dir`; fall back to it otherwise. Write the resolved value
        // back so later consumers (e.g. the tenants.toml path) see the same base the
        // store paths derived from.
        let data_dir = std::env::var("NVB_DATA_DIR").unwrap_or_else(|_| cfg.data_dir.clone());
        cfg.resolve_store_paths(&data_dir);
        cfg.data_dir = data_dir;
        // Management bot identity resolves last: it needs the profiles AND the
        // final data_dir (for its store_path default).
        if let Some(m) = cfg.management.as_mut() {
            resolve_management(m, &cfg.servers, &cfg.data_dir);
        }
        // Timing profile: `NVB_TIMING` preset first, explicit `[timing]` keys
        // override (#38). Defaults are byte-identical to the historical consts.
        cfg.timing = cfg.timing_raw.resolve(timing_preset_from_env().as_deref())?;
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
            resolve_bridge_server(b, servers);
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
    ///
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
            resolve_bridge_bot_user_id(b);
        }
    }

    /// Fill each bridge's omitted (empty) `matrix.device_id` from the bridge `name`
    /// (slug, `bridge-<index>` when unnamed) — the SAME leaf as `store_path`, so a
    /// bridge's device and store stay consistent and unique-by-name. The device id is
    /// stable across restarts (tied to the name), which the crypto store + Element
    /// Call identity mapping require. An explicit `device_id` is left as-is.
    pub fn resolve_device_ids(&mut self) {
        for (i, b) in self.bridges.iter_mut().enumerate() {
            let leaf = bridge_leaf(b, i);
            resolve_bridge_device_id(b, &leaf);
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
        for (i, b) in self.bridges.iter_mut().enumerate() {
            let leaf = bridge_leaf(b, i);
            resolve_bridge_store_path(b, data_dir, &leaf);
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
                standby_nickname: default_standby_nickname(),
            },
            servers: std::collections::HashMap::new(),
            data_dir: default_data_dir(),
            // Env-var mode is the minimal single-bridge deployment; the
            // management bot and admission limits are config-file features.
            management: None,
            limits: LimitsConfig::default(),
            timing_raw: TimingOverrides::default(),
            // Env-var mode has no `[timing]` keys; the `NVB_TIMING` preset
            // still applies (the harness reaches for it in both modes).
            timing: TimingOverrides::default().resolve(timing_preset_from_env().as_deref())?,
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
                    prune_stale_devices: None,
                    rotate_ghost_sessions: None,
                },
                livekit: LiveKitConfig {
                    lk_jwt_url,
                    url,
                    media_e2ee,
                    key_transport,
                },
                // Env-var mode is operator-authored: no owner record.
                owner: None,
                created: None,
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
        cfg.data_dir = data_dir;
        Ok(cfg)
    }
}
