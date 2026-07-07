use crate::*;

/// The uniqueness-key sets of a set of live (or about-to-be-live) bridges, used to
/// validate a further bridge against them without re-validating the whole set.
/// Hard keys mirror `Config::validate`'s cross-bridge errors; soft keys mirror its
/// warnings.
#[derive(Debug, Default, Clone)]
pub struct LiveKeys {
    /// Bridge names (labels) — the supervisor keys running bridges by label, so a
    /// duplicate would collide in its registry and in the puppet pool's channel map.
    pub names: std::collections::HashSet<String>,
    /// Hard: `(homeserver_url, bot_user_id, device_id)` — two matrix-sdk sessions
    /// on one device steal each other's to-device messages.
    pub devices: std::collections::HashSet<(String, String, String)>,
    /// Hard: `store_path` — two bridges sharing one SQLite store lock/corrupt.
    pub stores: std::collections::HashSet<String>,
    /// Soft: Discord `(guild_id, channel_id)` — two bridges on one channel fight
    /// over bots.
    pub channels: std::collections::HashSet<(String, String)>,
    /// Soft: Matrix room_id — almost always a copy-paste slip.
    pub rooms: std::collections::HashSet<String>,
}

impl LiveKeys {
    /// Collect the key sets of an existing bridge set (e.g. the static config's
    /// bridges, or the supervisor's running set).
    pub fn from_bridges<'a>(bridges: impl IntoIterator<Item = &'a BridgeConfig>) -> Self {
        let mut keys = Self::default();
        for b in bridges {
            keys.insert(b);
        }
        keys
    }

    /// Reserve the management bot's identity keys so no bridge (static or
    /// runtime tenant) can claim its store or device.
    pub fn reserve_management(&mut self, m: &ManagementConfig) {
        self.devices.insert((
            m.homeserver_url.clone(),
            m.bot_user_id.clone(),
            m.device_id.clone(),
        ));
        self.stores.insert(m.store_path.clone());
    }

    /// Add one bridge's keys (call after it passes `validate_bridge_against`).
    pub fn insert(&mut self, b: &BridgeConfig) {
        if let Some(name) = &b.name {
            self.names.insert(name.clone());
        }
        let m = &b.matrix;
        self.devices.insert((
            m.homeserver_url.clone(),
            m.bot_user_id.clone(),
            m.device_id.clone(),
        ));
        self.stores.insert(m.store_path.clone());
        self.channels.insert((
            b.discord.guild_id.clone(),
            b.discord.channel_id.clone(),
        ));
        self.rooms.insert(m.room_id.clone());
    }
}

/// Validate one (already-resolved) bridge against a set of live bridges' keys.
/// Hard failures error; likely-mistakes are logged as warnings. Does NOT mutate
/// `live` — call [`LiveKeys::insert`] after acceptance. The per-bridge counterpart
/// of `Config::validate`'s cross-bridge checks, with identical error text.
pub fn validate_bridge_against(
    b: &BridgeConfig,
    servers: &std::collections::HashMap<String, ServerProfile>,
    live: &LiveKeys,
) -> Result<(), ConfigError> {
    // Hard: a `server = "<name>"` reference must resolve to a defined
    // `[server.<name>]` profile (otherwise the merge silently filled nothing).
    if let Some(name) = &b.server {
        if !servers.contains_key(name) {
            return Err(ConfigError::Validation(format!(
                "bridge '{}': references unknown server profile '{name}' — \
                 define a [server.{name}] block (or set the fields inline in \
                 [bridge.matrix])",
                b.label()
            )));
        }
    }

    // Hard: after profile-merge + auto-derive, every required Matrix field must be
    // present. Empty here means the bridge neither inherited it (no/empty profile)
    // nor set it inline.
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

    // Hard: distinct (homeserver_url, bot_user_id, device_id) per bridge — two
    // matrix-sdk sessions on one device steal each other's to-device messages
    // (the encrypted media key arrives once per device), silently breaking E2EE.
    if live.devices.contains(&(
        m.homeserver_url.clone(),
        m.bot_user_id.clone(),
        m.device_id.clone(),
    )) {
        return Err(ConfigError::Validation(format!(
            "duplicate (homeserver_url, bot_user_id, device_id) across bridges \
             for bot '{}' device '{}' — each bridge needs its own device_id \
             (omit it to auto-derive a distinct one from the bridge name)",
            m.bot_user_id, m.device_id
        )));
    }

    // Hard: distinct store_path per bridge — a shared SQLite store locks/corrupts,
    // and ghost Olm stores nest under it.
    if live.stores.contains(&m.store_path) {
        return Err(ConfigError::Validation(format!(
            "duplicate matrix store_path '{}' across bridges — each bridge \
             needs its own store",
            m.store_path
        )));
    }

    // Soft: explicitly pinning `key_transport = "olm"` while explicitly forcing
    // `media_e2ee = false` is contradictory — Olm transport is ignored with E2EE off.
    if b.livekit.key_transport == Some(KeyTransport::Olm) && b.livekit.media_e2ee == Some(false) {
        tracing::warn!(
            room_id = %m.room_id,
            "key_transport = \"olm\" set but media_e2ee = false — Olm key transport \
             is ignored; remove the media_e2ee override (auto-detect) or set it true"
        );
    }

    // Soft: a room_id reused across bridges is almost always a copy-paste slip.
    if live.rooms.contains(&m.room_id) {
        tracing::warn!(
            bridge = %b.label(),
            room_id = %m.room_id,
            "duplicate Matrix room_id across bridges"
        );
    }

    // Soft: same Discord voice channel bridged twice (a bot can only be in one VC
    // per guild — two bridges on one channel would fight over bots).
    if live
        .channels
        .contains(&(b.discord.guild_id.clone(), b.discord.channel_id.clone()))
    {
        tracing::warn!(
            bridge = %b.label(),
            guild_id = %b.discord.guild_id,
            channel_id = %b.discord.channel_id,
            "duplicate Discord (guild, channel) across bridges"
        );
    }

    Ok(())
}

impl Config {
    /// Validate cross-bridge invariants. Hard errors abort startup; softer
    /// likely-mistakes are logged as warnings.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.bridges.is_empty() {
            return Err(ConfigError::Validation(
                "config must define at least one [[bridge]]".to_string(),
            ));
        }
        self.validate_allowing_no_bridges()
    }

    /// [`validate`](Self::validate) minus the ≥1-`[[bridge]]` requirement — for a
    /// hosting deployment whose bridges all come from `tenants.toml` (#45). The
    /// caller is responsible for requiring at least one bridge across the merged
    /// static + tenant set.
    pub fn validate_allowing_no_bridges(&self) -> Result<(), ConfigError> {
        // Hard: the shared pool must have at least one bot — every active
        // channel needs an anchor, so a pool of zero can bridge nothing.
        if self.puppets.tokens.is_empty() {
            return Err(ConfigError::Validation(
                "config must define at least one puppet token in [puppets]".to_string(),
            ));
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

        // Management bot (when configured) is validated first and its identity
        // keys reserved, so no bridge — static here, or a runtime tenant later
        // via the same seeded LiveKeys — can claim its store or device.
        let mut live = LiveKeys::default();
        if let Some(m) = &self.management {
            self.validate_management(m)?;
            live.reserve_management(m);
        }

        // Per-bridge checks (hard: server refs, required matrix fields, unique
        // (homeserver, bot, device) tuple, unique store_path; soft: olm/e2ee
        // contradiction, duplicate room / (guild, channel)), folded so each bridge
        // is validated against everything accepted before it — the same primitive
        // the supervisor uses to admit a runtime-added bridge against the live set.
        for b in &self.bridges {
            validate_bridge_against(b, &self.servers, &live)?;
            live.insert(b);
        }

        Ok(())
    }

    /// Hard checks for the `[management]` block (already resolved): a usable
    /// allowlist, a resolvable server reference, and a complete identity.
    fn validate_management(&self, m: &ManagementConfig) -> Result<(), ConfigError> {
        if m.allow.is_empty() {
            return Err(ConfigError::Validation(
                "[management]: `allow` must list at least one operator MXID — \
                 an empty allowlist makes the management bot a no-op"
                    .to_string(),
            ));
        }
        for mxid in &m.allow {
            if !mxid.starts_with('@') || !mxid.contains(':') {
                return Err(ConfigError::Validation(format!(
                    "[management]: allow entry '{mxid}' is not a full MXID \
                     (expected @localpart:domain)"
                )));
            }
        }
        if self.servers.is_empty() {
            return Err(ConfigError::Validation(
                "[management] requires at least one [server.<name>] profile — the \
                 bot and every `link`ed tenant inherit their Matrix identity from a \
                 profile"
                    .to_string(),
            ));
        }
        match &m.server {
            Some(name) if !self.servers.contains_key(name) => {
                return Err(ConfigError::Validation(format!(
                    "[management]: references unknown server profile '{name}' — \
                     define a [server.{name}] block"
                )));
            }
            None if self.servers.len() > 1 => {
                return Err(ConfigError::Validation(format!(
                    "[management]: `server` is required when {} [server.<name>] \
                     profiles are defined (it is only implied with exactly one)",
                    self.servers.len()
                )));
            }
            _ => {}
        }
        let missing: Vec<&str> = [
            ("homeserver_url", m.homeserver_url.is_empty()),
            ("bot_user_id (or sender_localpart)", m.bot_user_id.is_empty()),
            ("appservice_token", m.appservice_token.is_empty()),
        ]
        .into_iter()
        .filter_map(|(field, empty)| empty.then_some(field))
        .collect();
        if !missing.is_empty() {
            return Err(ConfigError::Validation(format!(
                "[management]: missing required field(s) after profile merge: {}",
                missing.join(", ")
            )));
        }
        if m.require_discord_approval {
            if m.approval_bot_index >= self.puppets.tokens.len() {
                return Err(ConfigError::Validation(format!(
                    "[management]: approval_bot_index {} is out of range — \
                     [puppets] defines {} token(s)",
                    m.approval_bot_index,
                    self.puppets.tokens.len()
                )));
            }
            if m.approval_timeout_secs == 0 {
                return Err(ConfigError::Validation(
                    "[management]: approval_timeout_secs must be ≥ 1 — a zero \
                     TTL would expire every pending link before it could be \
                     approved"
                        .to_string(),
                ));
            }
        }
        if m.open_registration && !(m.require_room_authority && m.require_discord_approval) {
            return Err(ConfigError::Validation(
                "[management]: open_registration requires BOTH \
                 require_room_authority and require_discord_approval — \
                 without two-sided auth, any Matrix user could bridge rooms \
                 and guilds they don't control"
                    .to_string(),
            ));
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
        if let Some(m) = &self.management {
            tracing::info!(
                server = ?m.server,
                bot_user_id = %m.bot_user_id,
                device_id = %m.device_id,
                store_path = %m.store_path,
                allow = ?m.allow,
                "management bot configured"
            );
        }
    }
}
