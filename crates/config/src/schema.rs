use crate::ConfigError;
use serde::Deserialize;
use std::time::Duration;

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
    /// Optional Matrix management bot (#4): operators DM it to add/remove
    /// tenant bridges at runtime (it rewrites `tenants.toml` and triggers the
    /// same reconcile SIGHUP uses). TOML `[management]` maps here; absent means
    /// no management bot runs.
    #[serde(default)]
    pub management: Option<ManagementConfig>,
    /// Admission-control limits (#47 hardening). TOML `[limits]` maps here;
    /// every field is optional and an absent field means unlimited — the
    /// pre-limits behavior. Absent section ⇒ all unlimited.
    #[serde(default)]
    pub limits: LimitsConfig,
    /// Raw `[timing]` overrides as written in the TOML (each key optional).
    /// Resolved into `timing` at load time together with the `NVB_TIMING` env
    /// preset; consumers read the resolved `timing`, never this.
    #[serde(rename = "timing", default)]
    pub(crate) timing_raw: TimingOverrides,
    /// The resolved grace-timer profile (#38 Tier-2/3 testing): the `NVB_TIMING`
    /// preset applied first, then any explicit `[timing]` keys on top. Defaults
    /// are byte-identical to the historical hardcoded values — production
    /// behavior is unchanged unless timing is deliberately configured.
    #[serde(skip)]
    pub timing: TimingConfig,
}

/// The `[limits]` block: operator-wide admission budgets (#47). These are
/// blunt denial-of-service guards for a hosted deployment, not fairness
/// scheduling — each is a hard cap checked at its admission point.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
pub struct LimitsConfig {
    /// Maximum number of tenant bridges admitted (boot, SIGHUP reload, and the
    /// management bot's `link` all validate against it; the whole desired set
    /// is rejected beyond it, matching the all-or-nothing reload contract).
    /// Static `[[bridge]]` entries are operator-authored and do NOT count.
    #[serde(default)]
    pub max_tenants: Option<usize>,
    /// Maximum concurrent ghosts (= bridged Discord speakers) per bridge. At
    /// the cap a new speaker is simply not bridged to Matrix (WARN logged,
    /// existing ghosts unaffected); a slot frees when a ghost is released.
    /// Bounds the super-linear Olm key-distribution cost per room.
    #[serde(default)]
    pub max_ghosts_per_bridge: Option<usize>,
    /// Per-operator (MXID) cooldown in seconds between state-changing
    /// management-bot commands (`link` / `unlink`), so spawn/teardown can't be
    /// thrashed. `list`/`help` are exempt.
    #[serde(default)]
    pub mgmt_cooldown_secs: Option<u64>,
    /// Maximum tenants a single non-operator owner (MXID) may have at once
    /// (#43 open registration). Checked at `link` for non-allowlisted senders
    /// only — operators are bounded by `max_tenants` alone. `None` ⇒ unlimited.
    #[serde(default)]
    pub max_tenants_per_owner: Option<usize>,
}

/// The resolved grace-timer profile (#38). Every lifecycle timer the bridge
/// runs on, in milliseconds. `Default` is **byte-identical to the historical
/// hardcoded constants** — the only reason to touch these is the automated
/// test harness (compressed graces make Tier-2/3 scenarios run in seconds
/// instead of minutes). A non-default profile is loudly warned about at
/// startup: it must never reach an unsuspecting production deployment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimingConfig {
    /// Grace before a Matrix membership "leave" releases the puppet. Element
    /// Call memberships spuriously flap active→inactive→active (~2-3 s gap,
    /// ~every 90 s, MSC4140 expiry/heartbeat) — a reappear within the grace
    /// cancels the release. Historical value 12 s.
    pub leave_grace_ms: u64,
    /// How long a non-ghost `m.call.member` must be continuously absent from
    /// the LiveKit roster before the stale-membership reaper (#25) empties it.
    /// Historical value 45 s.
    pub reap_grace_ms: u64,
    /// How often, while a call is active, the reaper sweeps for phantom
    /// memberships. Historical value 20 s.
    pub reap_interval_ms: u64,
    /// How long a capture-only anchor (no Matrix participant of its own)
    /// lingers in the Discord channel before it leaves — absorbs a Matrix
    /// membership flap without churning the songbird/SSRC session.
    /// Historical value 90 s.
    pub anchor_idle_grace_ms: u64,
    /// Base quarantine cooldown after a puppet's voice join times out; grows
    /// `base * 2^n` per consecutive failure. Historical value 15 s.
    pub quarantine_base_ms: u64,
    /// Cap on the exponential quarantine backoff. Historical value 120 s.
    pub quarantine_max_ms: u64,
    /// How often the reconcile sweep probes each active puppet's voice
    /// connection (backstop for a silent connection death). Historical
    /// value 15 s.
    pub voice_health_sweep_ms: u64,
}

impl Default for TimingConfig {
    fn default() -> Self {
        // The pre-#38 hardcoded constants, exactly.
        Self {
            leave_grace_ms: 12_000,
            reap_grace_ms: 45_000,
            reap_interval_ms: 20_000,
            anchor_idle_grace_ms: 90_000,
            quarantine_base_ms: 15_000,
            quarantine_max_ms: 120_000,
            voice_health_sweep_ms: 15_000,
        }
    }
}

impl TimingConfig {
    /// The `NVB_TIMING=compressed` preset: every grace at 1-3 s so hermetic
    /// and live test scenarios complete in seconds. Relative ordering of the
    /// timers is preserved (sweep/interval ticks faster than the graces they
    /// feed; quarantine still backs off toward its cap).
    pub fn compressed() -> Self {
        Self {
            leave_grace_ms: 2_000,
            reap_grace_ms: 3_000,
            reap_interval_ms: 1_000,
            anchor_idle_grace_ms: 3_000,
            quarantine_base_ms: 1_000,
            quarantine_max_ms: 3_000,
            voice_health_sweep_ms: 1_000,
        }
    }

    /// True when the profile is exactly the production default — the guard the
    /// startup warn keys on.
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }

    pub fn leave_grace(&self) -> Duration {
        Duration::from_millis(self.leave_grace_ms)
    }
    pub fn reap_grace(&self) -> Duration {
        Duration::from_millis(self.reap_grace_ms)
    }
    pub fn reap_interval(&self) -> Duration {
        Duration::from_millis(self.reap_interval_ms)
    }
    pub fn anchor_idle_grace(&self) -> Duration {
        Duration::from_millis(self.anchor_idle_grace_ms)
    }
    pub fn quarantine_base(&self) -> Duration {
        Duration::from_millis(self.quarantine_base_ms)
    }
    pub fn quarantine_max(&self) -> Duration {
        Duration::from_millis(self.quarantine_max_ms)
    }
    pub fn voice_health_sweep(&self) -> Duration {
        Duration::from_millis(self.voice_health_sweep_ms)
    }
}

/// The raw `[timing]` block: per-key overrides, each optional. Kept separate
/// from [`TimingConfig`] so resolution can honor "preset first, explicit keys
/// override" — after plain deserialization an explicitly-written default value
/// would be indistinguishable from an absent key.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
pub struct TimingOverrides {
    #[serde(default)]
    pub leave_grace_ms: Option<u64>,
    #[serde(default)]
    pub reap_grace_ms: Option<u64>,
    #[serde(default)]
    pub reap_interval_ms: Option<u64>,
    #[serde(default)]
    pub anchor_idle_grace_ms: Option<u64>,
    #[serde(default)]
    pub quarantine_base_ms: Option<u64>,
    #[serde(default)]
    pub quarantine_max_ms: Option<u64>,
    #[serde(default)]
    pub voice_health_sweep_ms: Option<u64>,
}

impl TimingOverrides {
    /// Resolve the effective timing profile: start from the preset (`None` /
    /// `"default"` ⇒ production defaults, `"compressed"` ⇒
    /// [`TimingConfig::compressed`]), then apply each explicitly-set key on
    /// top. Zero is rejected for every field — a zero-period `tokio` interval
    /// panics, and a zero grace would make the debounces meaningless in a way
    /// that only surfaces as racy runtime behavior.
    pub fn resolve(&self, preset: Option<&str>) -> Result<TimingConfig, ConfigError> {
        let mut t = match preset {
            None | Some("") | Some("default") => TimingConfig::default(),
            Some("compressed") => TimingConfig::compressed(),
            Some(other) => {
                return Err(ConfigError::InvalidEnv(format!(
                    "NVB_TIMING='{other}' (expected 'default' or 'compressed')"
                )));
            }
        };
        macro_rules! apply {
            ($field:ident) => {
                if let Some(v) = self.$field {
                    if v == 0 {
                        return Err(ConfigError::Validation(format!(
                            "[timing] {} must be > 0",
                            stringify!($field)
                        )));
                    }
                    t.$field = v;
                }
            };
        }
        apply!(leave_grace_ms);
        apply!(reap_grace_ms);
        apply!(reap_interval_ms);
        apply!(anchor_idle_grace_ms);
        apply!(quarantine_base_ms);
        apply!(quarantine_max_ms);
        apply!(voice_health_sweep_ms);
        Ok(t)
    }
}

/// The `[management]` block: a Matrix bot that lets allowlisted operators
/// manage tenant bridges over DM (#4). Inherits its homeserver/appservice
/// identity from a `[server.<name>]` profile like bridges do; runs as the same
/// appservice bot account with its own device + store.
#[derive(Debug, Clone, Deserialize)]
pub struct ManagementConfig {
    /// Name of the `[server.<name>]` profile supplying the homeserver/appservice
    /// identity. Optional when exactly one profile is defined (it is implied);
    /// required otherwise.
    #[serde(default)]
    pub server: Option<String>,
    /// Operator MXIDs allowed to command the bot. Also gates room-invite
    /// auto-join: the bot only joins rooms it is invited to by these users.
    /// Must be non-empty — an empty allowlist would make the bot a no-op.
    pub allow: Vec<String>,
    /// Display name for the management bot account (best-effort, set at
    /// startup). Defaults to a built-in name when omitted.
    #[serde(default)]
    pub display_name: Option<String>,
    /// Device id base for the bot's crypto store. Auto-derives to `management`
    /// when omitted (runtime mints `<base>-<rand>` per store, like bridges).
    #[serde(default)]
    pub device_id: String,
    /// Crypto/state store directory. Auto-derives to `<data_dir>/management`
    /// when omitted.
    #[serde(default)]
    pub store_path: String,
    /// Two-sided link authority, Matrix side (#47): when `true`, a `link`
    /// sender must be a joined member of the target room with power level ≥
    /// the room's `state_default` (i.e. they could configure the room
    /// themselves) — not just an allowlisted operator. Default `false`, the
    /// pre-#47 operator model.
    #[serde(default)]
    pub require_room_authority: bool,
    /// Two-sided link authority, Discord side (#47): when `true`, a `link`
    /// that passes every probe is held PENDING (nothing written) until someone
    /// with Manage Server permission in the target guild runs
    /// `/bridge approve code:<code>` on the designated puppet bot. Default
    /// `false`.
    #[serde(default)]
    pub require_discord_approval: bool,
    /// How long a pending (unapproved) link stays claimable, in seconds.
    /// Default 600 (10 minutes).
    #[serde(default = "default_approval_timeout_secs")]
    pub approval_timeout_secs: u64,
    /// Index into `[puppets] tokens` of the bot that registers and serves the
    /// `/bridge approve` command. Default 0 (the first pool bot). That bot's
    /// application must be (re-)invited with the `applications.commands` OAuth
    /// scope or the command will not appear in guilds.
    #[serde(default)]
    pub approval_bot_index: usize,
    /// Open registration (#43): when `true`, Matrix users NOT on `allow` may
    /// also DM the bot (direct rooms only) and use the self-serve command
    /// subset — `link` (two-sided auth mandatory: `validate()` requires both
    /// `require_*` knobs on), `unlink`/`list` scoped to tenants they own, and
    /// `help`. Operator commands (`ban`/`unban`/`bans`, unscoped
    /// unlink/list) stay allowlist-only. Default `false`: the bot ignores
    /// non-allowlisted users entirely, the pre-#43 behavior.
    #[serde(default)]
    pub open_registration: bool,
    /// Public URL of the instance's privacy policy (#43 ToS follow-ups,
    /// Developer Terms §5a). When set it is appended to the `link` disclosure
    /// texts (pending-approval reply, `/bridge approve` outcome, link
    /// success). Also link it in each Discord application's Developer-Portal
    /// privacy-policy field — that part can't be automated.
    #[serde(default)]
    pub privacy_policy_url: Option<String>,
    // --- resolved from the referenced [server.<name>] profile ---
    #[serde(default)]
    pub homeserver_url: String,
    #[serde(default)]
    pub matrix_domain: Option<String>,
    #[serde(default)]
    pub bot_user_id: String,
    #[serde(default)]
    pub sender_localpart: Option<String>,
    #[serde(default)]
    pub appservice_token: String,
}

impl ManagementConfig {
    /// Base URL to use for `.well-known/matrix/client` discovery — same
    /// semantics as [`MatrixConfig::well_known_base_url`]: `matrix_domain` (the
    /// public address that actually serves `.well-known`) wins over a possibly
    /// internal `homeserver_url`.
    pub fn well_known_base_url(&self) -> String {
        match &self.matrix_domain {
            Some(domain) => format!("https://{domain}"),
            None => self.homeserver_url.trim_end_matches('/').to_owned(),
        }
    }
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
    /// WHOLE nickname (no prefix applied) a capture-only anchor wears while it
    /// idles in a voice channel during the post-call grace window (#59), so
    /// Discord users can tell "the bridge is idling here briefly" from an
    /// actual bridged participant. Default `"[Matrix: Standby]"`; truncated to
    /// the 32-char cap.
    #[serde(default = "default_standby_nickname")]
    pub standby_nickname: String,
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
    /// See `MatrixConfig::prune_stale_devices`; bridges referencing this profile
    /// inherit it unless they set their own value.
    #[serde(default)]
    pub prune_stale_devices: Option<bool>,
    /// See `MatrixConfig::rotate_ghost_sessions`; bridges referencing this profile
    /// inherit it unless they set their own value.
    #[serde(default)]
    pub rotate_ghost_sessions: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
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
    /// Owner record (#47 abuse controls): the MXID that created this entry via
    /// the management bot's `link`. Absent on hand-written entries. Only
    /// matched by `ban owner` — it never affects bridge behavior. NOTE it
    /// participates in the reconcile fingerprint like any field, so hand-editing
    /// it bounces the bridge (harmless, but know why).
    #[serde(default)]
    pub owner: Option<String>,
    /// When the entry was created via `link`, unix epoch seconds (display with
    /// `date -d @<n>`). Absent on hand-written entries.
    #[serde(default)]
    pub created: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
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

pub(crate) fn default_nickname_prefix() -> String {
    "[Matrix] ".to_string()
}

pub(crate) fn default_standby_nickname() -> String {
    "[Matrix: Standby]".to_string()
}

fn default_approval_timeout_secs() -> u64 {
    600
}

pub(crate) fn default_data_dir() -> String {
    "./nether-store".to_string()
}

/// Lowercase a bridge name into a filesystem-safe leaf: alphanumerics kept,
/// every other run collapsed to a single `-`, and leading/trailing `-` trimmed.
/// Empty result (e.g. a name of only punctuation) falls back to `"bridge"`.
pub(crate) fn slugify(name: &str) -> String {
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
pub(crate) fn server_name_from_url(homeserver_url: &str) -> Option<String> {
    let rest = homeserver_url
        .strip_prefix("https://")
        .or_else(|| homeserver_url.strip_prefix("http://"))?;
    let host_port = rest.split('/').next()?;
    if host_port.is_empty() { None } else { Some(host_port.to_owned()) }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
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
    /// Delete this store's *retired* device ids from the homeserver at startup
    /// (device hygiene, #53). History-scoped: only ids this store itself re-minted
    /// away from are ever deleted, so another deployment's devices are structurally
    /// safe. Optional override; unset defaults to on. Inherited from the
    /// `[server.<name>]` profile when omitted.
    #[serde(default)]
    pub prune_stale_devices: Option<bool>,
    /// Opportunistically rotate each ghost's outbound Olm sessions (#52): on call
    /// membership changes (rate-limited) and when the sessions age past a bound.
    /// A listener whose inbound session for a ghost died can otherwise never hear
    /// that Discord speaker again until the bridge restarts (send-only ghosts never
    /// receive the client's `m.dummy` recovery). Only affects the `olm` transport.
    /// Optional override; unset defaults to on. Inherited from the
    /// `[server.<name>]` profile when omitted.
    #[serde(default)]
    pub rotate_ghost_sessions: Option<bool>,
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

#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
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

/// The stable auto-derive leaf for a bridge's device_id / store_path: the slug of
/// its `name` when set, else the positional `bridge-{i}` fallback. Runtime-added
/// bridges must always carry a name (positions renumber across add/remove), so the
/// fallback only exists for static configs.
pub(crate) fn bridge_leaf(b: &BridgeConfig, i: usize) -> String {
    match &b.name {
        Some(name) => slugify(name),
        None => format!("bridge-{i}"),
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
