#![allow(dead_code)] // secret fields are read in later phases

use serde::Deserialize;
use std::time::Duration;
use thiserror::Error;

mod banlist;
pub mod links;
mod optout;
pub mod pairings;
pub mod relays;
pub mod companions;
mod self_serve;
mod tenants;
mod tenants_edit;
pub use banlist::{BanKind, BanList, add_ban, banlist_path, load_banlist, remove_ban};
pub use self_serve::{
    add_self_serve, load_self_serve_allow, open_self_serve, remove_self_serve,
    save_self_serve_allow, self_serve_allow_path,
};
pub use optout::{
    ApprovalOutcome, DiscordNotice, OptoutList, add_optout, load_optout, optout_path,
    remove_optout,
};
pub use tenants::{TenantsFile, list_tenant_names, load_tenants, tenant_owner_for_room, tenants_path};
pub use tenants_edit::{
    NewTenant, PriorContent, add_tenant_entry, remove_tenant_entries_matching,
    remove_tenant_entry, restore_tenants_file, tenant_discord_channel, tenant_room_id, tenant_guilds,
    tenant_owners, tenant_rooms,
};

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
    /// The companion interface (ADR 0004): an authenticated HTTP listener that
    /// hands keyed companion tools time-bounded leases. TOML `[companions]`
    /// maps here; absent (or `enabled = false`) means no listener runs.
    #[serde(default)]
    pub companions: CompanionsConfig,
    /// Raw `[timing]` overrides as written in the TOML (each key optional).
    /// Resolved into `timing` at load time together with the `NVB_TIMING` env
    /// preset; consumers read the resolved `timing`, never this.
    #[serde(rename = "timing", default)]
    timing_raw: TimingOverrides,
    /// The resolved grace-timer profile (#38 Tier-2/3 testing): the `NVB_TIMING`
    /// preset applied first, then any explicit `[timing]` keys on top. Defaults
    /// are byte-identical to the historical hardcoded values — production
    /// behavior is unchanged unless timing is deliberately configured.
    #[serde(skip)]
    pub timing: TimingConfig,
}

/// The `[companions]` block (ADR 0004). Off by default: nothing listens unless an
/// operator enables it, and the listener is meant to sit behind a TLS reverse
/// proxy (it speaks plain HTTP).
#[derive(Debug, Clone, Deserialize)]
pub struct CompanionsConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Socket address of the plain-HTTP listener (`/_nvb/v1`).
    #[serde(default = "default_companions_listen")]
    pub listen: String,
    /// A lease not renewed within this many hours ends.
    #[serde(default = "default_max_lease_hours")]
    pub max_lease_hours: u64,
    /// How long a lease survives without its event stream connected (never
    /// connected yet, or dropped) before it ends.
    #[serde(default = "default_sse_grace_secs")]
    pub sse_grace_secs: u64,
    /// Seconds between SSE keep-alive comments. Proxies such as Cloudflare cut a
    /// quiet response after ~100 s; 25 s keeps well clear of that.
    #[serde(default = "default_sse_keepalive_secs")]
    pub sse_keepalive_secs: u64,
    /// Concurrent leases one key may hold.
    #[serde(default = "default_leases_per_key")]
    pub max_leases_per_key: usize,
    /// Lease requests one key may make per minute.
    #[serde(default = "default_lease_requests_per_minute")]
    pub lease_requests_per_minute: u32,
    /// Failed authentications one client address may make per minute before it
    /// is answered 429.
    #[serde(default = "default_auth_failures_per_minute")]
    pub auth_failures_per_minute: u32,
    /// Request header carrying the real client address when the listener sits
    /// behind a proxy (e.g. `CF-Connecting-IP`). Unset ⇒ the TCP peer address.
    /// Only set it when every request comes through that proxy.
    #[serde(default)]
    pub client_ip_header: Option<String>,
}

fn default_companions_listen() -> String {
    "127.0.0.1:8787".into()
}
fn default_max_lease_hours() -> u64 {
    6
}
fn default_sse_grace_secs() -> u64 {
    30
}
fn default_sse_keepalive_secs() -> u64 {
    25
}
fn default_leases_per_key() -> usize {
    4
}
fn default_lease_requests_per_minute() -> u32 {
    10
}
fn default_auth_failures_per_minute() -> u32 {
    20
}

impl Default for CompanionsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            listen: default_companions_listen(),
            max_lease_hours: default_max_lease_hours(),
            sse_grace_secs: default_sse_grace_secs(),
            sse_keepalive_secs: default_sse_keepalive_secs(),
            max_leases_per_key: default_leases_per_key(),
            lease_requests_per_minute: default_lease_requests_per_minute(),
            auth_failures_per_minute: default_auth_failures_per_minute(),
            client_ip_header: None,
        }
    }
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
    /// Maximum tenant bridges that may target one Discord guild (#77 anti-Sybil).
    /// `max_tenants_per_owner` keys on the — federation-reachable, thus
    /// throwaway-able — owner MXID; the Discord Manage-Server requirement is the
    /// stronger anti-Sybil anchor (a real guild is hard to forge at scale), so
    /// this caps by the guild being bridged. Checked at `link` for
    /// non-operators; pending links count. `None` ⇒ unlimited.
    #[serde(default)]
    pub max_tenants_per_guild: Option<usize>,
    /// Per-owner (MXID) `link`-attempt budget within a sliding window (#77): at
    /// most this many attempts that reach the probe stage per
    /// `link_attempt_window_secs`. A velocity limit beyond the single-shot
    /// `mgmt_cooldown_secs` spacing — it bounds hammering of the expensive
    /// probe/approval flow. Checked at `link` for non-operators. `None` ⇒
    /// unlimited.
    #[serde(default)]
    pub max_link_attempts: Option<usize>,
    /// Window (seconds) for `max_link_attempts` (#77). Ignored unless
    /// `max_link_attempts` is set; defaults to 3600 (one hour) when omitted.
    #[serde(default)]
    pub link_attempt_window_secs: Option<u64>,
    /// Global (all-senders) cap on `link`-probe + invite-auto-join attempts per
    /// window (#84). Unlike `max_link_attempts` this is NOT keyed on the sender
    /// MXID, so a federated-MXID Sybil swarm cannot get fresh budget per throwaway
    /// identity — it bounds total untrusted expensive-action volume. Checked for
    /// non-operators. `None` ⇒ unlimited; REQUIRED when
    /// `[management] open_registration` is on.
    #[serde(default)]
    pub max_global_link_attempts: Option<usize>,
    /// Window (seconds) for `max_global_link_attempts` (#84). Ignored unless that
    /// knob is set; defaults to 60 s when omitted.
    #[serde(default)]
    pub global_link_window_secs: Option<u64>,
    /// How long (seconds) a `link` of a room the bot could not join stays
    /// "awaiting the requester's invite" before lapsing (#111). Within this
    /// window the requester may invite the bot to that room and the link
    /// resumes automatically; afterwards they just run `link` again. Defaults
    /// to 1800 (30 minutes) when omitted.
    #[serde(default)]
    pub invite_intent_ttl_secs: Option<u64>,
}

impl LimitsConfig {
    /// The resolved link-attempt rate budget: `(attempts, window)` when
    /// `max_link_attempts` is set (window defaulting to one hour), else `None`
    /// (#77). Single-knob activation — the window is an optional override.
    pub fn link_attempt_budget(&self) -> Option<(usize, Duration)> {
        self.max_link_attempts.map(|attempts| {
            (
                attempts,
                Duration::from_secs(self.link_attempt_window_secs.unwrap_or(3600)),
            )
        })
    }

    /// The resolved GLOBAL link-attempt budget `(attempts, window)` (#84) —
    /// `Some` only when `max_global_link_attempts` is set (window defaulting to
    /// 60 s). A cross-sender velocity cap on the expensive probe/join flow.
    pub fn global_link_budget(&self) -> Option<(usize, Duration)> {
        self.max_global_link_attempts.map(|attempts| {
            (
                attempts,
                Duration::from_secs(self.global_link_window_secs.unwrap_or(60)),
            )
        })
    }
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
    /// Base delay before the supervisor restarts a bridge whose task failed;
    /// grows `base * 2^n` per consecutive failure (#130). A bridge that stays
    /// up longer than `bridge_restart_max_ms` resets the count.
    pub bridge_restart_base_ms: u64,
    /// Cap on the exponential bridge-restart backoff — also the "this bridge is
    /// stable again" threshold that resets the failure count.
    pub bridge_restart_max_ms: u64,
    /// How often the reconcile sweep probes each active puppet's voice
    /// connection (backstop for a silent connection death). Historical
    /// value 15 s.
    pub voice_health_sweep_ms: u64,
    /// Minimum spacing between a ghost's membership-driven Olm session
    /// rotations (#52) — rate-limits rotations so Element Call's ~90 s
    /// membership flaps don't churn one-time keys on every flap. Historical
    /// value 90 s.
    pub session_rotate_min_interval_ms: u64,
    /// Grace before a paired member's Discord session leaving the channel takes
    /// effect while their Matrix half is still in the call (ADR 0001 §5): a
    /// reconnect within it neither falls back to a puppet nor sends the
    /// failover notice. Historical value: none (immediate) before 0.4.7.
    pub paired_leave_grace_ms: u64,
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
            bridge_restart_base_ms: 5_000,
            bridge_restart_max_ms: 300_000,
            voice_health_sweep_ms: 15_000,
            session_rotate_min_interval_ms: 90_000,
            paired_leave_grace_ms: 30_000,
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
            bridge_restart_base_ms: 1_000,
            bridge_restart_max_ms: 3_000,
            voice_health_sweep_ms: 1_000,
            session_rotate_min_interval_ms: 3_000,
            paired_leave_grace_ms: 2_000,
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
    pub fn bridge_restart_base(&self) -> Duration {
        Duration::from_millis(self.bridge_restart_base_ms)
    }
    pub fn bridge_restart_max(&self) -> Duration {
        Duration::from_millis(self.bridge_restart_max_ms)
    }
    /// The backoff before restart attempt `n` (0-based) of a failed bridge:
    /// `base * 2^n`, capped at `bridge_restart_max`. Saturating, so a long-lived
    /// failure streak can't overflow the shift.
    pub fn bridge_restart_delay(&self, consecutive_failures: u32) -> Duration {
        let base = self.bridge_restart_base_ms;
        let scaled = base.saturating_mul(1u64 << consecutive_failures.min(32));
        Duration::from_millis(scaled.min(self.bridge_restart_max_ms))
    }
    pub fn voice_health_sweep(&self) -> Duration {
        Duration::from_millis(self.voice_health_sweep_ms)
    }
    pub fn session_rotate_min_interval(&self) -> Duration {
        Duration::from_millis(self.session_rotate_min_interval_ms)
    }
    pub fn paired_leave_grace(&self) -> Duration {
        Duration::from_millis(self.paired_leave_grace_ms)
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
    pub bridge_restart_base_ms: Option<u64>,
    #[serde(default)]
    pub bridge_restart_max_ms: Option<u64>,
    #[serde(default)]
    pub voice_health_sweep_ms: Option<u64>,
    #[serde(default)]
    pub session_rotate_min_interval_ms: Option<u64>,
    #[serde(default)]
    pub paired_leave_grace_ms: Option<u64>,
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
        apply!(bridge_restart_base_ms);
        apply!(bridge_restart_max_ms);
        apply!(voice_health_sweep_ms);
        apply!(session_rotate_min_interval_ms);
        apply!(paired_leave_grace_ms);
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
    /// Human name of the approval bot (#80), surfaced in the pending-code reply
    /// so a self-serve user knows which Discord bot to run `/bridge approve` on.
    /// The mgmt bot cannot learn the approval bot's live Discord username, so the
    /// operator supplies it. Unset ⇒ the reply omits the name. Set it to the
    /// display name of the `[puppets]` app at `approval_bot_index`.
    #[serde(default)]
    pub approval_bot_name: Option<String>,
    /// Open registration (#43): when `true`, Matrix users NOT on `allow` may
    /// also DM the bot (direct rooms only) and use the self-serve command
    /// subset — `link` (two-sided auth mandatory: `validate()` requires both
    /// `require_*` knobs on), `unlink`/`list` scoped to tenants they own, and
    /// `help`. Operator commands (`ban`/`unban`/`bans`, unscoped
    /// unlink/list) stay allowlist-only. Default `false`: the bot ignores
    /// non-allowlisted users entirely, the pre-#43 behavior.
    #[serde(default)]
    pub open_registration: bool,
    /// Self-serve allowlist (#82): an invite-only middle tier between operators
    /// and fully-open registration. When present, only these MXIDs (plus
    /// operators) get the self-serve tier under `open_registration`; everyone
    /// else is ignored. This is a one-time SEED — at startup it is written to
    /// `<data_dir>/self_serve_allow.toml` if that file is absent, and the file
    /// (editable live via the `serve-allow`/`serve-remove`/`serve-open` DM
    /// commands) is the source of truth thereafter. Absent ⇒ unrestricted (the
    /// pre-#82 behavior: every Matrix user may self-serve when open registration
    /// is on).
    #[serde(default)]
    pub self_serve_allow: Option<Vec<String>>,
    /// Public URL of the instance's privacy policy (#43 ToS follow-ups,
    /// Developer Terms §5a). When set it is appended to the `link` disclosure
    /// texts (pending-approval reply, `/bridge approve` outcome, link
    /// success). Also link it in each Discord application's Developer-Portal
    /// privacy-policy field — that part can't be automated.
    #[serde(default)]
    pub privacy_policy_url: Option<String>,
    /// Public URL of the instance's user guide (#92). When set it is appended
    /// to the DM greeting, `help`, the pending-approval reply, and the
    /// probe/shape errors whose fixes the guide walks through — so a user who
    /// discovers the bot cold can find the walkthrough (how to get IDs, the
    /// puppet invite links, the power-level setup).
    #[serde(default)]
    pub docs_url: Option<String>,
    /// Free-text notice appended to the DM greeting and `help` (#94) — e.g. an
    /// open-beta disclaimer with the instance's help-room address.
    #[serde(default)]
    pub instance_notice: Option<String>,
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
    /// Discord user ids this bridge treats as bridge-owned unconditionally (ADR
    /// 0001 §4): never ghosted, never captured, never counted as humans. The
    /// operator override for accounts that will not pair, and the only mechanism
    /// on instances without a management bot.
    #[serde(default)]
    pub excluded_discord_user_ids: Vec<String>,
    /// Matrix user ids this bridge treats as bridge-owned unconditionally: never
    /// puppeted, never counted as humans (e.g. a companion client's account on an instance
    /// without a management bot to approve it).
    #[serde(default)]
    pub excluded_matrix_user_ids: Vec<String>,
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

fn default_nickname_prefix() -> String {
    "[Matrix] ".to_string()
}

fn default_standby_nickname() -> String {
    "[Matrix: Standby]".to_string()
}

fn default_approval_timeout_secs() -> u64 {
    600
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
fn bridge_leaf(b: &BridgeConfig, i: usize) -> String {
    match &b.name {
        Some(name) => slugify(name),
        None => format!("bridge-{i}"),
    }
}

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

    // Hard: exclusion overrides must be well-formed ids (a typo here would
    // silently exclude nobody).
    for id in &b.excluded_discord_user_ids {
        if id.parse::<u64>().map_or(true, |v| v == 0) {
            return Err(ConfigError::Validation(format!(
                "bridge '{}': excluded_discord_user_ids entry {id:?} is not a Discord user id",
                b.label()
            )));
        }
    }
    for id in &b.excluded_matrix_user_ids {
        if !(id.starts_with('@') && id.contains(':') && id.len() > 3) {
            return Err(ConfigError::Validation(format!(
                "bridge '{}': excluded_matrix_user_ids entry {id:?} is not a Matrix user id (@user:server)",
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
            companions: CompanionsConfig::default(),
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
                excluded_discord_user_ids: Vec::new(),
                excluded_matrix_user_ids: Vec::new(),
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

        if self.companions.enabled {
            let c = &self.companions;
            if c.listen.parse::<std::net::SocketAddr>().is_err() {
                return Err(ConfigError::Validation(format!(
                    "[companions] listen = \"{}\" is not a socket address (e.g. 127.0.0.1:8787)",
                    c.listen
                )));
            }
            if c.max_lease_hours == 0 || c.sse_keepalive_secs == 0 || c.max_leases_per_key == 0 {
                return Err(ConfigError::Validation(
                    "[companions] max_lease_hours, sse_keepalive_secs and max_leases_per_key must be > 0".into(),
                ));
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
        // Self-serve allowlist seed (#82): same MXID shape as `allow`.
        for mxid in m.self_serve_allow.iter().flatten() {
            if !mxid.starts_with('@') || !mxid.contains(':') {
                return Err(ConfigError::Validation(format!(
                    "[management]: self_serve_allow entry '{mxid}' is not a full MXID \
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
        // Anti-abuse budgets are mandatory on the untrusted public surface (#79):
        // open_registration exposes a DM command path to any (federated) Matrix
        // user, so shipping it without admission caps leaves zero velocity
        // limiting against a Sybil swarm. Require the caps that bound per-owner
        // and per-guild growth and the link-attempt velocity budget.
        if m.open_registration {
            let l = &self.limits;
            let missing: Vec<&str> = [
                ("max_tenants_per_owner", l.max_tenants_per_owner.is_none()),
                ("max_tenants_per_guild", l.max_tenants_per_guild.is_none()),
                ("max_link_attempts", l.max_link_attempts.is_none()),
                ("max_global_link_attempts", l.max_global_link_attempts.is_none()),
            ]
            .into_iter()
            .filter_map(|(field, unset)| unset.then_some(field))
            .collect();
            if !missing.is_empty() {
                return Err(ConfigError::Validation(format!(
                    "[management]: open_registration exposes an untrusted public DM surface, \
                     so [limits] must set anti-abuse budgets — missing: {}. Recommended: \
                     max_tenants_per_owner = 2, max_tenants_per_guild = 3, \
                     max_link_attempts = 5, max_global_link_attempts = 30, \
                     and mgmt_cooldown_secs = 30.",
                    missing.join(", ")
                )));
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

    /// Parse a single resolved `BridgeConfig` out of a one-bridge TOML config.
    fn one_bridge(name: Option<&str>, room: &str, store: &str, chan: &str) -> BridgeConfig {
        let toml = format!(
            "{}{}",
            puppets_block(&["tok"]),
            bridge_block(name, room, store, chan)
        );
        let cfg: Config = toml::from_str(&toml).unwrap();
        cfg.bridges.into_iter().next().unwrap()
    }

    #[test]
    fn live_keys_collect_and_admit_a_distinct_bridge() {
        let a = one_bridge(Some("a"), "!a:x", "/s/a", "1");
        let b = one_bridge(Some("b"), "!b:x", "/s/b", "2");
        let live = LiveKeys::from_bridges([&a]);
        assert!(live.names.contains("a"));
        assert!(live.stores.contains("/s/a"));
        assert!(live.channels.contains(&("111".into(), "1".into())));
        let servers = std::collections::HashMap::new();
        validate_bridge_against(&b, &servers, &live).expect("distinct bridge admitted");
    }

    #[test]
    fn validate_against_rejects_device_tuple_collision() {
        let a = one_bridge(Some("a"), "!a:x", "/s/a", "1");
        // Same homeserver/bot and an explicitly identical device_id.
        let mut b = one_bridge(Some("b"), "!b:x", "/s/b", "2");
        b.matrix.device_id = a.matrix.device_id.clone();
        let live = LiveKeys::from_bridges([&a]);
        let servers = std::collections::HashMap::new();
        let err = validate_bridge_against(&b, &servers, &live).unwrap_err();
        assert!(
            err.to_string().contains("duplicate (homeserver_url, bot_user_id, device_id)"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_against_rejects_store_collision() {
        let a = one_bridge(Some("a"), "!a:x", "/s/shared", "1");
        let mut b = one_bridge(Some("b"), "!b:x", "/s/b", "2");
        b.matrix.store_path = "/s/shared".into();
        let live = LiveKeys::from_bridges([&a]);
        let servers = std::collections::HashMap::new();
        let err = validate_bridge_against(&b, &servers, &live).unwrap_err();
        assert!(
            err.to_string().contains("duplicate matrix store_path '/s/shared'"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_against_rejects_unknown_server_ref_and_missing_fields() {
        let live = LiveKeys::default();
        let servers = std::collections::HashMap::new();
        let mut b = one_bridge(Some("b"), "!b:x", "/s/b", "2");
        b.server = Some("nope".into());
        let err = validate_bridge_against(&b, &servers, &live).unwrap_err();
        assert!(
            err.to_string().contains("references unknown server profile 'nope'"),
            "unexpected error: {err}"
        );
        let mut c = one_bridge(Some("c"), "!c:x", "/s/c", "3");
        c.matrix.appservice_token = String::new();
        let err = validate_bridge_against(&c, &servers, &live).unwrap_err();
        assert!(
            err.to_string().contains("missing required matrix field(s): appservice_token"),
            "unexpected error: {err}"
        );
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
    fn limits_absent_means_unlimited_and_parses_when_present() {
        // Absent [limits] section ⇒ every cap None (unlimited, pre-#47 behavior).
        let raw = format!(
            "{}{}",
            puppets_block(&["Bot a"]),
            bridge_block(Some("a"), "!a:nether.im", "./sa", "222"),
        );
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        assert_eq!(cfg.limits.max_tenants, None);
        assert_eq!(cfg.limits.max_ghosts_per_bridge, None);
        assert_eq!(cfg.limits.mgmt_cooldown_secs, None);

        // Present section with a subset of fields: set ones parse, rest stay None.
        let raw = format!(
            "[limits]\nmax_tenants = 25\nmgmt_cooldown_secs = 30\n{}{}",
            puppets_block(&["Bot a"]),
            bridge_block(Some("a"), "!a:nether.im", "./sa", "222"),
        );
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        assert_eq!(cfg.limits.max_tenants, Some(25));
        assert_eq!(cfg.limits.max_ghosts_per_bridge, None);
        assert_eq!(cfg.limits.mgmt_cooldown_secs, Some(30));
        cfg.validate().expect("limits never invalidate a config");
    }

    #[test]
    fn anti_sybil_limits_parse_and_resolve_the_link_window() {
        // #77 knobs absent ⇒ unlimited, and no attempt budget resolves.
        let l = LimitsConfig::default();
        assert_eq!(l.max_tenants_per_guild, None);
        assert_eq!(l.max_link_attempts, None);
        assert_eq!(l.link_attempt_budget(), None);

        // Present: per-guild cap parses; the attempt budget activates on the
        // single `max_link_attempts` knob, defaulting the window to one hour.
        let raw = format!(
            "[limits]\nmax_tenants_per_guild = 2\nmax_link_attempts = 5\n{}{}",
            puppets_block(&["Bot a"]),
            bridge_block(Some("a"), "!a:nether.im", "./sa", "222"),
        );
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        assert_eq!(cfg.limits.max_tenants_per_guild, Some(2));
        assert_eq!(
            cfg.limits.link_attempt_budget(),
            Some((5, std::time::Duration::from_secs(3600)))
        );
        cfg.validate().expect("anti-sybil limits never invalidate a config");

        // An explicit window overrides the default.
        let raw = format!(
            "[limits]\nmax_link_attempts = 3\nlink_attempt_window_secs = 120\n{}{}",
            puppets_block(&["Bot a"]),
            bridge_block(Some("a"), "!a:nether.im", "./sa", "222"),
        );
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        assert_eq!(
            cfg.limits.link_attempt_budget(),
            Some((3, std::time::Duration::from_secs(120)))
        );

        // The window alone (no budget) does NOT activate the gate.
        let raw = format!(
            "[limits]\nlink_attempt_window_secs = 120\n{}{}",
            puppets_block(&["Bot a"]),
            bridge_block(Some("a"), "!a:nether.im", "./sa", "222"),
        );
        let cfg: Config = toml::from_str(&raw).expect("parse failed");
        assert_eq!(cfg.limits.link_attempt_budget(), None);
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
        if let Some(m) = cfg.management.as_mut() {
            resolve_management(m, &cfg.servers, "./nether-store");
        }
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
        assert_eq!(m.prune_stale_devices, None); // unset everywhere → default-on at use site
        cfg.validate().expect("should validate");
    }

    #[test]
    fn prune_stale_devices_inherits_from_profile_and_bridge_wins() {
        // Profile-level prune_stale_devices flows to the bridge; an inline
        // [bridge.matrix] value beats it (the standard profile-merge contract).
        let raw = format!(
            "{}[server.nether]\nhomeserver_url = \"https://nether.im\"\nbot_user_id = \"@voicebridge_as:nether.im\"\nappservice_id = \"voicebridge\"\nappservice_token = \"hs_tok\"\nprune_stale_devices = false\n\n[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!a:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n\n[[bridge]]\nname = \"b\"\nserver = \"nether\"\nroom_id = \"!b:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"333\"\n[bridge.matrix]\nprune_stale_devices = true\n",
            puppets_block(&["Bot a"]),
        );
        let cfg = loaded(&raw);
        assert_eq!(cfg.bridges[0].matrix.prune_stale_devices, Some(false)); // inherited
        assert_eq!(cfg.bridges[1].matrix.prune_stale_devices, Some(true)); // override wins
    }

    #[test]
    fn rotate_ghost_sessions_inherits_from_profile_and_bridge_wins() {
        // Same profile-merge contract as prune_stale_devices (#52 knob).
        let raw = format!(
            "{}[server.nether]\nhomeserver_url = \"https://nether.im\"\nbot_user_id = \"@voicebridge_as:nether.im\"\nappservice_id = \"voicebridge\"\nappservice_token = \"hs_tok\"\nrotate_ghost_sessions = false\n\n[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!a:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n\n[[bridge]]\nname = \"b\"\nserver = \"nether\"\nroom_id = \"!b:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"333\"\n[bridge.matrix]\nrotate_ghost_sessions = true\n",
            puppets_block(&["Bot a"]),
        );
        let cfg = loaded(&raw);
        assert_eq!(cfg.bridges[0].matrix.rotate_ghost_sessions, Some(false)); // inherited
        assert_eq!(cfg.bridges[1].matrix.rotate_ghost_sessions, Some(true)); // override wins
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

    // ---- [management] (#4) ----

    fn management_block(server: Option<&str>, allow: &[&str]) -> String {
        let server_line = server.map(|s| format!("server = \"{s}\"\n")).unwrap_or_default();
        let allow_list =
            allow.iter().map(|a| format!("\"{a}\"")).collect::<Vec<_>>().join(", ");
        format!("[management]\n{server_line}allow = [{allow_list}]\n\n")
    }

    #[test]
    fn management_resolves_from_sole_profile() {
        // With exactly one [server.*] profile, `server` is implied; identity
        // fields inherit and device/store derive to the `management` leaf.
        let raw = format!(
            "{}{}{}[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!a:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
            server_block("nether"),
            management_block(None, &["@dark:nether.im"]),
        );
        let cfg = loaded(&raw);
        let m = cfg.management.as_ref().expect("management parsed");
        assert_eq!(m.server.as_deref(), Some("nether"));
        assert_eq!(m.homeserver_url, "https://nether.im");
        assert_eq!(m.bot_user_id, "@voicebridge_as:nether.im");
        assert_eq!(m.appservice_token, "hs_tok");
        assert_eq!(m.device_id, "management");
        assert_eq!(m.store_path, "./nether-store/management");
        cfg.validate().expect("should validate");
    }

    #[test]
    fn management_derives_bot_user_id_from_sender_localpart() {
        let raw = format!(
            "{}{}{}[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!a:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
            server_block_localpart("nether", "voicebridge"),
            management_block(None, &["@dark:nether.im"]),
        );
        let cfg = loaded(&raw);
        let m = cfg.management.as_ref().unwrap();
        assert_eq!(m.bot_user_id, "@voicebridge_bot:nether.im");
        cfg.validate().expect("should validate");
    }

    #[test]
    fn open_registration_requires_anti_abuse_limits() {
        // open_registration with two-sided auth but NO [limits] caps is refused
        // (#79): the public DM surface must ship with admission budgets.
        let base = format!(
            "{}{}[management]\nallow = [\"@dark:nether.im\"]\nrequire_room_authority = true\nrequire_discord_approval = true\nopen_registration = true\n\n",
            puppets_block(&["Bot a"]),
            server_block("nether"),
        );
        let err = loaded(&base).validate_allowing_no_bridges().unwrap_err();
        assert!(
            err.to_string().contains("anti-abuse budgets")
                && err.to_string().contains("max_tenants_per_owner"),
            "unexpected: {err}"
        );
        // Adding the required caps makes it validate.
        let ok = format!(
            "{}[limits]\nmax_tenants_per_owner = 2\nmax_tenants_per_guild = 3\nmax_link_attempts = 5\nmax_global_link_attempts = 30\n\n",
            base
        );
        loaded(&ok).validate_allowing_no_bridges().expect("should validate with caps set");
    }

    #[test]
    fn management_rejects_empty_allowlist() {
        let raw = format!(
            "{}{}[management]\nallow = []\n\n[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!a:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
            server_block("nether"),
        );
        let err = loaded(&raw).validate().unwrap_err();
        assert!(err.to_string().contains("allow"), "unexpected: {err}");
    }

    #[test]
    fn management_rejects_malformed_mxid() {
        let raw = format!(
            "{}{}{}[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!a:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
            server_block("nether"),
            management_block(None, &["dark:nether.im"]),
        );
        let err = loaded(&raw).validate().unwrap_err();
        assert!(err.to_string().contains("not a full MXID"), "unexpected: {err}");
    }

    #[test]
    fn management_rejects_malformed_self_serve_allow() {
        let mgmt = "[management]\nserver = \"nether\"\nallow = [\"@dark:nether.im\"]\nself_serve_allow = [\"notanmxid\"]\n\n";
        let raw = format!(
            "{}{}{mgmt}[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!a:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
            server_block("nether"),
        );
        let err = loaded(&raw).validate().unwrap_err();
        assert!(err.to_string().contains("self_serve_allow entry"), "unexpected: {err}");
    }

    #[test]
    fn management_requires_server_ref_with_multiple_profiles() {
        let raw = format!(
            "{}{}{}{}[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!a:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
            server_block("nether"),
            server_block("other"),
            management_block(None, &["@dark:nether.im"]),
        );
        let err = loaded(&raw).validate().unwrap_err();
        assert!(err.to_string().contains("`server` is required"), "unexpected: {err}");
    }

    #[test]
    fn management_rejects_unknown_server_ref() {
        let raw = format!(
            "{}{}{}[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!a:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n",
            puppets_block(&["Bot a"]),
            server_block("nether"),
            management_block(Some("rogue"), &["@dark:nether.im"]),
        );
        let err = loaded(&raw).validate().unwrap_err();
        assert!(
            err.to_string().contains("unknown server profile 'rogue'"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn management_store_is_reserved_against_bridges() {
        // A bridge explicitly claiming the management store_path is a hard error
        // (same duplicate-store rule as bridge-vs-bridge).
        let raw = format!(
            "{}{}{}[[bridge]]\nname = \"a\"\nserver = \"nether\"\nroom_id = \"!a:nether.im\"\n[bridge.discord]\nguild_id = \"111\"\nchannel_id = \"222\"\n[bridge.matrix]\nstore_path = \"./nether-store/management\"\n",
            puppets_block(&["Bot a"]),
            server_block("nether"),
            management_block(None, &["@dark:nether.im"]),
        );
        let err = loaded(&raw).validate().unwrap_err();
        assert!(
            err.to_string().contains("duplicate matrix store_path"),
            "unexpected: {err}"
        );
    }

    // --- [timing] / NVB_TIMING resolution (#38 C1). Tests drive the pure
    // `TimingOverrides::resolve` with an explicit preset argument instead of
    // touching the process env (the env test at the bottom of this module is
    // the suite's sole env reader/writer).

    #[test]
    fn timing_defaults_match_historical_consts() {
        // The values the hardcoded constants had before #38 — byte-identical.
        let t = TimingConfig::default();
        assert_eq!(t.leave_grace_ms, 12_000);
        assert_eq!(t.reap_grace_ms, 45_000);
        assert_eq!(t.reap_interval_ms, 20_000);
        assert_eq!(t.anchor_idle_grace_ms, 90_000);
        assert_eq!(t.quarantine_base_ms, 15_000);
        assert_eq!(t.quarantine_max_ms, 120_000);
        assert_eq!(t.voice_health_sweep_ms, 15_000);
        assert!(t.is_default());
        assert!(!TimingConfig::compressed().is_default());
    }

    #[test]
    fn timing_absent_section_resolves_to_default() {
        let cfg: Config = toml::from_str("").expect("parse failed");
        let t = cfg.timing_raw.resolve(None).expect("resolve failed");
        assert!(t.is_default());
        // "default"/empty preset spellings are equivalent to no preset.
        assert!(cfg.timing_raw.resolve(Some("default")).unwrap().is_default());
    }

    #[test]
    fn timing_toml_key_overrides_default() {
        let cfg: Config =
            toml::from_str("[timing]\nleave_grace_ms = 5000\n").expect("parse failed");
        let t = cfg.timing_raw.resolve(None).expect("resolve failed");
        assert_eq!(t.leave_grace_ms, 5_000);
        assert_eq!(t.reap_grace_ms, 45_000); // untouched keys keep defaults
        assert!(!t.is_default());
        assert_eq!(t.leave_grace(), Duration::from_millis(5_000));
    }

    #[test]
    fn timing_explicit_key_beats_compressed_preset() {
        let cfg: Config =
            toml::from_str("[timing]\nreap_grace_ms = 7000\n").expect("parse failed");
        let t = cfg.timing_raw.resolve(Some("compressed")).expect("resolve failed");
        // Preset first, explicit keys on top.
        assert_eq!(t.reap_grace_ms, 7_000);
        assert_eq!(t.leave_grace_ms, TimingConfig::compressed().leave_grace_ms);
        assert_eq!(t.reap_interval_ms, TimingConfig::compressed().reap_interval_ms);
    }

    #[test]
    fn timing_rejects_unknown_preset_and_zero_values() {
        let empty = TimingOverrides::default();
        let err = empty.resolve(Some("fast")).unwrap_err();
        assert!(err.to_string().contains("NVB_TIMING"), "unexpected: {err}");

        // Zero is rejected for every field: a zero-period tokio interval panics.
        let cfg: Config =
            toml::from_str("[timing]\nreap_interval_ms = 0\n").expect("parse failed");
        let err = cfg.timing_raw.resolve(None).unwrap_err();
        assert!(
            err.to_string().contains("reap_interval_ms must be > 0"),
            "unexpected: {err}"
        );
    }
}
