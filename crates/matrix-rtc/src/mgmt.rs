//! The Matrix management bot (#4) and its supervisor control-plane types.
//!
//! Operators DM the bot to add/remove tenant bridges at runtime. Commands
//! mutate `tenants.toml` (the declarative desired state from #45) and then ask
//! the supervisor — over [`ControlMsg`] — to run the exact same reconcile a
//! SIGHUP triggers, so there is a single lifecycle code path no matter who
//! initiates a change. If the supervisor rejects the reconcile, the bot rolls
//! the file back, so file and running set never diverge.
//!
//! The control types live in this crate (not the binary) because the sender —
//! the bot — is here; the supervisor in `nvb-bridge` imports them and selects
//! the receiver into its existing run loop.

use tokio::sync::{mpsc, oneshot};

/// A request from a control frontend (today: the management bot) to the
/// supervisor. Replies go over per-message oneshots so the frontend can relay
/// the outcome to the human who asked.
pub enum ControlMsg {
    /// Re-read `tenants.toml` and reconcile the running tenant set against it —
    /// the SIGHUP path, with the verdict reported back instead of only logged.
    Reconcile {
        reply: oneshot::Sender<Result<ReconcileSummary, String>>,
    },
    /// Snapshot of every running bridge (static and tenant).
    Status {
        reply: oneshot::Sender<Vec<BridgeStatus>>,
    },
    /// Is `guild_id` known (via gateway `ready`/`guild_create`) to at least one
    /// puppet bot? `false` may mean "not invited" OR "bots still connecting" —
    /// the `link` preflight treats it as a warning, never a rejection.
    GuildCoverage {
        guild_id: u64,
        reply: oneshot::Sender<bool>,
    },
    /// Per-puppet-bot guild footprint for the operator `status` command (#94).
    PoolFootprint {
        reply: oneshot::Sender<Vec<BotFootprint>>,
    },
}

/// One puppet bot's guild footprint, as reported by
/// [`ControlMsg::PoolFootprint`]. Guild knowledge comes from the gateway
/// (`ready`/`guild_create`), so shortly after startup it can under-report.
#[derive(Debug, Clone)]
pub struct BotFootprint {
    /// Index into the `[puppets]` token list (bot 0 is the approval/command bot).
    pub bot: usize,
    /// Gateway `ready` fired and the bot's command loop is live.
    pub connected: bool,
    pub guilds: Vec<u64>,
}

/// What an accepted reconcile did. `failed` lists entries whose plan was
/// accepted but whose spawn errored (the file remains the retry vehicle, as on
/// the SIGHUP path). Replacements tear down asynchronously and respawn after
/// the old future drops, so a respawn failure surfaces in the logs, not here.
#[derive(Debug, Default, Clone)]
pub struct ReconcileSummary {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub replaced: Vec<String>,
    pub failed: Vec<(String, String)>,
    pub unchanged: usize,
}

/// One running bridge, as reported by [`ControlMsg::Status`].
#[derive(Debug, Clone)]
pub struct BridgeStatus {
    pub label: String,
    /// `true` for a tenants.toml bridge, `false` for a static `[[bridge]]`.
    pub is_tenant: bool,
    pub room_id: String,
    pub guild_id: String,
    pub channel_id: String,
    /// Owner record (#47): the MXID that `link`ed this tenant, when known.
    pub owner: Option<String>,
    /// `created` epoch seconds from the tenant entry, when recorded.
    pub created: Option<i64>,
    /// Seconds since the supervisor spawned this bridge future (#94).
    pub uptime_secs: u64,
}

/// Convenience alias for the supervisor-facing sender handle.
pub type ControlTx = mpsc::UnboundedSender<ControlMsg>;

/// A Discord-side `/bridge approve <code>` invocation (#47 two-sided link
/// auth), delivered from the puppet pool via an adapter task in the binary
/// (the discord and matrix-rtc crates don't know each other; the binary
/// bridges their two mirror types). The invoker's Manage Server permission was
/// already verified against the interaction payload on the Discord side.
pub struct DiscordApproval {
    /// The code as typed (normalization is our job).
    pub code: String,
    /// Guild the command was invoked in — must match the pending link's guild.
    pub guild_id: u64,
    /// Approving Discord user, `name (id)`, for the audit trail and the
    /// Matrix-side notification.
    pub invoker: String,
    /// Outcome for the interaction follow-up: text plus, on success, the
    /// disclosure notice for the bridged voice channel (#64).
    pub reply: oneshot::Sender<ApprovalOutcome>,
}

/// The management bot's Discord-pool IO, bundled so `run`'s signature stays
/// flat: `/bridge approve` invocations in (#47), disclosure notices out (#64).
/// Either side may be absent (approval only exists with two-sided auth; the
/// notice channel is wired whenever the pool runs).
#[derive(Default)]
pub struct PoolIo {
    pub approval_rx: Option<mpsc::UnboundedReceiver<DiscordApproval>>,
    pub notice_tx: Option<mpsc::UnboundedSender<DiscordNotice>>,
}

use anyhow::{Context, Result};
use matrix_sdk::{
    Client, Room, RoomState, SessionMeta,
    authentication::{SessionTokens, matrix::MatrixSession},
    config::SyncSettings,
    deserialized_responses::SyncOrStrippedState,
    event_handler::Ctx,
    ruma::{
        OwnedUserId, UserId,
        events::room::member::{
            MembershipState, RoomMemberEventContent, StrippedRoomMemberEvent,
        },
        events::room::message::{
            MessageType, OriginalSyncRoomMessageEvent, RoomMessageEventContent,
        },
    },
};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex as StdMutex, PoisonError};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

use nvb_config::{ApprovalOutcome, BanKind, DiscordNotice, ManagementConfig, NewTenant};

/// How long a command handler waits for the supervisor to answer a control
/// request. The reconcile itself is synchronous bookkeeping (teardown happens
/// asynchronously after it), so seconds mean the supervisor loop is wedged.
const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);

/// Everything a command handler needs, shared into the SDK event handlers via
/// `add_event_handler_context`.
struct BotCtx {
    own_user: OwnedUserId,
    allow: HashSet<String>,
    control: ControlTx,
    tenants_path: String,
    /// Names of the static `[server.<name>]` profiles a `link` may reference;
    /// with exactly one, it is the default.
    server_names: Vec<String>,
    homeserver_url: String,
    appservice_token: String,
    /// Shared HTTP client for the raw `link` preflight probes (power-level GET,
    /// well-known discovery). The join probe builds its own inside
    /// `appservice_join_room`.
    http: reqwest::Client,
    /// Base URL for `.well-known/matrix/client` discovery — `matrix_domain`
    /// semantics, NOT necessarily `homeserver_url` (which may be an internal
    /// proxy address in Docker deployments).
    well_known_base: String,
    /// Serializes tenants.toml mutations: the link/unlink flow is a
    /// read-modify-write of the file plus a reconcile round-trip, and two
    /// concurrent commands interleaving those steps could drop an edit or roll
    /// back the wrong prior content.
    mutate_lock: tokio::sync::Mutex<()>,
    /// Per-operator cooldown between state-changing commands (`[limits]
    /// mgmt_cooldown_secs`, #47). `None` ⇒ unlimited.
    cooldown: Option<Duration>,
    /// Last state-change attempt per operator MXID, for the cooldown gate.
    mutate_stamps: StdMutex<HashMap<String, Instant>>,
    /// Last accepted-invite time per inviter MXID, for the per-sender invite
    /// cooldown (#89a) — reuses the `cooldown` duration (`mgmt_cooldown_secs`).
    invite_stamps: StdMutex<HashMap<String, Instant>>,
    /// `<data_dir>/banlist.toml` (#47 abuse controls) — deny-lists enforced by
    /// `link` and edited by `ban`/`unban`.
    banlist_path: String,
    /// `<data_dir>/audit.jsonl` (#47) — append-only record of every
    /// state-changing command and its outcome.
    audit_path: String,
    /// Two-sided link auth, Matrix side (#47/#86): require the `link` sender to
    /// be a joined member of the target room with an explicit individual
    /// moderator grant (see [`evaluate_sender_authority`]).
    require_room_authority: bool,
    /// Two-sided link auth, Discord side (#47): `Some(ttl)` when a `link` is
    /// held pending until `/bridge approve <code>`; `None` = commit directly.
    approval_ttl: Option<Duration>,
    /// Pending (probed, unapproved) links, keyed by approval code. Pruned
    /// lazily on insert/lookup; capped at [`PENDING_LINK_CAP`].
    pending: StdMutex<HashMap<String, PendingLink>>,
    /// Open registration (#43): non-allowlisted users get the self-serve
    /// command subset (DM-only). `false` ⇒ they are ignored entirely.
    open_registration: bool,
    /// Self-serve allowlist (#82), invite-only middle tier. `None` ⇒ unrestricted
    /// (every Matrix user may self-serve under `open_registration`); `Some(set)`
    /// ⇒ only these MXIDs (plus operators). Live source of truth, persisted to
    /// `self_serve_allow_path`, edited by the `serve-*` commands. See
    /// [`may_self_serve`](Self::may_self_serve).
    self_serve_allow: StdMutex<Option<HashSet<String>>>,
    /// `<data_dir>/self_serve_allow.toml` (#82) — persistence for the above.
    self_serve_allow_path: String,
    /// Display name of the approval bot (#80), interpolated into the pending-code
    /// reply so the user knows which Discord bot to run `/bridge approve` on.
    /// `None` ⇒ the reply omits the name.
    approval_bot_name: Option<String>,
    /// `[limits] max_tenants_per_owner` — per-owner tenant cap, enforced at
    /// `link` for non-operators only. `None` ⇒ unlimited.
    max_tenants_per_owner: Option<usize>,
    /// `[limits] max_tenants_per_guild` — per-guild tenant cap, enforced at
    /// `link` for non-operators only (#77 anti-Sybil). `None` ⇒ unlimited.
    max_tenants_per_guild: Option<usize>,
    /// `[limits]` per-owner `link`-attempt sliding-window budget
    /// `(attempts, window)` (#77), enforced at `link` for non-operators only.
    /// `None` ⇒ unlimited.
    link_attempt_budget: Option<(usize, Duration)>,
    /// Per-owner `link`-attempt timestamps within the current window, for the
    /// [`link_attempt_gate`](Self::link_attempt_gate).
    link_attempts: StdMutex<HashMap<String, Vec<Instant>>>,
    /// `[limits]` GLOBAL (all-senders) `link`+invite attempt budget
    /// `(attempts, window)` (#84), enforced for non-operators. `None` ⇒ unlimited.
    global_link_budget: Option<(usize, Duration)>,
    /// Global attempt timestamps within the current window, for the
    /// [`global_link_gate`](Self::global_link_gate) — one shared sliding window
    /// across every sender, so throwaway-MXID churn can't reset it.
    global_link_attempts: StdMutex<Vec<Instant>>,
    /// Public privacy-policy URL (#43 ToS follow-ups): appended to the `link`
    /// disclosure texts when configured.
    privacy_policy_url: Option<String>,
    /// Public user-guide URL (#92): appended to the greeting, `help`, and the
    /// replies whose fix lives in the guide.
    docs_url: Option<String>,
    /// Free-text instance notice (#94, e.g. the open-beta disclaimer + help
    /// room): appended to the greeting and `help`.
    instance_notice: Option<String>,
    /// Pending `announce` awaiting `announce confirm` (#94): the text and its
    /// confirmation deadline. One at a time, operator-tier only.
    pending_announce: StdMutex<Option<(String, Instant)>>,
    /// Per-room stamp of the last #95 room-misconfigured notification, gating
    /// re-notification to [`MISCONFIG_NOTIFY_COOLDOWN`].
    notify_stamps: StdMutex<HashMap<String, Instant>>,
    /// Rooms whose invite is currently being handled (#97): dedupes the
    /// sync's redelivery of a pending invite while a slow federated join is
    /// in flight. Entries are removed when handling finishes.
    invite_inflight: StdMutex<HashSet<String>>,
    /// Disclosure notices to the Discord pool (#64): posted into a bridged
    /// voice channel's text chat by the command bot. Best-effort.
    notice_tx: Option<mpsc::UnboundedSender<DiscordNotice>>,
}

impl BotCtx {
    /// `"\nPrivacy policy: <url>"` when configured, else empty — appended to
    /// the disclosure-bearing replies.
    fn policy_line(&self) -> String {
        self.privacy_policy_url
            .as_deref()
            .map(|u| format!("\nPrivacy policy: {u}"))
            .unwrap_or_default()
    }

    /// `"\nGuide: <url>"` when configured, else empty — appended to the
    /// greeting, `help`, and the replies whose fix the guide explains (#92).
    fn docs_line(&self) -> String {
        docs_line(self.docs_url.as_deref())
    }

    /// `"\n<notice>"` when configured, else empty — the instance notice (#94),
    /// appended to the greeting and `help` after the guide line.
    fn notice_line(&self) -> String {
        notice_line(self.instance_notice.as_deref())
    }

    /// Post a disclosure notice into a Discord channel via the pool's command
    /// bot (#64). Best-effort: no notice wiring, an absent/unparsable channel
    /// id, or a closed channel all degrade silently (the notice is a courtesy,
    /// never a gate).
    fn send_notice(&self, channel_id: Option<&str>, message: &str) {
        let (Some(tx), Some(ch)) = (&self.notice_tx, channel_id) else { return };
        let Ok(id) = ch.parse::<u64>() else { return };
        let _ = tx.send(DiscordNotice { channel_id: id, message: message.to_string() });
    }
}

impl BotCtx {
    /// Operators are the `allow` list; everyone else is a self-serve user
    /// (when `open_registration` is on) or ignored (when off).
    fn is_operator(&self, sender: &str) -> bool {
        self.allow.contains(sender)
    }

    /// May `sender` (a NON-operator) use the self-serve tier? (#43/#82) Requires
    /// `open_registration`, AND — when a self-serve allowlist is configured —
    /// membership in it. `None` allowlist ⇒ unrestricted (every user). Operators
    /// bypass this entirely (they are checked first at each call site).
    fn may_self_serve(&self, sender: &str) -> bool {
        self.open_registration
            && self
                .self_serve_allow
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .as_ref()
                .is_none_or(|set| set.contains(sender))
    }
}

/// A `link` that passed every probe and now awaits its Discord-side approval.
struct PendingLink {
    tenant: NewTenant,
    /// The MXID that requested the link (also `tenant.owner`).
    requester: String,
    /// Room the `link` command was issued in — outcome notification target.
    origin_room: matrix_sdk::ruma::OwnedRoomId,
    /// Probe warnings, carried into the eventual success reply.
    warnings: Vec<String>,
    created: Instant,
    /// Whether the `link` probe CREATED the bot's membership of `tenant.room_id`
    /// (#87). When this pending expires unapproved, the maintenance sweep may
    /// leave that room — but only if `probe_joined` (a bridge's room, which the
    /// bot was already in, is never reaped).
    probe_joined: bool,
}

/// Cap on simultaneously pending links: bounds memory against a `link` spammer
/// (each entry is tiny, so this is generous) while leaving room for honest
/// concurrent onboarding.
const PENDING_LINK_CAP: usize = 16;

/// How often the management bot runs its maintenance sweep (#87/#89b reaps +
/// the #95 room power-level check). Not urgent — pending links are already
/// bounded by [`PENDING_LINK_CAP`] — so a slow tick.
const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(300);

/// [`MAINTENANCE_INTERVAL`] with the `NVB_MAINTENANCE_INTERVAL_SECS` env
/// override (#95, min-clamped to 5 s) so live drills and tests can compress
/// the sweep without a config knob.
fn maintenance_interval() -> Duration {
    std::env::var("NVB_MAINTENANCE_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|s| Duration::from_secs(s.max(5)))
        .unwrap_or(MAINTENANCE_INTERVAL)
}

/// Per-room cooldown on the #95 room-misconfigured notification, so a broken
/// room nags its members/owner once a day, not every sweep. In-memory: a
/// restart re-notifies once, which is fine.
const MISCONFIG_NOTIFY_COOLDOWN: Duration = Duration::from_secs(24 * 3600);

/// Most pending links one homeserver's users may hold at once (#88). Bounds
/// concentration of the shared [`PENDING_LINK_CAP`] table by a federated Sybil
/// set sharing one homeserver; the home server (invite-gated) is exempt.
const PER_DOMAIN_PENDING_CAP: usize = 3;

/// Code alphabet: uppercase alphanumerics minus the ambiguous 0/O/1/I — codes
/// are relayed human-to-human across two chat systems.
const CODE_ALPHABET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
const CODE_LEN: usize = 6;

/// Mint an approval code, `NVB-XXXXXX`. 32^6 ≈ 10^9 states against a cap of
/// 16 live entries and a Discord-side actor limited to guessing through slash
/// commands — collision and brute force are both out of reach.
fn mint_approval_code() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let body: String = (0..CODE_LEN)
        .map(|_| CODE_ALPHABET[rng.gen_range(0..CODE_ALPHABET.len())] as char)
        .collect();
    format!("NVB-{body}")
}

/// Normalize a user-typed approval code: trim, uppercase, allow the `NVB-`
/// prefix to be omitted.
fn normalize_code(input: &str) -> String {
    let up = input.trim().to_ascii_uppercase();
    if up.starts_with("NVB-") { up } else { format!("NVB-{up}") }
}

fn prune_expired_pending(map: &mut HashMap<String, PendingLink>, now: Instant, ttl: Duration) {
    map.retain(|_, p| now.saturating_duration_since(p.created) < ttl);
}

/// Insert a pending link under a fresh code, enforcing the cap. Returns the
/// code, or a user-facing refusal when the table is full.
fn insert_pending(
    map: &mut HashMap<String, PendingLink>,
    pending: PendingLink,
    now: Instant,
    ttl: Duration,
) -> Result<String, String> {
    prune_expired_pending(map, now, ttl);
    if map.len() >= PENDING_LINK_CAP {
        return Err(format!(
            "There are already {PENDING_LINK_CAP} links awaiting Discord approval — try again \
             once one is approved or expires."
        ));
    }
    loop {
        let code = mint_approval_code();
        if !map.contains_key(&code) {
            map.insert(code.clone(), pending);
            return Ok(code);
        }
    }
}

/// Claim a pending link by (normalized) code + guild. A guild mismatch leaves
/// the entry in place (a typo from the wrong server must not burn the code).
fn take_pending(
    map: &mut HashMap<String, PendingLink>,
    code: &str,
    guild_id: u64,
    now: Instant,
    ttl: Duration,
) -> Result<PendingLink, String> {
    prune_expired_pending(map, now, ttl);
    let Some(p) = map.remove(code) else {
        return Err(format!(
            "Unknown or expired code `{code}` — codes expire after {} minutes; run `link` on \
             the Matrix side again for a fresh one.",
            ttl.as_secs().div_ceil(60)
        ));
    };
    if p.tenant.guild_id != guild_id.to_string() {
        // Put it back: a typo from the wrong server must not burn the code.
        map.insert(code.to_owned(), p);
        return Err(format!(
            "Code `{code}` belongs to a link for a different Discord server — run the command \
             in the server the link targets."
        ));
    }
    Ok(p)
}

/// Decide which rooms a maintenance sweep should reap (#87): for each EXPIRED
/// pending link the probe joined, the room may be left — unless a running bridge
/// still holds it (`safe_rooms`) or another LIVE (non-expired) pending still
/// targets it. Returns `(codes_to_prune, rooms_to_leave)`; all expired codes are
/// pruned, only probe-created & unused rooms are left. Pure, for unit testing —
/// the caller performs the map mutation and the best-effort leaves.
fn abandoned_probe_rooms(
    pending: &HashMap<String, PendingLink>,
    safe_rooms: &HashSet<String>,
    now: Instant,
    ttl: Duration,
) -> (Vec<String>, Vec<String>) {
    let expired = |p: &PendingLink| now.saturating_duration_since(p.created) >= ttl;
    let live_rooms: HashSet<&str> = pending
        .values()
        .filter(|p| !expired(p))
        .map(|p| p.tenant.room_id.as_str())
        .collect();
    let mut prune = Vec::new();
    let mut leave = Vec::new();
    for (code, p) in pending {
        if !expired(p) {
            continue;
        }
        prune.push(code.clone());
        let room = &p.tenant.room_id;
        if p.probe_joined
            && !safe_rooms.contains(room)
            && !live_rooms.contains(room.as_str())
            && !leave.contains(room)
        {
            leave.push(room.clone());
        }
    }
    (prune, leave)
}

/// The homeserver domain of a full MXID (`@user:example.org` → `example.org`),
/// or `None` if it isn't `@localpart:domain`-shaped (#88).
fn mxid_domain(mxid: &str) -> Option<&str> {
    mxid.strip_prefix('@')?.split_once(':').map(|(_, d)| d)
}

/// One management-bot maintenance sweep (#87/#89b): leave rooms left behind by
/// abandoned pending links and empty DM rooms — never a room a bridge holds.
/// Best-effort; the whole sweep is skipped if the running-bridge set can't be
/// read (fail-safe, the lesson from the alpha.16 revert).
async fn run_maintenance(ctx: &BotCtx, client: &Client) {
    let Some(statuses) = query_status(ctx).await else {
        tracing::debug!("maintenance: supervisor did not answer — skipping sweep");
        return;
    };

    // #95 tiers 1+2: detect bridged rooms whose power levels block the ghosts
    // and notify. Runs for every deployment (static bridges break too).
    check_room_call_power(ctx, client, &statuses).await;

    // The reap duties below serve only the open-registration surface
    // (strangers probing / abandoning rooms).
    if !ctx.open_registration {
        return;
    }

    // Never leave a room with a bridge: the running set (static + tenant) unioned
    // with the configured tenant rooms (covers a tenant momentarily not running).
    let mut safe_rooms: HashSet<String> = statuses.into_iter().map(|s| s.room_id).collect();
    if let Ok(rooms) = nvb_config::tenant_rooms(&ctx.tenants_path) {
        safe_rooms.extend(rooms);
    }

    // #87: reap rooms from expired, unapproved, probe-created pending links.
    if let Some(ttl) = ctx.approval_ttl {
        let leave = {
            let mut map = ctx.pending.lock().unwrap_or_else(PoisonError::into_inner);
            let (prune, leave) = abandoned_probe_rooms(&map, &safe_rooms, Instant::now(), ttl);
            for code in prune {
                map.remove(&code);
            }
            leave
        };
        for room in leave {
            leave_room_best_effort(ctx, &room, "abandoned pending link (#87)").await;
        }
    }

    // #89b: leave DM rooms that are now empty (only the bot remains), unless a
    // bridge holds them. A room with a PENDING invite is not abandoned (#97):
    // reaping it retracts the invite out from under the human. The pending
    // check must ask the SERVER — the SDK's `invited_members_count()` reads
    // the lagging store summary and read 0 for a 3-second-old room whose
    // invite was pending the whole time (alpha.25's gate; #100 mode 1,
    // reproduced 2/2). One extra GET per reap CANDIDATE (rare) is cheap, and
    // an error fails safe: never reap on uncertainty.
    for room in client.joined_rooms() {
        if room.joined_members_count() <= 1 {
            let id = room.room_id().as_str();
            if safe_rooms.contains(id) {
                continue;
            }
            match crate::matrix::appservice_pending_invites(
                &ctx.http,
                &ctx.homeserver_url,
                &ctx.appservice_token,
                id,
                ctx.own_user.as_str(),
            )
            .await
            {
                Ok(0) => leave_room_best_effort(ctx, id, "empty DM room (#89b)").await,
                Ok(n) => {
                    tracing::debug!(room_id = %id, pending = n, "maintenance: empty room has pending invites — not reaping")
                }
                Err(e) => {
                    tracing::warn!(room_id = %id, error = %e, "maintenance: pending-invite check failed — not reaping (fail-safe)")
                }
            }
        }
    }
}

/// #95 tiers 1+2: for every bridged room (static AND tenant), read
/// `m.room.power_levels` and check that the ghosts can post
/// `m.call.member`. A room that can't silently loses the whole
/// Discord→Matrix direction, so on detection: post a chat message into the
/// room itself (the bot is a member, and chat stays writable at PL 0 even
/// when state events are locked — the exact asymmetry causing the breakage),
/// DM the tenant owner when known, append a `room_misconfigured` audit
/// event, and WARN for the ops tooling. Per-room [`MISCONFIG_NOTIFY_COOLDOWN`].
async fn check_room_call_power(ctx: &BotCtx, client: &Client, statuses: &[BridgeStatus]) {
    for b in statuses {
        let pl = match crate::matrix::get_room_state(
            &ctx.http,
            &ctx.homeserver_url,
            &ctx.appservice_token,
            &b.room_id,
            "m.room.power_levels",
            "",
            ctx.own_user.as_str(),
        )
        .await
        {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!(room = %b.room_id, error = %e, "maintenance: power-level read failed — skipping check");
                continue;
            }
        };
        let check = evaluate_call_member_power(pl.as_ref());
        if check.ok() {
            continue;
        }
        // Deliver only once the SDK client sees the room AS JOINED — right
        // after a restart the sweep can beat the initial sync, and the bot's
        // appservice-asserted raw join leaves the LOCAL state Invited until a
        // sync lands (the same race `greet_when_joined` retries around), so
        // `room.send()` would refuse. Notifying (or stamping the cooldown)
        // before delivery can succeed would silence the in-room notice for a
        // whole cooldown window. Defer to the next sweep instead.
        let room = match matrix_sdk::ruma::RoomId::parse(b.room_id.as_str()) {
            Ok(rid) => match client.get_room(&rid) {
                Some(room) if room.state() == RoomState::Joined => room,
                _ => {
                    tracing::debug!(room = %b.room_id, "maintenance: room not (yet) locally joined — deferring misconfig notification to the next sweep");
                    continue;
                }
            },
            Err(e) => {
                tracing::warn!(room = %b.room_id, error = %e, "maintenance: unparsable room id");
                continue;
            }
        };
        // Once per room per cooldown window; a gated repeat stays silent.
        {
            let mut stamps =
                ctx.notify_stamps.lock().unwrap_or_else(PoisonError::into_inner);
            if cooldown_check(&mut stamps, &b.room_id, Instant::now(), MISCONFIG_NOTIFY_COOLDOWN)
                .is_some()
            {
                continue;
            }
        }
        let notice = misconfig_notice(&b.label, check.required, check.ghost_pl, &ctx.docs_line());
        reply(&room, &notice).await;
        if let Some(owner) = &b.owner {
            if let Err(e) = dm_user(
                client,
                owner,
                &format!("Heads-up about your bridge '{}' ({}):\n{notice}", b.label, b.room_id),
            )
            .await
            {
                tracing::warn!(owner = %owner, error = %e, "maintenance: owner DM failed");
            }
        }
        audit(ctx, "maintenance", "room_misconfigured", serde_json::json!({
            "bridge": b.label,
            "room": b.room_id,
            "required": check.required,
            "ghost_pl": check.ghost_pl,
            "owner": b.owner,
        }));
        tracing::warn!(
            bridge = %b.label,
            room = %b.room_id,
            required = check.required,
            ghost_pl = check.ghost_pl,
            "bridged room misconfigured: ghosts cannot post m.call.member — Discord→Matrix broken there"
        );
    }
}

/// Pure renderer for the #95 room-misconfigured notice (unit-tested).
fn misconfig_notice(label: &str, required: i64, ghost_pl: i64, docs: &str) -> String {
    format!(
        "⚠️ This room's permissions are blocking the voice bridge '{label}': \
         Discord speakers cannot appear in this room's call (posting \
         `{CALL_MEMBER_EVENT}` needs power level {required}, but participants \
         join at {ghost_pl}).\n\
         Fix: start an Element Call in this room once — or via /devtools set \
         `events[\"{CALL_MEMBER_EVENT}\"] = 0` in `m.room.power_levels`. \
         Element's \"Roles & Permissions\" UI shows \"Default\" either way, so \
         check the raw event. The bridge re-checks every few minutes and will \
         recover on its own once fixed.{docs}"
    )
}

/// Best-effort appservice leave with a uniform log line (#87/#89b).
async fn leave_room_best_effort(ctx: &BotCtx, room_id: &str, reason: &str) {
    match crate::matrix::appservice_leave_room(
        &ctx.homeserver_url,
        &ctx.appservice_token,
        room_id,
        ctx.own_user.as_str(),
    )
    .await
    {
        Ok(()) => tracing::info!(room_id, reason, "maintenance: left room"),
        Err(e) => tracing::warn!(room_id, reason, error = %e, "maintenance: failed to leave room"),
    }
}

impl BotCtx {
    /// The `link`/`unlink` rate gate (#47): `Some(remaining_secs)` ⇒ `sender`
    /// is still cooling down (nothing stamped); `None` ⇒ allowed, and this
    /// attempt is stamped. Stamped on *attempts* that reach a state change
    /// (past the cheap shape checks), successful or not — the budget guards
    /// spawn/teardown work, not outcomes.
    fn mutate_cooldown(&self, sender: &str) -> Option<u64> {
        let cooldown = self.cooldown?;
        let mut stamps = self
            .mutate_stamps
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let now = Instant::now();
        // Lazy sweep (#84): drop stamps older than the cooldown so the map can't
        // grow unbounded keyed by throwaway MXIDs on the open surface.
        prune_stale_stamps(&mut stamps, now, cooldown);
        cooldown_check(&mut stamps, sender, now, cooldown)
    }

    /// Per-sender invite cooldown (#89a): `Some(remaining_secs)` ⇒ `sender` sent
    /// an accepted invite too recently (nothing stamped); `None` ⇒ allowed and
    /// stamped. Reuses the `mgmt_cooldown_secs` duration and the same lazy sweep
    /// as [`mutate_cooldown`]; a separate map so invites and commands don't share
    /// one budget. `None` when the cooldown is unset (unlimited).
    fn invite_cooldown(&self, sender: &str) -> Option<u64> {
        let cooldown = self.cooldown?;
        let mut stamps = self
            .invite_stamps
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let now = Instant::now();
        prune_stale_stamps(&mut stamps, now, cooldown);
        // PEEK only (#97): the window is armed by `stamp_invite_cooldown`
        // after a successful join, never by the check itself — otherwise a
        // duplicate delivery of the invite being joined trips its own stamp.
        cooldown_peek(&stamps, sender, now, cooldown)
    }

    /// Arm the #89a per-sender invite window (#97): called only after a
    /// successful invite auto-join.
    fn stamp_invite_cooldown(&self, sender: &str) {
        if self.cooldown.is_none() {
            return;
        }
        self.invite_stamps
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(sender.to_string(), Instant::now());
    }

    /// The GLOBAL (all-senders) `link`+invite velocity gate (#84):
    /// `Some(retry_secs)` ⇒ the shared budget for the current window is spent
    /// (nothing recorded); `None` ⇒ allowed, and this attempt is recorded. Not
    /// keyed on the sender, so federated-MXID churn can't reset it; self-serve
    /// only — the caller applies it to non-operators.
    fn global_link_gate(&self) -> Option<u64> {
        let (budget, window) = self.global_link_budget?;
        let mut attempts = self
            .global_link_attempts
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        window_check(&mut attempts, Instant::now(), window, budget)
    }

    /// The `link`-attempt rate gate (#77): `Some(retry_secs)` ⇒ `sender` has
    /// spent their attempt budget for the current window (nothing recorded);
    /// `None` ⇒ allowed, and this attempt is recorded. A velocity limit on the
    /// probe/approval flow beyond the single-shot [`mutate_cooldown`]; self-serve
    /// only — the caller applies it to non-operators.
    ///
    /// [`mutate_cooldown`]: Self::mutate_cooldown
    fn link_attempt_gate(&self, sender: &str) -> Option<u64> {
        let (budget, window) = self.link_attempt_budget?;
        let mut attempts = self
            .link_attempts
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let now = Instant::now();
        let entry = attempts.entry(sender.to_owned()).or_default();
        let result = window_check(entry, now, window, budget);
        // Lazy sweep (#84): a sender whose window has fully aged out leaves an
        // empty Vec behind — drop the key so the outer map can't grow unbounded
        // one-per-MXID on the open surface.
        if entry.is_empty() {
            attempts.remove(sender);
        }
        result
    }
}

/// Drop timestamp entries older than `ttl` from a per-sender stamp map (#84).
/// Bounds unbounded growth keyed by throwaway MXIDs. Split out for unit testing.
fn prune_stale_stamps(stamps: &mut HashMap<String, Instant>, now: Instant, ttl: Duration) {
    stamps.retain(|_, &mut t| now.saturating_duration_since(t) < ttl);
}

/// Roll `<data_dir>/audit.jsonl` past which the append-only trail is rotated to
/// `audit.jsonl.1` (one generation kept) so open-surface report/link/invite spam
/// can't fill the data-dir disk (#85). 64 MiB × 2 bounded footprint.
const AUDIT_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// If `path` is at/over `max` bytes, rotate it to `<path>.1` (overwriting the
/// previous generation), so the next append starts a fresh file. Best-effort:
/// returns quietly if the file is absent/small or the rename fails. Split out
/// for unit testing (#85).
fn rotate_if_large(path: &str, max: u64) {
    match std::fs::metadata(path) {
        Ok(m) if m.len() >= max => {
            if let Err(e) = std::fs::rename(path, format!("{path}.1")) {
                tracing::warn!(error = %e, path, "audit log rotation failed");
            }
        }
        _ => {}
    }
}

/// Append one audit event (#47): `{"ts":<epoch secs>,"actor":…,"event":…,…}`
/// per line to `<data_dir>/audit.jsonl`. Best-effort — a failed write WARNs
/// and never blocks the command (the audit trail is forensic, not a gate).
fn audit(ctx: &BotCtx, actor: &str, event: &str, detail: serde_json::Value) {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let line = serde_json::json!({ "ts": ts, "actor": actor, "event": event, "detail": detail });
    rotate_if_large(&ctx.audit_path, AUDIT_MAX_BYTES);
    let write = || -> std::io::Result<()> {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&ctx.audit_path)?;
        writeln!(f, "{line}")
    };
    if let Err(e) = write() {
        tracing::warn!(error = %e, path = %ctx.audit_path, "audit log write failed");
    }
}

/// Pure core of the cooldown gate, split out for unit testing: returns the
/// whole seconds remaining (≥ 1) when `sender`'s last stamp is younger than
/// `cooldown`, else stamps `now` and clears them to proceed.
fn cooldown_check(
    stamps: &mut HashMap<String, Instant>,
    sender: &str,
    now: Instant,
    cooldown: Duration,
) -> Option<u64> {
    if let Some(last) = stamps.get(sender) {
        let elapsed = now.saturating_duration_since(*last);
        if elapsed < cooldown {
            return Some((cooldown - elapsed).as_secs().max(1));
        }
    }
    stamps.insert(sender.to_owned(), now);
    None
}

/// Non-stamping sibling of [`cooldown_check`] (#97): report the remaining
/// window without arming it. Same rounding semantics (whole seconds, ≥ 1).
fn cooldown_peek(
    stamps: &HashMap<String, Instant>,
    sender: &str,
    now: Instant,
    cooldown: Duration,
) -> Option<u64> {
    let last = stamps.get(sender)?;
    let elapsed = now.saturating_duration_since(*last);
    if elapsed < cooldown {
        Some((cooldown - elapsed).as_secs().max(1))
    } else {
        None
    }
}

/// Pure core of the link-attempt gate (#77), split out for unit testing. Prunes
/// stamps older than `window`; if fewer than `budget` remain it records `now`
/// and returns `None`, otherwise returns the whole seconds (≥ 1) until the
/// oldest surviving stamp ages out of the window — nothing is recorded.
fn window_check(
    attempts: &mut Vec<Instant>,
    now: Instant,
    window: Duration,
    budget: usize,
) -> Option<u64> {
    attempts.retain(|&t| now.saturating_duration_since(t) < window);
    if attempts.len() >= budget {
        // The oldest surviving stamp is the first to leave the window, freeing
        // the next slot. (Empty only when budget == 0 — then fall back to a
        // full window.)
        let oldest = attempts.iter().min().copied().unwrap_or(now);
        let wait = window.saturating_sub(now.saturating_duration_since(oldest));
        return Some(wait.as_secs().max(1));
    }
    attempts.push(now);
    None
}

/// Run the management bot to completion (cancelled by `shutdown`).
///
/// Builds its own matrix-sdk client on the appservice bot account with a
/// dedicated device + store (same identity rules as bridges: the device id is
/// minted per store, #30), auto-joins room invites from allowlisted operators,
/// and serves the `link` / `unlink` / `list` / `help` commands.
pub async fn run(
    cfg: ManagementConfig,
    server_names: Vec<String>,
    data_dir: String,
    limits: nvb_config::LimitsConfig,
    control: ControlTx,
    pool_io: PoolIo,
    shutdown: CancellationToken,
) -> Result<()> {
    // Pair the device identity with the crypto store (#30) — same rule as the
    // bridge bot: a wiped/copied store must re-provision, never reuse the bare
    // configured id with fresh keys.
    let resolved =
        crate::matrix::resolve_store_device_id(&cfg.store_path, &cfg.device_id);
    let device_id = resolved.id;

    let bot_user_id: OwnedUserId = cfg
        .bot_user_id
        .parse()
        .with_context(|| format!("parsing management bot_user_id '{}'", cfg.bot_user_id))?;

    let client = Client::builder()
        .homeserver_url(&cfg.homeserver_url)
        .sqlite_store(&cfg.store_path, None)
        .build()
        .await
        .context("building management-bot client")?;

    let access_token = crate::matrix::appservice_login_as(
        &cfg.homeserver_url,
        &cfg.appservice_token,
        bot_user_id.localpart(),
        &device_id,
    )
    .await
    .context("management-bot appservice login")?;
    client
        .restore_session(MatrixSession {
            meta: SessionMeta {
                user_id: bot_user_id.clone(),
                device_id: device_id.as_str().into(),
            },
            tokens: SessionTokens {
                access_token,
                refresh_token: None,
            },
        })
        .await
        .context("restoring management-bot session")?;
    tracing::info!(user_id = %bot_user_id, device_id = %device_id, "management bot session restored");

    // One bounded sync round: uploads this device's Olm keys (so operators'
    // encrypted DMs can reach it) and populates local room state. Handlers are
    // registered AFTER it, so the backlog consumed here is never replayed as
    // commands.
    match tokio::time::timeout(Duration::from_secs(60), client.sync_once(SyncSettings::default()))
        .await
    {
        Ok(Ok(_)) => tracing::info!("management bot initial sync complete"),
        Ok(Err(e)) => return Err(anyhow::anyhow!("management bot initial sync failed: {e}")),
        Err(_) => return Err(anyhow::anyhow!("management bot initial sync timed out after 60 s")),
    }

    // Best-effort profile. The account is shared with the bridges (same
    // appservice bot user, whose E2EE bridges set their own display name via
    // apply_bot_profile), so only touch it when the operator explicitly
    // configured a management display name.
    if let Some(name) = cfg.display_name.as_deref() {
        if let Err(e) = client.account().set_display_name(Some(name)).await {
            tracing::warn!(error = %e, "failed to set management bot display name");
        }
    }

    // Self-serve allowlist (#82): seed the file from the config list once (only
    // when the file is absent — the file is the live source of truth thereafter,
    // edited by the `serve-*` commands), then load it. `None` ⇒ unrestricted.
    let self_serve_allow_path = nvb_config::self_serve_allow_path(&data_dir);
    if let Some(seed) = &cfg.self_serve_allow {
        if nvb_config::load_self_serve_allow(&self_serve_allow_path)
            .map_err(|e| anyhow::anyhow!("reading self-serve allowlist: {e}"))?
            .is_none()
        {
            nvb_config::save_self_serve_allow(&self_serve_allow_path, seed)
                .map_err(|e| anyhow::anyhow!("seeding self-serve allowlist: {e}"))?;
            tracing::info!(count = seed.len(), "seeded self-serve allowlist from config (#82)");
        }
    }
    let self_serve_allow = nvb_config::load_self_serve_allow(&self_serve_allow_path)
        .map_err(|e| anyhow::anyhow!("reading self-serve allowlist: {e}"))?
        .map(|v| v.into_iter().collect::<HashSet<String>>());
    if let Some(set) = &self_serve_allow {
        tracing::info!(count = set.len(), "self-serve allowlist active — invite-only tier (#82)");
    }

    let ctx = Arc::new(BotCtx {
        own_user: bot_user_id.clone(),
        allow: cfg.allow.iter().cloned().collect(),
        control,
        tenants_path: nvb_config::tenants_path(&data_dir),
        server_names,
        homeserver_url: cfg.homeserver_url.clone(),
        appservice_token: cfg.appservice_token.clone(),
        http: reqwest::Client::builder()
            .use_rustls_tls()
            .build()
            .context("building management-bot http client")?,
        well_known_base: cfg.well_known_base_url(),
        mutate_lock: tokio::sync::Mutex::new(()),
        cooldown: limits.mgmt_cooldown_secs.map(Duration::from_secs),
        mutate_stamps: StdMutex::new(HashMap::new()),
        invite_stamps: StdMutex::new(HashMap::new()),
        banlist_path: nvb_config::banlist_path(&data_dir),
        audit_path: format!("{}/audit.jsonl", data_dir.trim_end_matches('/')),
        require_room_authority: cfg.require_room_authority,
        approval_ttl: cfg
            .require_discord_approval
            .then(|| Duration::from_secs(cfg.approval_timeout_secs)),
        pending: StdMutex::new(HashMap::new()),
        open_registration: cfg.open_registration,
        self_serve_allow: StdMutex::new(self_serve_allow),
        self_serve_allow_path,
        approval_bot_name: cfg.approval_bot_name.clone(),
        max_tenants_per_owner: limits.max_tenants_per_owner,
        max_tenants_per_guild: limits.max_tenants_per_guild,
        link_attempt_budget: limits.link_attempt_budget(),
        link_attempts: StdMutex::new(HashMap::new()),
        global_link_budget: limits.global_link_budget(),
        global_link_attempts: StdMutex::new(Vec::new()),
        privacy_policy_url: cfg.privacy_policy_url.clone(),
        docs_url: cfg.docs_url.clone(),
        instance_notice: cfg.instance_notice.clone(),
        pending_announce: StdMutex::new(None),
        notify_stamps: StdMutex::new(HashMap::new()),
        invite_inflight: StdMutex::new(HashSet::new()),
        notice_tx: pool_io.notice_tx,
    });

    // Discord-side approvals (#47 two-sided link auth): consume `/bridge
    // approve` invocations forwarded by the puppet pool. Separate task so a
    // slow commit (reconcile round-trip) never stalls the sync loop.
    if let Some(mut rx) = pool_io.approval_rx {
        let ctx = ctx.clone();
        let client = client.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    msg = rx.recv() => match msg {
                        Some(approval) => handle_discord_approval(&ctx, &client, approval).await,
                        None => break,
                    }
                }
            }
        });
    }

    // Periodic maintenance: the #95 room power-level check runs for EVERY
    // deployment (a static bridge's room can be misconfigured too — it
    // happened in prod), while the #87/#89b reaps stay gated on
    // open_registration inside `run_maintenance` (only that surface
    // accumulates stranger-probed rooms).
    {
        let ctx = ctx.clone();
        let client = client.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(maintenance_interval());
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => break,
                    _ = tick.tick() => run_maintenance(&ctx, &client).await,
                }
            }
        });
    }

    // Invites that arrived while the bot was offline (consumed by the initial
    // sync above, so the live handler never sees them).
    for room in client.invited_rooms() {
        let invite = stripped_invite_details(&room, &bot_user_id).await;
        handle_invite(&ctx, invite.as_ref(), &room).await;
    }

    client.add_event_handler_context(ctx.clone());

    // Live invites.
    client.add_event_handler(
        |ev: StrippedRoomMemberEvent, room: Room, Ctx(ctx): Ctx<Arc<BotCtx>>| async move {
            if ev.state_key != ctx.own_user
                || ev.content.membership != MembershipState::Invite
                || room.state() != RoomState::Invited
            {
                return;
            }
            let invite = InviteDetails {
                sender: ev.sender.to_string(),
                is_direct: ev.content.is_direct.unwrap_or(false),
            };
            handle_invite(&ctx, Some(&invite), &room).await;
        },
    );

    // Commands.
    client.add_event_handler(
        |ev: OriginalSyncRoomMessageEvent, room: Room, Ctx(ctx): Ctx<Arc<BotCtx>>| async move {
            if room.state() != RoomState::Joined || ev.sender == ctx.own_user {
                return;
            }
            // DM-only gate (#71): the shared bot account is joined to every
            // bridged voice room, so parsing/replying anywhere but a direct chat
            // would leak usage text, operator-only refusals, ban notices, and
            // approval codes into those rooms. Operators keep full function — they
            // DM the bot. Fronted before EVERY reply path (including `Parsed::Usage`,
            // which used to bypass the multi-user check).
            if !is_command_room(room.joined_members_count()) {
                return;
            }
            let MessageType::Text(text) = &ev.content.msgtype else { return };
            let sender = ev.sender.as_str();
            if !ctx.is_operator(sender) && !ctx.may_self_serve(sender) {
                tracing::debug!(sender = %ev.sender, "ignoring message from non-self-serve user");
                return;
            }
            match parse_command(&text.body) {
                Parsed::Cmd(cmd) => {
                    // Self-serve tier (#43): non-operators get the scoped
                    // subset; operator verbs answer with a refusal rather
                    // than silence (the sender is a legitimate user here).
                    if !ctx.is_operator(sender) && !allowed_for_self_serve(&cmd) {
                        reply(&room, "That command is operator-only.").await;
                        return;
                    }
                    // Deny-listed owners lose the self-serve tier entirely
                    // (operators are never banned-gated; they run `unban`).
                    if !ctx.is_operator(sender) {
                        match nvb_config::load_banlist(&ctx.banlist_path) {
                            Ok(bans) if bans.contains(BanKind::Owner, sender) => {
                                reply(&room, "🚫 You are banned from using this bridge.").await;
                                return;
                            }
                            Ok(_) => {}
                            Err(e) => {
                                // Same fail-closed posture as `link`.
                                tracing::error!(error = %e, "ban list unreadable — refusing self-serve command (fail-closed)");
                                reply(&room, "The ban list is unreadable — commands are disabled until an operator fixes it.").await;
                                return;
                            }
                        }
                    }
                    handle_command(&ctx, &room, sender, cmd).await
                }
                Parsed::Usage(usage) => reply(&room, &usage).await,
                // Unrecognized chatter: the handler already gated to a DM above,
                // so help out unconditionally.
                Parsed::Unknown => {
                    reply(&room, "Unrecognized command — send `help` for the command list.").await;
                }
            }
        },
    );

    tracing::info!("management bot ready");

    // Sync forever, mirroring the bridge sync loop's retry policy: auth errors
    // are fatal, transient network errors back off and retry.
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                tracing::info!("management bot stopping (shutdown)");
                return Ok(());
            }
            res = client.sync(SyncSettings::default()) => match res {
                Ok(()) => return Ok(()),
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("401") || msg.contains("403") || msg.contains("M_UNKNOWN_TOKEN") {
                        return Err(anyhow::anyhow!("management bot sync fatal auth error: {e}"));
                    }
                    tracing::warn!(error = %e, "management bot sync error — retrying in 5 s");
                    tokio::select! {
                        _ = shutdown.cancelled() => return Ok(()),
                        _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                    }
                }
            }
        }
    }
}

/// What we know about a pending invite: who sent it and whether it is a
/// direct (DM) invite — the latter gates self-serve auto-join (#43).
struct InviteDetails {
    sender: String,
    is_direct: bool,
}

/// Details of a pending invite, read from the stripped `m.room.member` state
/// for our own user (the SDK does not carry an inviter field on the `Room`).
async fn stripped_invite_details(room: &Room, own: &UserId) -> Option<InviteDetails> {
    let raw = room
        .get_state_event_static_for_key::<RoomMemberEventContent, _>(own)
        .await
        .ok()??;
    match raw.deserialize().ok()? {
        SyncOrStrippedState::Stripped(ev) => Some(InviteDetails {
            sender: ev.sender.to_string(),
            is_direct: ev.content.is_direct.unwrap_or(false),
        }),
        SyncOrStrippedState::Sync(_) => None,
    }
}

/// Accept or reject a room invite. Operators may invite the bot anywhere;
/// with `open_registration` (#43) anyone else may open a DM (direct invites
/// only — a self-serve user can't pull the bot into arbitrary rooms, and a
/// deny-listed owner can't open one at all). Joins via the appservice-asserted
/// `POST /join` — the client's own appservice-login token is guest-classified
/// on some homeservers (Tuwunel) and would 403 on rooms without `guest_access`
/// (see `matrix::appservice_join_room`).
async fn handle_invite(ctx: &BotCtx, invite: Option<&InviteDetails>, room: &Room) {
    let room_id = room.room_id().to_owned();
    // #97: sync redelivers a still-pending invite while a slow (federated)
    // join is in flight, and can surface it once more after the join lands.
    // Neither is a NEW invite. Without these guards the duplicate walked into
    // the #89a cooldown its own first pass had just stamped and DECLINED the
    // just-joined room — join-then-leave, live against the first real user
    // (2026-07-12). Guard 1: the room is already joined locally.
    if room.state() == RoomState::Joined {
        tracing::debug!(room_id = %room_id, "invite for an already-joined room — ignoring duplicate delivery");
        return;
    }
    // Guard 2: a handler for this room is already running (in-flight set;
    // the entry is removed when that handler finishes, so a genuinely new
    // invite after a failed join can retry).
    {
        let mut inflight =
            ctx.invite_inflight.lock().unwrap_or_else(PoisonError::into_inner);
        if !inflight.insert(room_id.to_string()) {
            tracing::debug!(room_id = %room_id, "invite already being handled — ignoring duplicate delivery");
            return;
        }
    }
    // Guard 3 (#110): Guard 1 reads the SDK's LOCAL state, which lags the raw
    // appservice join by a full sync cycle — and over FEDERATION the invite
    // redelivery arrives INSIDE that window (~0.5 s live on the #109 fixture),
    // so it sailed past both guards, hit the #89a cooldown its own first pass
    // had just stamped, and declined the just-joined room (join-then-leave;
    // same-server Tuwunel never redelivers, which is why 23 scenarios stayed
    // green). The server is the ground truth for our membership: if it already
    // has us joined, this "invite" is a redelivery, never a decline candidate.
    let already_joined = crate::matrix::appservice_is_room_joined(
        &ctx.http,
        &ctx.homeserver_url,
        &ctx.appservice_token,
        room_id.as_str(),
        ctx.own_user.as_str(),
    )
    .await
    // On a failed read, proceed as a new invite: the gate may re-accept (an
    // idempotent join) but must never mistakenly IGNORE a genuinely new one.
    .unwrap_or(false);
    if already_joined {
        tracing::info!(room_id = %room_id, "invite for a room the server already has us joined to — ignoring federated redelivery (#110)");
    } else {
        handle_invite_gated(ctx, invite, room).await;
    }
    ctx.invite_inflight
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(room_id.as_str());
}

async fn handle_invite_gated(ctx: &BotCtx, invite: Option<&InviteDetails>, room: &Room) {
    let room_id = room.room_id().to_owned();
    // Reachability decisions feed the audit trail (#78): who tried to open a
    // DM with the bot, from which room, and whether it was let in — the signal
    // a public operator watches for probing. `banned` is the fail-closed
    // reject; `false` accept never gets that far.
    let inviter = invite.map(|i| i.sender.as_str()).unwrap_or("<unknown>");
    let accept = match invite {
        Some(i) if ctx.is_operator(&i.sender) => {
            tracing::info!(room_id = %room_id, inviter = %i.sender, "accepting invite from allowlisted operator");
            audit(ctx, &i.sender, "invite_accepted", serde_json::json!({
                "room_id": room_id.as_str(), "reason": "operator",
            }));
            true
        }
        Some(i) if ctx.may_self_serve(&i.sender) => {
            let banned = match nvb_config::load_banlist(&ctx.banlist_path) {
                Ok(bans) => bans.contains(BanKind::Owner, &i.sender),
                // Fail closed, same posture as `link`.
                Err(e) => {
                    tracing::error!(error = %e, "ban list unreadable — rejecting self-serve invite (fail-closed)");
                    true
                }
            };
            if banned {
                tracing::warn!(room_id = %room_id, inviter = %i.sender, "rejecting invite from banned user");
                audit(ctx, &i.sender, "invite_rejected", serde_json::json!({
                    "room_id": room_id.as_str(), "reason": "banned",
                }));
                false
            } else if !i.is_direct {
                tracing::info!(room_id = %room_id, inviter = %i.sender, "rejecting non-direct invite from self-serve user (DMs only)");
                audit(ctx, &i.sender, "invite_rejected", serde_json::json!({
                    "room_id": room_id.as_str(), "reason": "not_direct",
                }));
                false
            } else if let Some(retry) = ctx.global_link_gate() {
                // Global velocity gate (#84): shed self-serve invite auto-joins
                // under load so a federated-MXID swarm can't grow the bot's
                // joined-room set without bound. The inviter can retry later.
                tracing::warn!(room_id = %room_id, inviter = %i.sender, retry, "shedding self-serve invite under global load (#84)");
                audit(ctx, &i.sender, "invite_rejected", serde_json::json!({
                    "room_id": room_id.as_str(), "reason": "global_budget",
                }));
                false
            } else {
                tracing::info!(room_id = %room_id, inviter = %i.sender, "accepting DM invite from self-serve user (open registration)");
                audit(ctx, &i.sender, "invite_accepted", serde_json::json!({
                    "room_id": room_id.as_str(), "reason": "open_registration",
                }));
                true
            }
        }
        _ => {
            tracing::warn!(
                room_id = %room_id,
                inviter = %inviter,
                "rejecting invite from non-allowlisted user"
            );
            audit(ctx, inviter, "invite_rejected", serde_json::json!({
                "room_id": room_id.as_str(), "reason": "not_allowlisted",
            }));
            false
        }
    };
    if !accept {
        if let Err(e) = room.leave().await {
            tracing::warn!(room_id = %room_id, error = %e, "failed to reject invite");
        }
        return;
    }
    // Per-sender invite cooldown (#89a): a self-serve user can't churn invite
    // auto-joins (each grows the bot's joined-room set). Operators exempt; inert
    // when `mgmt_cooldown_secs` is unset.
    if let Some(i) = invite {
        if !ctx.is_operator(&i.sender) {
            if let Some(remaining) = ctx.invite_cooldown(&i.sender) {
                tracing::info!(room_id = %room_id, inviter = %i.sender, remaining, "invite cooldown — declining self-serve invite (#89a)");
                audit(ctx, &i.sender, "invite_rejected", serde_json::json!({
                    "room_id": room_id.as_str(), "reason": "cooldown",
                }));
                if let Err(e) = room.leave().await {
                    tracing::warn!(room_id = %room_id, error = %e, "failed to decline cooled-down invite");
                }
                return;
            }
        }
    }
    match tokio::time::timeout(
        Duration::from_secs(30),
        crate::matrix::appservice_join_room(
            &ctx.homeserver_url,
            &ctx.appservice_token,
            room_id.as_str(),
            ctx.own_user.as_str(),
            // The inviter's server can always resolve the room (#103) — a
            // helpful hint for domainless v12 room ids, harmless otherwise.
            invite.and_then(|i| mxid_domain(&i.sender)),
        ),
    )
    .await
    {
        Ok(Ok(())) => {
            // #97: the #89a cooldown is stamped only on a SUCCESSFUL join —
            // a failed or duplicate attempt must never arm the tripwire that
            // declines the next delivery of the same invite.
            if let Some(i) = invite {
                if !ctx.is_operator(&i.sender) {
                    ctx.stamp_invite_cooldown(&i.sender);
                }
            }
            greet_when_joined(room.clone(), ctx.docs_url.clone(), ctx.instance_notice.clone())
        }
        Ok(Err(e)) => {
            tracing::error!(room_id = %room_id, error = %e, "failed to join invited room")
        }
        Err(_) => tracing::error!(room_id = %room_id, "join timed out after 30 s"),
    }
}

/// Post the greeting once the SDK's local state catches up with the join
/// (the appservice-asserted join is a raw HTTP call, so the `Room` stays in
/// Invited state until the next sync — sending immediately would fail).
/// Detached + best-effort: the greeting is a nicety, `help` always works.
fn greet_when_joined(room: Room, docs_url: Option<String>, instance_notice: Option<String>) {
    tokio::spawn(async move {
        for delay in [1u64, 3, 10] {
            tokio::time::sleep(Duration::from_secs(delay)).await;
            let current = room.client().get_room(room.room_id());
            if let Some(r) = current {
                if r.state() == RoomState::Joined {
                    // Defeat a spoofed `is_direct` (#85): a real DM has ≤2 joined
                    // members (inviter + bot). If the invite pulled the bot into a
                    // group room, leave instead of greeting — the command gate
                    // already ignores >2-member rooms, so it'd just be dead weight.
                    if !is_command_room(r.joined_members_count()) {
                        tracing::warn!(room_id = %r.room_id(), members = r.joined_members_count(), "invited room is not a DM (spoofed is_direct?) — leaving (#85)");
                        if let Err(e) = r.leave().await {
                            tracing::warn!(room_id = %r.room_id(), error = %e, "failed to leave non-DM invited room");
                        }
                        return;
                    }
                    reply(&r, &format!(
                        "👋 I link Discord voice channels to Matrix rooms so both sides can talk. Send `help` for commands.{}{}",
                        docs_line(docs_url.as_deref()),
                        notice_line(instance_notice.as_deref())
                    ))
                    .await;
                    return;
                }
            }
        }
        tracing::warn!(room_id = %room.room_id(), "room never reached joined state — skipping greeting");
    });
}

/// `"\nGuide: <url>"` when a docs url is configured, else empty (#92). Free
/// function so the ctx-less greeting task can use it too.
fn docs_line(url: Option<&str>) -> String {
    url.map(|u| format!("\nGuide: {u}")).unwrap_or_default()
}

/// `"\n<notice>"` when an instance notice is configured, else empty (#94).
/// Free function so the ctx-less greeting task can use it too.
fn notice_line(notice: Option<&str>) -> String {
    notice.map(|n| format!("\n{n}")).unwrap_or_default()
}

/// Best-effort reply into `room` (the SDK encrypts automatically in an
/// encrypted room).
async fn reply(room: &Room, text: &str) {
    if let Err(e) = room.send(RoomMessageEventContent::text_plain(text)).await {
        tracing::warn!(room_id = %room.room_id(), error = %e, "failed to send reply");
    }
}

/// Is `room` a direct chat the bot may serve commands in (#71)? The bot account
/// is SHARED with the bridges — it is a joined member of every bridged voice
/// room, and a `link` probe permanently joins it to arbitrary user-named rooms —
/// so the self-serve command tier's DM-only contract (#43) must be enforced on
/// the command path, not just at the invite gate. A joined-member count of two
/// (bot + one human) is the same heuristic the `Parsed::Unknown` arm already
/// used; one (the pre-join state race) also counts as direct. Anything larger is
/// a shared/bridged room where NO command, refusal, ban notice, or approval code
/// may fire.
fn is_command_room(joined_members_count: u64) -> bool {
    joined_members_count <= 2
}

// ---- command grammar ----

#[derive(Debug, PartialEq)]
enum Command {
    Help,
    List,
    Link {
        name: String,
        room_id: String,
        guild_id: String,
        channel_id: String,
        server: Option<String>,
    },
    Unlink {
        name: String,
    },
    Ban {
        kind: String,
        value: String,
    },
    Unban {
        kind: String,
        value: String,
    },
    Bans,
    /// File an abuse report (#78) — recorded for an operator to review, never
    /// acted on automatically. Self-serve tier.
    Report {
        kind: String,
        value: String,
        reason: Option<String>,
    },
    /// Show recent abuse reports (#78) — operator-only review surface.
    Reports {
        count: usize,
    },
    /// Self-serve allowlist edits (#82) — operator-only. `SelfServe` carries the
    /// verb (`allow`/`remove`/`open`/`list`) + optional MXID arg.
    ServeAllow {
        mxid: String,
    },
    ServeRemove {
        mxid: String,
    },
    ServeOpen,
    ServeList,
    /// Operator: puppet-pool guild footprint (#94).
    Status,
    /// Operator: per-bridge uptime/owner + process RSS (#94).
    Stats,
    /// Operator: DM `text` to `mxid` as the bot (#94).
    Msg {
        mxid: String,
        text: String,
    },
    /// Operator: mass-DM every tenant owner (#94). Two-step: `announce <text>`
    /// stages, `announce confirm` sends.
    Announce {
        text: String,
    },
    AnnounceConfirm,
}

#[derive(Debug, PartialEq)]
enum Parsed {
    Cmd(Command),
    /// A known verb with wrong arguments → the usage string to send back.
    Usage(String),
    /// Not one of our verbs — stay silent outside DMs.
    Unknown,
}

const LINK_USAGE: &str = "Usage: `link <name> <matrix-room-id> <discord-guild-id> <discord-channel-id> [server]`\nQuote names containing spaces: `link \"Guest Voice\" !abc:example.org 123456 789012`";
const UNLINK_USAGE: &str = "Usage: `unlink <name>`";
const BAN_USAGE: &str =
    "Usage: `ban <owner|guild|room> <value>` — e.g. `ban owner @evil:example.org`, `ban guild 123456`, `ban room !abc:example.org`. Banning also unlinks every matching tenant.";
const SERVE_USAGE: &str =
    "Usage: `serve-allow <@mxid:server>` / `serve-remove <@mxid:server>` — restrict self-serve to an allowlist (#82). `serve-open` reverts to open (everyone). `serve-list` shows the current list.";
const UNBAN_USAGE: &str = "Usage: `unban <owner|guild|room> <value>`";
const REPORT_USAGE: &str = "Usage: `report <owner|guild|room> <value> [reason]` — flag an abusive bridge for an operator to review, e.g. `report guild 123456 harassing users`. This does not remove the bridge; an operator decides.";
const REPORTS_USAGE: &str = "Usage: `reports [count]` — show the most recent abuse reports (default 10).";
const MSG_USAGE: &str = "Usage: `msg <@mxid:server> <text>` — DM a user as the bridge bot, e.g. `msg @user:example.org please trim your bridge usage`.";
const ANNOUNCE_USAGE: &str = "Usage: `announce <text>` — stage a DM to every bridge owner, then `announce confirm` within 2 minutes to send it. Use sparingly.";

/// Longest free-text `report` reason kept in the audit trail. Reports come from
/// untrusted self-serve users, so cap the bytes appended per line (#78).
const REPORT_REASON_MAX: usize = 280;
/// Default / ceiling for how many recent reports the `reports` command renders.
const DEFAULT_REPORTS: usize = 10;
const MAX_REPORTS: usize = 50;

const HELP_TEXT: &str = "Voice bridge management commands:\n  list — all bridges (static + tenants) and whether they are running\n  link <name> <matrix-room-id> <discord-guild-id> <discord-channel-id> [server] — bridge a Discord voice channel to a Matrix room\n  unlink <name> — remove a bridge created with link\n  ban <owner|guild|room> <value> — deny-list an owner MXID / Discord guild / Matrix room AND unlink every matching tenant\n  unban <owner|guild|room> <value> — remove a ban\n  bans — show the deny-lists\n  report <owner|guild|room> <value> [reason] — file an abuse report (recorded, not acted on)\n  reports [count] — show recent abuse reports, each with the `ban` to run\n  serve-allow <@mxid> / serve-remove <@mxid> — restrict the self-serve tier to an allowlist; serve-open reopens to everyone; serve-list shows it\n  status — puppet-pool guild footprint (Discord verification tripwire)\n  stats — per-bridge uptime + owner, and process memory\n  msg <@mxid> <text> — DM a user as the bridge bot\n  announce <text> — stage a DM to every bridge owner; `announce confirm` sends it\n  help — this text\nQuote names containing spaces: link \"Guest Voice\" !abc:example.org 123456 789012";

/// Help for self-serve users (#43): only the verbs they may run, plus the
/// consent flow they will hit.
const SELF_SERVE_HELP_TEXT: &str = "Voice bridge commands:\n  link <name> <matrix-room-id> <discord-guild-id> <discord-channel-id> — bridge a Discord voice channel to a Matrix room. You must be a moderator of the Matrix room, and someone with Manage Server on the Discord side must approve the link with the `/bridge approve` command and the code you'll receive.\n  unlink <name> — remove a bridge you created\n  list — your bridges\n  report <owner|guild|room> <value> [reason] — report an abusive bridge for an operator to review\n  help — this text\nQuote names containing spaces: link \"Guest Voice\" !abc:example.org 123456 789012";

/// Posted into a bridged voice channel's text chat when its link is removed
/// (#64) — by `unlink` and by a ban's teardown sweep. Deliberately neutral:
/// the channel learns the bridge is gone, not why.
const UNLINK_NOTICE_TEXT: &str =
    "🔌 This voice channel is no longer bridged to Matrix — the link was removed.";

/// The self-serve command tier (#43): which verbs a non-operator may run when
/// `open_registration` is on. Ownership scoping inside `list`/`unlink` narrows
/// further; `ban`/`unban`/`bans` are operator-only.
fn allowed_for_self_serve(cmd: &Command) -> bool {
    matches!(
        cmd,
        Command::Help
            | Command::List
            | Command::Link { .. }
            | Command::Unlink { .. }
            | Command::Report { .. }
    )
}

/// Parse one message body. The verb is case-insensitive; arguments support
/// double-quoting for names with spaces.
fn parse_command(body: &str) -> Parsed {
    let Some(tokens) = tokenize(body.trim()) else {
        return Parsed::Unknown; // unbalanced quote in chatter — not for us
    };
    let Some((verb, args)) = tokens.split_first() else {
        return Parsed::Unknown;
    };
    match verb.to_ascii_lowercase().as_str() {
        "help" => Parsed::Cmd(Command::Help),
        "list" => Parsed::Cmd(Command::List),
        "link" => match args {
            [name, room_id, guild_id, channel_id, rest @ ..] if rest.len() <= 1 => {
                Parsed::Cmd(Command::Link {
                    name: name.clone(),
                    room_id: room_id.clone(),
                    guild_id: guild_id.clone(),
                    channel_id: channel_id.clone(),
                    server: rest.first().cloned(),
                })
            }
            _ => Parsed::Usage(LINK_USAGE.to_string()),
        },
        "unlink" => match args {
            [name] => Parsed::Cmd(Command::Unlink { name: name.clone() }),
            _ => Parsed::Usage(UNLINK_USAGE.to_string()),
        },
        "ban" => match args {
            [kind, value] => Parsed::Cmd(Command::Ban { kind: kind.clone(), value: value.clone() }),
            _ => Parsed::Usage(BAN_USAGE.to_string()),
        },
        "unban" => match args {
            [kind, value] => {
                Parsed::Cmd(Command::Unban { kind: kind.clone(), value: value.clone() })
            }
            _ => Parsed::Usage(UNBAN_USAGE.to_string()),
        },
        "bans" => match args {
            [] => Parsed::Cmd(Command::Bans),
            _ => Parsed::Usage("Usage: `bans`".to_string()),
        },
        "report" => match args {
            // At least a kind + value; everything after is a free-text reason.
            [kind, value, reason @ ..] => Parsed::Cmd(Command::Report {
                kind: kind.clone(),
                value: value.clone(),
                reason: (!reason.is_empty()).then(|| reason.join(" ")),
            }),
            _ => Parsed::Usage(REPORT_USAGE.to_string()),
        },
        "reports" => match args {
            [] => Parsed::Cmd(Command::Reports { count: DEFAULT_REPORTS }),
            [n] => match n.parse::<usize>() {
                Ok(c) if c >= 1 => Parsed::Cmd(Command::Reports { count: c.min(MAX_REPORTS) }),
                _ => Parsed::Usage(REPORTS_USAGE.to_string()),
            },
            _ => Parsed::Usage(REPORTS_USAGE.to_string()),
        },
        "serve-allow" => match args {
            [mxid] => Parsed::Cmd(Command::ServeAllow { mxid: mxid.clone() }),
            _ => Parsed::Usage(SERVE_USAGE.to_string()),
        },
        "serve-remove" => match args {
            [mxid] => Parsed::Cmd(Command::ServeRemove { mxid: mxid.clone() }),
            _ => Parsed::Usage(SERVE_USAGE.to_string()),
        },
        "serve-open" => match args {
            [] => Parsed::Cmd(Command::ServeOpen),
            _ => Parsed::Usage("Usage: `serve-open`".to_string()),
        },
        "serve-list" => match args {
            [] => Parsed::Cmd(Command::ServeList),
            _ => Parsed::Usage("Usage: `serve-list`".to_string()),
        },
        "status" => match args {
            [] => Parsed::Cmd(Command::Status),
            _ => Parsed::Usage("Usage: `status`".to_string()),
        },
        "stats" => match args {
            [] => Parsed::Cmd(Command::Stats),
            _ => Parsed::Usage("Usage: `stats`".to_string()),
        },
        "msg" => match args {
            [mxid, text @ ..] if !text.is_empty() => Parsed::Cmd(Command::Msg {
                mxid: mxid.clone(),
                text: text.join(" "),
            }),
            _ => Parsed::Usage(MSG_USAGE.to_string()),
        },
        "announce" => match args {
            [one] if one == "confirm" => Parsed::Cmd(Command::AnnounceConfirm),
            text if !text.is_empty() => {
                Parsed::Cmd(Command::Announce { text: text.join(" ") })
            }
            _ => Parsed::Usage(ANNOUNCE_USAGE.to_string()),
        },
        _ => Parsed::Unknown,
    }
}

/// Whitespace tokenizer with double-quote grouping. `None` on an unbalanced
/// quote.
fn tokenize(s: &str) -> Option<Vec<String>> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut had_token = false;
    for c in s.chars() {
        match c {
            '"' => {
                in_quotes = !in_quotes;
                had_token = true; // `""` is an (empty) token
            }
            c if c.is_whitespace() && !in_quotes => {
                if had_token {
                    out.push(std::mem::take(&mut cur));
                    had_token = false;
                }
            }
            c => {
                cur.push(c);
                had_token = true;
            }
        }
    }
    if in_quotes {
        return None;
    }
    if had_token {
        out.push(cur);
    }
    Some(out)
}

// ---- command execution ----

async fn handle_command(ctx: &BotCtx, room: &Room, sender: &str, cmd: Command) {
    match cmd {
        Command::Help => {
            let text =
                if ctx.is_operator(sender) { HELP_TEXT } else { SELF_SERVE_HELP_TEXT };
            reply(room, &format!("{text}{}{}", ctx.docs_line(), ctx.notice_line())).await
        }
        Command::List => handle_list(ctx, room, sender).await,
        Command::Unlink { name } => handle_unlink(ctx, room, sender, &name).await,
        Command::Link {
            name,
            room_id,
            guild_id,
            channel_id,
            server,
        } => handle_link(ctx, room, sender, name, room_id, guild_id, channel_id, server).await,
        Command::Ban { kind, value } => handle_ban(ctx, room, sender, &kind, &value).await,
        Command::Unban { kind, value } => handle_unban(ctx, room, sender, &kind, &value).await,
        Command::Bans => handle_bans(ctx, room).await,
        Command::Report { kind, value, reason } => {
            handle_report(ctx, room, sender, &kind, &value, reason.as_deref()).await
        }
        Command::Reports { count } => handle_reports(ctx, room, count).await,
        Command::ServeAllow { mxid } => handle_serve_allow(ctx, room, sender, &mxid).await,
        Command::ServeRemove { mxid } => handle_serve_remove(ctx, room, sender, &mxid).await,
        Command::ServeOpen => handle_serve_open(ctx, room, sender).await,
        Command::ServeList => handle_serve_list(ctx, room).await,
        Command::Status => handle_status(ctx, room).await,
        Command::Stats => handle_stats(ctx, room).await,
        Command::Msg { mxid, text } => handle_msg(ctx, room, sender, &mxid, &text).await,
        Command::Announce { text } => handle_announce_stage(ctx, room, text).await,
        Command::AnnounceConfirm => handle_announce_confirm(ctx, room, sender).await,
    }
}

/// Ask the supervisor for the running set. `None` = control channel down or
/// timed out (caller reports).
async fn query_status(ctx: &BotCtx) -> Option<Vec<BridgeStatus>> {
    let (tx, rx) = oneshot::channel();
    ctx.control.send(ControlMsg::Status { reply: tx }).ok()?;
    tokio::time::timeout(CONTROL_TIMEOUT, rx).await.ok()?.ok()
}

/// Ask the supervisor for the per-puppet-bot guild footprint (#94). `None` =
/// control channel down or timed out (caller reports).
async fn query_pool_footprint(ctx: &BotCtx) -> Option<Vec<BotFootprint>> {
    let (tx, rx) = oneshot::channel();
    ctx.control.send(ControlMsg::PoolFootprint { reply: tx }).ok()?;
    tokio::time::timeout(CONTROL_TIMEOUT, rx).await.ok()?.ok()
}

/// Ask the supervisor to reconcile against the (just-rewritten) tenants file.
/// Outer `None` = no answer (channel down / timeout); inner result is the
/// reconcile verdict.
async fn request_reconcile(ctx: &BotCtx) -> Option<Result<ReconcileSummary, String>> {
    let (tx, rx) = oneshot::channel();
    ctx.control.send(ControlMsg::Reconcile { reply: tx }).ok()?;
    tokio::time::timeout(CONTROL_TIMEOUT, rx).await.ok()?.ok()
}

/// Ask the supervisor whether any puppet bot is a member of `guild_id`.
/// `None` = no answer (channel down / timeout — caller reports "could not
/// verify"); the answer itself is computed synchronously from pool memory.
async fn query_guild_coverage(ctx: &BotCtx, guild_id: u64) -> Option<bool> {
    let (tx, rx) = oneshot::channel();
    ctx.control
        .send(ControlMsg::GuildCoverage { guild_id, reply: tx })
        .ok()?;
    tokio::time::timeout(CONTROL_TIMEOUT, rx).await.ok()?.ok()
}

// ---- link preflight (#47 slice 1) ----

/// Verdict of a room's `m.room.power_levels` for the bridge's ghost users:
/// can a freshly-joined ghost post `org.matrix.msc3401.call.member`?
#[derive(Debug, PartialEq, Eq)]
struct CallPowerCheck {
    /// Effective power level required to post the call-member state event.
    required: i64,
    /// Power level a freshly-joined appservice ghost gets. Ghosts are minted
    /// dynamically, so they are never in the `users` map — `users_default`
    /// is what they will have.
    ghost_pl: i64,
}

impl CallPowerCheck {
    fn ok(&self) -> bool {
        self.ghost_pl >= self.required
    }
}

/// Pure evaluation of a room's power levels (unit-tested below). `content` is
/// the raw `m.room.power_levels` event content; `None` means the event is
/// absent (GET 404). Per spec an absent event implies `state_default = 0`
/// (would pass), but such rooms are practically nonexistent and an absent
/// event here more likely means something unexpected — fail safe by treating
/// it as the spec default-with-event value of 50.
fn evaluate_call_member_power(content: Option<&serde_json::Value>) -> CallPowerCheck {
    let Some(c) = content else {
        return CallPowerCheck { required: 50, ghost_pl: 0 };
    };
    let required = c
        .get("events")
        .and_then(|e| e.get(CALL_MEMBER_EVENT))
        .and_then(pl_int)
        .or_else(|| c.get("state_default").and_then(pl_int))
        .unwrap_or(50);
    let ghost_pl = c.get("users_default").and_then(pl_int).unwrap_or(0);
    CallPowerCheck { required, ghost_pl }
}

const CALL_MEMBER_EVENT: &str = "org.matrix.msc3401.call.member";

/// The Element "Moderator" power level — the floor an explicit grant must reach
/// to count as room authority (#86).
const MODERATOR_FLOOR: i64 = 50;

/// Verdict of the Matrix-side link-authority check (#47/#86): may `sender`
/// bridge this room? The bar is an INDIVIDUAL moderator grant — the sender must
/// be a joined member with an EXPLICIT `users[sender]` power-level entry that is
/// both above the room's `users_default` and at least [`MODERATOR_FLOOR`]. Keying
/// on an explicit, above-default grant (rather than `state_default`) is immune to
/// a room lowering `state_default` OR raising `users_default` to make every
/// member look authorized — the vector from the #79 review (finding 6).
#[derive(Debug, PartialEq, Eq)]
struct SenderAuthority {
    joined: bool,
    /// The sender's effective PL (explicit `users[]` entry, else `users_default`).
    sender_pl: i64,
    /// Whether the sender has an EXPLICIT `users[]` entry (not just the default).
    explicit: bool,
    /// The room's `users_default` — the bar the explicit grant must exceed.
    users_default: i64,
    /// v12+ (MSC4289): the sender is a room CREATOR — implicit infinite power,
    /// and FORBIDDEN from appearing in `users[]`, so the explicit-grant check
    /// can never see them (#112: a v12 creator could not link their own room,
    /// live on the #109 fixture). Creators are a fixed, unforgeable set in the
    /// create event — recognizing them keeps the #79-finding-6 immunity (no
    /// defaults manipulation can mint one).
    creator: bool,
}

impl SenderAuthority {
    fn ok(&self) -> bool {
        self.joined
            && (self.creator
                || (self.explicit
                    && self.sender_pl > self.users_default
                    && self.sender_pl >= MODERATOR_FLOOR))
    }
}

/// Pure evaluation of a sender's authority over a room (unit-tested below).
/// `pl_content` is the raw `m.room.power_levels` content (`None` = absent → no
/// explicit grants, so fail-safe reject); `member_content` is the sender's
/// `m.room.member` content (`None` = no membership event → not joined);
/// `creators` is the room's v12+ creator set ([`read_room_creators`] — empty
/// for pre-v12 rooms or on any read failure, fail-closed to the grant path).
fn evaluate_sender_authority(
    pl_content: Option<&serde_json::Value>,
    member_content: Option<&serde_json::Value>,
    sender: &str,
    creators: &[String],
) -> SenderAuthority {
    let joined = member_content
        .and_then(|m| m.get("membership"))
        .and_then(|v| v.as_str())
        == Some("join");
    let (sender_pl, explicit, users_default) = match pl_content {
        Some(c) => {
            let users_default = c.get("users_default").and_then(pl_int).unwrap_or(0);
            let explicit_pl = c.get("users").and_then(|u| u.get(sender)).and_then(pl_int);
            (explicit_pl.unwrap_or(users_default), explicit_pl.is_some(), users_default)
        }
        None => (0, false, 0),
    };
    let creator = creators.iter().any(|c| c == sender);
    SenderAuthority { joined, sender_pl, explicit, users_default, creator }
}

/// The room's v12+ CREATOR set (MSC4289), or empty for pre-v12 rooms / on any
/// read failure (fail-closed to the explicit-grant path). v12 room ids ARE the
/// create event id (MSC4291: `!<hash>` ↔ `$<hash>`), so the full create event
/// — whose `sender` is the first creator — is fetchable directly; any
/// `additional_creators` ride the create content.
async fn read_room_creators(ctx: &BotCtx, room_id: &str) -> Vec<String> {
    let create = crate::matrix::get_room_state(
        &ctx.http,
        &ctx.homeserver_url,
        &ctx.appservice_token,
        room_id,
        "m.room.create",
        "",
        ctx.own_user.as_str(),
    )
    .await
    .ok()
    .flatten();
    let Some(create) = create else { return Vec::new() };
    let v12plus = create
        .get("room_version")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse::<u64>().ok())
        .is_some_and(|v| v >= 12);
    if !v12plus {
        return Vec::new();
    }
    let mut creators = Vec::new();
    if let Some(hash) = room_id.strip_prefix('!') {
        let event_id = format!("${hash}");
        if let Ok(Some(ev)) = crate::matrix::get_room_event(
            &ctx.http,
            &ctx.homeserver_url,
            &ctx.appservice_token,
            room_id,
            &event_id,
            ctx.own_user.as_str(),
        )
        .await
        {
            if let Some(s) = ev.get("sender").and_then(|v| v.as_str()) {
                creators.push(s.to_owned());
            }
        }
    }
    if let Some(extra) = create.get("additional_creators").and_then(|v| v.as_array()) {
        creators.extend(extra.iter().filter_map(|v| v.as_str().map(str::to_owned)));
    }
    creators
}

/// Read the room's power levels + `sender`'s membership over the CS API and
/// evaluate their authority (#86). `Err` is a short reason on a read failure —
/// fail-closed: an unreadable room must never authorize. Used to re-check
/// authority at approval time (the probe was only a snapshot at `link` time).
async fn read_sender_authority(
    ctx: &BotCtx,
    room_id: &str,
    sender: &str,
) -> Result<SenderAuthority, String> {
    let pl = crate::matrix::get_room_state(
        &ctx.http,
        &ctx.homeserver_url,
        &ctx.appservice_token,
        room_id,
        "m.room.power_levels",
        "",
        ctx.own_user.as_str(),
    )
    .await
    .map_err(|e| format!("could not read power levels: {e}"))?;
    let member = crate::matrix::get_room_state(
        &ctx.http,
        &ctx.homeserver_url,
        &ctx.appservice_token,
        room_id,
        "m.room.member",
        sender,
        ctx.own_user.as_str(),
    )
    .await
    .map_err(|e| format!("could not read membership: {e}"))?;
    let creators = read_room_creators(ctx, room_id).await;
    Ok(evaluate_sender_authority(pl.as_ref(), member.as_ref(), sender, &creators))
}

/// Power-level values are canonically integers, but legacy rooms (and some
/// servers) carry them as numeric strings — accept both.
fn pl_int(v: &serde_json::Value) -> Option<i64> {
    match v {
        serde_json::Value::Number(n) => n.as_i64(),
        serde_json::Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// Warnings from probes that can have benign false negatives (currently only
/// guild coverage — bots learn their guilds at gateway `ready`, so early after
/// startup a legitimately-invited guild may not be known yet).
struct ProbeReport {
    warnings: Vec<String>,
    /// Whether the bot was ALREADY a member before the probe joined the room
    /// (#87). `false` ⇒ the probe created the membership, so an abandoned pending
    /// link may later reap it; `true` ⇒ a running bridge holds it, never reap.
    already_member: bool,
}

/// The reply when the probe's initial room JOIN fails (#111). The remedy depends
/// on the requester's tier: an operator may invite the bot into any room (operator
/// invites are always accepted), so they get the invite-me path; a self-serve
/// user's invite to a non-operator is auto-rejected by the DMs-only gate, so
/// "invite the bot first" is unsatisfiable for them — they instead get the
/// make-it-publicly-joinable / ask-the-operator remedy.
fn join_failure_reply(room_id: &str, err: &str, bot: &str, requester_is_operator: bool) -> String {
    let remedy = if requester_is_operator {
        format!(
            "If the room is invite-only or restricted, invite {bot} to it first, then run \
             `link` again."
        )
    } else {
        "If the room is invite-only or restricted, make it publicly joinable and run `link` \
         again, or ask the instance operator to link it for you. (I can't accept a room \
         invite from a non-operator, so inviting me won't work.)"
            .to_string()
    };
    format!("❌ The bridge bot could not join {room_id}: {err}\n{remedy}\nNothing was written.")
}

/// Run every room/guild prerequisite probe BEFORE tenants.toml is touched.
/// `Err` carries the complete user-facing reply text — probe failures are
/// operator errors, not bugs, so they go to the DM rather than the log.
///
/// The probe must JOIN the room to read its state. On the untrusted
/// open-registration surface a hostile `link` naming a room the sender has no
/// power in would otherwise pin the shared bot there permanently (#83). So the
/// probe records whether the bot was ALREADY a member before joining (a running
/// bridge keeps it in its room — all bridges share one MXID) and, on any
/// post-join failure, leaves the room ONLY if the probe itself created the
/// membership. A fully-successful probe retains the join the bridge needs.
/// `authority_sender`: when set (two-sided link auth, #47/#86), the probe also
/// verifies that this MXID is a joined member of the room with an explicit
/// individual moderator grant (see [`evaluate_sender_authority`]) — an operator
/// allowlist entry alone no longer authorizes bridging an arbitrary room.
async fn probe_room_prereqs(
    ctx: &BotCtx,
    room_id: &str,
    guild_id: &str,
    authority_sender: Option<&str>,
    // Resolution hint for the first-contact join (#103): the requester's
    // server domain — required in practice for domainless v12 room ids.
    via_hint: Option<&str>,
    // #111: tailors the join-failure remedy to the requester's tier — a self-serve
    // user can't invite the bot (DMs-only gate), so "invite me first" is a dead end
    // for them.
    requester_is_operator: bool,
) -> Result<ProbeReport, String> {
    // Was the bot already a member before this probe? (A running bridge keeps it
    // joined.) If the read fails, assume YES — never risk leaving a bridged room
    // (#83, fail-safe). Only a membership the probe CREATES may be reaped.
    let already_member = crate::matrix::appservice_is_room_joined(
        &ctx.http,
        &ctx.homeserver_url,
        &ctx.appservice_token,
        room_id,
        ctx.own_user.as_str(),
    )
    .await
    .unwrap_or(true);

    // 1. Join (hard): covers restricted/invite-only rooms and typo'd room ids.
    match tokio::time::timeout(
        Duration::from_secs(15),
        crate::matrix::appservice_join_room(
            &ctx.homeserver_url,
            &ctx.appservice_token,
            room_id,
            ctx.own_user.as_str(),
            via_hint,
        ),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            // The join itself failed → nothing to leave.
            return Err(join_failure_reply(
                room_id,
                &e.to_string(),
                ctx.own_user.as_str(),
                requester_is_operator,
            ));
        }
        Err(_) => {
            return Err(format!(
                "❌ Joining {room_id} timed out after 15 s — is the homeserver reachable?\n\
                 Nothing was written."
            ));
        }
    }

    // Run the post-join checks; on failure, reap the membership only if the
    // probe created it (#83) — a room with an active bridge stays joined.
    match probe_after_join(ctx, room_id, guild_id, authority_sender).await {
        Ok(mut report) => {
            // Carry whether the probe created the join, so an abandoned pending
            // link can later reap the room but never a bridge's room (#87).
            report.already_member = already_member;
            Ok(report)
        }
        Err(msg) => {
            if !already_member {
                match crate::matrix::appservice_leave_room(
                    &ctx.homeserver_url,
                    &ctx.appservice_token,
                    room_id,
                    ctx.own_user.as_str(),
                )
                .await
                {
                    Ok(()) => tracing::info!(room_id, "left room after a failed link probe (#83 — probe-created join reaped)"),
                    Err(e) => tracing::warn!(room_id, error = %e, "failed to leave room after a failed link probe (#83)"),
                }
            }
            Err(msg)
        }
    }
}

/// The post-join prerequisite checks, split out so [`probe_room_prereqs`] can
/// reap a probe-created membership on failure (#83).
async fn probe_after_join(
    ctx: &BotCtx,
    room_id: &str,
    guild_id: &str,
    authority_sender: Option<&str>,
) -> Result<ProbeReport, String> {
    // 2. Power levels (hard): a room that blocks ghosts from posting
    // `org.matrix.msc3401.call.member` silently breaks the Discord→Matrix
    // direction, so refuse to link it. Must run after the join (a non-member
    // GET 403s).
    let pl_content = tokio::time::timeout(
        Duration::from_secs(10),
        crate::matrix::get_room_state(
            &ctx.http,
            &ctx.homeserver_url,
            &ctx.appservice_token,
            room_id,
            "m.room.power_levels",
            "",
            ctx.own_user.as_str(),
        ),
    )
    .await
    .map_err(|_| {
        "❌ Reading the room's power levels timed out after 10 s.\nNothing was written."
            .to_string()
    })?
    .map_err(|e| {
        format!(
            "❌ Could not read the room's power levels: {e}\nNothing was written."
        )
    })?;
    let check = evaluate_call_member_power(pl_content.as_ref());
    if !check.ok() {
        return Err(format!(
            "❌ The room's power levels block the bridge: posting `{CALL_MEMBER_EVENT}` needs power level {}, but the bridge's ghost users join at level {} — Matrix would never see the Discord side of the call.\n\
             Fix: start an Element Call in the room once (that sets the level), or via /devtools set `events[\"{CALL_MEMBER_EVENT}\"] = 0` in `m.room.power_levels`. Element's \"Roles & Permissions\" UI shows \"Default\" either way, so check the raw event.\n\
             Nothing was written.{}",
            check.required, check.ghost_pl, ctx.docs_line()
        ));
    }

    // 2b. Matrix-side link authority (#47, only with `require_room_authority`):
    // the sender must hold moderator power in the room they are bridging.
    // Reuses the power-levels content just fetched; only the sender's own
    // membership event is an extra GET.
    if let Some(sender) = authority_sender {
        let member_content = tokio::time::timeout(
            Duration::from_secs(10),
            crate::matrix::get_room_state(
                &ctx.http,
                &ctx.homeserver_url,
                &ctx.appservice_token,
                room_id,
                "m.room.member",
                sender,
                ctx.own_user.as_str(),
            ),
        )
        .await
        .map_err(|_| {
            "❌ Reading your room membership timed out after 10 s.\nNothing was written."
                .to_string()
        })?
        .map_err(|e| {
            format!("❌ Could not read your room membership: {e}\nNothing was written.")
        })?;
        let creators = read_room_creators(ctx, room_id).await;
        let authority = evaluate_sender_authority(
            pl_content.as_ref(),
            member_content.as_ref(),
            sender,
            &creators,
        );
        if !authority.ok() {
            let reason = if !authority.joined {
                "you are not a joined member of it".to_string()
            } else if !authority.explicit || authority.sender_pl <= authority.users_default {
                "you have no individual moderator role there (your power comes from the \
                 room's default, which everyone shares)"
                    .to_string()
            } else {
                format!(
                    "your power level there is {} but linking needs ≥ {}",
                    authority.sender_pl, MODERATOR_FLOOR
                )
            };
            return Err(format!(
                "❌ Two-sided link auth is on: only an individual moderator of {room_id} \
                 (someone explicitly granted power ≥ {MODERATOR_FLOOR}, above the room default) \
                 may bridge it, and {reason}.\n\
                 Ask a room admin to grant you a moderator role, or have an existing \
                 moderator run `link`.\n\
                 Nothing was written."
            ));
        }
    }

    // 3. MatrixRTC focus (hard): a `link`-created tenant always uses the
    // default LiveKit config, so this discovery is exactly what the spawned
    // bridge will do.
    match tokio::time::timeout(
        Duration::from_secs(10),
        crate::rtc::well_known::discover_livekit_focus(&ctx.http, &ctx.well_known_base, room_id),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            return Err(format!(
                "❌ MatrixRTC does not appear to be set up on this homeserver: {e}\n\
                 `{}/.well-known/matrix/client` must publish an `org.matrix.msc4143.rtc_foci` LiveKit entry (lk-jwt-service + LiveKit). Without it no call can be bridged.\n\
                 Nothing was written.",
                ctx.well_known_base
            ));
        }
        Err(_) => {
            return Err(format!(
                "❌ MatrixRTC discovery timed out after 10 s (`{}/.well-known/matrix/client`).\n\
                 Nothing was written.",
                ctx.well_known_base
            ));
        }
    }

    // 4. Discord guild coverage (WARN only): guilds are learned per-bot at
    // gateway `ready`, so a false negative is expected shortly after startup.
    let mut warnings = Vec::new();
    match guild_id.parse::<u64>() {
        Ok(gid) => match query_guild_coverage(ctx, gid).await {
            Some(true) => {}
            Some(false) => warnings.push(format!(
                "⚠️ No puppet bot reports membership of guild {guild_id} yet — Matrix speakers will not be voiced on Discord until the puppet bots are invited to that guild. (If the process just started, the bots may still be connecting.)"
            )),
            None => warnings.push(
                "⚠️ Could not verify puppet-bot guild membership (no answer from the supervisor)."
                    .to_string(),
            ),
        },
        // Past is_numeric_id this only means a >u64 digit string.
        Err(_) => warnings.push(format!(
            "⚠️ Could not verify puppet-bot guild membership ('{guild_id}' is not a valid guild id)."
        )),
    }

    // `already_member` is filled in by `probe_room_prereqs` (which knows it).
    Ok(ProbeReport { warnings, already_member: false })
}

async fn handle_list(ctx: &BotCtx, room: &Room, sender: &str) {
    // Self-serve users (#43) see only tenants they own; operators see all.
    let only_owned_by = (!ctx.is_operator(sender)).then_some(sender);
    let Some(running) = query_status(ctx).await else {
        reply(room, "The supervisor did not respond — check the bridge logs.").await;
        return;
    };
    let mut lines: Vec<String> = Vec::new();
    for b in &running {
        if let Some(me) = only_owned_by {
            if b.owner.as_deref() != Some(me) {
                continue;
            }
        }
        let owner = b.owner.as_deref().map(|o| format!(", owner {o}")).unwrap_or_default();
        lines.push(format!(
            "• {} — {}, room {}, guild {} channel {}{owner} [running]",
            b.label,
            if b.is_tenant { "tenant" } else { "static" },
            b.room_id,
            b.guild_id,
            b.channel_id,
        ));
    }
    // Entries in tenants.toml that are NOT running (e.g. their spawn failed).
    let running_labels: HashSet<&str> = running.iter().map(|b| b.label.as_str()).collect();
    match nvb_config::tenant_owners(&ctx.tenants_path) {
        Ok(entries) => {
            for (name, owner) in entries {
                if let Some(me) = only_owned_by {
                    if owner.as_deref() != Some(me) {
                        continue;
                    }
                }
                if !running_labels.contains(name.as_str()) {
                    lines.push(format!("• {name} — tenant in tenants.toml but NOT running (check logs)"));
                }
            }
        }
        Err(e) => lines.push(format!("(could not read tenants.toml: {e})")),
    }
    if lines.is_empty() {
        let text = if only_owned_by.is_some() {
            "You have no bridges. Create one with `link` — send `help` for usage."
        } else {
            "No bridges. Create one with `link` — send `help` for usage."
        };
        reply(room, text).await;
    } else {
        reply(room, &format!("Bridges:\n{}", lines.join("\n"))).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_link(
    ctx: &BotCtx,
    room: &Room,
    sender: &str,
    name: String,
    room_id: String,
    guild_id: String,
    channel_id: String,
    server: Option<String>,
) {
    // Cheap shape checks first — friendlier than a reconcile rejection.
    if name.is_empty() {
        reply(room, "The bridge name must not be empty.").await;
        return;
    }
    if !plausible_room_id(&room_id) {
        // Log it too — these rejections were previously reply-only and
        // invisible in triage (#103 was diagnosed blind because of that).
        tracing::info!(sender = %sender, room_id = %room_id, "rejecting link: implausible room id");
        reply(room, &format!("'{room_id}' does not look like a Matrix room id (expected `!localpart:domain` or the newer domainless `!…` form). Use the room's internal id, not an alias (Element: room Settings → Advanced → \"Internal room ID\").{}", ctx.docs_line())).await;
        return;
    }
    if !is_numeric_id(&guild_id) || !is_numeric_id(&channel_id) {
        reply(room, &format!("Discord guild and channel ids must be numeric snowflakes (in Discord, enable User Settings → Advanced → Developer Mode, then right-click the server / channel → Copy ID).{}", ctx.docs_line())).await;
        return;
    }
    let server = match server {
        Some(s) if ctx.server_names.iter().any(|n| n == &s) => s,
        Some(s) => {
            reply(room, &format!(
                "Unknown server profile '{s}' — available: {}.",
                ctx.server_names.join(", ")
            ))
            .await;
            return;
        }
        None if ctx.server_names.len() == 1 => ctx.server_names[0].clone(),
        None => {
            reply(room, &format!(
                "Multiple server profiles are configured — pick one: `link \"{name}\" {room_id} {guild_id} {channel_id} <server>` (available: {}).",
                ctx.server_names.join(", ")
            ))
            .await;
            return;
        }
    };

    // Ban enforcement (#47): before anything is spent on this request. A ban
    // list that exists but cannot be read fails CLOSED — silently ignoring the
    // deny-lists would un-ban everyone on a corrupt file.
    match nvb_config::load_banlist(&ctx.banlist_path) {
        Ok(bans) => {
            let hit = if bans.contains(BanKind::Owner, sender) {
                Some(("owner", sender.to_string()))
            } else if bans.contains(BanKind::Guild, &guild_id) {
                Some(("guild", guild_id.clone()))
            } else if bans.contains(BanKind::Room, &room_id) {
                Some(("room", room_id.clone()))
            } else {
                None
            };
            if let Some((kind, value)) = hit {
                audit(ctx, sender, "link_rejected", serde_json::json!({
                    "name": name, "reason": "banned", "kind": kind, "value": value,
                }));
                reply(room, &format!("🚫 Rejected: {kind} `{value}` is banned.")).await;
                return;
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "ban list unreadable — refusing link (fail-closed)");
            reply(room, "The ban list is unreadable — link refused until an operator fixes it.")
                .await;
            return;
        }
    }

    // Per-owner tenant cap (#43): self-serve users only — operators are
    // bounded by the global max_tenants alone. Pending links count too, or a
    // user could park cap-dodging links awaiting approval.
    if let (false, Some(cap)) = (ctx.is_operator(sender), ctx.max_tenants_per_owner) {
        let existing = match nvb_config::tenant_owners(&ctx.tenants_path) {
            Ok(entries) => entries
                .iter()
                .filter(|(_, o)| o.as_deref() == Some(sender))
                .count(),
            Err(e) => {
                reply(room, &format!("Cannot read tenants.toml: {e}")).await;
                return;
            }
        };
        let pending = {
            let map = ctx.pending.lock().unwrap_or_else(PoisonError::into_inner);
            map.values().filter(|p| p.requester == sender).count()
        };
        if existing + pending >= cap {
            audit(ctx, sender, "link_rejected", serde_json::json!({
                "name": name, "reason": "owner cap",
                "existing": existing, "pending": pending, "cap": cap,
            }));
            reply(room, &format!(
                "You already have {existing} bridge(s){} — the limit is {cap} per user. `unlink` one first.",
                if pending > 0 { format!(" and {pending} awaiting approval") } else { String::new() }
            ))
            .await;
            return;
        }
    }

    // Per-guild tenant cap (#77 anti-Sybil): self-serve users only. The Discord
    // Manage-Server gate is the strongest anti-Sybil anchor, so cap by the guild
    // being targeted — a federation-reachable owner MXID is throwaway, a real
    // guild is not. Committed tenants for the guild count regardless of owner;
    // pending links count only for THIS requester (#79): a pending guild_id is
    // attacker-supplied and unproven until Discord approval, so counting a third
    // party's pendings would let anyone park links on a victim guild's public
    // snowflake and block its legitimate admin from onboarding.
    if let (false, Some(cap)) = (ctx.is_operator(sender), ctx.max_tenants_per_guild) {
        let existing = match nvb_config::tenant_guilds(&ctx.tenants_path) {
            Ok(entries) => entries
                .iter()
                .filter(|(_, g)| g.as_deref() == Some(guild_id.as_str()))
                .count(),
            Err(e) => {
                reply(room, &format!("Cannot read tenants.toml: {e}")).await;
                return;
            }
        };
        let pending = {
            let map = ctx.pending.lock().unwrap_or_else(PoisonError::into_inner);
            map.values()
                .filter(|p| p.tenant.guild_id == guild_id && p.requester == sender)
                .count()
        };
        if existing + pending >= cap {
            audit(ctx, sender, "link_rejected", serde_json::json!({
                "name": name, "reason": "guild cap", "guild_id": guild_id,
                "existing": existing, "pending": pending, "cap": cap,
            }));
            reply(room, &format!(
                "This Discord guild already has {existing} bridge(s){} — the limit is {cap} per guild.",
                if pending > 0 { format!(" and {pending} awaiting approval") } else { String::new() }
            ))
            .await;
            return;
        }
    }

    // Per-foreign-domain pending cap (#88): bound how much of the shared pending
    // table one homeserver's users can hold, so a federated-MXID Sybil set can't
    // concentrate it and starve honest onboarding. The home server is exempt —
    // it is invite-gated (not a Sybil vector) and capping it would throttle
    // legitimate concurrent onboarding.
    if !ctx.is_operator(sender) {
        if let Some(domain) = mxid_domain(sender) {
            if domain != ctx.own_user.server_name().as_str() {
                let pending = {
                    let map = ctx.pending.lock().unwrap_or_else(PoisonError::into_inner);
                    map.values()
                        .filter(|p| mxid_domain(&p.requester) == Some(domain))
                        .count()
                };
                if pending >= PER_DOMAIN_PENDING_CAP {
                    audit(ctx, sender, "link_rejected", serde_json::json!({
                        "name": name, "reason": "domain pending cap",
                        "domain": domain, "pending": pending, "cap": PER_DOMAIN_PENDING_CAP,
                    }));
                    reply(room, &format!(
                        "⏳ Too many links from your homeserver are awaiting approval ({pending}) — try again once some are approved or expire."
                    ))
                    .await;
                    return;
                }
            }
        }
    }

    // Rate gate (#47): past the cheap shape checks (a typo doesn't burn the
    // budget), before the expensive probes and the state change they precede.
    if let Some(remaining) = ctx.mutate_cooldown(sender) {
        reply(room, &format!("⏳ Rate limited — try again in {remaining}s.")).await;
        return;
    }

    // Link-attempt velocity budget (#77): self-serve users only. The cooldown
    // above is a single-shot spacing; this bounds sustained hammering of the
    // probe/approval flow over a longer window. Checked after the cooldown
    // stamps, so one honest retry-after-cooldown spends one attempt.
    if !ctx.is_operator(sender) {
        if let Some(retry) = ctx.link_attempt_gate(sender) {
            audit(ctx, sender, "link_rejected", serde_json::json!({
                "name": name, "reason": "attempt budget", "retry_secs": retry,
            }));
            reply(room, &format!(
                "⏳ Too many link attempts — try again in {retry}s."
            ))
            .await;
            return;
        }
    }

    // Global velocity gate (#84): a single shared window across ALL self-serve
    // senders, so a federated-MXID Sybil swarm can't dodge the per-owner budgets
    // above by rotating throwaway identities. Non-operators only.
    if !ctx.is_operator(sender) {
        if let Some(retry) = ctx.global_link_gate() {
            audit(ctx, sender, "link_rejected", serde_json::json!({
                "name": name, "reason": "global attempt budget", "retry_secs": retry,
            }));
            reply(room, &format!(
                "⏳ The bridge is handling a lot of link requests right now — try again in {retry}s."
            ))
            .await;
            return;
        }
    }

    // Preflight probes (#47): verify the room/guild prerequisites BEFORE
    // touching tenants.toml — a failed probe replies and writes nothing.
    // Runs outside the mutate lock (slow network I/O; a duplicate-name race
    // is still caught by add_tenant_entry's own rejection). With
    // `require_room_authority` the probe also checks the SENDER's power in
    // the room (two-sided link auth, Matrix side).
    let authority_sender = ctx.require_room_authority.then_some(sender);
    let report = match probe_room_prereqs(
        ctx,
        &room_id,
        &guild_id,
        authority_sender,
        mxid_domain(sender),
        ctx.is_operator(sender),
    )
    .await
    {
        Ok(r) => r,
        Err(msg) => {
            reply(room, &msg).await;
            return;
        }
    };

    let tenant = NewTenant {
        name: name.clone(),
        server,
        room_id: room_id.clone(),
        guild_id: guild_id.clone(),
        channel_id: channel_id.clone(),
        // Owner record (#47): who created this tenant, for `ban owner` and the
        // audit trail. Epoch seconds; display with `date -d @<n>`.
        owner: Some(sender.to_owned()),
        created: Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
        ),
    };

    // Two-sided link auth, Discord side (#47): hold the fully-probed link
    // pending until someone with Manage Server in the target guild claims the
    // code via `/bridge approve` — nothing is written until then.
    if let Some(ttl) = ctx.approval_ttl {
        let pending = PendingLink {
            tenant,
            requester: sender.to_owned(),
            origin_room: room.room_id().to_owned(),
            probe_joined: !report.already_member,
            warnings: report.warnings,
            created: Instant::now(),
        };
        let minted = {
            let mut map = ctx.pending.lock().unwrap_or_else(PoisonError::into_inner);
            insert_pending(&mut map, pending, Instant::now(), ttl)
        };
        match minted {
            Ok(code) => {
                audit(ctx, sender, "link_pending", serde_json::json!({
                    "name": name, "room_id": room_id, "guild_id": guild_id,
                    "channel_id": channel_id, "code": code,
                }));
                // Name the approval bot when the operator configured it (#80) —
                // the mgmt bot can't learn its live Discord username.
                let bot_clause = match &ctx.approval_bot_name {
                    Some(n) => format!("on the **{n}** bot"),
                    None => "on this instance's bridge bot".to_string(),
                };
                reply(room, &format!(
                    "⏳ All room checks passed — Discord-side approval required.\n\
                     Have someone with **Manage Server** in Discord guild {guild_id} run:\n\
                     `/bridge approve code:{code}`\n\
                     Run it {bot_clause}. If the `/bridge` command doesn't appear there, that \
                     bot's application must be invited to the guild with the \
                     `applications.commands` scope.\n\
                     The code expires in {} minutes; nothing is written until it is approved.{}{}",
                    ttl.as_secs().div_ceil(60),
                    ctx.docs_line(),
                    ctx.policy_line()
                ))
                .await;
            }
            Err(msg) => reply(room, &msg).await,
        }
        return;
    }

    let (outcome, _ok) = commit_link(ctx, tenant, &report.warnings, sender, None).await;
    reply(room, &outcome).await;
}

/// Write `tenant` into tenants.toml and reconcile, returning the user-facing
/// outcome text (shared by the direct `link` path and the Discord-approval
/// path). `actor` is the requesting MXID (audit attribution); `approved_by`
/// is the Discord approver when the commit came through `/bridge approve`.
async fn commit_link(
    ctx: &BotCtx,
    tenant: NewTenant,
    warnings: &[String],
    actor: &str,
    approved_by: Option<&str>,
) -> (String, bool) {
    let name = tenant.name.clone();
    let room_id = tenant.room_id.clone();
    let guild_id = tenant.guild_id.clone();
    let channel_id = tenant.channel_id.clone();

    let _guard = ctx.mutate_lock.lock().await;
    let prior = match nvb_config::add_tenant_entry(&ctx.tenants_path, &tenant) {
        Ok(p) => p,
        Err(e) => return (format!("Cannot add '{name}': {e}"), false),
    };

    match request_reconcile(ctx).await {
        Some(Ok(summary)) => {
            if let Some((_, err)) = summary.failed.iter().find(|(l, _)| l == &name) {
                audit(ctx, actor, "link_failed_spawn", serde_json::json!({
                    "name": name, "error": err,
                }));
                (format!(
                    "The entry was accepted but the bridge FAILED to start: {err}\nIt stays in tenants.toml — fix the cause and `unlink`/`link` again (or reload)."
                ), false)
            } else if summary.added.iter().any(|l| l == &name) {
                audit(ctx, actor, "link_ok", serde_json::json!({
                    "name": name, "room_id": room_id, "guild_id": guild_id,
                    "channel_id": channel_id, "approved_by": approved_by,
                }));
                let guild_line = if warnings.is_empty() {
                    format!("✓ puppet bots present in guild {guild_id}")
                } else {
                    warnings.join("\n")
                };
                (format!(
                    "✅ Bridge '{name}' is up: {room_id} ↔ guild {guild_id} channel {channel_id}.\n\
                     Verified before linking:\n\
                     ✓ bridge bot joined the room\n\
                     ✓ call power levels allow the bridge's ghosts\n\
                     ✓ MatrixRTC focus discovered\n\
                     {guild_line}\n\
                     Not verifiable here: the puppet bots need Connect, Speak, Use Voice Activity and Change Nickname on the voice channel.{}",
                    ctx.policy_line()
                ), true)
            } else {
                (format!(
                    "Reconcile ran but '{name}' was not reported as added (unexpected) — check the bridge logs."
                ), false)
            }
        }
        Some(Err(rejection)) => {
            // All-or-nothing reject: roll the file back so it never lies about
            // the running set.
            if let Err(e) = nvb_config::restore_tenants_file(&ctx.tenants_path, prior) {
                tracing::error!(error = %e, "rollback of tenants.toml failed after rejected reconcile");
            }
            audit(ctx, actor, "link_rejected", serde_json::json!({
                "name": name, "reason": rejection,
            }));
            (format!("❌ Rejected: {rejection}\ntenants.toml was rolled back."), false)
        }
        None => {
            audit(ctx, actor, "link_unconfirmed", serde_json::json!({ "name": name }));
            ("The supervisor did not respond — the entry was written to tenants.toml but may not be running. Check the bridge logs.".to_string(), false)
        }
    }
}

/// Serve one Discord-side `/bridge approve` (#47): claim the pending link,
/// re-check the deny-lists, commit, and notify both sides. The invoker's
/// Manage Server permission was verified on the Discord side; here the code
/// itself is the capability, scoped to the guild it was minted for.
async fn handle_discord_approval(ctx: &BotCtx, client: &Client, approval: DiscordApproval) {
    let Some(ttl) = ctx.approval_ttl else {
        // Unreachable in practice: the channel is only wired when approval is
        // configured. Answer anyway so the interaction never hangs.
        let _ = approval.reply.send(ApprovalOutcome::text_only(
            "Discord-side approval is not enabled on this instance.",
        ));
        return;
    };
    let code = normalize_code(&approval.code);
    let taken = {
        let mut map = ctx.pending.lock().unwrap_or_else(PoisonError::into_inner);
        take_pending(&mut map, &code, approval.guild_id, Instant::now(), ttl)
    };
    let pending = match taken {
        Ok(p) => p,
        Err(msg) => {
            // `msg` distinguishes unknown/expired from a guild mismatch — keep
            // that in the audit trail, not just in the ephemeral reply.
            audit(ctx, &approval.invoker, "approve_rejected", serde_json::json!({
                "code": code, "guild_id": approval.guild_id.to_string(),
                "reason": &msg,
            }));
            let _ = approval.reply.send(ApprovalOutcome::text_only(msg));
            return;
        }
    };

    // Ban re-check: a ban may have landed while the link sat pending; an
    // approval must not resurrect it. Same fail-closed posture as `link`.
    match nvb_config::load_banlist(&ctx.banlist_path) {
        Ok(bans) => {
            if bans.contains(BanKind::Owner, &pending.requester)
                || bans.contains(BanKind::Guild, &pending.tenant.guild_id)
                || bans.contains(BanKind::Room, &pending.tenant.room_id)
            {
                audit(ctx, &approval.invoker, "approve_rejected", serde_json::json!({
                    "code": code, "reason": "banned",
                }));
                let _ = approval.reply.send(ApprovalOutcome::text_only(
                    "🚫 This link was banned while awaiting approval.",
                ));
                return;
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "ban list unreadable — refusing approval (fail-closed)");
            let _ = approval.reply.send(ApprovalOutcome::text_only(
                "The ban list is unreadable — approval refused until an operator fixes it.",
            ));
            return;
        }
    }

    // Room-authority re-check (#86 TOCTOU): the probe was a snapshot at `link`
    // time; a requester demoted or gone from the room between then and approval
    // must not slip a bridge through. Only when two-sided room authority is on.
    if ctx.require_room_authority {
        match read_sender_authority(ctx, &pending.tenant.room_id, &pending.requester).await {
            Ok(auth) if auth.ok() => {}
            Ok(_) => {
                audit(ctx, &approval.invoker, "approve_rejected", serde_json::json!({
                    "code": code, "reason": "authority lost",
                }));
                let _ = approval.reply.send(ApprovalOutcome::text_only(
                    "🚫 The Matrix requester no longer holds a moderator role in the room — approval refused.",
                ));
                return;
            }
            Err(e) => {
                tracing::error!(error = %e, "room authority unreadable at approval — refusing (fail-closed)");
                let _ = approval.reply.send(ApprovalOutcome::text_only(
                    "Could not re-verify the requester's room authority — approval refused; run `link` again.",
                ));
                return;
            }
        }
    }

    tracing::info!(
        code = %code,
        invoker = %approval.invoker,
        tenant = %pending.tenant.name,
        "Discord-side link approval accepted"
    );
    // The bridged voice channel, for the #64 disclosure notice (String id in
    // the tenant record; a parse failure just skips the notice).
    let notice_channel_id = pending.tenant.channel_id.parse::<u64>().ok();
    let (outcome, succeeded) = commit_link(
        ctx,
        pending.tenant,
        &pending.warnings,
        &pending.requester,
        Some(&approval.invoker),
    )
    .await;

    // Tell the Matrix requester in the room they linked from (best-effort —
    // the Discord follow-up carries the same text either way).
    if let Some(origin) = client.get_room(&pending.origin_room) {
        reply(&origin, &format!("Discord approval by {}:\n{outcome}", approval.invoker)).await;
    }
    // #64: the in-channel disclosure notice, posted Discord-side into the
    // bridged voice channel's text chat (Dev Policy §2/§3 — visible to people
    // who join later and never saw the approval).
    let notice = succeeded.then(|| {
        format!(
            "🔗 This voice channel is now bridged to a Matrix room — voice is relayed off-platform (live only, never recorded). Approved by {}.{}",
            approval.invoker,
            ctx.policy_line()
        )
    });
    let _ = approval.reply.send(ApprovalOutcome {
        text: outcome,
        notice_channel_id: if notice.is_some() { notice_channel_id } else { None },
        notice,
    });
}

async fn handle_unlink(ctx: &BotCtx, room: &Room, sender: &str, name: &str) {
    // Rate gate (#47) — same budget as `link` (they share the stamp map, so an
    // operator can't alternate the two to thrash spawn/teardown).
    if let Some(remaining) = ctx.mutate_cooldown(sender) {
        reply(room, &format!("⏳ Rate limited — try again in {remaining}s.")).await;
        return;
    }
    let _guard = ctx.mutate_lock.lock().await;
    // Self-serve tier (#43): non-operators may only unlink tenants they own.
    // Checked under the mutate lock so the owner can't change underneath us.
    if !ctx.is_operator(sender) {
        match nvb_config::tenant_owners(&ctx.tenants_path) {
            Ok(entries) => match entries.iter().find(|(n, _)| n == name) {
                Some((_, owner)) if owner.as_deref() == Some(sender) => {}
                Some(_) => {
                    reply(room, &format!(
                        "You may only unlink bridges you created — '{name}' is not yours."
                    ))
                    .await;
                    return;
                }
                None => {
                    reply(room, &format!("Cannot remove '{name}': tenants file has no bridge named '{name}'")).await;
                    return;
                }
            },
            Err(e) => {
                reply(room, &format!("Cannot read tenants.toml: {e}")).await;
                return;
            }
        }
    }
    // The bridged channel, read before the entry disappears — target of the
    // unlink disclosure notice (#64). Best-effort.
    let notice_channel =
        nvb_config::tenant_discord_channel(&ctx.tenants_path, name).ok().flatten();
    let prior = match nvb_config::remove_tenant_entry(&ctx.tenants_path, name) {
        Ok(p) => p,
        Err(e) => {
            reply(room, &format!("Cannot remove '{name}': {e}")).await;
            return;
        }
    };
    match request_reconcile(ctx).await {
        Some(Ok(summary)) => {
            audit(ctx, sender, "unlink_ok", serde_json::json!({
                "name": name, "was_running": summary.removed.iter().any(|l| l == name),
            }));
            ctx.send_notice(
                notice_channel.as_deref(),
                UNLINK_NOTICE_TEXT,
            );
            if summary.removed.iter().any(|l| l == name) {
                reply(room, &format!("🛑 Bridge '{name}' is stopping (teardown takes a few seconds).")).await;
            } else {
                reply(room, &format!(
                    "'{name}' was removed from tenants.toml (it was not running)."
                ))
                .await;
            }
        }
        Some(Err(rejection)) => {
            if let Err(e) = nvb_config::restore_tenants_file(&ctx.tenants_path, prior) {
                tracing::error!(error = %e, "rollback of tenants.toml failed after rejected reconcile");
            }
            audit(ctx, sender, "unlink_rejected", serde_json::json!({
                "name": name, "reason": rejection,
            }));
            reply(room, &format!("❌ Rejected: {rejection}\ntenants.toml was rolled back.")).await;
        }
        None => {
            audit(ctx, sender, "unlink_unconfirmed", serde_json::json!({ "name": name }));
            reply(room, "The supervisor did not respond — the entry was removed from tenants.toml but the bridge may still be running. Check the bridge logs.").await;
        }
    }
}

/// The `ban` command (#47): record the ban, then tear down every matching
/// tenant through the same declarative path everything else uses (remove the
/// entries from tenants.toml + reconcile) — no separate kill mechanism to
/// drift. Static bridges are operator-authored and out of scope.
async fn handle_ban(ctx: &BotCtx, room: &Room, sender: &str, kind_str: &str, value: &str) {
    let Some(kind) = BanKind::parse(kind_str) else {
        reply(room, BAN_USAGE).await;
        return;
    };
    if !ban_value_shape_ok(kind, value) {
        reply(room, &format!("'{value}' does not look like a {} — {}", kind.as_str(), BAN_USAGE))
            .await;
        return;
    }
    if let Some(remaining) = ctx.mutate_cooldown(sender) {
        reply(room, &format!("⏳ Rate limited — try again in {remaining}s.")).await;
        return;
    }
    let _guard = ctx.mutate_lock.lock().await;

    match nvb_config::add_ban(&ctx.banlist_path, kind, value) {
        Ok(true) => {}
        Ok(false) => {
            reply(room, &format!("{} `{value}` is already banned.", kind.as_str())).await;
            return;
        }
        Err(e) => {
            reply(room, &format!("Cannot write the ban list: {e}")).await;
            return;
        }
    }

    // Teardown sweep: unlink every tenant the new ban matches.
    let (removed, prior) = match nvb_config::remove_tenant_entries_matching(
        &ctx.tenants_path,
        |owner, guild, room_id| match kind {
            BanKind::Owner => owner == Some(value),
            BanKind::Guild => guild == Some(value),
            BanKind::Room => room_id == Some(value),
        },
    ) {
        Ok(r) => r,
        Err(e) => {
            audit(ctx, sender, "ban", serde_json::json!({
                "kind": kind.as_str(), "value": value, "teardown_error": e.to_string(),
            }));
            reply(room, &format!(
                "🔨 Banned {} `{value}`, but scanning tenants.toml failed: {e}\nMatching tenants may still be running — check the file by hand.",
                kind.as_str()
            ))
            .await;
            return;
        }
    };

    if removed.is_empty() {
        audit(ctx, sender, "ban", serde_json::json!({
            "kind": kind.as_str(), "value": value, "removed": [],
        }));
        reply(room, &format!(
            "🔨 Banned {} `{value}`. No running tenants matched; future `link`s are refused.",
            kind.as_str()
        ))
        .await;
        return;
    }

    match request_reconcile(ctx).await {
        Some(Ok(_)) => {
            let names: Vec<&str> = removed.iter().map(|(n, _)| n.as_str()).collect();
            audit(ctx, sender, "ban", serde_json::json!({
                "kind": kind.as_str(), "value": value, "removed": names,
            }));
            // Disclose the teardown to each affected channel (#64) — same
            // neutral text as unlink; the ban itself is not broadcast.
            for (_, chan) in &removed {
                ctx.send_notice(chan.as_deref(), UNLINK_NOTICE_TEXT);
            }
            reply(room, &format!(
                "🔨 Banned {} `{value}` and unlinked {} tenant(s): {}.",
                kind.as_str(),
                removed.len(),
                names.join(", ")
            ))
            .await;
        }
        Some(Err(rejection)) => {
            // Keep the ban, restore the entries: a rejected reconcile must not
            // leave the file lying about the running set. The ban still blocks
            // future links; the teardown can be retried.
            if let Err(e) = nvb_config::restore_tenants_file(&ctx.tenants_path, prior) {
                tracing::error!(error = %e, "rollback of tenants.toml failed after rejected reconcile");
            }
            audit(ctx, sender, "ban", serde_json::json!({
                "kind": kind.as_str(), "value": value, "teardown_rejected": rejection,
            }));
            reply(room, &format!(
                "🔨 Banned {} `{value}`, but the teardown reconcile was rejected: {rejection}\ntenants.toml was rolled back — matching tenants are still running.",
                kind.as_str()
            ))
            .await;
        }
        None => {
            audit(ctx, sender, "ban", serde_json::json!({
                "kind": kind.as_str(), "value": value,
                "removed": removed.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
                "reconcile": "no answer",
            }));
            reply(room, "The ban was recorded and the entries removed, but the supervisor did not respond — matching bridges may still be running. Check the bridge logs.").await;
        }
    }
}

async fn handle_unban(ctx: &BotCtx, room: &Room, sender: &str, kind_str: &str, value: &str) {
    let Some(kind) = BanKind::parse(kind_str) else {
        reply(room, UNBAN_USAGE).await;
        return;
    };
    if let Some(remaining) = ctx.mutate_cooldown(sender) {
        reply(room, &format!("⏳ Rate limited — try again in {remaining}s.")).await;
        return;
    }
    let _guard = ctx.mutate_lock.lock().await;
    match nvb_config::remove_ban(&ctx.banlist_path, kind, value) {
        Ok(true) => {
            audit(ctx, sender, "unban", serde_json::json!({
                "kind": kind.as_str(), "value": value,
            }));
            reply(room, &format!("✅ Unbanned {} `{value}`.", kind.as_str())).await;
        }
        Ok(false) => {
            reply(room, &format!("{} `{value}` was not banned.", kind.as_str())).await;
        }
        Err(e) => {
            reply(room, &format!("Cannot write the ban list: {e}")).await;
        }
    }
}

async fn handle_bans(ctx: &BotCtx, room: &Room) {
    match nvb_config::load_banlist(&ctx.banlist_path) {
        Ok(bans) if bans.is_empty() => reply(room, "No bans.").await,
        Ok(bans) => {
            let mut lines = Vec::new();
            for (label, list) in
                [("owners", &bans.owners), ("guilds", &bans.guilds), ("rooms", &bans.rooms)]
            {
                if !list.is_empty() {
                    lines.push(format!("• {label}: {}", list.join(", ")));
                }
            }
            reply(room, &lines.join("\n")).await;
        }
        Err(e) => reply(room, &format!("Cannot read the ban list: {e}")).await,
    }
}

// ---- self-serve allowlist commands (#82, operator-only) ----

/// Reload the in-memory self-serve allowlist from the file after an edit (#82),
/// so [`BotCtx::may_self_serve`] reflects the change immediately.
fn refresh_self_serve(ctx: &BotCtx) {
    match nvb_config::load_self_serve_allow(&ctx.self_serve_allow_path) {
        Ok(loaded) => {
            *ctx.self_serve_allow.lock().unwrap_or_else(PoisonError::into_inner) =
                loaded.map(|v| v.into_iter().collect());
        }
        Err(e) => tracing::error!(error = %e, "failed to reload self-serve allowlist after edit"),
    }
}

fn self_serve_count(ctx: &BotCtx) -> Option<usize> {
    ctx.self_serve_allow
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .as_ref()
        .map(|s| s.len())
}

async fn handle_serve_allow(ctx: &BotCtx, room: &Room, sender: &str, mxid: &str) {
    if !mxid.starts_with('@') || !mxid.contains(':') {
        reply(room, &format!("'{mxid}' is not a full MXID (expected @localpart:server).")).await;
        return;
    }
    let _guard = ctx.mutate_lock.lock().await;
    match nvb_config::add_self_serve(&ctx.self_serve_allow_path, mxid) {
        Ok(changed) => {
            refresh_self_serve(ctx);
            audit(ctx, sender, "serve_allow", serde_json::json!({ "mxid": mxid, "added": changed }));
            let n = self_serve_count(ctx).unwrap_or(0);
            reply(room, &format!(
                "✅ `{mxid}` may now use self-serve. The self-serve tier is restricted to {n} user(s) (plus operators) — `serve-open` reopens it to everyone."
            ))
            .await;
        }
        Err(e) => reply(room, &format!("Cannot write the self-serve allowlist: {e}")).await,
    }
}

async fn handle_serve_remove(ctx: &BotCtx, room: &Room, sender: &str, mxid: &str) {
    let _guard = ctx.mutate_lock.lock().await;
    match nvb_config::remove_self_serve(&ctx.self_serve_allow_path, mxid) {
        Ok(true) => {
            refresh_self_serve(ctx);
            audit(ctx, sender, "serve_remove", serde_json::json!({ "mxid": mxid }));
            reply(room, &format!(
                "✅ `{mxid}` removed from the self-serve allowlist. (The list stays in force — `serve-open` to reopen to everyone.)"
            ))
            .await;
        }
        Ok(false) => {
            reply(room, &format!(
                "`{mxid}` was not on the allowlist (or self-serve is already open to everyone)."
            ))
            .await;
        }
        Err(e) => reply(room, &format!("Cannot write the self-serve allowlist: {e}")).await,
    }
}

async fn handle_serve_open(ctx: &BotCtx, room: &Room, sender: &str) {
    let _guard = ctx.mutate_lock.lock().await;
    match nvb_config::open_self_serve(&ctx.self_serve_allow_path) {
        Ok(was_restricted) => {
            refresh_self_serve(ctx);
            audit(ctx, sender, "serve_open", serde_json::json!({ "was_restricted": was_restricted }));
            if was_restricted {
                reply(room, "🔓 Self-serve is now OPEN to every Matrix user (allowlist removed).").await;
            } else {
                reply(room, "Self-serve was already open to everyone.").await;
            }
        }
        Err(e) => reply(room, &format!("Cannot remove the self-serve allowlist: {e}")).await,
    }
}

async fn handle_serve_list(ctx: &BotCtx, room: &Room) {
    match nvb_config::load_self_serve_allow(&ctx.self_serve_allow_path) {
        Ok(None) => {
            reply(room, "Self-serve is OPEN — every Matrix user may self-serve (when open_registration is on).").await
        }
        Ok(Some(list)) if list.is_empty() => {
            reply(room, "Self-serve is RESTRICTED but the allowlist is empty — only operators may self-serve. Add users with `serve-allow`.").await
        }
        Ok(Some(mut list)) => {
            list.sort();
            let lines: Vec<String> = list.iter().map(|m| format!("• {m}")).collect();
            reply(room, &format!("Self-serve allowlist ({}):\n{}", list.len(), lines.join("\n"))).await;
        }
        Err(e) => reply(room, &format!("Cannot read the self-serve allowlist: {e}")).await,
    }
}

const SUPERVISOR_SILENT: &str = "The supervisor did not respond — check the bridge logs.";

/// Operator `status` (#94): the puppet pool's guild footprint, rendered
/// against the Discord 100-server App Verification gate.
async fn handle_status(ctx: &BotCtx, room: &Room) {
    let Some(rows) = query_pool_footprint(ctx).await else {
        reply(room, SUPERVISOR_SILENT).await;
        return;
    };
    let tenants = nvb_config::tenant_guilds(&ctx.tenants_path).unwrap_or_default();
    reply(room, &render_footprint(&rows, &tenants)).await;
}

/// Pure renderer for `status`, split out for unit tests (#94).
fn render_footprint(rows: &[BotFootprint], tenants: &[(String, Option<String>)]) -> String {
    let mut out = String::from("Puppet-pool guild footprint:\n");
    let mut union: HashSet<u64> = HashSet::new();
    for r in rows {
        union.extend(r.guilds.iter().copied());
        let state = if r.connected { "connected" } else { "NOT CONNECTED" };
        let tripwire = if r.guilds.len() >= VERIFICATION_TRIPWIRE {
            " ⚠ nearing Discord's 100-server App Verification gate — start verification for this app (see docs/nether-voicebridge-verification-runbook.md)"
        } else {
            ""
        };
        out.push_str(&format!("• bot {}: {} guild(s), {}{}\n", r.bot, r.guilds.len(), state, tripwire));
    }
    out.push_str(&format!("Distinct guilds served: {}\n", union.len()));
    // Tenant bridges per guild (from tenants.toml — static bridges not counted).
    let mut per_guild: HashMap<String, usize> = HashMap::new();
    for (_, g) in tenants {
        if let Some(g) = g {
            *per_guild.entry(g.clone()).or_default() += 1;
        }
    }
    if per_guild.is_empty() {
        out.push_str("Tenant bridges: none");
    } else {
        let mut entries: Vec<_> = per_guild.into_iter().collect();
        entries.sort();
        out.push_str("Tenant bridges per guild: ");
        out.push_str(
            &entries.iter().map(|(g, n)| format!("{g}×{n}")).collect::<Vec<_>>().join(", "),
        );
    }
    out
}

/// Per-bot guild count at which `status` starts warning about the Discord
/// App Verification gate (100 servers hard; start the paperwork early).
const VERIFICATION_TRIPWIRE: usize = 60;

/// Operator `stats` (#94, minimal tier): per-bridge uptime/owner/created plus
/// whole-process RSS. Honest limitation: tenants share one process, so
/// per-tenant CPU/memory attribution needs per-tenant processes (roadmap).
async fn handle_stats(ctx: &BotCtx, room: &Room) {
    let Some(mut list) = query_status(ctx).await else {
        reply(room, SUPERVISOR_SILENT).await;
        return;
    };
    list.sort_by(|a, b| a.label.cmp(&b.label));
    let now_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let mut out = format!("Running bridges ({}):\n", list.len());
    for b in &list {
        let owner = b.owner.as_deref().unwrap_or("(static)");
        let age = match b.created {
            Some(c) if now_epoch >= c => format!(", linked {} ago", human_secs((now_epoch - c) as u64)),
            _ => String::new(),
        };
        out.push_str(&format!(
            "• {} — {} — up {}{}\n",
            b.label,
            owner,
            human_secs(b.uptime_secs),
            age
        ));
    }
    match proc_rss_mib() {
        Some(mib) => out.push_str(&format!("Process RSS: {mib} MiB")),
        None => out.push_str("Process RSS: unavailable"),
    }
    out.push_str(" — shared across all bridges; per-tenant CPU/memory attribution needs per-tenant processes.");
    reply(room, &out).await;
}

/// `VmRSS` of this process in MiB, from `/proc/self/status` (Linux-only,
/// best-effort — `None` elsewhere).
fn proc_rss_mib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let kb: u64 = status
        .lines()
        .find(|l| l.starts_with("VmRSS:"))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()?;
    Some(kb / 1024)
}

/// `"3d 4h"` / `"2h 5m"` / `"42s"` — coarse human duration for `stats`.
fn human_secs(s: u64) -> String {
    let (d, h, m) = (s / 86_400, (s % 86_400) / 3_600, (s % 3_600) / 60);
    if d > 0 {
        format!("{d}d {h}h")
    } else if h > 0 {
        format!("{h}h {m}m")
    } else if m > 0 {
        format!("{m}m")
    } else {
        format!("{s}s")
    }
}

/// DM `text` to `mxid` as the bot, reusing an existing direct room when the
/// SDK knows one (#94). Extracted so `msg` and `announce` share the path.
async fn dm_user(client: &Client, mxid: &str, text: &str) -> Result<(), String> {
    let uid = UserId::parse(mxid).map_err(|e| e.to_string())?;
    let dm = client.create_dm(&uid).await.map_err(|e| e.to_string())?;
    // #97 residual, #100 mode 2: in an encrypted DM the SDK computes megolm
    // recipients from its LOCAL member list, which lags a fresh `create_dm` —
    // sending immediately shares the room key with nobody, and the recipient
    // gets a PERMANENTLY undecryptable first message (the SDK won't re-share
    // to other users on key request). `get_member` pulls the membership from
    // the server into the store; retry briefly (same trick as
    // `greet_when_joined`) before giving up and sending anyway — a possibly
    // undecryptable message still beats silently dropping operator outreach.
    for delay in [0u64, 1, 3, 10] {
        if delay > 0 {
            tokio::time::sleep(Duration::from_secs(delay)).await;
        }
        match dm.get_member(&uid).await {
            Ok(Some(_)) => break,
            Ok(None) => continue,
            Err(e) => {
                tracing::debug!(target_user = %mxid, error = %e, "dm_user: member fetch failed — retrying");
            }
        }
    }
    dm.send(RoomMessageEventContent::text_plain(text)).await.map_err(|e| e.to_string())?;
    Ok(())
}

/// Operator `msg` (#94): one-off DM to a specific user, audit-logged.
async fn handle_msg(ctx: &BotCtx, room: &Room, sender: &str, mxid: &str, text: &str) {
    if !(mxid.starts_with('@') && mxid.contains(':')) {
        reply(room, &format!("'{mxid}' is not a full MXID (expected @localpart:server).")).await;
        return;
    }
    let body = format!("📣 Message from the bridge operator:\n{text}");
    match dm_user(&room.client(), mxid, &body).await {
        Ok(()) => {
            audit(ctx, sender, "operator_msg", serde_json::json!({ "to": mxid }));
            reply(room, &format!("✅ Sent to {mxid}.")).await;
        }
        Err(e) => reply(room, &format!("❌ Could not DM {mxid}: {e}")).await,
    }
}

/// How long a staged `announce` stays confirmable (#94).
const ANNOUNCE_CONFIRM_WINDOW: Duration = Duration::from_secs(120);

/// Distinct owners an `announce` reaches (#94): every tenant owner, deduped,
/// minus ownerless legacy entries and the operators themselves. Pure for
/// unit tests.
fn announce_recipients(
    owners: &[(String, Option<String>)],
    operators: &HashSet<String>,
) -> Vec<String> {
    let mut out: Vec<String> = owners
        .iter()
        .filter_map(|(_, o)| o.clone())
        .filter(|o| !operators.contains(o))
        .collect();
    out.sort();
    out.dedup();
    out
}

/// Operator `announce <text>` (#94): stage the mass DM and report who it
/// would reach; nothing sends until `announce confirm`.
async fn handle_announce_stage(ctx: &BotCtx, room: &Room, text: String) {
    let owners = match nvb_config::tenant_owners(&ctx.tenants_path) {
        Ok(o) => o,
        Err(e) => {
            reply(room, &format!("Cannot read tenants.toml: {e}")).await;
            return;
        }
    };
    let recipients = announce_recipients(&owners, &ctx.allow);
    if recipients.is_empty() {
        reply(room, "No tenant owners to announce to (operators and ownerless entries are skipped).").await;
        return;
    }
    *ctx.pending_announce.lock().unwrap_or_else(PoisonError::into_inner) =
        Some((text.clone(), Instant::now()));
    reply(room, &format!(
        "Staged announcement to {} owner(s):\n—\n{text}\n—\nSend `announce confirm` within {} minutes to deliver it. Use sparingly.",
        recipients.len(),
        ANNOUNCE_CONFIRM_WINDOW.as_secs() / 60
    ))
    .await;
}

/// Operator `announce confirm` (#94): deliver the staged announcement to
/// every recipient, tally the result, audit it.
async fn handle_announce_confirm(ctx: &BotCtx, room: &Room, sender: &str) {
    let staged = ctx
        .pending_announce
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take();
    let Some((text, staged_at)) = staged else {
        reply(room, "Nothing staged — `announce <text>` first.").await;
        return;
    };
    if staged_at.elapsed() > ANNOUNCE_CONFIRM_WINDOW {
        reply(room, "The staged announcement expired — `announce <text>` again.").await;
        return;
    }
    let owners = match nvb_config::tenant_owners(&ctx.tenants_path) {
        Ok(o) => o,
        Err(e) => {
            reply(room, &format!("Cannot read tenants.toml: {e}")).await;
            return;
        }
    };
    let recipients = announce_recipients(&owners, &ctx.allow);
    let body = format!("📣 Announcement from the bridge operator:\n{text}");
    let client = room.client();
    let (mut sent, mut failed) = (0usize, Vec::new());
    for r in &recipients {
        match dm_user(&client, r, &body).await {
            Ok(()) => sent += 1,
            Err(e) => failed.push(format!("{r} ({e})")),
        }
    }
    audit(ctx, sender, "announce", serde_json::json!({
        "recipients": recipients.len(),
        "sent": sent,
        "failed": failed.len(),
    }));
    let mut out = format!("📣 Announcement delivered to {sent}/{} owner(s).", recipients.len());
    if !failed.is_empty() {
        out.push_str(&format!("\nFailed: {}", failed.join(", ")));
    }
    reply(room, &out).await;
}

/// The `report` command (#78 Need 2): let anyone who can reach the bot flag an
/// abusive bridge. A report is *reactive tooling* — it never acts on its own
/// (auto-banning on untrusted input would itself be an abuse vector). It is
/// recorded to the audit trail for an operator to review via `reports` and, if
/// warranted, `ban`. Self-serve tier, so a public instance hears from the
/// people most likely to be harmed.
async fn handle_report(
    ctx: &BotCtx,
    room: &Room,
    sender: &str,
    kind_str: &str,
    value: &str,
    reason: Option<&str>,
) {
    reply(room, &report_core(ctx, sender, kind_str, value, reason)).await;
}

/// Pure core of `report`, split out for unit testing (mirroring
/// `cooldown_check`/`window_check`): validate → rate-gate → append the audit
/// event → return the reply text. The only side effects are the best-effort
/// audit write and the shared cooldown stamp.
fn report_core(
    ctx: &BotCtx,
    sender: &str,
    kind_str: &str,
    value: &str,
    reason: Option<&str>,
) -> String {
    let Some(kind) = BanKind::parse(kind_str) else {
        return REPORT_USAGE.to_string();
    };
    if !ban_value_shape_ok(kind, value) {
        return format!("'{value}' does not look like a {} — {}", kind.as_str(), REPORT_USAGE);
    }
    // Share the state-change cooldown so a self-serve user can't flood the
    // audit trail with reports. Operators are trusted (and would `ban`, not
    // spam-report), so they are not gated — matching the self-serve-only shape
    // of the other velocity limits (#77).
    if !ctx.is_operator(sender) {
        if let Some(remaining) = ctx.mutate_cooldown(sender) {
            return format!("⏳ Rate limited — try again in {remaining}s.");
        }
    }
    // Keep the trail bounded: trim, cap length (by chars so we never split a
    // UTF-8 boundary), and drop an empty reason to `None`.
    let reason = reason.and_then(|r| {
        let trimmed = r.trim();
        if trimmed.is_empty() {
            None
        } else if trimmed.chars().count() > REPORT_REASON_MAX {
            Some(trimmed.chars().take(REPORT_REASON_MAX).collect::<String>())
        } else {
            Some(trimmed.to_string())
        }
    });
    tracing::info!(reporter = %sender, kind = kind.as_str(), value, "abuse report filed");
    audit(ctx, sender, "report", serde_json::json!({
        "kind": kind.as_str(), "value": value, "reason": reason,
    }));
    format!(
        "🚩 Thanks — your report about {} `{value}` has been recorded for an operator to review. \
         Bridges are not removed automatically; an operator will decide.",
        kind.as_str()
    )
}

/// The `reports` command (#78 Need 1): render the recent `report` events from
/// the audit trail so an operator can spot patterns and act — each line carries
/// the exact `ban` to run, wiring the report path straight into the deny-list
/// tooling. Operator-only (excluded from [`allowed_for_self_serve`]).
async fn handle_reports(ctx: &BotCtx, room: &Room, count: usize) {
    reply(room, &format_reports(&ctx.audit_path, count)).await;
}

/// One `report` event recovered from the audit trail.
struct ReportView {
    ts: u64,
    actor: String,
    kind: String,
    value: String,
    reason: Option<String>,
}

/// Parse `<data_dir>/audit.jsonl` and return the last `count` `report` events,
/// oldest→newest. Best-effort: a missing/short/garbled trail (or a non-report
/// line) is simply skipped, never an error — the audit log is forensic, and a
/// query over it must not fail loudly.
fn recent_reports(audit_path: &str, count: usize) -> Vec<ReportView> {
    let Ok(body) = std::fs::read_to_string(audit_path) else {
        return Vec::new();
    };
    let mut out: Vec<ReportView> = body
        .lines()
        .filter_map(|line| {
            let v: serde_json::Value = serde_json::from_str(line).ok()?;
            if v.get("event").and_then(|e| e.as_str()) != Some("report") {
                return None;
            }
            let detail = v.get("detail")?;
            Some(ReportView {
                ts: v.get("ts").and_then(|t| t.as_u64()).unwrap_or(0),
                actor: v.get("actor").and_then(|a| a.as_str()).unwrap_or("<unknown>").to_string(),
                kind: detail.get("kind").and_then(|k| k.as_str())?.to_string(),
                value: detail.get("value").and_then(|x| x.as_str())?.to_string(),
                reason: detail
                    .get("reason")
                    .and_then(|r| r.as_str())
                    .map(str::to_string),
            })
        })
        .collect();
    // Keep the tail (most recent); the file is append-ordered.
    if out.len() > count {
        out.drain(0..out.len() - count);
    }
    out
}

/// Render `recent_reports` for an operator: one bullet per report with the
/// reporter, target, reason, and a copy-paste `ban` command.
fn format_reports(audit_path: &str, count: usize) -> String {
    let reports = recent_reports(audit_path, count);
    if reports.is_empty() {
        return "No reports on file.".to_string();
    }
    let mut lines = vec![format!("Last {} report(s), oldest first:", reports.len())];
    for r in &reports {
        let reason = r.reason.as_deref().map(|s| format!(" — \"{s}\"")).unwrap_or_default();
        lines.push(format!(
            "• [t={}] {} reported {} `{}`{}  → `ban {} {}`",
            r.ts, r.actor, r.kind, r.value, reason, r.kind, r.value
        ));
    }
    lines.join("\n")
}

/// Cheap shape check for a ban value, mirroring `link`'s checks: catches a
/// swapped kind/value before it lands in the deny-list.
fn ban_value_shape_ok(kind: BanKind, value: &str) -> bool {
    match kind {
        BanKind::Owner => value.starts_with('@') && value.contains(':'),
        BanKind::Guild => is_numeric_id(value),
        BanKind::Room => plausible_room_id(value),
    }
}

/// Discord snowflakes are decimal digits (17-20 today, but don't be pedantic).
fn is_numeric_id(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

/// Both room-id generations (#103): legacy `!localpart:domain` AND the v12+
/// domainless hash form `!<opaque>`. `!` + non-empty + no whitespace is the
/// gate — the join/state probes are the real validation (and ruma parses both
/// forms fine; a real user's homeserver mints the new form today).
fn plausible_room_id(s: &str) -> bool {
    s.starts_with('!') && s.len() > 1 && !s.contains(char::is_whitespace)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plausible_room_id_accepts_both_generations() {
        // Legacy domain form.
        assert!(plausible_room_id("!abc:example.org"));
        // v12+ domainless hash form — the exact shape a real user's server
        // minted (#103); the old `contains(':')` check rejected it.
        assert!(plausible_room_id("!HZxp_Dy3wHbASuHxk8Th0qhzGHl3Bxuj3t1IvMMH54U"));
        // Still rejects the obvious non-ids.
        assert!(!plausible_room_id("#alias:example.org"));
        assert!(!plausible_room_id("!"));
        assert!(!plausible_room_id("!has space:example.org"));
        assert!(!plausible_room_id("room-without-sigil"));
    }

    #[test]
    fn cooldown_peek_never_stamps() {
        // #97: peeking must not arm the window — the arm happens explicitly
        // after a successful join. A duplicate delivery peeking its own
        // sender must see a CLEAN window until the join lands.
        let mut stamps = HashMap::new();
        let cd = Duration::from_secs(30);
        let t0 = Instant::now();
        assert_eq!(cooldown_peek(&stamps, "@u:x", t0, cd), None);
        // Peeking again (the duplicate) still sees clean — nothing stamped.
        assert_eq!(cooldown_peek(&stamps, "@u:x", t0 + Duration::from_secs(3), cd), None);
        // After the explicit stamp (successful join), the window is armed…
        stamps.insert("@u:x".to_string(), t0 + Duration::from_secs(3));
        assert_eq!(
            cooldown_peek(&stamps, "@u:x", t0 + Duration::from_secs(4), cd),
            Some(29)
        );
        // …matching cooldown_check's rounding (whole secs, ≥1)…
        assert_eq!(
            cooldown_peek(&stamps, "@u:x", t0 + Duration::from_millis(32_900), cd),
            Some(1)
        );
        // …and expires like it too.
        assert_eq!(
            cooldown_peek(&stamps, "@u:x", t0 + Duration::from_secs(34), cd),
            None
        );
    }

    #[test]
    fn misconfig_notice_names_event_fix_and_docs() {
        let n = misconfig_notice("Game Night", 50, 0, "\nGuide: https://x.test/g");
        assert!(n.contains("voice bridge 'Game Night'"));
        assert!(n.contains("org.matrix.msc3401.call.member"));
        assert!(n.contains("needs power level 50"));
        assert!(n.contains("join at 0"));
        assert!(n.contains("Element Call"));
        assert!(n.contains("/devtools"));
        assert!(n.ends_with("\nGuide: https://x.test/g"));
        // Without a docs url the notice is self-contained.
        assert!(!misconfig_notice("x", 50, 0, "").contains("Guide:"));
    }

    #[test]
    fn misconfig_cooldown_gates_per_room() {
        // The #95 notify path rides cooldown_check keyed by room id: first
        // sighting notifies (and stamps), repeats inside the window stay
        // silent, and a second room is independent.
        let mut stamps = HashMap::new();
        let t0 = Instant::now();
        assert_eq!(cooldown_check(&mut stamps, "!a:x", t0, MISCONFIG_NOTIFY_COOLDOWN), None);
        assert!(
            cooldown_check(&mut stamps, "!a:x", t0 + Duration::from_secs(300), MISCONFIG_NOTIFY_COOLDOWN)
                .is_some()
        );
        assert_eq!(cooldown_check(&mut stamps, "!b:x", t0, MISCONFIG_NOTIFY_COOLDOWN), None);
        // Past the window the room re-notifies.
        assert_eq!(
            cooldown_check(&mut stamps, "!a:x", t0 + MISCONFIG_NOTIFY_COOLDOWN + Duration::from_secs(1), MISCONFIG_NOTIFY_COOLDOWN),
            None
        );
    }

    #[test]
    fn parse_operator_tooling_verbs() {
        // #94: status/stats take no args; msg needs mxid + text; announce
        // distinguishes the confirm word from announcement text.
        assert_eq!(parse_command("status"), Parsed::Cmd(Command::Status));
        assert_eq!(parse_command("stats"), Parsed::Cmd(Command::Stats));
        assert!(matches!(parse_command("status now"), Parsed::Usage(_)));
        assert_eq!(
            parse_command("msg @u:x.org please trim usage"),
            Parsed::Cmd(Command::Msg { mxid: "@u:x.org".into(), text: "please trim usage".into() })
        );
        assert!(matches!(parse_command("msg @u:x.org"), Parsed::Usage(_)));
        assert_eq!(
            parse_command("announce maintenance at noon"),
            Parsed::Cmd(Command::Announce { text: "maintenance at noon".into() })
        );
        assert_eq!(parse_command("announce confirm"), Parsed::Cmd(Command::AnnounceConfirm));
        assert!(matches!(parse_command("announce"), Parsed::Usage(_)));
        // None of the new verbs are self-serve reachable.
        for cmd in [
            Command::Status,
            Command::Stats,
            Command::Msg { mxid: String::new(), text: String::new() },
            Command::Announce { text: String::new() },
            Command::AnnounceConfirm,
        ] {
            assert!(!allowed_for_self_serve(&cmd));
        }
    }

    #[test]
    fn announce_recipients_dedup_and_skip() {
        let owners = vec![
            ("a".to_string(), Some("@one:x".to_string())),
            ("b".to_string(), Some("@one:x".to_string())), // dup owner
            ("c".to_string(), Some("@op:x".to_string())),  // operator
            ("d".to_string(), None),                       // legacy, ownerless
            ("e".to_string(), Some("@two:y".to_string())),
        ];
        let ops: HashSet<String> = ["@op:x".to_string()].into();
        assert_eq!(announce_recipients(&owners, &ops), vec!["@one:x", "@two:y"]);
        assert!(announce_recipients(&[], &ops).is_empty());
    }

    #[test]
    fn footprint_render_marks_tripwire() {
        let rows = vec![
            BotFootprint { bot: 0, connected: true, guilds: vec![1, 2] },
            BotFootprint { bot: 1, connected: false, guilds: (0..65).collect() },
        ];
        let tenants = vec![
            ("t1".to_string(), Some("1".to_string())),
            ("t2".to_string(), Some("1".to_string())),
        ];
        let s = render_footprint(&rows, &tenants);
        assert!(s.contains("bot 0: 2 guild(s), connected"));
        assert!(s.contains("bot 1: 65 guild(s), NOT CONNECTED ⚠"));
        assert!(s.contains("Distinct guilds served: 65"));
        assert!(s.contains("1×2"));
        // Under the tripwire → no warning marker on bot 0's line.
        assert!(!s.lines().next().unwrap_or("").contains('⚠'));
    }

    #[test]
    fn human_secs_buckets() {
        assert_eq!(human_secs(42), "42s");
        assert_eq!(human_secs(300), "5m");
        assert_eq!(human_secs(7500), "2h 5m");
        assert_eq!(human_secs(273_600), "3d 4h");
    }

    #[test]
    fn docs_line_present_and_absent() {
        // Unset ⇒ empty, so every reply that appends it is byte-identical to
        // the pre-#92 text.
        assert_eq!(docs_line(None), "");
        // Set ⇒ a leading-newline suffix, safe to append mid-reply.
        assert_eq!(
            docs_line(Some("https://example.test/guide")),
            "\nGuide: https://example.test/guide"
        );
    }

    #[test]
    fn command_room_is_dm_only() {
        // Bot + one human (or the pre-join race with just the bot) → a DM the
        // command path serves.
        assert!(is_command_room(2));
        assert!(is_command_room(1));
        // A shared/bridged voice room the bot account also sits in → never a
        // command room, so no reply/refusal/ban/approval-code can fire there.
        assert!(!is_command_room(3));
        assert!(!is_command_room(50));
    }

    #[test]
    fn cooldown_gates_within_window_and_stamps_attempts() {
        let mut stamps = HashMap::new();
        let cd = Duration::from_secs(30);
        let t0 = Instant::now();

        // First attempt: allowed and stamped.
        assert_eq!(cooldown_check(&mut stamps, "@op:x", t0, cd), None);
        // Immediately again: gated, full window remaining (rounded ≥ 1).
        assert_eq!(cooldown_check(&mut stamps, "@op:x", t0, cd), Some(30));
        // Mid-window: remaining counts down…
        assert_eq!(
            cooldown_check(&mut stamps, "@op:x", t0 + Duration::from_secs(29), cd),
            Some(1)
        );
        // …and sub-second remainders report 1, never a confusing "0s".
        assert_eq!(
            cooldown_check(&mut stamps, "@op:x", t0 + Duration::from_millis(29_500), cd),
            Some(1)
        );
        // A gated attempt did NOT re-stamp: the window still expires at t0+30.
        assert_eq!(
            cooldown_check(&mut stamps, "@op:x", t0 + Duration::from_secs(30), cd),
            None
        );
        // Cooldowns are PER operator — a different MXID is independent.
        assert_eq!(cooldown_check(&mut stamps, "@other:x", t0, cd), None);
    }

    #[test]
    fn window_budget_gates_and_recovers_as_stamps_age_out() {
        let window = Duration::from_secs(3600);
        let budget = 3usize;
        let t0 = Instant::now();
        let mut a = Vec::new();

        // First `budget` attempts inside the window are all allowed + recorded.
        assert_eq!(window_check(&mut a, t0, window, budget), None);
        assert_eq!(window_check(&mut a, t0 + Duration::from_secs(1), window, budget), None);
        assert_eq!(window_check(&mut a, t0 + Duration::from_secs(2), window, budget), None);
        // The 4th within the window is gated — retry when the OLDEST (t0) ages
        // out, i.e. the full window minus the 2 s already elapsed.
        assert_eq!(
            window_check(&mut a, t0 + Duration::from_secs(2), window, budget),
            Some(3598)
        );
        // A gated attempt is NOT recorded: the count stays at budget, so the
        // retry estimate is stable rather than ratcheting the window forward.
        assert_eq!(
            window_check(&mut a, t0 + Duration::from_secs(3), window, budget),
            Some(3597)
        );
        // Once the oldest stamps leave the window, a slot frees again. At
        // t0+3601 both t0 (elapsed 3601) and t0+1 (elapsed exactly 3600, and the
        // window is a strict `<`) have aged out; only t0+2 survives.
        let later = t0 + Duration::from_secs(3601);
        assert_eq!(window_check(&mut a, later, window, budget), None);
        // t0 and t0+1 pruned, t0+2 still live, plus `later` → 2 stamps.
        assert_eq!(a.len(), 2);
    }

    #[test]
    fn prune_stale_stamps_drops_aged_keys_only() {
        let ttl = Duration::from_secs(30);
        let now = Instant::now();
        let mut m = HashMap::new();
        m.insert("@fresh:x".to_string(), now);
        m.insert("@old:x".to_string(), now - Duration::from_secs(31));
        m.insert("@edge:x".to_string(), now - Duration::from_secs(29));
        prune_stale_stamps(&mut m, now, ttl);
        assert!(m.contains_key("@fresh:x"));
        assert!(m.contains_key("@edge:x"), "29s < 30s ttl survives");
        assert!(!m.contains_key("@old:x"), "31s ≥ 30s ttl pruned");
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn mxid_domain_extraction() {
        assert_eq!(mxid_domain("@user:example.org"), Some("example.org"));
        assert_eq!(mxid_domain("@bot:nether.im"), Some("nether.im"));
        // split_once on the first ':' keeps a port/path in the domain part.
        assert_eq!(mxid_domain("@a:b:c"), Some("b:c"));
        assert_eq!(mxid_domain("nocolon"), None);
        assert_eq!(mxid_domain("user:example.org"), None); // no leading '@'
        assert_eq!(mxid_domain("@nodomain"), None);
    }

    #[test]
    fn abandoned_probe_rooms_reaps_only_expired_probe_created_unbridged() {
        let ttl = Duration::from_secs(600);
        // Build with additive offsets so we never subtract from `Instant::now()`.
        let base = Instant::now();
        let now = base + Duration::from_secs(700); // 700 ≥ ttl ⇒ `base` entries are expired
        let live = base + Duration::from_secs(695); // age 5 s ⇒ not expired
        let mk = |room: &str, probe_joined: bool, created: Instant| {
            let mut p = test_pending("111", created);
            p.tenant.room_id = room.to_string();
            p.probe_joined = probe_joined;
            p
        };
        let mut map = HashMap::new();
        map.insert("EXP_PROBE".to_string(), mk("!reap:x", true, base)); // → leave
        map.insert("EXP_NOTPROBE".to_string(), mk("!wasthere:x", false, base)); // prune only (bot already there)
        map.insert("EXP_SAFE".to_string(), mk("!bridged:x", true, base)); // prune only (bridged)
        map.insert("LIVE".to_string(), mk("!livermtwin:x", true, live)); // kept (not expired)
        map.insert("EXP_LIVE_TWIN".to_string(), mk("!livermtwin:x", true, base)); // prune only (room still live-pending)

        let safe: HashSet<String> = ["!bridged:x".to_string()].into_iter().collect();
        let (mut prune, leave) = abandoned_probe_rooms(&map, &safe, now, ttl);
        prune.sort();
        assert_eq!(prune, vec!["EXP_LIVE_TWIN", "EXP_NOTPROBE", "EXP_PROBE", "EXP_SAFE"]);
        assert_eq!(leave, vec!["!reap:x".to_string()]);
    }

    #[test]
    fn audit_rotates_only_past_the_size_cap() {
        let dir = test_data_dir();
        let path = format!("{}/audit.jsonl", dir.to_str().unwrap());
        // Below the cap: no rotation.
        std::fs::write(&path, "x".repeat(100)).unwrap();
        rotate_if_large(&path, 1000);
        assert!(std::fs::metadata(&path).is_ok());
        assert!(std::fs::metadata(format!("{path}.1")).is_err(), "no rotation below cap");
        // At/over the cap: rotate to .1, original path is now free for a fresh append.
        std::fs::write(&path, "y".repeat(1000)).unwrap();
        rotate_if_large(&path, 1000);
        assert!(std::fs::metadata(&path).is_err(), "current file moved aside");
        assert_eq!(std::fs::read_to_string(format!("{path}.1")).unwrap().len(), 1000);
        // A missing file is a quiet no-op.
        rotate_if_large(&format!("{}/nope.jsonl", dir.to_str().unwrap()), 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn window_budget_of_zero_refuses_everything() {
        let window = Duration::from_secs(60);
        let t0 = Instant::now();
        let mut a = Vec::new();
        // A zero budget admits nothing (odd but valid) and records nothing.
        assert_eq!(window_check(&mut a, t0, window, 0), Some(60));
        assert!(a.is_empty());
    }

    #[test]
    fn parses_ban_unban_and_bans() {
        assert_eq!(
            parse_command("ban owner @evil:x"),
            Parsed::Cmd(Command::Ban { kind: "owner".into(), value: "@evil:x".into() })
        );
        assert_eq!(
            parse_command("UNBAN guild 123"),
            Parsed::Cmd(Command::Unban { kind: "guild".into(), value: "123".into() })
        );
        assert_eq!(parse_command("bans"), Parsed::Cmd(Command::Bans));
        // Wrong arity → usage, not Unknown (these are our verbs).
        assert!(matches!(parse_command("ban owner"), Parsed::Usage(_)));
        assert!(matches!(parse_command("ban a b c"), Parsed::Usage(_)));
        assert!(matches!(parse_command("bans now"), Parsed::Usage(_)));
    }

    #[test]
    fn parses_serve_commands() {
        assert_eq!(
            parse_command("serve-allow @a:x"),
            Parsed::Cmd(Command::ServeAllow { mxid: "@a:x".into() })
        );
        assert_eq!(
            parse_command("serve-remove @a:x"),
            Parsed::Cmd(Command::ServeRemove { mxid: "@a:x".into() })
        );
        assert_eq!(parse_command("serve-open"), Parsed::Cmd(Command::ServeOpen));
        assert_eq!(parse_command("serve-list"), Parsed::Cmd(Command::ServeList));
        assert!(matches!(parse_command("serve-allow"), Parsed::Usage(_)));
        assert!(matches!(parse_command("serve-open now"), Parsed::Usage(_)));
        // The serve-* commands are operator-only (not in the self-serve tier).
        assert!(!allowed_for_self_serve(&Command::ServeOpen));
        assert!(!allowed_for_self_serve(&Command::ServeList));
        assert!(!allowed_for_self_serve(&Command::ServeAllow { mxid: "@a:x".into() }));
    }

    #[test]
    fn may_self_serve_respects_open_reg_and_allowlist() {
        let dir = test_data_dir();
        let (mut ctx, _rx) = approval_ctx(&dir, Duration::from_secs(600));
        // open_registration = true, allowlist None ⇒ unrestricted.
        assert!(ctx.may_self_serve("@anyone:x"));
        // Restrict to a set.
        *ctx.self_serve_allow.lock().unwrap() =
            Some(["@a:x".to_string()].into_iter().collect());
        assert!(ctx.may_self_serve("@a:x"));
        assert!(!ctx.may_self_serve("@b:x"));
        // A present-but-empty list ⇒ only operators self-serve (nobody here).
        *ctx.self_serve_allow.lock().unwrap() = Some(HashSet::new());
        assert!(!ctx.may_self_serve("@a:x"));
        // open_registration off ⇒ never, regardless of the list.
        ctx.open_registration = false;
        *ctx.self_serve_allow.lock().unwrap() = None;
        assert!(!ctx.may_self_serve("@a:x"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn parses_report_and_reports() {
        // kind + value, no reason.
        assert_eq!(
            parse_command("report guild 123456"),
            Parsed::Cmd(Command::Report {
                kind: "guild".into(),
                value: "123456".into(),
                reason: None,
            })
        );
        // Trailing tokens collapse into a single free-text reason.
        assert_eq!(
            parse_command("report owner @evil:x harassing people in voice"),
            Parsed::Cmd(Command::Report {
                kind: "owner".into(),
                value: "@evil:x".into(),
                reason: Some("harassing people in voice".into()),
            })
        );
        // A quoted reason is one token but still a reason.
        assert_eq!(
            parse_command("report room !r:x \"spam and slurs\""),
            Parsed::Cmd(Command::Report {
                kind: "room".into(),
                value: "!r:x".into(),
                reason: Some("spam and slurs".into()),
            })
        );
        // Too few args → usage (our verb).
        assert!(matches!(parse_command("report guild"), Parsed::Usage(_)));
        assert!(matches!(parse_command("report"), Parsed::Usage(_)));

        // `reports` defaults, clamps, and rejects junk.
        assert_eq!(parse_command("reports"), Parsed::Cmd(Command::Reports { count: DEFAULT_REPORTS }));
        assert_eq!(parse_command("reports 5"), Parsed::Cmd(Command::Reports { count: 5 }));
        assert_eq!(
            parse_command("reports 9999"),
            Parsed::Cmd(Command::Reports { count: MAX_REPORTS })
        );
        assert!(matches!(parse_command("reports 0"), Parsed::Usage(_)));
        assert!(matches!(parse_command("reports abc"), Parsed::Usage(_)));
        assert!(matches!(parse_command("reports 1 2"), Parsed::Usage(_)));
    }

    #[test]
    fn report_is_self_serve_but_reports_is_operator_only() {
        assert!(allowed_for_self_serve(&Command::Report {
            kind: "guild".into(),
            value: "1".into(),
            reason: None,
        }));
        assert!(!allowed_for_self_serve(&Command::Reports { count: 10 }));
    }

    #[test]
    fn recent_reports_filters_orders_and_caps() {
        let dir = test_data_dir();
        let path = format!("{}/audit.jsonl", dir.to_str().unwrap());
        // A trail mixing non-report events with three reports, plus a garbage
        // line that must be skipped, not error.
        let lines = [
            r#"{"ts":1,"actor":"@op:x","event":"link_ok","detail":{"name":"a"}}"#,
            r#"{"ts":2,"actor":"@a:x","event":"report","detail":{"kind":"guild","value":"111","reason":"spam"}}"#,
            "not json at all",
            r#"{"ts":3,"actor":"@b:x","event":"report","detail":{"kind":"owner","value":"@e:x","reason":null}}"#,
            r#"{"ts":4,"actor":"@op:x","event":"ban","detail":{"kind":"guild","value":"111"}}"#,
            r#"{"ts":5,"actor":"@c:x","event":"report","detail":{"kind":"room","value":"!r:x","reason":"slurs"}}"#,
        ];
        std::fs::write(&path, lines.join("\n")).unwrap();

        // All three reports, oldest→newest, non-report events dropped.
        let all = recent_reports(&path, 10);
        assert_eq!(all.iter().map(|r| r.ts).collect::<Vec<_>>(), vec![2, 3, 5]);
        assert_eq!(all[0].value, "111");
        assert_eq!(all[1].reason, None);
        assert_eq!(all[2].kind, "room");

        // The cap keeps the most-recent tail.
        let last_two = recent_reports(&path, 2);
        assert_eq!(last_two.iter().map(|r| r.ts).collect::<Vec<_>>(), vec![3, 5]);

        // Rendered output carries a copy-paste ban for each.
        let rendered = format_reports(&path, 10);
        assert!(rendered.contains("`ban guild 111`"), "{rendered}");
        assert!(rendered.contains("`ban room !r:x`"), "{rendered}");
        assert!(rendered.contains("\"spam\""), "reason shown: {rendered}");

        // A missing / empty trail is a friendly no-op, not an error.
        assert_eq!(format_reports(&format!("{}/nope.jsonl", dir.to_str().unwrap()), 10), "No reports on file.");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Read the single audit line `report_core` appended (tests file exactly one
    /// report), as `(event, detail)`.
    fn read_one_audit(dir: &std::path::Path) -> (String, serde_json::Value) {
        let body = std::fs::read_to_string(format!("{}/audit.jsonl", dir.to_str().unwrap()))
            .expect("audit file written");
        let v: serde_json::Value =
            serde_json::from_str(body.lines().next().expect("one line")).expect("valid json");
        (v["event"].as_str().unwrap().to_string(), v["detail"].clone())
    }

    #[test]
    fn report_core_records_audit_event_and_acks() {
        let dir = test_data_dir();
        let (ctx, _rx) = approval_ctx(&dir, Duration::from_secs(600));
        let ack = report_core(&ctx, "@victim:x", "guild", "123456", Some("  harassment  "));
        assert!(ack.contains("has been recorded"), "ack: {ack}");
        let (event, detail) = read_one_audit(&dir);
        assert_eq!(event, "report");
        assert_eq!(detail["kind"], "guild");
        assert_eq!(detail["value"], "123456");
        // Reason is trimmed.
        assert_eq!(detail["reason"], "harassment");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn report_core_rejects_bad_kind_and_shape_without_auditing() {
        let dir = test_data_dir();
        let (ctx, _rx) = approval_ctx(&dir, Duration::from_secs(600));
        // Unknown kind → usage, nothing written.
        assert!(report_core(&ctx, "@u:x", "nonsense", "x", None).contains("Usage:"));
        // Right kind, wrong-shaped value → shape refusal.
        let bad = report_core(&ctx, "@u:x", "owner", "not-an-mxid", None);
        assert!(bad.contains("does not look like a owner"), "{bad}");
        // No audit file materialized from either rejection.
        assert!(
            std::fs::read_to_string(format!("{}/audit.jsonl", dir.to_str().unwrap()))
                .map(|s| s.trim().is_empty())
                .unwrap_or(true),
            "rejected reports must not touch the trail"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn report_core_truncates_overlong_reason() {
        let dir = test_data_dir();
        let (ctx, _rx) = approval_ctx(&dir, Duration::from_secs(600));
        let long = "é".repeat(REPORT_REASON_MAX + 50); // multibyte, to prove char-safe cut
        report_core(&ctx, "@u:x", "room", "!r:x", Some(&long));
        let (_event, detail) = read_one_audit(&dir);
        let stored = detail["reason"].as_str().unwrap();
        assert_eq!(stored.chars().count(), REPORT_REASON_MAX);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn report_core_rate_limits_non_operators() {
        let dir = test_data_dir();
        let (mut ctx, _rx) = approval_ctx(&dir, Duration::from_secs(600));
        ctx.cooldown = Some(Duration::from_secs(30));
        // First self-serve report goes through…
        assert!(report_core(&ctx, "@spammer:x", "guild", "1", None).contains("recorded"));
        // …the immediate second is velocity-gated (nothing new written).
        let gated = report_core(&ctx, "@spammer:x", "guild", "2", None);
        assert!(gated.contains("Rate limited"), "{gated}");
        let body =
            std::fs::read_to_string(format!("{}/audit.jsonl", dir.to_str().unwrap())).unwrap();
        assert_eq!(body.lines().count(), 1, "only the first report is audited");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn ban_value_shapes() {
        assert!(ban_value_shape_ok(BanKind::Owner, "@evil:example.org"));
        assert!(!ban_value_shape_ok(BanKind::Owner, "evil"));
        assert!(ban_value_shape_ok(BanKind::Guild, "123456"));
        assert!(!ban_value_shape_ok(BanKind::Guild, "@evil:x"));
        assert!(ban_value_shape_ok(BanKind::Room, "!abc:example.org"));
        assert!(!ban_value_shape_ok(BanKind::Room, "#alias:example.org"));
    }

    #[test]
    fn tokenize_plain_and_quoted() {
        assert_eq!(
            tokenize("link \"Guest Voice\" !r:x 1 2").unwrap(),
            vec!["link", "Guest Voice", "!r:x", "1", "2"]
        );
        assert_eq!(tokenize("  list  ").unwrap(), vec!["list"]);
        assert_eq!(tokenize("").unwrap(), Vec::<String>::new());
        assert!(tokenize("link \"unbalanced").is_none());
    }

    #[test]
    fn parse_verbs_case_insensitive() {
        assert_eq!(parse_command("HELP"), Parsed::Cmd(Command::Help));
        assert_eq!(parse_command("List"), Parsed::Cmd(Command::List));
    }

    #[test]
    fn parse_link_with_and_without_server() {
        assert_eq!(
            parse_command("link \"Guest Voice\" !r:nether.im 111 222"),
            Parsed::Cmd(Command::Link {
                name: "Guest Voice".into(),
                room_id: "!r:nether.im".into(),
                guild_id: "111".into(),
                channel_id: "222".into(),
                server: None,
            })
        );
        assert_eq!(
            parse_command("link g !r:x 1 2 nether"),
            Parsed::Cmd(Command::Link {
                name: "g".into(),
                room_id: "!r:x".into(),
                guild_id: "1".into(),
                channel_id: "2".into(),
                server: Some("nether".into()),
            })
        );
    }

    #[test]
    fn parse_link_wrong_arity_is_usage() {
        assert!(matches!(parse_command("link onlyname"), Parsed::Usage(_)));
        assert!(matches!(parse_command("link a b c d e f"), Parsed::Usage(_)));
        assert!(matches!(parse_command("unlink"), Parsed::Usage(_)));
        assert!(matches!(parse_command("unlink a b"), Parsed::Usage(_)));
    }

    #[test]
    fn parse_chatter_is_unknown() {
        assert_eq!(parse_command("hello there"), Parsed::Unknown);
        assert_eq!(parse_command(""), Parsed::Unknown);
        assert_eq!(parse_command("\"stray quote"), Parsed::Unknown);
    }

    #[test]
    fn numeric_id_check() {
        assert!(is_numeric_id("123456789012345678"));
        assert!(!is_numeric_id(""));
        assert!(!is_numeric_id("12a4"));
        assert!(!is_numeric_id("#general"));
    }

    fn pl(json: serde_json::Value) -> CallPowerCheck {
        evaluate_call_member_power(Some(&json))
    }

    #[test]
    fn call_power_events_override() {
        // The properly-configured Element Call room.
        let c = pl(serde_json::json!({
            "events": { "org.matrix.msc3401.call.member": 0 },
            "state_default": 50,
        }));
        assert_eq!(c, CallPowerCheck { required: 0, ghost_pl: 0 });
        assert!(c.ok());

        // An explicit blocking override.
        assert!(!pl(serde_json::json!({
            "events": { "org.matrix.msc3401.call.member": 50 },
            "state_default": 0,
        }))
        .ok());
    }

    #[test]
    fn call_power_state_default_fallback() {
        assert!(pl(serde_json::json!({ "state_default": 0 })).ok());
        // No override, no state_default → spec default 50 (the verified
        // 2026-06-16 silent breakage: a room left at default state PL).
        let c = pl(serde_json::json!({ "users_default": 0 }));
        assert_eq!(c, CallPowerCheck { required: 50, ghost_pl: 0 });
        assert!(!c.ok());
    }

    #[test]
    fn call_power_users_default_counts() {
        // Ghosts join at users_default; a room that raises it alongside
        // state_default still works.
        assert!(pl(serde_json::json!({
            "state_default": 50,
            "users_default": 50,
        }))
        .ok());
    }

    #[test]
    fn call_power_accepts_string_encoded_levels() {
        // Legacy rooms carry power levels as numeric strings.
        assert!(pl(serde_json::json!({
            "events": { "org.matrix.msc3401.call.member": "0" },
            "state_default": "50",
        }))
        .ok());
        assert!(!pl(serde_json::json!({ "state_default": "50" })).ok());
    }

    #[test]
    fn call_power_absent_event_fails_safe() {
        let c = evaluate_call_member_power(None);
        assert_eq!(c, CallPowerCheck { required: 50, ghost_pl: 0 });
        assert!(!c.ok());
    }

    // ---- two-sided link auth (#47) ----

    fn joined() -> serde_json::Value {
        serde_json::json!({ "membership": "join" })
    }

    #[test]
    fn sender_authority_needs_explicit_grant_above_default_and_floor() {
        let pl = serde_json::json!({
            "users_default": 0,
            "users": { "@mod:x": 50, "@admin:x": 100 },
        });
        // Explicit entries above the default and ≥ 50 → authorized.
        assert!(evaluate_sender_authority(Some(&pl), Some(&joined()), "@mod:x", &[]).ok());
        assert!(evaluate_sender_authority(Some(&pl), Some(&joined()), "@admin:x", &[]).ok());
        // Not in the users map → only the default (0), no explicit grant → refused.
        let plain = evaluate_sender_authority(Some(&pl), Some(&joined()), "@user:x", &[]);
        assert!(!plain.ok());
        assert!(!plain.explicit);
    }

    #[test]
    fn sender_authority_rejects_lowered_state_default_trick() {
        // #86: a room that lowered state_default to 0 used to make any member
        // "authorized" (sender_pl 0 ≥ state_default 0). Now state_default is
        // irrelevant — no explicit grant means no authority.
        let pl = serde_json::json!({ "state_default": 0, "users_default": 0 });
        assert!(!evaluate_sender_authority(Some(&pl), Some(&joined()), "@member:x", &[]).ok());
    }

    #[test]
    fn sender_authority_rejects_raised_users_default() {
        // #86: a room that raised users_default to 50 gives every member PL 50.
        // Without an EXPLICIT above-default grant that is not authority.
        let pl = serde_json::json!({ "users_default": 50 });
        let a = evaluate_sender_authority(Some(&pl), Some(&joined()), "@member:x", &[]);
        assert_eq!(a.sender_pl, 50);
        assert!(!a.explicit);
        assert!(!a.ok(), "PL 50 from the default alone is not authority");
        // Even an explicit entry EQUAL to the default (not above it) is not enough.
        let pl2 = serde_json::json!({ "users_default": 50, "users": { "@x:x": 50 } });
        assert!(!evaluate_sender_authority(Some(&pl2), Some(&joined()), "@x:x", &[]).ok());
        // An explicit entry ABOVE the raised default is authority.
        let pl3 = serde_json::json!({ "users_default": 50, "users": { "@x:x": 51 } });
        assert!(evaluate_sender_authority(Some(&pl3), Some(&joined()), "@x:x", &[]).ok());
    }

    #[test]
    fn sender_authority_explicit_grant_must_reach_the_floor() {
        // Explicit and above default, but below the 50 moderator floor → refused.
        let pl = serde_json::json!({ "users_default": 0, "users": { "@helper:x": 25 } });
        assert!(!evaluate_sender_authority(Some(&pl), Some(&joined()), "@helper:x", &[]).ok());
    }

    #[test]
    fn sender_authority_requires_joined_membership() {
        let pl = serde_json::json!({ "users": { "@u:x": 50 } });
        // Power is sufficient but the sender must actually be joined.
        assert!(!evaluate_sender_authority(Some(&pl), None, "@u:x", &[]).ok());
        let invited = serde_json::json!({ "membership": "invite" });
        assert!(!evaluate_sender_authority(Some(&pl), Some(&invited), "@u:x", &[]).ok());
        assert!(evaluate_sender_authority(Some(&pl), Some(&joined()), "@u:x", &[]).ok());
    }

    #[test]
    fn sender_authority_absent_pl_fails_safe() {
        // No power_levels event → no explicit grants → refused.
        let a = evaluate_sender_authority(None, Some(&joined()), "@u:x", &[]);
        assert!(!a.explicit);
        assert!(!a.ok());
    }

    #[test]
    fn sender_authority_accepts_string_encoded_levels() {
        let pl = serde_json::json!({
            "users_default": "0",
            "users": { "@mod:x": "50" },
        });
        assert!(evaluate_sender_authority(Some(&pl), Some(&joined()), "@mod:x", &[]).ok());
    }

    #[test]
    fn sender_authority_recognizes_v12_creators() {
        // #112: v12 creators hold implicit infinite power and are FORBIDDEN
        // from users[] — the explicit-grant path can never see them. A room's
        // creator set (from the create event) must authorize directly.
        let creators = vec!["@creator:x".to_string(), "@cofounder:y".to_string()];
        // v12 rooms commonly have NO users[] entry at all for the creator.
        let pl = serde_json::json!({ "users_default": 0 });
        let a = evaluate_sender_authority(Some(&pl), Some(&joined()), "@creator:x", &creators);
        assert!(a.creator);
        assert!(a.ok(), "a joined v12 creator is room authority");
        assert!(
            evaluate_sender_authority(Some(&pl), Some(&joined()), "@cofounder:y", &creators).ok(),
            "additional_creators count too"
        );
        // Still gated on joined membership…
        assert!(!evaluate_sender_authority(Some(&pl), None, "@creator:x", &creators).ok());
        // …and a non-creator without a grant stays refused.
        assert!(!evaluate_sender_authority(Some(&pl), Some(&joined()), "@member:x", &creators).ok());
        // Absent power_levels does not defeat creatorship (creator power is
        // implicit, not a PL entry).
        assert!(evaluate_sender_authority(None, Some(&joined()), "@creator:x", &creators).ok());
    }

    #[test]
    fn approval_code_shape_and_normalization() {
        let code = mint_approval_code();
        assert!(code.starts_with("NVB-"), "{code}");
        assert_eq!(code.len(), 4 + CODE_LEN);
        assert!(code[4..].bytes().all(|b| CODE_ALPHABET.contains(&b)), "{code}");

        assert_eq!(normalize_code("nvb-abc234"), "NVB-ABC234");
        assert_eq!(normalize_code("  ABC234 "), "NVB-ABC234");
        assert_eq!(normalize_code("NVB-ABC234"), "NVB-ABC234");
    }

    fn test_pending(guild: &str, created: Instant) -> PendingLink {
        PendingLink {
            tenant: NewTenant {
                name: "t".into(),
                server: "s".into(),
                room_id: "!r:x".into(),
                guild_id: guild.into(),
                channel_id: "2".into(),
                owner: Some("@op:x".into()),
                created: Some(0),
            },
            requester: "@op:x".into(),
            origin_room: "!dm:x".try_into().unwrap(),
            warnings: Vec::new(),
            created,
            probe_joined: true,
        }
    }

    #[test]
    fn pending_take_matches_guild_and_burns_only_on_match() {
        let ttl = Duration::from_secs(600);
        let now = Instant::now();
        let mut map = HashMap::new();
        let code = insert_pending(&mut map, test_pending("111", now), now, ttl).unwrap();

        // Wrong guild: refused AND the entry survives for the right server.
        assert!(take_pending(&mut map, &code, 999, now, ttl).is_err());
        assert_eq!(map.len(), 1);

        // Right guild: claimed exactly once.
        let p = take_pending(&mut map, &code, 111, now, ttl).unwrap();
        assert_eq!(p.tenant.guild_id, "111");
        assert!(take_pending(&mut map, &code, 111, now, ttl).is_err());
    }

    #[test]
    fn pending_expires_after_ttl() {
        let ttl = Duration::from_secs(600);
        let t0 = Instant::now();
        let mut map = HashMap::new();
        let code = insert_pending(&mut map, test_pending("111", t0), t0, ttl).unwrap();
        // One second before expiry: still claimable...
        assert!(map.contains_key(&code));
        prune_expired_pending(&mut map, t0 + Duration::from_secs(599), ttl);
        assert!(map.contains_key(&code));
        // ...at/after the TTL: gone.
        assert!(take_pending(&mut map, &code, 111, t0 + Duration::from_secs(600), ttl).is_err());
        assert!(map.is_empty());
    }

    #[test]
    fn self_serve_tier_gates_operator_verbs() {
        assert!(allowed_for_self_serve(&Command::Help));
        assert!(allowed_for_self_serve(&Command::List));
        assert!(allowed_for_self_serve(&Command::Link {
            name: "n".into(),
            room_id: "!r:x".into(),
            guild_id: "1".into(),
            channel_id: "2".into(),
            server: None,
        }));
        assert!(allowed_for_self_serve(&Command::Unlink { name: "n".into() }));
        assert!(!allowed_for_self_serve(&Command::Ban {
            kind: "owner".into(),
            value: "@x:y".into()
        }));
        assert!(!allowed_for_self_serve(&Command::Unban {
            kind: "owner".into(),
            value: "@x:y".into()
        }));
        assert!(!allowed_for_self_serve(&Command::Bans));
    }

    #[test]
    fn pending_cap_refuses_but_expiry_frees() {
        let ttl = Duration::from_secs(600);
        let t0 = Instant::now();
        let mut map = HashMap::new();
        for _ in 0..PENDING_LINK_CAP {
            insert_pending(&mut map, test_pending("111", t0), t0, ttl).unwrap();
        }
        assert!(insert_pending(&mut map, test_pending("111", t0), t0, ttl).is_err());
        // Once the old ones age out, inserts are accepted again.
        let later = t0 + Duration::from_secs(600);
        assert!(insert_pending(&mut map, test_pending("111", later), later, ttl).is_ok());
        assert_eq!(map.len(), 1);
    }

    // ── #111: join-failure reply is tier-aware ───────────────────────────────

    #[test]
    fn join_failure_reply_operator_says_invite_me() {
        let msg = join_failure_reply("!r:x.org", "403 Forbidden", "@bot:x.org", true);
        assert!(msg.contains("invite @bot:x.org to it first"), "{msg}");
        assert!(msg.contains("!r:x.org") && msg.contains("403 Forbidden"), "{msg}");
        assert!(msg.contains("Nothing was written."), "{msg}");
    }

    #[test]
    fn join_failure_reply_self_serve_drops_dead_end_invite_advice() {
        let msg = join_failure_reply("!r:x.org", "403 Forbidden", "@bot:x.org", false);
        // A self-serve user's invite to the bot is auto-rejected, so the remedy must
        // NOT tell them to invite it first — it offers the reachable alternatives.
        assert!(!msg.contains("invite @bot:x.org to it first"), "{msg}");
        assert!(msg.contains("publicly joinable"), "{msg}");
        assert!(msg.contains("ask the instance operator"), "{msg}");
        assert!(msg.contains("Nothing was written."), "{msg}");
    }

    // ── Discord-side approval redemption (#76 part 2) ────────────────────────
    //
    // The live e2e scenario drives the Matrix half (link → pending code) against
    // the real bridge, but a bot cannot invoke the `/bridge approve` slash
    // command, so the redemption half is exercised here by injecting a
    // `DiscordApproval` straight into `handle_discord_approval` — the same code
    // path the pool's approval task runs (`run`, mgmt.rs:486). No homeserver:
    // the origin-room notification uses an OFFLINE client (`get_room` → None,
    // the best-effort skip), and the supervisor is a mock answering `Reconcile`.

    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A private, unique-per-test data dir (tenants/banlist/audit live here).
    fn test_data_dir() -> PathBuf {
        static N: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "nvb-mgmt-approval-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).expect("create test data dir");
        // add_tenant_entry appends to an existing file — start from empty.
        std::fs::write(nvb_config::tenants_path(dir.to_str().unwrap()), "").expect("seed tenants");
        dir
    }

    /// Build a `BotCtx` wired to `dir` with Discord approval enabled (`ttl`),
    /// returning it plus the supervisor receiver the caller drives.
    fn approval_ctx(
        dir: &std::path::Path,
        ttl: Duration,
    ) -> (BotCtx, mpsc::UnboundedReceiver<ControlMsg>) {
        let d = dir.to_str().unwrap();
        let (control, control_rx) = mpsc::unbounded_channel();
        let ctx = BotCtx {
            own_user: "@bot:example.org".try_into().unwrap(),
            allow: HashSet::new(),
            control,
            tenants_path: nvb_config::tenants_path(d),
            server_names: vec!["test".to_string()],
            homeserver_url: "https://example.invalid".to_string(),
            appservice_token: "t".to_string(),
            http: reqwest::Client::new(),
            well_known_base: "https://example.invalid".to_string(),
            mutate_lock: tokio::sync::Mutex::new(()),
            cooldown: None,
            mutate_stamps: StdMutex::new(HashMap::new()),
            invite_stamps: StdMutex::new(HashMap::new()),
            banlist_path: nvb_config::banlist_path(d),
            audit_path: format!("{d}/audit.jsonl"),
            // The #86 commit-time authority re-check does a live room read; these
            // offline approval tests exercise the commit/ban/expiry logic, not
            // authority, so keep it off (a live room can't be reached here).
            require_room_authority: false,
            approval_ttl: Some(ttl),
            pending: StdMutex::new(HashMap::new()),
            open_registration: true,
            self_serve_allow: StdMutex::new(None),
            self_serve_allow_path: format!("{d}/self_serve_allow.toml"),
            approval_bot_name: None,
            max_tenants_per_owner: Some(1),
            max_tenants_per_guild: None,
            link_attempt_budget: None,
            link_attempts: StdMutex::new(HashMap::new()),
            global_link_budget: None,
            global_link_attempts: StdMutex::new(Vec::new()),
            privacy_policy_url: None,
            docs_url: None,
            instance_notice: None,
            pending_announce: StdMutex::new(None),
            notify_stamps: StdMutex::new(HashMap::new()),
            invite_inflight: StdMutex::new(HashSet::new()),
            notice_tx: None,
        };
        (ctx, control_rx)
    }

    /// A pending link on guild 111, room `!r:x`, channel 222, owner `@u:x`.
    fn seed_one_pending(ctx: &BotCtx, ttl: Duration) -> String {
        let mut map = ctx.pending.lock().unwrap_or_else(PoisonError::into_inner);
        insert_pending(&mut map, test_pending("111", Instant::now()), Instant::now(), ttl)
            .expect("insert pending")
    }

    /// An offline client: constructed without contacting any server, so
    /// `get_room` returns `None` and the origin-room notification is skipped.
    async fn offline_client() -> Client {
        Client::builder()
            .homeserver_url("https://example.invalid")
            .build()
            .await
            .expect("offline client builds")
    }

    fn approval(code: &str, guild_id: u64) -> (DiscordApproval, oneshot::Receiver<ApprovalOutcome>) {
        let (reply, rx) = oneshot::channel();
        (
            DiscordApproval {
                code: code.to_string(),
                guild_id,
                invoker: "Alice (12345)".to_string(),
                reply,
            },
            rx,
        )
    }

    #[tokio::test]
    async fn approval_commits_tenant_and_returns_success_notice() {
        let dir = test_data_dir();
        let ttl = Duration::from_secs(600);
        let (ctx, mut control_rx) = approval_ctx(&dir, ttl);
        let code = seed_one_pending(&ctx, ttl);

        // Mock supervisor: accept the reconcile, reporting the tenant as added.
        let sup = tokio::spawn(async move {
            if let Some(ControlMsg::Reconcile { reply }) = control_rx.recv().await {
                let _ = reply.send(Ok(ReconcileSummary {
                    added: vec!["t".to_string()],
                    ..Default::default()
                }));
            }
        });

        let client = offline_client().await;
        let (ap, rx) = approval(&code, 111);
        handle_discord_approval(&ctx, &client, ap).await;
        let outcome = rx.await.expect("approval replied");

        assert!(outcome.text.contains("is up"), "expected success text, got: {}", outcome.text);
        assert!(outcome.notice.is_some(), "a committed link posts the #64 disclosure notice");
        // test_pending's channel id is "2" → the notice targets that channel.
        assert_eq!(outcome.notice_channel_id, Some(2), "notice targets the bridged channel");
        // The tenant was written to tenants.toml, and the pending entry consumed.
        let body = std::fs::read_to_string(&ctx.tenants_path).unwrap();
        assert!(body.contains("!r:x"), "tenant persisted: {body}");
        assert!(ctx.pending.lock().unwrap().is_empty(), "code consumed");
        sup.await.unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn approval_wrong_guild_keeps_code_and_writes_nothing() {
        let dir = test_data_dir();
        let ttl = Duration::from_secs(600);
        let (ctx, _control_rx) = approval_ctx(&dir, ttl);
        let code = seed_one_pending(&ctx, ttl);

        let client = offline_client().await;
        let (ap, rx) = approval(&code, 999); // pending is guild 111
        handle_discord_approval(&ctx, &client, ap).await;
        let outcome = rx.await.expect("approval replied");

        assert!(
            outcome.text.contains("different Discord server"),
            "guild mismatch message, got: {}",
            outcome.text
        );
        // Code survives for the correct server; tenants.toml untouched.
        assert!(ctx.pending.lock().unwrap().contains_key(&code), "code not burned on mismatch");
        assert!(std::fs::read_to_string(&ctx.tenants_path).unwrap().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn approval_expired_code_is_refused() {
        let dir = test_data_dir();
        let ttl = Duration::from_millis(1);
        let (ctx, _control_rx) = approval_ctx(&dir, ttl);
        let code = seed_one_pending(&ctx, ttl);
        tokio::time::sleep(Duration::from_millis(10)).await; // age past the TTL

        let client = offline_client().await;
        let (ap, rx) = approval(&code, 111);
        handle_discord_approval(&ctx, &client, ap).await;
        let outcome = rx.await.expect("approval replied");

        assert!(
            outcome.text.contains("Unknown or expired"),
            "expired message, got: {}",
            outcome.text
        );
        assert!(std::fs::read_to_string(&ctx.tenants_path).unwrap().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn approval_refused_if_banned_while_pending() {
        let dir = test_data_dir();
        let ttl = Duration::from_secs(600);
        let (ctx, _control_rx) = approval_ctx(&dir, ttl);
        let code = seed_one_pending(&ctx, ttl);
        // A ban lands on the pending link's room while it waits for approval.
        nvb_config::add_ban(&ctx.banlist_path, BanKind::Room, "!r:x").unwrap();

        let client = offline_client().await;
        let (ap, rx) = approval(&code, 111);
        handle_discord_approval(&ctx, &client, ap).await;
        let outcome = rx.await.expect("approval replied");

        assert!(
            outcome.text.contains("banned while awaiting approval"),
            "ban re-check message, got: {}",
            outcome.text
        );
        assert!(std::fs::read_to_string(&ctx.tenants_path).unwrap().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }
}
