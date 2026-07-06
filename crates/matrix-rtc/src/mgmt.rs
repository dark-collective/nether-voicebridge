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
    /// Outcome text for the ephemeral interaction follow-up.
    pub reply: oneshot::Sender<String>,
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

use nvb_config::{BanKind, ManagementConfig, NewTenant};

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
    /// `<data_dir>/banlist.toml` (#47 abuse controls) — deny-lists enforced by
    /// `link` and edited by `ban`/`unban`.
    banlist_path: String,
    /// `<data_dir>/audit.jsonl` (#47) — append-only record of every
    /// state-changing command and its outcome.
    audit_path: String,
    /// Two-sided link auth, Matrix side (#47): require the `link` sender to be
    /// a joined member of the target room with PL ≥ its `state_default`.
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
    /// `[limits] max_tenants_per_owner` — per-owner tenant cap, enforced at
    /// `link` for non-operators only. `None` ⇒ unlimited.
    max_tenants_per_owner: Option<usize>,
}

impl BotCtx {
    /// Operators are the `allow` list; everyone else is a self-serve user
    /// (when `open_registration` is on) or ignored (when off).
    fn is_operator(&self, sender: &str) -> bool {
        self.allow.contains(sender)
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
}

/// Cap on simultaneously pending links: bounds memory against a `link` spammer
/// (each entry is tiny, so this is generous) while leaving room for honest
/// concurrent onboarding.
const PENDING_LINK_CAP: usize = 16;

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
        cooldown_check(&mut stamps, sender, Instant::now(), cooldown)
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
    approval_rx: Option<mpsc::UnboundedReceiver<DiscordApproval>>,
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
        banlist_path: nvb_config::banlist_path(&data_dir),
        audit_path: format!("{}/audit.jsonl", data_dir.trim_end_matches('/')),
        require_room_authority: cfg.require_room_authority,
        approval_ttl: cfg
            .require_discord_approval
            .then(|| Duration::from_secs(cfg.approval_timeout_secs)),
        pending: StdMutex::new(HashMap::new()),
        open_registration: cfg.open_registration,
        max_tenants_per_owner: limits.max_tenants_per_owner,
    });

    // Discord-side approvals (#47 two-sided link auth): consume `/bridge
    // approve` invocations forwarded by the puppet pool. Separate task so a
    // slow commit (reconcile round-trip) never stalls the sync loop.
    if let Some(mut rx) = approval_rx {
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
            let MessageType::Text(text) = &ev.content.msgtype else { return };
            let sender = ev.sender.as_str();
            if !ctx.is_operator(sender) && !ctx.open_registration {
                tracing::debug!(sender = %ev.sender, "ignoring message from non-allowlisted user");
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
                // Unrecognized chatter: help out in a DM, stay silent in a
                // multi-user room (the bot may share a room with real traffic).
                Parsed::Unknown => {
                    if room.joined_members_count() <= 2 {
                        reply(&room, "Unrecognized command — send `help` for the command list.")
                            .await;
                    }
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
    let accept = match invite {
        Some(i) if ctx.is_operator(&i.sender) => {
            tracing::info!(room_id = %room_id, inviter = %i.sender, "accepting invite from allowlisted operator");
            true
        }
        Some(i) if ctx.open_registration => {
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
                false
            } else if !i.is_direct {
                tracing::info!(room_id = %room_id, inviter = %i.sender, "rejecting non-direct invite from self-serve user (DMs only)");
                false
            } else {
                tracing::info!(room_id = %room_id, inviter = %i.sender, "accepting DM invite from self-serve user (open registration)");
                true
            }
        }
        _ => {
            tracing::warn!(
                room_id = %room_id,
                inviter = invite.map(|i| i.sender.as_str()).unwrap_or("<unknown>"),
                "rejecting invite from non-allowlisted user"
            );
            false
        }
    };
    if !accept {
        if let Err(e) = room.leave().await {
            tracing::warn!(room_id = %room_id, error = %e, "failed to reject invite");
        }
        return;
    }
    match tokio::time::timeout(
        Duration::from_secs(30),
        crate::matrix::appservice_join_room(
            &ctx.homeserver_url,
            &ctx.appservice_token,
            room_id.as_str(),
            ctx.own_user.as_str(),
        ),
    )
    .await
    {
        Ok(Ok(())) => greet_when_joined(room.clone()),
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
fn greet_when_joined(room: Room) {
    tokio::spawn(async move {
        for delay in [1u64, 3, 10] {
            tokio::time::sleep(Duration::from_secs(delay)).await;
            let current = room.client().get_room(room.room_id());
            if let Some(r) = current {
                if r.state() == RoomState::Joined {
                    reply(&r, "👋 Voice bridge management — send `help` for commands.").await;
                    return;
                }
            }
        }
        tracing::warn!(room_id = %room.room_id(), "room never reached joined state — skipping greeting");
    });
}

/// Best-effort reply into `room` (the SDK encrypts automatically in an
/// encrypted room).
async fn reply(room: &Room, text: &str) {
    if let Err(e) = room.send(RoomMessageEventContent::text_plain(text)).await {
        tracing::warn!(room_id = %room.room_id(), error = %e, "failed to send reply");
    }
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
const UNBAN_USAGE: &str = "Usage: `unban <owner|guild|room> <value>`";

const HELP_TEXT: &str = "Voice bridge management commands:\n  list — all bridges (static + tenants) and whether they are running\n  link <name> <matrix-room-id> <discord-guild-id> <discord-channel-id> [server] — bridge a Discord voice channel to a Matrix room\n  unlink <name> — remove a bridge created with link\n  ban <owner|guild|room> <value> — deny-list an owner MXID / Discord guild / Matrix room AND unlink every matching tenant\n  unban <owner|guild|room> <value> — remove a ban\n  bans — show the deny-lists\n  help — this text\nQuote names containing spaces: link \"Guest Voice\" !abc:example.org 123456 789012";

/// Help for self-serve users (#43): only the verbs they may run, plus the
/// consent flow they will hit.
const SELF_SERVE_HELP_TEXT: &str = "Voice bridge commands:\n  link <name> <matrix-room-id> <discord-guild-id> <discord-channel-id> — bridge a Discord voice channel to a Matrix room. You must be a moderator of the Matrix room, and someone with Manage Server on the Discord side must approve the link with the `/bridge approve` command and the code you'll receive.\n  unlink <name> — remove a bridge you created\n  list — your bridges\n  help — this text\nQuote names containing spaces: link \"Guest Voice\" !abc:example.org 123456 789012";

/// The self-serve command tier (#43): which verbs a non-operator may run when
/// `open_registration` is on. Ownership scoping inside `list`/`unlink` narrows
/// further; `ban`/`unban`/`bans` are operator-only.
fn allowed_for_self_serve(cmd: &Command) -> bool {
    matches!(
        cmd,
        Command::Help | Command::List | Command::Link { .. } | Command::Unlink { .. }
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
            reply(room, text).await
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
    }
}

/// Ask the supervisor for the running set. `None` = control channel down or
/// timed out (caller reports).
async fn query_status(ctx: &BotCtx) -> Option<Vec<BridgeStatus>> {
    let (tx, rx) = oneshot::channel();
    ctx.control.send(ControlMsg::Status { reply: tx }).ok()?;
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

/// Verdict of the Matrix-side link-authority check (#47): may `sender` bridge
/// this room? The bar is "could configure the room themselves": joined, with
/// power level ≥ the room's `state_default`.
#[derive(Debug, PartialEq, Eq)]
struct SenderAuthority {
    joined: bool,
    sender_pl: i64,
    required: i64,
}

impl SenderAuthority {
    fn ok(&self) -> bool {
        self.joined && self.sender_pl >= self.required
    }
}

/// Pure evaluation of a sender's authority over a room (unit-tested below).
/// `pl_content` is the raw `m.room.power_levels` content (`None` = absent →
/// fail-safe required=50, same posture as the call-member check);
/// `member_content` is the sender's `m.room.member` content (`None` = no
/// membership event → not joined).
fn evaluate_sender_authority(
    pl_content: Option<&serde_json::Value>,
    member_content: Option<&serde_json::Value>,
    sender: &str,
) -> SenderAuthority {
    let joined = member_content
        .and_then(|m| m.get("membership"))
        .and_then(|v| v.as_str())
        == Some("join");
    let (sender_pl, required) = match pl_content {
        Some(c) => {
            let users_default = c.get("users_default").and_then(pl_int).unwrap_or(0);
            let sender_pl = c
                .get("users")
                .and_then(|u| u.get(sender))
                .and_then(pl_int)
                .unwrap_or(users_default);
            let required = c.get("state_default").and_then(pl_int).unwrap_or(50);
            (sender_pl, required)
        }
        None => (0, 50),
    };
    SenderAuthority { joined, sender_pl, required }
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
}

/// Run every room/guild prerequisite probe BEFORE tenants.toml is touched.
/// `Err` carries the complete user-facing reply text — probe failures are
/// operator errors, not bugs, so they go to the DM rather than the log.
///
/// Side effect: the join probe (deliberately idempotent with the later bridge
/// spawn) leaves the bot joined to the room even when a later probe fails.
/// That is harmless — a retried `link` reuses the membership, and the bridge
/// needs it anyway. (`unlink` never leaves rooms either; out of scope here.)
/// `authority_sender`: when set (two-sided link auth, #47), the probe also
/// verifies that this MXID is a joined member of the room with power level ≥
/// the room's `state_default` — an operator allowlist entry alone no longer
/// authorizes bridging an arbitrary room.
async fn probe_room_prereqs(
    ctx: &BotCtx,
    room_id: &str,
    guild_id: &str,
    authority_sender: Option<&str>,
) -> Result<ProbeReport, String> {
    // 1. Join (hard): covers restricted/invite-only rooms and typo'd room ids.
    match tokio::time::timeout(
        Duration::from_secs(15),
        crate::matrix::appservice_join_room(
            &ctx.homeserver_url,
            &ctx.appservice_token,
            room_id,
            ctx.own_user.as_str(),
        ),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            return Err(format!(
                "❌ The bridge bot could not join {room_id}: {e}\n\
                 If the room is invite-only or restricted, invite {} to it first, then run `link` again.\n\
                 Nothing was written.",
                ctx.own_user
            ));
        }
        Err(_) => {
            return Err(format!(
                "❌ Joining {room_id} timed out after 15 s — is the homeserver reachable?\n\
                 Nothing was written."
            ));
        }
    }

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
             Nothing was written.",
            check.required, check.ghost_pl
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
        let authority =
            evaluate_sender_authority(pl_content.as_ref(), member_content.as_ref(), sender);
        if !authority.ok() {
            let reason = if !authority.joined {
                "you are not a joined member of it".to_string()
            } else {
                format!(
                    "your power level there is {} but linking needs ≥ {} (the room's \
                     `state_default`)",
                    authority.sender_pl, authority.required
                )
            };
            return Err(format!(
                "❌ Two-sided link auth is on: only someone with moderator power in {room_id} \
                 may bridge it, and {reason}.\n\
                 Join the room and have an admin raise your power level, or ask a room \
                 moderator to run `link`.\n\
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

    Ok(ProbeReport { warnings })
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
    if !room_id.starts_with('!') || !room_id.contains(':') {
        reply(room, &format!("'{room_id}' does not look like a Matrix room id (expected `!localpart:domain`). Use the room's internal id, not an alias.")).await;
        return;
    }
    if !is_numeric_id(&guild_id) || !is_numeric_id(&channel_id) {
        reply(room, "Discord guild and channel ids must be numeric snowflakes (Server Settings → enable Developer Mode → right-click → Copy ID).").await;
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

    // Rate gate (#47): past the cheap shape checks (a typo doesn't burn the
    // budget), before the expensive probes and the state change they precede.
    if let Some(remaining) = ctx.mutate_cooldown(sender) {
        reply(room, &format!("⏳ Rate limited — try again in {remaining}s.")).await;
        return;
    }

    // Preflight probes (#47): verify the room/guild prerequisites BEFORE
    // touching tenants.toml — a failed probe replies and writes nothing.
    // Runs outside the mutate lock (slow network I/O; a duplicate-name race
    // is still caught by add_tenant_entry's own rejection). With
    // `require_room_authority` the probe also checks the SENDER's power in
    // the room (two-sided link auth, Matrix side).
    let authority_sender = ctx.require_room_authority.then_some(sender);
    let report = match probe_room_prereqs(ctx, &room_id, &guild_id, authority_sender).await {
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
                reply(room, &format!(
                    "⏳ All room checks passed — Discord-side approval required.\n\
                     Have someone with **Manage Server** permission in Discord guild {guild_id} run:\n\
                     `/bridge approve code:{code}`\n\
                     The code expires in {} minutes; nothing is written until it is approved.",
                    ttl.as_secs().div_ceil(60)
                ))
                .await;
            }
            Err(msg) => reply(room, &msg).await,
        }
        return;
    }

    let outcome = commit_link(ctx, tenant, &report.warnings, sender, None).await;
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
) -> String {
    let name = tenant.name.clone();
    let room_id = tenant.room_id.clone();
    let guild_id = tenant.guild_id.clone();
    let channel_id = tenant.channel_id.clone();

    let _guard = ctx.mutate_lock.lock().await;
    let prior = match nvb_config::add_tenant_entry(&ctx.tenants_path, &tenant) {
        Ok(p) => p,
        Err(e) => return format!("Cannot add '{name}': {e}"),
    };

    match request_reconcile(ctx).await {
        Some(Ok(summary)) => {
            if let Some((_, err)) = summary.failed.iter().find(|(l, _)| l == &name) {
                audit(ctx, actor, "link_failed_spawn", serde_json::json!({
                    "name": name, "error": err,
                }));
                format!(
                    "The entry was accepted but the bridge FAILED to start: {err}\nIt stays in tenants.toml — fix the cause and `unlink`/`link` again (or reload)."
                )
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
                format!(
                    "✅ Bridge '{name}' is up: {room_id} ↔ guild {guild_id} channel {channel_id}.\n\
                     Verified before linking:\n\
                     ✓ bridge bot joined the room\n\
                     ✓ call power levels allow the bridge's ghosts\n\
                     ✓ MatrixRTC focus discovered\n\
                     {guild_line}\n\
                     Not verifiable here: the puppet bots need Connect, Speak, Use Voice Activity and Change Nickname on the voice channel."
                )
            } else {
                format!(
                    "Reconcile ran but '{name}' was not reported as added (unexpected) — check the bridge logs."
                )
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
            format!("❌ Rejected: {rejection}\ntenants.toml was rolled back.")
        }
        None => {
            audit(ctx, actor, "link_unconfirmed", serde_json::json!({ "name": name }));
            "The supervisor did not respond — the entry was written to tenants.toml but may not be running. Check the bridge logs.".to_string()
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
        let _ = approval
            .reply
            .send("Discord-side approval is not enabled on this instance.".to_string());
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
            let _ = approval.reply.send(msg);
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
                let _ = approval
                    .reply
                    .send("🚫 This link was banned while awaiting approval.".to_string());
                return;
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "ban list unreadable — refusing approval (fail-closed)");
            let _ = approval.reply.send(
                "The ban list is unreadable — approval refused until an operator fixes it."
                    .to_string(),
            );
            return;
        }
    }

    tracing::info!(
        code = %code,
        invoker = %approval.invoker,
        tenant = %pending.tenant.name,
        "Discord-side link approval accepted"
    );
    let outcome = commit_link(
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
    let _ = approval.reply.send(outcome);
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
            audit(ctx, sender, "ban", serde_json::json!({
                "kind": kind.as_str(), "value": value, "removed": removed,
            }));
            reply(room, &format!(
                "🔨 Banned {} `{value}` and unlinked {} tenant(s): {}.",
                kind.as_str(),
                removed.len(),
                removed.join(", ")
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
                "kind": kind.as_str(), "value": value, "removed": removed,
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

/// Cheap shape check for a ban value, mirroring `link`'s checks: catches a
/// swapped kind/value before it lands in the deny-list.
fn ban_value_shape_ok(kind: BanKind, value: &str) -> bool {
    match kind {
        BanKind::Owner => value.starts_with('@') && value.contains(':'),
        BanKind::Guild => is_numeric_id(value),
        BanKind::Room => value.starts_with('!') && value.contains(':'),
    }
}

/// Discord snowflakes are decimal digits (17-20 today, but don't be pedantic).
fn is_numeric_id(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn sender_authority_users_map_override() {
        let pl = serde_json::json!({
            "state_default": 50,
            "users_default": 0,
            "users": { "@mod:x": 50, "@admin:x": 100 },
        });
        assert!(evaluate_sender_authority(Some(&pl), Some(&joined()), "@mod:x").ok());
        assert!(evaluate_sender_authority(Some(&pl), Some(&joined()), "@admin:x").ok());
        // Not in the users map → users_default 0 < 50.
        let plain = evaluate_sender_authority(Some(&pl), Some(&joined()), "@user:x");
        assert_eq!(plain, SenderAuthority { joined: true, sender_pl: 0, required: 50 });
        assert!(!plain.ok());
    }

    #[test]
    fn sender_authority_requires_joined_membership() {
        let pl = serde_json::json!({ "state_default": 0 });
        // Power is sufficient (0 ≥ 0) but the sender must actually be joined.
        assert!(!evaluate_sender_authority(Some(&pl), None, "@u:x").ok());
        let invited = serde_json::json!({ "membership": "invite" });
        assert!(!evaluate_sender_authority(Some(&pl), Some(&invited), "@u:x").ok());
        assert!(evaluate_sender_authority(Some(&pl), Some(&joined()), "@u:x").ok());
    }

    #[test]
    fn sender_authority_absent_pl_fails_safe() {
        let a = evaluate_sender_authority(None, Some(&joined()), "@u:x");
        assert_eq!(a, SenderAuthority { joined: true, sender_pl: 0, required: 50 });
        assert!(!a.ok());
    }

    #[test]
    fn sender_authority_accepts_string_encoded_levels() {
        let pl = serde_json::json!({
            "state_default": "50",
            "users": { "@mod:x": "50" },
        });
        assert!(evaluate_sender_authority(Some(&pl), Some(&joined()), "@mod:x").ok());
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
}
