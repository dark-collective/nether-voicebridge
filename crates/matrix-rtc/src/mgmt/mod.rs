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
    /// Outcome for the interaction follow-up: text plus, on success, the
    /// disclosure notice for the bridged voice channel (#64).
    pub reply: oneshot::Sender<ApprovalOutcome>,
}

use anyhow::{Context, Result};
use matrix_sdk::{
    Client, Room, RoomState, SessionMeta,
    authentication::{SessionTokens, matrix::MatrixSession},
    config::SyncSettings,
    event_handler::Ctx,
    ruma::{
        OwnedUserId,
        events::room::member::{MembershipState, StrippedRoomMemberEvent},
        events::room::message::{MessageType, OriginalSyncRoomMessageEvent},
    },
};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

use nvb_config::{ApprovalOutcome, BanKind, ManagementConfig};

mod authority;
mod handlers;
mod parse;
mod pending;
#[cfg(test)]
mod tests;

use handlers::{InviteDetails, handle_command, handle_discord_approval, handle_invite, reply, stripped_invite_details};
use parse::{Parsed, allowed_for_self_serve, parse_command};
use pending::PendingLink;

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
    /// Public privacy-policy URL (#43 ToS follow-ups): appended to the `link`
    /// disclosure texts when configured.
    privacy_policy_url: Option<String>,
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
}

impl BotCtx {
    /// Operators are the `allow` list; everyone else is a self-serve user
    /// (when `open_registration` is on) or ignored (when off).
    fn is_operator(&self, sender: &str) -> bool {
        self.allow.contains(sender)
    }
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
        privacy_policy_url: cfg.privacy_policy_url.clone(),
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
