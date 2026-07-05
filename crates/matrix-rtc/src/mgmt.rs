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
}

/// Convenience alias for the supervisor-facing sender handle.
pub type ControlTx = mpsc::UnboundedSender<ControlMsg>;

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
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use nvb_config::{ManagementConfig, NewTenant};

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
    /// Serializes tenants.toml mutations: the link/unlink flow is a
    /// read-modify-write of the file plus a reconcile round-trip, and two
    /// concurrent commands interleaving those steps could drop an edit or roll
    /// back the wrong prior content.
    mutate_lock: tokio::sync::Mutex<()>,
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
    control: ControlTx,
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
        mutate_lock: tokio::sync::Mutex::new(()),
    });

    // Invites that arrived while the bot was offline (consumed by the initial
    // sync above, so the live handler never sees them).
    for room in client.invited_rooms() {
        let sender = stripped_invite_sender(&room, &bot_user_id).await;
        handle_invite(&ctx, sender.as_deref(), &room).await;
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
            handle_invite(&ctx, Some(ev.sender.as_str()), &room).await;
        },
    );

    // Commands.
    client.add_event_handler(
        |ev: OriginalSyncRoomMessageEvent, room: Room, Ctx(ctx): Ctx<Arc<BotCtx>>| async move {
            if room.state() != RoomState::Joined || ev.sender == ctx.own_user {
                return;
            }
            let MessageType::Text(text) = &ev.content.msgtype else { return };
            if !ctx.allow.contains(ev.sender.as_str()) {
                tracing::debug!(sender = %ev.sender, "ignoring message from non-allowlisted user");
                return;
            }
            match parse_command(&text.body) {
                Parsed::Cmd(cmd) => handle_command(&ctx, &room, cmd).await,
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

/// The inviter of a pending invite, read from the stripped `m.room.member`
/// state for our own user (the SDK does not carry an inviter field on the
/// `Room`).
async fn stripped_invite_sender(room: &Room, own: &UserId) -> Option<String> {
    let raw = room
        .get_state_event_static_for_key::<RoomMemberEventContent, _>(own)
        .await
        .ok()??;
    match raw.deserialize().ok()? {
        SyncOrStrippedState::Stripped(ev) => Some(ev.sender.to_string()),
        SyncOrStrippedState::Sync(_) => None,
    }
}

/// Accept (allowlisted inviter) or reject a room invite. Joins via the
/// appservice-asserted `POST /join` — the client's own appservice-login token
/// is guest-classified on some homeservers (Tuwunel) and would 403 on rooms
/// without `guest_access` (see `matrix::appservice_join_room`).
async fn handle_invite(ctx: &BotCtx, sender: Option<&str>, room: &Room) {
    let room_id = room.room_id().to_owned();
    match sender {
        Some(s) if ctx.allow.contains(s) => {
            tracing::info!(room_id = %room_id, inviter = %s, "accepting invite from allowlisted operator");
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
        _ => {
            tracing::warn!(
                room_id = %room_id,
                inviter = sender.unwrap_or("<unknown>"),
                "rejecting invite from non-allowlisted user"
            );
            if let Err(e) = room.leave().await {
                tracing::warn!(room_id = %room_id, error = %e, "failed to reject invite");
            }
        }
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

const HELP_TEXT: &str = "Voice bridge management commands:\n  list — all bridges (static + tenants) and whether they are running\n  link <name> <matrix-room-id> <discord-guild-id> <discord-channel-id> [server] — bridge a Discord voice channel to a Matrix room\n  unlink <name> — remove a bridge created with link\n  help — this text\nQuote names containing spaces: link \"Guest Voice\" !abc:example.org 123456 789012";

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

async fn handle_command(ctx: &BotCtx, room: &Room, cmd: Command) {
    match cmd {
        Command::Help => reply(room, HELP_TEXT).await,
        Command::List => handle_list(ctx, room).await,
        Command::Unlink { name } => handle_unlink(ctx, room, &name).await,
        Command::Link {
            name,
            room_id,
            guild_id,
            channel_id,
            server,
        } => handle_link(ctx, room, name, room_id, guild_id, channel_id, server).await,
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

async fn handle_list(ctx: &BotCtx, room: &Room) {
    let Some(running) = query_status(ctx).await else {
        reply(room, "The supervisor did not respond — check the bridge logs.").await;
        return;
    };
    let mut lines: Vec<String> = Vec::new();
    for b in &running {
        lines.push(format!(
            "• {} — {}, room {}, guild {} channel {} [running]",
            b.label,
            if b.is_tenant { "tenant" } else { "static" },
            b.room_id,
            b.guild_id,
            b.channel_id,
        ));
    }
    // Entries in tenants.toml that are NOT running (e.g. their spawn failed).
    let running_labels: HashSet<&str> = running.iter().map(|b| b.label.as_str()).collect();
    match nvb_config::list_tenant_names(&ctx.tenants_path) {
        Ok(names) => {
            for name in names {
                if !running_labels.contains(name.as_str()) {
                    lines.push(format!("• {name} — tenant in tenants.toml but NOT running (check logs)"));
                }
            }
        }
        Err(e) => lines.push(format!("(could not read tenants.toml: {e})")),
    }
    if lines.is_empty() {
        reply(room, "No bridges. Create one with `link` — send `help` for usage.").await;
    } else {
        reply(room, &format!("Bridges:\n{}", lines.join("\n"))).await;
    }
}

async fn handle_link(
    ctx: &BotCtx,
    room: &Room,
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

    let tenant = NewTenant {
        name: name.clone(),
        server,
        room_id: room_id.clone(),
        guild_id: guild_id.clone(),
        channel_id: channel_id.clone(),
    };
    let _guard = ctx.mutate_lock.lock().await;
    let prior = match nvb_config::add_tenant_entry(&ctx.tenants_path, &tenant) {
        Ok(p) => p,
        Err(e) => {
            reply(room, &format!("Cannot add '{name}': {e}")).await;
            return;
        }
    };

    match request_reconcile(ctx).await {
        Some(Ok(summary)) => {
            if let Some((_, err)) = summary.failed.iter().find(|(l, _)| l == &name) {
                reply(room, &format!(
                    "The entry was accepted but the bridge FAILED to start: {err}\nIt stays in tenants.toml — fix the cause and `unlink`/`link` again (or reload)."
                ))
                .await;
            } else if summary.added.iter().any(|l| l == &name) {
                reply(room, &format!(
                    "✅ Bridge '{name}' is up: {room_id} ↔ guild {guild_id} channel {channel_id}.\n\
                     Checklist if no audio flows:\n\
                     • the Matrix room must allow power level 0 for `org.matrix.msc3401.call.member` (starting a proper Element Call in the room once sets this)\n\
                     • a restricted/invite-only room needs the bridge bot invited\n\
                     • every puppet bot must be invited to the Discord guild with Connect, Speak, Use Voice Activity, Change Nickname"
                ))
                .await;
            } else {
                reply(room, &format!(
                    "Reconcile ran but '{name}' was not reported as added (unexpected) — check the bridge logs."
                ))
                .await;
            }
        }
        Some(Err(rejection)) => {
            // All-or-nothing reject: roll the file back so it never lies about
            // the running set.
            if let Err(e) = nvb_config::restore_tenants_file(&ctx.tenants_path, prior) {
                tracing::error!(error = %e, "rollback of tenants.toml failed after rejected reconcile");
            }
            reply(room, &format!("❌ Rejected: {rejection}\ntenants.toml was rolled back.")).await;
        }
        None => {
            reply(room, "The supervisor did not respond — the entry was written to tenants.toml but may not be running. Check the bridge logs.").await;
        }
    }
}

async fn handle_unlink(ctx: &BotCtx, room: &Room, name: &str) {
    let _guard = ctx.mutate_lock.lock().await;
    let prior = match nvb_config::remove_tenant_entry(&ctx.tenants_path, name) {
        Ok(p) => p,
        Err(e) => {
            reply(room, &format!("Cannot remove '{name}': {e}")).await;
            return;
        }
    };
    match request_reconcile(ctx).await {
        Some(Ok(summary)) => {
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
            reply(room, &format!("❌ Rejected: {rejection}\ntenants.toml was rolled back.")).await;
        }
        None => {
            reply(room, "The supervisor did not respond — the entry was removed from tenants.toml but the bridge may still be running. Check the bridge logs.").await;
        }
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
}
