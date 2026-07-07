//! Command handlers, supervisor queries, invite handling, and the audit /
//! cooldown machinery for the management bot.

use std::collections::{HashMap, HashSet};
use std::sync::PoisonError;
use std::time::{Duration, Instant};

use matrix_sdk::{
    Client, Room, RoomState,
    deserialized_responses::SyncOrStrippedState,
    ruma::{
        UserId,
        events::room::member::RoomMemberEventContent,
        events::room::message::RoomMessageEventContent,
    },
};
use tokio::sync::oneshot;

use nvb_config::{ApprovalOutcome, BanKind, NewTenant};

use super::authority::probe_room_prereqs;
use super::parse::{BAN_USAGE, Command, HELP_TEXT, SELF_SERVE_HELP_TEXT, UNBAN_USAGE};
use super::pending::{PendingLink, insert_pending, normalize_code, take_pending};
use super::{BotCtx, BridgeStatus, ControlMsg, DiscordApproval, ReconcileSummary};

/// How long a command handler waits for the supervisor to answer a control
/// request. The reconcile itself is synchronous bookkeeping (teardown happens
/// asynchronously after it), so seconds mean the supervisor loop is wedged.
const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);

/// What we know about a pending invite: who sent it and whether it is a
/// direct (DM) invite — the latter gates self-serve auto-join (#43).
pub(super) struct InviteDetails {
    pub(super) sender: String,
    pub(super) is_direct: bool,
}

/// Details of a pending invite, read from the stripped `m.room.member` state
/// for our own user (the SDK does not carry an inviter field on the `Room`).
pub(super) async fn stripped_invite_details(room: &Room, own: &UserId) -> Option<InviteDetails> {
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
pub(super) async fn handle_invite(ctx: &BotCtx, invite: Option<&InviteDetails>, room: &Room) {
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
pub(super) async fn reply(room: &Room, text: &str) {
    if let Err(e) = room.send(RoomMessageEventContent::text_plain(text)).await {
        tracing::warn!(room_id = %room.room_id(), error = %e, "failed to send reply");
    }
}

// ---- command execution ----

pub(super) async fn handle_command(ctx: &BotCtx, room: &Room, sender: &str, cmd: Command) {
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
pub(super) async fn query_guild_coverage(ctx: &BotCtx, guild_id: u64) -> Option<bool> {
    let (tx, rx) = oneshot::channel();
    ctx.control
        .send(ControlMsg::GuildCoverage { guild_id, reply: tx })
        .ok()?;
    tokio::time::timeout(CONTROL_TIMEOUT, rx).await.ok()?.ok()
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
                     The code expires in {} minutes; nothing is written until it is approved.{}",
                    ttl.as_secs().div_ceil(60),
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
pub(super) async fn handle_discord_approval(ctx: &BotCtx, client: &Client, approval: DiscordApproval) {
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
pub(super) fn ban_value_shape_ok(kind: BanKind, value: &str) -> bool {
    match kind {
        BanKind::Owner => value.starts_with('@') && value.contains(':'),
        BanKind::Guild => is_numeric_id(value),
        BanKind::Room => value.starts_with('!') && value.contains(':'),
    }
}

/// Discord snowflakes are decimal digits (17-20 today, but don't be pedantic).
pub(super) fn is_numeric_id(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
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
pub(super) fn cooldown_check(
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
