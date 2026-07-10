//! `/bridge` slash-command handling: approve (#47) and optout/optin (#63),
//! plus command registration and the parse/reply helpers.

use super::*;
use serenity::builder::{
    CreateCommand, CreateCommandOption, CreateInteractionResponse,
    CreateInteractionResponseMessage, EditInteractionResponse,
};
use serenity::model::application::{
    Command as ApplicationCommand, CommandData, CommandDataOptionValue, CommandInteraction,
    CommandOptionType, InteractionContext,
};

/// `/bridge approve <code>` (#47 two-sided link auth). The Manage Server gate
/// is enforced here from `member.permissions` (computed server-side by Discord
/// for interactions) — with `optout`/`optin` open to everyone, the command has
/// no `default_member_permissions`, so this check is the ONLY gate. The
/// code↔guild match happens at the pending-link table on the Matrix side.
pub(super) async fn handle_approve_command(
    ctx: &Context,
    cmd: &CommandInteraction,
    guild_id: GuildId,
    code: String,
    pool: &Arc<SharedPool>,
) {
    let perms = cmd.member.as_ref().and_then(|m| m.permissions);
    if !perms.is_some_and(|p| p.manage_guild() || p.administrator()) {
        respond_ephemeral(
            ctx,
            cmd,
            "You need the **Manage Server** permission to approve a bridge link.",
        )
        .await;
        return;
    }
    let Some(cfg) = pool.link_approval.get() else {
        respond_ephemeral(ctx, cmd, "Link approval is not enabled on this instance.").await;
        return;
    };

    // Defer PUBLICLY (not ephemeral): the approval outcome is the in-server
    // disclosure record that this channel is now bridged off-platform (#43 ToS
    // follow-ups, Dev Policy §2/§3) — the manager's consent and the result
    // stay visible to the server. Deferred because the commit (file write +
    // reconcile round-trip) can exceed the 3 s initial-response window.
    // Permission/usage errors above stay ephemeral — those are for the invoker
    // alone.
    if let Err(e) = cmd
        .create_response(
            &ctx.http,
            CreateInteractionResponse::Defer(CreateInteractionResponseMessage::new()),
        )
        .await
    {
        tracing::warn!(error = %e, "failed to defer /bridge approve");
        return;
    }

    let (otx, orx) = oneshot::channel();
    let approval = LinkApproval {
        code,
        guild_id: guild_id.get(),
        invoker: format!("{} ({})", cmd.user.name, cmd.user.id.get()),
    };
    let outcome = if cfg.tx.send((approval, otx)).is_err() {
        ApprovalOutcome::text_only(
            "The bridge's management service is not running — try again later.",
        )
    } else {
        match tokio::time::timeout(Duration::from_secs(60), orx).await {
            Ok(Ok(out)) => out,
            _ => ApprovalOutcome::text_only(
                "No answer from the bridge's management service — ask the operator to check the logs.",
            ),
        }
    };
    if let Err(e) = cmd
        .edit_response(&ctx.http, EditInteractionResponse::new().content(&outcome.text))
        .await
    {
        tracing::warn!(error = %e, "failed to send the /bridge approve outcome");
    }
    // #64: post the disclosure notice into the bridged voice channel's text
    // chat, so people joining later still see that the channel is relayed.
    // Best-effort — a bot without Send Messages there degrades to the public
    // approval record above.
    if let (Some(chid), Some(notice)) = (outcome.notice_channel_id, outcome.notice.as_deref()) {
        if let Err(e) = ChannelId::new(chid).say(&ctx.http, notice).await {
            tracing::warn!(channel_id = chid, error = %e, "could not post the bridge disclosure notice (missing Send Messages on the voice channel?)");
        }
    }
}

/// `/bridge optout` / `/bridge optin` (#63): available to EVERYONE — a user's
/// choice not to be bridged needs no permission. Persists to
/// `<data_dir>/optout.toml`, updates the live set (demux + ghost gates see it
/// immediately), and an opt-out retires any existing ghosts on the spot.
pub(super) async fn handle_optout_command(
    ctx: &Context,
    cmd: &CommandInteraction,
    pool: &Arc<SharedPool>,
    opting_out: bool,
) {
    let Some(path) = pool.optout_path.get() else {
        respond_ephemeral(ctx, cmd, "Opt-out is not enabled on this instance.").await;
        return;
    };
    let user_id = cmd.user.id.get();
    // Serialize the file RMW and the in-memory set update together so concurrent
    // optout/optin interactions can't lose an update or diverge the two (F5-2).
    // No `.await` is held under the guard (both ops are synchronous).
    let write = {
        let _guard = pool.optout_write_lock.lock().await;
        let write = if opting_out {
            nvb_config::add_optout(path, user_id)
        } else {
            nvb_config::remove_optout(path, user_id)
        };
        if write.is_ok() {
            let mut set = pool.optout_set.write().unwrap_or_else(PoisonError::into_inner);
            if opting_out {
                set.insert(user_id);
            } else {
                set.remove(&user_id);
            }
        }
        write
    };
    let changed = match write {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(user_id, error = %e, "failed to update the opt-out list");
            respond_ephemeral(
                ctx,
                cmd,
                "Could not update the opt-out list — try again later or contact the operator.",
            )
            .await;
            return;
        }
    };
    if opting_out {
        pool.apply_optout(user_id);
        tracing::info!(user_id, user = %cmd.user.name, changed, "user opted out of bridging");
        respond_ephemeral(
            ctx,
            cmd,
            "🔇 You are opted out of voice bridging on this instance: your voice will not be relayed to Matrix and you will not appear in bridged calls. Use `/bridge optin` to undo.",
        )
        .await;
    } else {
        tracing::info!(user_id, user = %cmd.user.name, changed, "user opted back into bridging");
        respond_ephemeral(
            ctx,
            cmd,
            "🔊 You are opted back into voice bridging. If you are in a bridged voice channel right now, leave and rejoin it to reappear on the Matrix side.",
        )
        .await;
    }
}

/// Register the global `/bridge` application command: `approve <code>` (#47),
/// `optout`/`optin` (#63). NO `default_member_permissions` — opt-out must be
/// usable by everyone, and Discord has no per-subcommand permissions, so the
/// Manage Server gate for `approve` lives server-side in
/// `handle_approve_command`. Guild-only. Registration is an upsert, so a
/// changed definition converges on restart.
pub(super) async fn register_bridge_command(ctx: &Context) -> Result<()> {
    ApplicationCommand::create_global_command(
        &ctx.http,
        CreateCommand::new("bridge")
            .description("Nether voice bridge")
            .contexts(vec![InteractionContext::Guild])
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::SubCommand,
                    "approve",
                    "Approve a pending Matrix-Discord voice bridge link (Manage Server only)",
                )
                .add_sub_option(
                    CreateCommandOption::new(
                        CommandOptionType::String,
                        "code",
                        "The approval code from the Matrix management bot",
                    )
                    .required(true),
                ),
            )
            .add_option(CreateCommandOption::new(
                CommandOptionType::SubCommand,
                "optout",
                "Opt out of voice bridging: never relay my voice or show me in bridged calls",
            ))
            .add_option(CreateCommandOption::new(
                CommandOptionType::SubCommand,
                "optin",
                "Opt back into voice bridging",
            )),
    )
    .await
    .map(|_| ())
    .map_err(|e| anyhow!("registering the /bridge command: {e}"))
}

/// A parsed `/bridge` invocation.
#[derive(Debug, PartialEq)]
pub(super) enum BridgeSub {
    Approve { code: String },
    Optout,
    Optin,
}

/// Pull the subcommand (and its arguments) out of a `/bridge` invocation.
pub(super) fn bridge_subcommand(data: &CommandData) -> Option<BridgeSub> {
    let opt = data.options.first()?;
    match (opt.name.as_str(), &opt.value) {
        ("approve", CommandDataOptionValue::SubCommand(subs)) => {
            let code = subs.iter().find_map(|sub| {
                if sub.name != "code" {
                    return None;
                }
                match &sub.value {
                    CommandDataOptionValue::String(v) if !v.trim().is_empty() => {
                        Some(v.trim().to_string())
                    }
                    _ => None,
                }
            })?;
            Some(BridgeSub::Approve { code })
        }
        ("optout", _) => Some(BridgeSub::Optout),
        ("optin", _) => Some(BridgeSub::Optin),
        _ => None,
    }
}

/// Immediate ephemeral reply to a slash-command interaction (best-effort).
pub(super) async fn respond_ephemeral(ctx: &Context, cmd: &CommandInteraction, text: &str) {
    if let Err(e) = cmd
        .create_response(
            &ctx.http,
            CreateInteractionResponse::Message(
                CreateInteractionResponseMessage::new().content(text).ephemeral(true),
            ),
        )
        .await
    {
        tracing::warn!(error = %e, "failed to reply to a /bridge interaction");
    }
}
