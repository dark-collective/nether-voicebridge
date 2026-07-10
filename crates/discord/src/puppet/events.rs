//! Per-puppet serenity gateway handler ([`PuppetHandler`]): ready / voice-state
//! / guild lifecycle / `/bridge` interaction dispatch, plus the member &
//! bot-id decode helpers it uses.

use super::*;
use super::commands::{
    bridge_subcommand, handle_approve_command, handle_optout_command, register_bridge_command,
    respond_ephemeral, BridgeSub,
};
use super::ghosts::{drop_ghost, ensure_ghost};
use super::voice::puppet_command_loop;
use serenity::async_trait;
use serenity::model::application::Interaction;

// ─── Puppet serenity client ────────────────────────────────────────────────────

/// Per-puppet gateway handler. On `ready` it captures a live [`Context`] and
/// spawns the (channel-agnostic) command loop; it never auto-joins.
pub(super) struct PuppetHandler {
    pub(super) pool: Arc<SharedPool>,
    pub(super) idx: usize,
}

#[async_trait]
impl EventHandler for PuppetHandler {
    #[tracing::instrument(parent = None, name = "puppet", skip_all, fields(idx = self.idx))]
    async fn ready(&self, ctx: Context, ready: Ready) {
        tracing::info!(idx = self.idx, bot_user = %ready.user.name, "puppet connected");

        // Record this bot's own user id so it is never minted as a ghost.
        self.pool
            .bot_ids
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(ready.user.id.get());
        // ...and its id→slot mapping, for routing our own voice-state events
        // (#62) — authoritative confirmation of the build-time token decode.
        self.pool
            .bot_index_by_id
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(ready.user.id.get(), self.idx);
        // Self-heal: if a startup race ghosted this bot before its id was known
        // (e.g. it was lingering in voice from a previous crash), retract it now.
        self.pool.retract_self_ghost(ready.user.id.get());

        // Seed this bot's guild membership from the ready payload so the allocator
        // knows which channels it can serve before any assign arrives.
        for ug in &ready.guilds {
            self.pool.note_bot_guild(self.idx, ug.id);
        }

        // #47/#63: the designated bot registers the global `/bridge` command
        // (approve for two-sided link auth; optout/optin whenever the opt-out
        // store is armed). Once per process — `ready` refires on reconnect and
        // registration is an upsert HTTP call we needn't repeat.
        if self.pool.command_bot_index() == self.idx
            && (self.pool.link_approval.get().is_some()
                || self.pool.optout_path.get().is_some())
            && !self.pool.command_registered.swap(true, Ordering::SeqCst)
        {
            match register_bridge_command(&ctx).await {
                Ok(()) => {
                    tracing::info!(idx = self.idx, "registered the /bridge command")
                }
                Err(e) => {
                    // Cleared so the next reconnect retries.
                    self.pool.command_registered.store(false, Ordering::SeqCst);
                    tracing::error!(idx = self.idx, error = %e, "failed to register the /bridge command");
                }
            }
        }

        // `ready` can fire again on reconnect; only the first run takes the rx.
        let slot = &self.pool.slots[self.idx];
        let Some(rx) = slot.cmd_rx.lock().unwrap_or_else(PoisonError::into_inner).take() else {
            return;
        };

        tokio::spawn(puppet_command_loop(
            ctx,
            self.idx,
            self.pool.nickname_prefix.clone(),
            self.pool.clone(),
            rx,
        ));
        slot.connected.store(true, Ordering::Relaxed);
    }

    /// Drive ghost **presence** off Discord voice-channel membership. Voice-state
    /// events reach every bot's gateway and need NO bot in the voice channel, so
    /// any bot can run this — presence (hence ghost visibility) is decoupled from
    /// the capture anchor. The shared per-channel presence set makes the create/
    /// retire fire exactly once regardless of how many bots see the event.
    #[tracing::instrument(parent = None, name = "puppet", skip_all, fields(idx = self.idx))]
    async fn voice_state_update(&self, ctx: Context, _old: Option<VoiceState>, new: VoiceState) {
        let Some(guild_id) = new.guild_id else { return };
        let user_id = new.user_id.get();
        // Our own puppet/anchor bots are never ghosts and never count as presence
        // (echo-exclusion, constraint #5). `ensure_ghost` double-checks bot_ids.
        // But a voice-state change for one of OUR bots can be a moderator
        // force-move (#62): the connection survives, so no `DriverDisconnect`
        // fires and the health sweep sees a connected (but misplaced) bot. Let
        // the bot's own command loop verify it is still in its assigned channel
        // — each bot forwards only its OWN event (every pool gateway sees every
        // update; one probe is enough), and the loop's recovery latch +
        // assigned-channel comparison make the probe idempotent.
        if self.pool.is_bridge_bot(user_id) {
            if self.pool.bot_index_of(user_id) == Some(self.idx) {
                // Delay the probe a beat: an instant rejoin races songbird's
                // own processing of the very move that fired this event (the
                // join request comes back "cancelled/dropped" — live-observed),
                // which needlessly quarantines the bot and fails over. After
                // the pause songbird's view has settled, so the loop's
                // `voice_displaced` check sees the mismatch and the SAME bot
                // rejoins cleanly; quarantine+failover stays as the backstop
                // if the rejoin still fails.
                let pool = self.pool.clone();
                let idx = self.idx;
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    pool.send_cmd(idx, PuppetCommand::VerifyConnected { guild: guild_id });
                });
            }
            return;
        }
        // Reconcile this user across every bridged channel in the guild: present in
        // `here` (if it's a bridged channel), absent from the rest. This handles a
        // plain join, a channel hop, and leaving voice entirely (channel_id None)
        // uniformly, without relying on the (often-absent) old voice state.
        let here = new.channel_id;
        // A muted Discord user (self-mute or server-mute) should show muted in
        // Element Call. Mirror it onto their ghost on EVERY update for the channel
        // they're in — a mute toggle is an update on an already-present user, so it
        // must run outside the `note_presence` transition guard below.
        let muted = new.self_mute || new.mute;
        for ch in self.pool.channels_in_guild(guild_id) {
            if Some(ch.channel_id) == here {
                if self.pool.note_presence(&ch.bridge, user_id, true) {
                    ensure_ghost(&ctx, &ch, user_id, new.member.as_ref()).await;
                }
                ch.capture_router.set_muted(&user_id.to_string(), muted);
            } else if self.pool.note_presence(&ch.bridge, user_id, false) {
                drop_ghost(&ch, user_id);
                // The last Discord speaker leaving may leave a capture-only anchor
                // (one held with no playback puppet) idle — release it if so.
                self.pool.maybe_release_idle_anchor(&ch.bridge);
            }
        }
    }

    /// On (re)connect to a guild: record membership (the allocator may now use
    /// this bot for that guild), and clear a stale nickname left by a previous
    /// crash while this bot is idle (never clobber an active assignment).
    #[tracing::instrument(parent = None, name = "puppet", skip_all, fields(idx = self.idx))]
    async fn guild_create(&self, ctx: Context, guild: serenity::model::guild::Guild, _is_new: Option<bool>) {
        self.pool.note_bot_guild(self.idx, guild.id);
        // Seed presence ghosts for Discord users ALREADY in a bridged voice channel
        // before this bot connected — pre-existing members arrive in the guild
        // payload, not as voice_state_update events, so without this they'd stay
        // invisible to Matrix until they next toggled their voice state. Idempotent
        // across the pool's bots via the shared presence set.
        for (uid, vs) in &guild.voice_states {
            let Some(chan_id) = vs.channel_id else { continue };
            let user_id = uid.get();
            if self.pool.is_bridge_bot(user_id) {
                continue;
            }
            if let Some(ch) = self.pool.by_channel(guild.id, chan_id) {
                if self.pool.note_presence(&ch.bridge, user_id, true) {
                    ensure_ghost(&ctx, &ch, user_id, vs.member.as_ref()).await;
                }
                // Seed the initial mute state for a user already muted before this
                // bot connected (their toggle won't arrive as a voice_state_update).
                ch.capture_router
                    .set_muted(&user_id.to_string(), vs.self_mute || vs.mute);
            }
        }
        if self.pool.bot_has_session_in(self.idx, guild.id) {
            return;
        }
        if let Err(e) = guild.id.edit_nickname(&ctx.http, None).await {
            tracing::debug!(idx = self.idx, guild = %guild.id, error = %e, "startup nickname reset failed (may already be clear)");
        }
    }

    /// The bot was removed from a guild (or it went unavailable): stop treating it
    /// as serviceable there.
    #[tracing::instrument(parent = None, name = "puppet", skip_all, fields(idx = self.idx))]
    async fn guild_delete(
        &self,
        _ctx: Context,
        incomplete: UnavailableGuild,
        _full: Option<serenity::model::guild::Guild>,
    ) {
        self.pool.forget_bot_guild(self.idx, incomplete.id);
    }

    /// `/bridge approve <code>` (#47 two-sided link auth). Only the designated
    /// bot's application registered the command, so only its gateway receives
    /// these. The Manage Server gate is checked here from `member.permissions`
    /// (computed server-side by Discord for interactions); the code↔guild match
    /// happens at the pending-link table on the Matrix side.
    #[tracing::instrument(parent = None, name = "puppet", skip_all, fields(idx = self.idx))]
    async fn interaction_create(&self, ctx: Context, interaction: Interaction) {
        if self.pool.command_bot_index() != self.idx {
            return;
        }
        let Interaction::Command(cmd) = interaction else { return };
        if cmd.data.name != "bridge" {
            return;
        }
        let Some(guild_id) = cmd.guild_id else {
            respond_ephemeral(&ctx, &cmd, "This command only works inside a Discord server.")
                .await;
            return;
        };
        match bridge_subcommand(&cmd.data) {
            Some(BridgeSub::Approve { code }) => {
                handle_approve_command(&ctx, &cmd, guild_id, code, &self.pool).await
            }
            Some(BridgeSub::Optout) => {
                handle_optout_command(&ctx, &cmd, &self.pool, true).await
            }
            Some(BridgeSub::Optin) => {
                handle_optout_command(&ctx, &cmd, &self.pool, false).await
            }
            None => {
                respond_ephemeral(
                    &ctx,
                    &cmd,
                    "Usage: `/bridge approve code:<code>`, `/bridge optout`, or `/bridge optin`",
                )
                .await
            }
        }
    }
}

/// Decode a bot's user id from its token. A Discord bot token is
/// `base64(user_id) "." base64(timestamp) "." hmac`; the first segment base64-
/// decodes to the decimal user id. Best-effort — returns `None` on any unexpected
/// shape (the caller then relies on the `ready` event to learn the id).
pub(super) fn decode_bot_user_id(token: &str) -> Option<u64> {
    use base64::Engine;
    // Tolerate a leading "Bot " and surrounding whitespace; tokens are stored raw.
    let first = token
        .trim()
        .strip_prefix("Bot ")
        .unwrap_or(token.trim())
        .split('.')
        .next()?;
    let bytes = base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(first)
        .or_else(|_| base64::engine::general_purpose::STANDARD.decode(first))
        .ok()?;
    std::str::from_utf8(&bytes).ok()?.parse::<u64>().ok()
}

/// Resolve a Discord member's display name: server nick → global name → username.
pub(super) fn member_display_name(m: &Member) -> String {
    m.nick
        .clone()
        .or_else(|| m.user.global_name.clone())
        .unwrap_or_else(|| m.user.name.clone())
}

/// Resolve a Discord member's avatar URL: per-guild (server) avatar → global
/// avatar → None. Uses `Member::avatar_url()` (guild avatar; None if unset),
/// NOT `Member::face()` — `face()` falls back to Discord's default silhouette,
/// which we don't want mirrored onto the ghost.
pub(super) fn member_avatar_url(m: &Member) -> Option<String> {
    m.avatar_url().or_else(|| m.user.avatar_url())
}
