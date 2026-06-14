//! Puppet pool — per-Matrix-speaker attribution on the Discord side (Phase 7).
//!
//! Each configured puppet token is a **separate** Discord bot that joins the
//! voice channel on demand, takes a Matrix speaker's display name as its
//! nickname, and plays only that speaker's audio.  The pool implements
//! [`PlaybackRouter`]: the Matrix RTC side calls [`assign`](PuppetPool::assign)
//! when a participant's track is subscribed and [`release`](PuppetPool::release)
//! when it ends.  When every puppet is busy, the overflow speaker falls back to
//! the shared listener mix (no audio is lost, just un-attributed).
//!
//! Lifecycle is on-demand *per participant*: a puppet joins/leaves voice only
//! when a Matrix user joins/leaves the call, so nickname edits stay well within
//! Discord's rate limits (no per-utterance churn).
//!
//! Echo prevention (constraint #5): each puppet registers its own SSRC into the
//! shared [`SharedExclude`] set on `DriverConnect`, so the listener bot — which
//! hears every speaker in the channel, puppets included — never re-captures
//! puppet playback and loops it back to Matrix.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, PoisonError, RwLock};

use anyhow::{anyhow, Result};
use serenity::{
    async_trait,
    model::prelude::*,
    prelude::*,
};
use songbird::{
    input::{Input, RawAdapter},
    CoreEvent, Event, EventContext, EventHandler as SongbirdEventHandler, SerenityInit,
};
use tokio::sync::mpsc;

use nvb_audio::mixer::PlaybackMixer;
use nvb_audio::routing::{MixerSink, ParticipantSink, PlaybackRouter};
use nvb_config::DiscordConfig;

use crate::{PlaybackSource, PLAYBACK_CHANNELS, PLAYBACK_SAMPLE_RATE};

/// Discord's hard cap on guild nicknames (Unicode characters).
const NICKNAME_MAX_CHARS: usize = 32;

/// Build a puppet nickname as `{prefix}{name}`, truncated to Discord's 32-char
/// cap on a char boundary (the prefix is preserved; the name is trimmed). An
/// empty prefix yields just the (possibly truncated) name.
fn build_nickname(prefix: &str, name: &str) -> String {
    let prefix_len = prefix.chars().count();
    if prefix_len >= NICKNAME_MAX_CHARS {
        return prefix.chars().take(NICKNAME_MAX_CHARS).collect();
    }
    let mut nick = String::with_capacity(prefix.len() + name.len());
    nick.push_str(prefix);
    nick.extend(name.chars().take(NICKNAME_MAX_CHARS - prefix_len));
    nick
}

/// Shared, mutable set of bridge-owned SSRCs to exclude from listener capture.
/// Owned by the pool; shared with the listener and every puppet.  Reads happen
/// on the listener's 50 Hz tick (one read-lock per tick), writes only on a
/// puppet join/leave — so an `RwLock` keeps the hot path uncontended.
pub type SharedExclude = Arc<RwLock<HashSet<u32>>>;

/// Commands sent from the (sync) pool allocator to a puppet's async command loop.
enum PuppetCommand {
    /// Join voice, start playback from this puppet's mixer, set the nickname.
    Assign { display_name: String },
    /// Mirror the Matrix participant's mute state onto this bot's voice connection.
    SetMuted(bool),
    /// Reset the nickname, leave voice, drop the SSRC exclusion.
    Release,
}

/// Where a participant's audio is currently routed.
enum Assign {
    /// A dedicated puppet (index into [`PuppetPool::slots`]).
    Puppet(usize),
    /// Overflow — mixed into the shared listener bot.
    Listener,
}

/// One puppet's shared state.  `cmd_tx`/`cmd_rx` connect the sync allocator to
/// the puppet's async serenity task; `mixer` is its single-participant playback
/// buffer; `current_ssrc` remembers the SSRC inserted into the exclude set so it
/// can be removed on release.
struct PuppetSlot {
    token: String,
    mixer: Arc<PlaybackMixer>,
    cmd_tx: mpsc::Sender<PuppetCommand>,
    /// Taken (once) by [`PuppetPool::run_puppets`] to drive this puppet's client.
    cmd_rx: StdMutex<Option<mpsc::Receiver<PuppetCommand>>>,
    current_ssrc: Arc<StdMutex<Option<u32>>>,
    /// True while assigned to a participant.  Only mutated under the
    /// `assignments` lock, so `Relaxed` ordering is sufficient.
    busy: AtomicBool,
}

/// The puppet pool / playback router.
pub struct PuppetPool {
    /// Shared listener mixer (overflow + Phase-6 fallback path).  Also read by
    /// the listener bot's `PlaybackSource`.
    listener_mixer: Arc<PlaybackMixer>,
    exclude: SharedExclude,
    /// Prefix for puppet nicknames (e.g. `"[Matrix] "`; empty disables).
    nickname_prefix: Arc<str>,
    slots: Vec<PuppetSlot>,
    /// Participant key → current routing.  The single source of truth for
    /// allocation; guarded so `assign`/`release` stay race-free.
    assignments: StdMutex<HashMap<String, Assign>>,
}

impl PuppetPool {
    /// Build the pool from config.  Cheap and synchronous — the serenity clients
    /// are not started until [`run_puppets`](Self::run_puppets), so the returned
    /// `Arc` can be handed to the Matrix side as a [`PlaybackRouter`] up front.
    pub fn new(cfg: &DiscordConfig) -> Arc<Self> {
        let slots = cfg
            .puppet_tokens
            .iter()
            .map(|tok| {
                let (cmd_tx, cmd_rx) = mpsc::channel(8);
                PuppetSlot {
                    token: tok.clone(),
                    mixer: Arc::new(PlaybackMixer::new()),
                    cmd_tx,
                    cmd_rx: StdMutex::new(Some(cmd_rx)),
                    current_ssrc: Arc::new(StdMutex::new(None)),
                    busy: AtomicBool::new(false),
                }
            })
            .collect();

        Arc::new(Self {
            listener_mixer: Arc::new(PlaybackMixer::new()),
            exclude: Arc::new(RwLock::new(HashSet::new())),
            nickname_prefix: Arc::from(cfg.nickname_prefix.as_str()),
            slots,
            assignments: StdMutex::new(HashMap::new()),
        })
    }

    /// The shared listener mixer (handed to the listener bot's playback source).
    pub fn listener_mixer(&self) -> Arc<PlaybackMixer> {
        self.listener_mixer.clone()
    }

    /// The shared echo-exclusion set (handed to the listener's capture handler).
    pub fn exclude(&self) -> SharedExclude {
        self.exclude.clone()
    }

    /// Start every puppet's serenity client and drive them to completion.
    /// Returns on the first client error.  Called once from `nvb_discord::run`.
    #[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %bridge))]
    pub async fn run_puppets(
        self: &Arc<Self>,
        guild_id: GuildId,
        channel_id: ChannelId,
        bridge: Arc<str>,
    ) -> Result<()> {
        if self.slots.is_empty() {
            tracing::info!("no puppet tokens configured — running listener-only");
            return Ok(());
        }

        let intents = GatewayIntents::GUILDS | GatewayIntents::GUILD_VOICE_STATES;
        let mut futs = Vec::with_capacity(self.slots.len());

        for (idx, slot) in self.slots.iter().enumerate() {
            let rx = slot
                .cmd_rx
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
                .ok_or_else(|| anyhow!("puppet {idx} command receiver already taken"))?;

            let handler = PuppetHandler {
                bridge: bridge.clone(),
                idx,
                guild_id,
                channel_id,
                mixer: slot.mixer.clone(),
                exclude: self.exclude.clone(),
                nickname_prefix: self.nickname_prefix.clone(),
                current_ssrc: slot.current_ssrc.clone(),
                cmd_rx: StdMutex::new(Some(rx)),
            };
            let token = slot.token.clone();

            futs.push(async move {
                // Puppets are TX-only (they only play): the default songbird
                // config does no Opus decode/DAVE-receive. DAVE *encryption* for
                // outgoing audio is handled by songbird automatically.
                let mut client = Client::builder(&token, intents)
                    .event_handler(handler)
                    .register_songbird_from_config(songbird::Config::default())
                    .await
                    .map_err(|e| anyhow!("failed to build puppet {idx} client: {e}"))?;
                tracing::info!(idx, "starting puppet gateway");
                client
                    .start()
                    .await
                    .map_err(|e| anyhow!("puppet {idx} client error: {e}"))
            });
        }

        futures_util::future::try_join_all(futs).await?;
        Ok(())
    }

    /// Build a sink that pushes under `key` to the given mixer.
    fn sink(&self, mixer: Arc<PlaybackMixer>, key: &str) -> Arc<dyn ParticipantSink> {
        Arc::new(MixerSink::new(mixer, key.to_owned()))
    }
}

impl PlaybackRouter for PuppetPool {
    fn assign(&self, key: &str, display_name: &str) -> Arc<dyn ParticipantSink> {
        let mut map = self.assignments.lock().unwrap_or_else(PoisonError::into_inner);

        // Idempotent: a re-subscribe for the same participant reuses its output.
        if let Some(existing) = map.get(key) {
            let mixer = match existing {
                Assign::Puppet(idx) => self.slots[*idx].mixer.clone(),
                Assign::Listener => self.listener_mixer.clone(),
            };
            return self.sink(mixer, key);
        }

        // Claim the first free puppet (mutated under the lock → Relaxed is fine).
        for (idx, slot) in self.slots.iter().enumerate() {
            if !slot.busy.swap(true, Ordering::Relaxed) {
                map.insert(key.to_owned(), Assign::Puppet(idx));
                if slot
                    .cmd_tx
                    .try_send(PuppetCommand::Assign {
                        display_name: display_name.to_owned(),
                    })
                    .is_err()
                {
                    tracing::warn!(idx, "puppet command channel full/closed on assign");
                }
                tracing::info!(key, display_name, idx, "assigned participant to puppet");
                return self.sink(slot.mixer.clone(), key);
            }
        }

        // Pool exhausted → overflow into the shared listener mix.
        map.insert(key.to_owned(), Assign::Listener);
        tracing::info!(key, display_name, "puppet pool exhausted — overflow to listener mix");
        self.sink(self.listener_mixer.clone(), key)
    }

    fn set_muted(&self, key: &str, muted: bool) {
        let map = self.assignments.lock().unwrap_or_else(PoisonError::into_inner);
        // Only dedicated puppets can mirror mute; overflow speakers share a bot.
        if let Some(Assign::Puppet(idx)) = map.get(key) {
            let slot = &self.slots[*idx];
            if slot.cmd_tx.try_send(PuppetCommand::SetMuted(muted)).is_err() {
                tracing::warn!(idx = *idx, "puppet command channel full/closed on set_muted");
            }
        }
    }

    fn release(&self, key: &str) {
        let mut map = self.assignments.lock().unwrap_or_else(PoisonError::into_inner);
        match map.remove(key) {
            Some(Assign::Puppet(idx)) => {
                let slot = &self.slots[idx];
                slot.mixer.clear();
                slot.busy.store(false, Ordering::Relaxed);
                if slot.cmd_tx.try_send(PuppetCommand::Release).is_err() {
                    tracing::warn!(idx, "puppet command channel full/closed on release");
                }
                tracing::info!(key, idx, "released puppet");
            }
            Some(Assign::Listener) => {
                self.listener_mixer.remove(key);
                tracing::info!(key, "released overflow participant from listener mix");
            }
            None => {}
        }
    }

    fn release_all(&self) {
        let mut map = self.assignments.lock().unwrap_or_else(PoisonError::into_inner);
        for (_key, assign) in map.drain() {
            if let Assign::Puppet(idx) = assign {
                let slot = &self.slots[idx];
                slot.mixer.clear();
                slot.busy.store(false, Ordering::Relaxed);
                let _ = slot.cmd_tx.try_send(PuppetCommand::Release);
            }
        }
        self.listener_mixer.clear();
    }
}

// ─── Puppet serenity client ────────────────────────────────────────────────────

/// Per-puppet gateway handler.  On `ready` it captures a live [`Context`] and
/// spawns the command loop; it never auto-joins (joins are command-driven).
struct PuppetHandler {
    bridge: Arc<str>,
    idx: usize,
    guild_id: GuildId,
    channel_id: ChannelId,
    mixer: Arc<PlaybackMixer>,
    exclude: SharedExclude,
    nickname_prefix: Arc<str>,
    current_ssrc: Arc<StdMutex<Option<u32>>>,
    cmd_rx: StdMutex<Option<mpsc::Receiver<PuppetCommand>>>,
}

#[async_trait]
impl EventHandler for PuppetHandler {
    #[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %self.bridge))]
    async fn ready(&self, ctx: Context, ready: Ready) {
        tracing::info!(idx = self.idx, bot_user = %ready.user.name, "puppet connected");

        // `ready` can fire again on reconnect; only the first run takes the rx
        // and owns the command loop.
        let Some(rx) = self
            .cmd_rx
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        else {
            return;
        };

        tokio::spawn(puppet_command_loop(
            self.bridge.clone(),
            ctx,
            self.idx,
            self.guild_id,
            self.channel_id,
            self.mixer.clone(),
            self.exclude.clone(),
            self.nickname_prefix.clone(),
            self.current_ssrc.clone(),
            rx,
        ));
    }
}

/// Drain assign/release commands for one puppet, driving its voice connection.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %bridge))]
async fn puppet_command_loop(
    bridge: Arc<str>,
    ctx: Context,
    idx: usize,
    guild_id: GuildId,
    channel_id: ChannelId,
    mixer: Arc<PlaybackMixer>,
    exclude: SharedExclude,
    nickname_prefix: Arc<str>,
    current_ssrc: Arc<StdMutex<Option<u32>>>,
    mut cmd_rx: mpsc::Receiver<PuppetCommand>,
) {
    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            PuppetCommand::Assign { display_name } => {
                puppet_join(
                    &bridge,
                    &ctx,
                    idx,
                    guild_id,
                    channel_id,
                    &mixer,
                    &exclude,
                    &nickname_prefix,
                    &current_ssrc,
                    &display_name,
                )
                .await;
            }
            PuppetCommand::SetMuted(muted) => {
                puppet_set_muted(&ctx, idx, guild_id, muted).await;
            }
            PuppetCommand::Release => {
                puppet_leave(&ctx, idx, guild_id, &exclude, &current_ssrc).await;
            }
        }
    }
    tracing::info!(idx, "puppet command loop exited");
}

/// Self-mute/unmute this puppet's voice connection to mirror the Matrix
/// participant's mute state (shows as muted in the Discord client).
async fn puppet_set_muted(ctx: &Context, idx: usize, guild_id: GuildId, muted: bool) {
    let Some(manager) = songbird::get(ctx).await else {
        return;
    };
    if let Some(call_lock) = manager.get(guild_id) {
        let mut call = call_lock.lock().await;
        if let Err(e) = call.mute(muted).await {
            tracing::warn!(idx, error = %e, muted, "failed to set puppet mute state");
        } else {
            tracing::info!(idx, muted, "puppet mute state mirrored");
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn puppet_join(
    bridge: &Arc<str>,
    ctx: &Context,
    idx: usize,
    guild_id: GuildId,
    channel_id: ChannelId,
    mixer: &Arc<PlaybackMixer>,
    exclude: &SharedExclude,
    nickname_prefix: &str,
    current_ssrc: &Arc<StdMutex<Option<u32>>>,
    display_name: &str,
) {
    let Some(manager) = songbird::get(ctx).await else {
        tracing::error!(idx, "songbird not registered on puppet client");
        return;
    };

    // Register the SSRC-exclusion handler and start the playback source on the
    // Call *before* connecting. `DriverConnect` (which carries our SSRC) fires
    // *during* `manager.join().await` — registering after the await returns
    // misses it entirely, so the puppet's SSRC never lands in the exclude set
    // and the listener loops our playback back to Matrix as an echo. Events go
    // to the driver's persistent event task, so pre-registration is safe; the
    // Call from `get_or_insert` is the same one `join` connects.
    let call_lock = manager.get_or_insert(guild_id);
    {
        let mut call = call_lock.lock().await;
        call.add_global_event(
            CoreEvent::DriverConnect.into(),
            PuppetSsrcRegistrar {
                bridge: bridge.clone(),
                idx,
                exclude: exclude.clone(),
                current_ssrc: current_ssrc.clone(),
            },
        );
        // Play this puppet's single-participant mixer into the channel.
        let source = PlaybackSource::new(mixer.clone());
        let input = Input::from(RawAdapter::new(source, PLAYBACK_SAMPLE_RATE, PLAYBACK_CHANNELS));
        let _handle = call.play_input(input);
    }

    // Now connect. DriverConnect dispatches into the handler registered above.
    if let Err(e) = manager.join(guild_id, channel_id).await {
        tracing::error!(idx, error = %e, "puppet failed to join voice channel");
        return;
    }

    // Nickname = prefix + Matrix display name (needs CHANGE_NICKNAME perm),
    // truncated to Discord's 32-char cap.
    let nick = build_nickname(nickname_prefix, display_name);
    if let Err(e) = guild_id.edit_nickname(&ctx.http, Some(nick.as_str())).await {
        tracing::warn!(idx, error = %e, "failed to set puppet nickname");
    }
    tracing::info!(idx, display_name, "puppet joined voice and assigned");
}

async fn puppet_leave(
    ctx: &Context,
    idx: usize,
    guild_id: GuildId,
    exclude: &SharedExclude,
    current_ssrc: &Arc<StdMutex<Option<u32>>>,
) {
    // Reset the nickname so the bot doesn't keep a departed user's name.
    if let Err(e) = guild_id.edit_nickname(&ctx.http, None).await {
        tracing::warn!(idx, error = %e, "failed to reset puppet nickname");
    }
    // Leave voice — drops the Call and its playback source.
    if let Some(manager) = songbird::get(ctx).await {
        if let Err(e) = manager.remove(guild_id).await {
            tracing::warn!(idx, error = %e, "puppet failed to leave voice channel");
        }
    }
    // Stop excluding our (now-gone) SSRC so a recycled SSRC isn't wrongly muted.
    let ssrc = current_ssrc
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take();
    if let Some(s) = ssrc {
        exclude
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&s);
        tracing::info!(idx, ssrc = s, "puppet SSRC removed from echo-exclusion");
    }
    tracing::info!(idx, "puppet left voice and released");
}

/// Songbird handler that records a puppet's own SSRC into the echo-exclusion set
/// as soon as its voice driver connects.
struct PuppetSsrcRegistrar {
    bridge: Arc<str>,
    idx: usize,
    exclude: SharedExclude,
    current_ssrc: Arc<StdMutex<Option<u32>>>,
}

#[async_trait]
impl SongbirdEventHandler for PuppetSsrcRegistrar {
    #[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %self.bridge))]
    async fn act(&self, ctx: &EventContext<'_>) -> Option<Event> {
        if let EventContext::DriverConnect(c) = ctx {
            self.exclude
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(c.ssrc);
            *self
                .current_ssrc
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(c.ssrc);
            tracing::info!(idx = self.idx, ssrc = c.ssrc, "puppet SSRC registered for echo-exclusion");
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::build_nickname;

    #[test]
    fn nickname_prefix_applied() {
        assert_eq!(build_nickname("[Matrix] ", "Dark Collective"), "[Matrix] Dark Collective");
    }

    #[test]
    fn nickname_empty_prefix_is_just_name() {
        assert_eq!(build_nickname("", "Victor"), "Victor");
    }

    #[test]
    fn nickname_truncated_to_32_chars() {
        let nick = build_nickname("[Matrix] ", "A Very Long Display Name That Exceeds The Limit");
        assert_eq!(nick.chars().count(), 32);
        assert!(nick.starts_with("[Matrix] A Very Long"));
    }

    #[test]
    fn nickname_truncation_respects_char_boundaries() {
        // Multibyte name; must not panic and must stay within the cap.
        let nick = build_nickname("[Matrix] ", &"é".repeat(40));
        assert!(nick.chars().count() <= 32);
        assert!(nick.starts_with("[Matrix] "));
    }
}
