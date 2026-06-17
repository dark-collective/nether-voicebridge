//! Shared puppet pool — a process-wide pool of Discord bots, dynamically
//! allocated across all bridged channels.
//!
//! Each configured token is a generic "voice worker" bot that can join any
//! channel on demand. The Matrix side of each bridge drives a per-channel
//! [`ChannelView`] (a [`PlaybackRouter`]): `assign` on call membership (a bot
//! joins, takes the participant's display name), `release` on leave, `set_muted`
//! to mirror mute. Per active channel, one bot is the **anchor**: it captures
//! all Discord speakers (Discord→Matrix) and plays that channel's shared
//! **overflow** mix. Speakers beyond the bots available to a channel are mixed
//! into that overflow (no audio lost) and the anchor is relabeled.
//!
//! Allocation (single global lock, [`SharedPool::alloc`]):
//! - A channel's first speaker takes a free bot as the anchor. If none is free,
//!   a spare (non-anchor) bot is **stolen** from the channel with the most slack
//!   (its most-recently-assigned dedicated speaker is demoted to overflow); if no
//!   channel has slack, the channel waits until a bot frees (logged).
//! - Within an active channel, extra speakers take free bots (dedicated puppets)
//!   or fall to overflow; freed bots promote the oldest overflow speaker (FCFS).
//!
//! Echo prevention (constraint #5): each puppet registers its own SSRC into its
//! channel's [`SharedExclude`] set on `DriverConnect`, so the anchor — which
//! hears every speaker including the puppets' own playback — never re-captures it.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, PoisonError, RwLock};
use std::time::Duration;

use anyhow::{anyhow, Result};
use serenity::{async_trait, model::prelude::*, prelude::*};
use songbird::{
    driver::{DecodeConfig, DecodeMode},
    input::{Input, RawAdapter},
    CoreEvent, Event, EventContext, EventHandler as SongbirdEventHandler, SerenityInit,
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use nvb_audio::mixer::PlaybackMixer;
use nvb_audio::routing::{CaptureRouter, ParticipantSink, PlaybackRouter};
use nvb_config::PuppetsConfig;

use crate::{
    BotIdSet, CaptureSink, ConnectionLogger, PlaybackSource, SpeakingLogger, SsrcMap, UserSinkMap,
    VoiceReceiver, PLAYBACK_CHANNELS, PLAYBACK_SAMPLE_RATE,
};

/// Sentinel for "no anchor currently elected" in [`ChannelState::anchor`].
const NO_ANCHOR: usize = usize::MAX;

/// How long a bot stays quarantined (skipped by the allocator) after its
/// `manager.join` timed out, before a recovery timer makes it eligible again.
const QUARANTINE_COOLDOWN: Duration = Duration::from_secs(120);

/// How long a capture-only anchor (one held in voice with no Matrix participant of
/// its own, only Discord speakers remaining) lingers before it leaves the Discord
/// channel. Long enough to absorb a Matrix membership flap (which would otherwise
/// churn the songbird/SSRC session) but bounded so no nameless bot idles in voice
/// once the call is genuinely empty of Matrix users.
const ANCHOR_IDLE_GRACE: Duration = Duration::from_secs(90);

/// Nickname (before the configured prefix) shown on the overflow-host puppet while
/// it is carrying more than one Matrix speaker (members beyond the puppet count).
const OVERFLOW_LABEL: &str = "Overflow";

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

/// Shared, mutable set of bridge-owned SSRCs to exclude from capture. One per
/// channel (the anchor of that channel hears only that channel's bots). Reads
/// happen on the anchor's 50 Hz capture tick; writes only on a puppet join/leave.
pub type SharedExclude = Arc<RwLock<HashSet<u32>>>;

/// Everything a bot needs to operate in a given channel, bundled so a single
/// [`PuppetCommand::Assign`] hands the bot's command loop its full context. The
/// loop remembers the last `Assign`'s ctx and reuses it for subsequent commands
/// (`BecomeAnchor`, `SetMuted`, `Release`, …) and for shutdown — so those
/// commands carry no channel of their own.
#[derive(Clone)]
struct ChannelCtx {
    bridge: Arc<str>,
    guild_id: GuildId,
    channel_id: ChannelId,
    /// This channel's echo-exclusion SSRC set.
    exclude: SharedExclude,
    /// This channel's SSRC → Discord user id (anchor demux attribution).
    ssrc_map: SsrcMap,
    /// This channel's Discord user id → ghost sink (presence + demux target).
    user_sinks: UserSinkMap,
    /// This bridge's ghost router (Discord→Matrix); the anchor mints/retires ghosts.
    capture_router: Arc<dyn CaptureRouter>,
    /// Optional Phase-3 WAV capture (per bridge).
    capture_sink: Option<Arc<CaptureSink>>,
    /// Global puppet-bot-id set — never minted as ghosts.
    bot_ids: BotIdSet,
    /// This channel's anchor marker (slot index, or [`NO_ANCHOR`]). The anchor's
    /// `VoiceReceiver` only captures while it equals its own idx.
    anchor: Arc<AtomicUsize>,
    /// This channel's shared overflow mix — played by the anchor.
    overflow_mixer: Arc<PlaybackMixer>,
}

/// Commands sent from the (sync) allocator to a puppet's async command loop.
enum PuppetCommand {
    /// Join `ctx`'s channel, start playback from this bot's own mixer, set the
    /// nickname. The loop stores `ctx` as its current channel.
    Assign {
        ctx: Box<ChannelCtx>,
        display_name: String,
    },
    /// Become the capture **anchor** of the current channel: register the
    /// Discord→Matrix capture handler + overflow playback, and seed ghosts for
    /// members already present.
    BecomeAnchor,
    /// Mirror the Matrix participant's mute onto this bot's voice connection.
    SetMuted(bool),
    /// Set this bot's nickname (relabel the overflow host). Prefix applied here.
    SetNickname(String),
    /// Reset the nickname, leave voice, drop the SSRC exclusion. Clears current ctx.
    Release,
    /// Like [`Release`](PuppetCommand::Release), but fire `done` once the puppet
    /// has actually left voice. Used by graceful shutdown to **await** teardown.
    ReleaseAndAck { done: oneshot::Sender<()> },
}

/// Where a participant's audio is currently routed within a channel. `seq` is a
/// monotonic arrival order used for FCFS: promote the oldest overflow speaker,
/// demote the newest dedicated speaker when stealing a bot.
enum Assign {
    /// A dedicated puppet (index into [`SharedPool::slots`]).
    Puppet { idx: usize, seq: u64 },
    /// Overflow — mixed into the channel's overflow mix played by the anchor.
    Overflow { name: String, muted: bool, seq: u64 },
}

/// One puppet bot's shared state. `cmd_tx`/`cmd_rx` connect the sync allocator to
/// the bot's async serenity task; `mixer` is its single-participant playback
/// buffer (reused across channels); `current_ssrc` remembers the SSRC inserted
/// into its current channel's exclude set so it can be removed on release.
struct PuppetSlot {
    token: String,
    mixer: Arc<PlaybackMixer>,
    cmd_tx: mpsc::Sender<PuppetCommand>,
    /// Taken (once) by [`SharedPool::run`] to drive this puppet's client.
    cmd_rx: StdMutex<Option<mpsc::Receiver<PuppetCommand>>>,
    current_ssrc: Arc<StdMutex<Option<u32>>>,
    /// Current participant display name — used to restore the anchor's nickname
    /// after the overflow relabel, and to carry the name on a steal-demotion.
    name: StdMutex<Option<String>>,
    /// The participant's intended mute (mirrored from Matrix). Tracked even when
    /// not *applied* — the anchor stays unmuted while hosting overflow.
    muted: AtomicBool,
    /// True while allocated to some channel. Mutated only under the alloc lock.
    busy: AtomicBool,
    /// Guilds this bot is a member of (learned from `ready`/`guild_create`). The
    /// allocator only assigns/steals a bot for a channel whose guild is in here,
    /// so a partitioned pool (some bots per guild) works and a missing invite
    /// surfaces as a clear "no puppet" warning instead of a silent join failure.
    guilds: RwLock<HashSet<GuildId>>,
    /// Set true once this puppet's gateway `ready` fired and its command loop is
    /// live. The shutdown drain only awaits `ReleaseAndAck` from connected bots.
    connected: Arc<AtomicBool>,
    /// Set true when this bot's `manager.join` timed out (a stuck voice gateway —
    /// seen after hopping channels in one guild). The allocator skips an unhealthy
    /// bot so failover picks a different one; a recovery timer clears it after
    /// `QUARANTINE_COOLDOWN` so the bot becomes eligible again.
    unhealthy: Arc<AtomicBool>,
}

/// A swappable audio route: the mixer a participant's frames currently flow into.
/// The pool re-points this (under the alloc lock) on promotion (overflow → freed
/// slot) and demotion (slot → overflow, when stealing). The held [`RoutedSink`]
/// then pushes to the new mixer with no change on the Matrix side.
type Route = Arc<StdMutex<Arc<PlaybackMixer>>>;

/// Per-channel allocation + capture state. Lives inside [`AllocState`] behind the
/// single pool lock; the `Arc` fields are also shared with the bots' command
/// loops (via [`ChannelCtx`]) and songbird handlers.
struct ChannelState {
    bridge: Arc<str>,
    guild_id: GuildId,
    channel_id: ChannelId,
    overflow_mixer: Arc<PlaybackMixer>,
    exclude: SharedExclude,
    ssrc_map: SsrcMap,
    user_sinks: UserSinkMap,
    capture_router: Arc<dyn CaptureRouter>,
    capture_sink: Option<Arc<CaptureSink>>,
    anchor: Arc<AtomicUsize>,
    /// Participant key → routing. Source of truth for this channel's allocation.
    assignments: HashMap<String, Assign>,
    /// Participant key → swappable route handle (shared with the Matrix-side sink).
    routes: HashMap<String, Route>,
    /// Discord user ids currently present in this voice channel (humans only —
    /// bridge bots excluded). Maintained from gateway voice-state events on ANY
    /// bot, under the alloc lock; set semantics make duplicate events from the
    /// pool's several bots idempotent, so ghost create/retire fires exactly once.
    /// Decoupled from the anchor: presence (hence ghost visibility) does not need
    /// a bot in the voice channel.
    presence: HashSet<u64>,
    /// Pending grace timer for a capture-only anchor (held with no Matrix
    /// participant). Cancelled when a participant is (re)assigned within the grace;
    /// fires `release_capture_only_anchor_if_idle` so the bot leaves voice if still
    /// idle. `Some` only while such a timer is armed.
    anchor_idle_release: Option<CancellationToken>,
}

impl ChannelState {
    /// Count overflow participants (routed to the shared mix, no dedicated puppet).
    fn overflow_count(&self) -> usize {
        self.assignments
            .values()
            .filter(|a| matches!(a, Assign::Overflow { .. }))
            .count()
    }

    /// Number of dedicated puppets currently serving this channel.
    fn puppet_count(&self) -> usize {
        self.assignments
            .values()
            .filter(|a| matches!(a, Assign::Puppet { .. }))
            .count()
    }
}

/// The global allocation state, guarded by [`SharedPool::alloc`].
struct AllocState {
    /// Channel key (bridge label) → its state.
    channels: HashMap<String, ChannelState>,
    /// Monotonic arrival counter for FCFS ordering.
    next_seq: u64,
}

/// The shared puppet pool. Built once per process; every bridge's Discord side is
/// a [`ChannelView`] into it.
pub struct SharedPool {
    /// Prefix for puppet nicknames (e.g. `"[Matrix] "`; empty disables).
    nickname_prefix: Arc<str>,
    slots: Vec<PuppetSlot>,
    /// All puppet bot user ids — never minted as ghosts (global; a bot is a bot
    /// in every channel).
    bot_ids: BotIdSet,
    alloc: StdMutex<AllocState>,
}

impl SharedPool {
    /// Build the pool from the shared `[puppets]` config. Cheap and synchronous —
    /// clients are not started until [`run`](Self::run), so the returned `Arc` and
    /// its [`ChannelView`]s can be handed to the Matrix side up front.
    pub fn new(cfg: &PuppetsConfig) -> Arc<Self> {
        let slots = cfg
            .tokens
            .iter()
            .map(|tok| {
                let (cmd_tx, cmd_rx) = mpsc::channel(8);
                PuppetSlot {
                    token: tok.clone(),
                    mixer: Arc::new(PlaybackMixer::new()),
                    cmd_tx,
                    cmd_rx: StdMutex::new(Some(cmd_rx)),
                    current_ssrc: Arc::new(StdMutex::new(None)),
                    name: StdMutex::new(None),
                    muted: AtomicBool::new(false),
                    busy: AtomicBool::new(false),
                    guilds: RwLock::new(HashSet::new()),
                    connected: Arc::new(AtomicBool::new(false)),
                    unhealthy: Arc::new(AtomicBool::new(false)),
                }
            })
            .collect();

        // Pre-populate bot_ids by decoding each puppet's user id from its token, so
        // echo-exclusion is correct BEFORE any gateway event. Otherwise a puppet
        // lingering in a voice channel (e.g. after an ungraceful crash) could be
        // ghosted by another bot's startup seeding before its own `ready` registers
        // it — an echo (constraint #5). Best-effort; `ready` re-confirms each id.
        let mut bot_ids = HashSet::new();
        for tok in &cfg.tokens {
            match decode_bot_user_id(tok) {
                Some(id) => {
                    bot_ids.insert(id);
                }
                None => tracing::warn!("could not decode a puppet bot user id from its token — relying on `ready` (a brief startup echo window is possible)"),
            }
        }

        Arc::new(Self {
            nickname_prefix: Arc::from(cfg.nickname_prefix.as_str()),
            slots,
            bot_ids: Arc::new(RwLock::new(bot_ids)),
            alloc: StdMutex::new(AllocState {
                channels: HashMap::new(),
                next_seq: 0,
            }),
        })
    }

    /// Register a bridge's channel and return its [`PlaybackRouter`] view. Parses
    /// the guild/channel ids and inserts a fresh [`ChannelState`]. The bridge label
    /// is the channel key (config validation keeps store_path unique; labels fall
    /// back to `bridge-{idx}`, so they are effectively unique).
    pub fn register_channel(
        self: &Arc<Self>,
        bridge: Arc<str>,
        guild_id: &str,
        channel_id: &str,
        capture_router: Arc<dyn CaptureRouter>,
        capture_sink: Option<Arc<CaptureSink>>,
    ) -> Result<Arc<ChannelView>> {
        let guild_id = GuildId::new(
            guild_id
                .parse()
                .map_err(|_| anyhow!("invalid guild_id: {guild_id}"))?,
        );
        let channel_id = ChannelId::new(
            channel_id
                .parse()
                .map_err(|_| anyhow!("invalid channel_id: {channel_id}"))?,
        );
        let state = ChannelState {
            bridge: bridge.clone(),
            guild_id,
            channel_id,
            overflow_mixer: Arc::new(PlaybackMixer::new()),
            exclude: Arc::new(RwLock::new(HashSet::new())),
            ssrc_map: Arc::new(RwLock::new(HashMap::new())),
            user_sinks: Arc::new(RwLock::new(HashMap::new())),
            capture_router,
            capture_sink,
            anchor: Arc::new(AtomicUsize::new(NO_ANCHOR)),
            assignments: HashMap::new(),
            routes: HashMap::new(),
            presence: HashSet::new(),
            anchor_idle_release: None,
        };
        let mut guard = self.alloc.lock().unwrap_or_else(PoisonError::into_inner);
        guard.channels.insert(bridge.to_string(), state);
        Ok(Arc::new(ChannelView {
            pool: self.clone(),
            key: bridge,
        }))
    }

    /// Start every puppet's serenity client and drive them to completion. Returns
    /// on the first client error. Called once from `main`.
    pub async fn run(self: &Arc<Self>, shutdown: CancellationToken) -> Result<()> {
        if self.slots.is_empty() {
            tracing::warn!("no puppet tokens configured — Discord side has no bots");
            return Ok(());
        }

        let intents = GatewayIntents::GUILDS | GatewayIntents::GUILD_VOICE_STATES;
        let mut futs = Vec::with_capacity(self.slots.len());

        for (idx, slot) in self.slots.iter().enumerate() {
            let handler = PuppetHandler {
                pool: self.clone(),
                idx,
            };
            let token = slot.token.clone();
            futs.push(async move {
                // Puppets both PLAY their participant and (when anchor) CAPTURE
                // Discord speakers, so they need Opus decode / DAVE-receive.
                let sb_config = songbird::Config::default()
                    .decode_mode(DecodeMode::Decode(DecodeConfig::default()));
                let mut client = Client::builder(&token, intents)
                    .event_handler(handler)
                    .register_songbird_from_config(sb_config)
                    .await
                    .map_err(|e| anyhow!("failed to build puppet {idx} client: {e}"))?;
                tracing::info!(idx, "starting puppet gateway");
                client
                    .start()
                    .await
                    .map_err(|e| anyhow!("puppet {idx} client error: {e}"))
            });
        }

        let clients = futures_util::future::try_join_all(futs);
        tokio::pin!(clients);

        tokio::select! {
            r = &mut clients => return r.map(|_| ()),
            _ = shutdown.cancelled() => {}
        }

        tracing::info!("Discord shutdown — draining puppets (leave voice, reset nicknames)");
        tokio::select! {
            _ = self.drain() => tracing::info!("puppets drained"),
            r = &mut clients => {
                if let Err(e) = r {
                    tracing::warn!(error = %e, "a puppet client exited during drain");
                }
            }
        }
        Ok(())
    }

    /// Graceful teardown: ask every connected puppet to leave voice + reset its
    /// nickname, and **await** confirmation. Driven off `connected` (not the
    /// assignment maps): on shutdown the Matrix side's per-track teardown fires
    /// its own fire-and-forget `release`s, which the FIFO command channel orders
    /// ahead of this awaited `ReleaseAndAck`.
    async fn drain(&self) {
        let mut acks = Vec::new();
        for slot in &self.slots {
            if !slot.connected.load(Ordering::Relaxed) {
                continue;
            }
            let (tx, rx) = oneshot::channel();
            if slot
                .cmd_tx
                .try_send(PuppetCommand::ReleaseAndAck { done: tx })
                .is_ok()
            {
                acks.push(rx);
            }
        }
        futures_util::future::join_all(acks).await;
    }

    /// Build the per-channel command context (clones the channel's `Arc` handles
    /// plus the global bot-id set).
    fn ctx_for(&self, ch: &ChannelState) -> ChannelCtx {
        ChannelCtx {
            bridge: ch.bridge.clone(),
            guild_id: ch.guild_id,
            channel_id: ch.channel_id,
            exclude: ch.exclude.clone(),
            ssrc_map: ch.ssrc_map.clone(),
            user_sinks: ch.user_sinks.clone(),
            capture_router: ch.capture_router.clone(),
            capture_sink: ch.capture_sink.clone(),
            bot_ids: self.bot_ids.clone(),
            anchor: ch.anchor.clone(),
            overflow_mixer: ch.overflow_mixer.clone(),
        }
    }

    /// Look up the bridged channel at `guild_id`/`channel_id` (regardless of which
    /// bot, if any, anchors it), returning its [`ChannelCtx`]. Used to attribute a
    /// voice member to its room without holding the anchor.
    fn by_channel(&self, guild_id: GuildId, channel_id: ChannelId) -> Option<ChannelCtx> {
        let guard = self.alloc.lock().unwrap_or_else(PoisonError::into_inner);
        let ch = guard
            .channels
            .values()
            .find(|ch| ch.guild_id == guild_id && ch.channel_id == channel_id)?;
        Some(self.ctx_for(ch))
    }

    /// Every bridged channel in `guild_id`, as [`ChannelCtx`]es — regardless of
    /// which bot (if any) anchors them. Lets ANY bot reconcile a voice-state event
    /// against all the guild's channels, so presence detection is not gated on
    /// holding the anchor.
    fn channels_in_guild(&self, guild_id: GuildId) -> Vec<ChannelCtx> {
        let guard = self.alloc.lock().unwrap_or_else(PoisonError::into_inner);
        guard
            .channels
            .values()
            .filter(|ch| ch.guild_id == guild_id)
            .map(|ch| self.ctx_for(ch))
            .collect()
    }

    /// Whether `user_id` is one of our own puppet bots (echo-exclusion: a bridge
    /// bot is never a ghost and never counts as Discord presence).
    fn is_bridge_bot(&self, user_id: u64) -> bool {
        self.bot_ids
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&user_id)
    }

    /// Retract any ghost wrongly minted for one of our OWN puppet bots. A puppet is
    /// never a Discord speaker, but a startup race can mint one: if the bot was
    /// already in a voice channel (e.g. a session lingering from a previous crash)
    /// when another bot's `guild_create` seeding scanned voice members, this bot's
    /// id was not yet in `bot_ids`, so it slipped past echo-exclusion (constraint
    /// #5). Called from `ready` the moment the id is recorded, so the echo
    /// self-corrects — typically before the ghost ever publishes or posts
    /// membership. Mirrors `drop_ghost`'s teardown.
    fn retract_self_ghost(&self, user_id: u64) {
        let mut guard = self.alloc.lock().unwrap_or_else(PoisonError::into_inner);
        for ch in guard.channels.values_mut() {
            ch.presence.remove(&user_id);
            let removed = ch
                .user_sinks
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&user_id)
                .is_some();
            if removed {
                ch.capture_router.release(&user_id.to_string());
                tracing::warn!(user_id, bridge = %ch.bridge, "retracted echo ghost minted for our own puppet bot before its ready (startup race)");
            }
        }
    }

    /// Record that `user_id` is now `present` (or gone) in `bridge`'s channel,
    /// returning `true` iff this call observed the **edge** — the user was newly
    /// inserted (0→1) or actually removed (1→0). Voice-state events reach every bot
    /// in the guild; the shared `presence` set under the alloc lock makes duplicates
    /// idempotent, so exactly one observer drives the ghost create/retire.
    fn note_presence(&self, bridge: &str, user_id: u64, present: bool) -> bool {
        let mut guard = self.alloc.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(ch) = guard.channels.get_mut(bridge) else {
            return false;
        };
        if present {
            ch.presence.insert(user_id)
        } else {
            ch.presence.remove(&user_id)
        }
    }

    // ── Allocation (all under `self.alloc`) ──────────────────────────────────

    /// Claim the first idle bot that is a member of `guild`, marking it busy.
    /// `None` if no capable bot is free. Called only under the alloc lock, so the
    /// load-then-store on `busy` is race-free.
    fn take_free_bot(&self, guild: GuildId) -> Option<usize> {
        for (i, slot) in self.slots.iter().enumerate() {
            if slot.busy.load(Ordering::Relaxed) {
                continue;
            }
            // Skip bots quarantined after a join timeout — picking one of those
            // would just fail to join again. A recovery timer clears the flag.
            if slot.unhealthy.load(Ordering::Relaxed) {
                continue;
            }
            if !slot
                .guilds
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .contains(&guild)
            {
                continue;
            }
            slot.busy.store(true, Ordering::Relaxed);
            return Some(i);
        }
        None
    }

    /// Whether bot `idx` is a member of `guild`.
    fn bot_in_guild(&self, idx: usize, guild: GuildId) -> bool {
        self.slots[idx]
            .guilds
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&guild)
    }

    /// Get or create the route handle for `key`, re-pointing it at `mixer`.
    fn route_to(routes: &mut HashMap<String, Route>, key: &str, mixer: Arc<PlaybackMixer>) -> Route {
        match routes.get(key) {
            Some(r) => {
                *r.lock().unwrap_or_else(PoisonError::into_inner) = mixer;
                r.clone()
            }
            None => {
                let r: Route = Arc::new(StdMutex::new(mixer));
                routes.insert(key.to_owned(), r.clone());
                r
            }
        }
    }

    /// Set the anchor's nickname to reflect overflow state: the overflow label
    /// while it hosts ≥1 overflow speaker, else its own participant's name.
    fn reconcile_anchor_label(&self, ch: &ChannelState) {
        let anchor = ch.anchor.load(Ordering::Relaxed);
        if anchor == NO_ANCHOR {
            return;
        }
        let name = if ch.overflow_count() > 0 {
            OVERFLOW_LABEL.to_string()
        } else {
            self.slots[anchor]
                .name
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
                .unwrap_or_default()
        };
        let _ = self.slots[anchor]
            .cmd_tx
            .try_send(PuppetCommand::SetNickname(name));
    }

    /// Apply the anchor's mute: forced **unmuted** while it hosts the overflow mix
    /// (so muting its own participant never silences the others), else its
    /// participant's intended mute.
    fn reconcile_anchor_mute(&self, ch: &ChannelState) {
        let anchor = ch.anchor.load(Ordering::Relaxed);
        if anchor == NO_ANCHOR {
            return;
        }
        let applied =
            ch.overflow_count() == 0 && self.slots[anchor].muted.load(Ordering::Relaxed);
        let _ = self.slots[anchor]
            .cmd_tx
            .try_send(PuppetCommand::SetMuted(applied));
    }

    /// Steal a spare bot for **activation** of a channel in `target_guild`: find
    /// the channel with the most slack (most dedicated puppets) that has a
    /// non-anchor dedicated bot **also a member of `target_guild`**, demote that
    /// bot's (most-recently-assigned) speaker into the donor's overflow, and
    /// return the freed bot (still marked busy — the caller repurposes it). The
    /// bot is sent a `Release` so its loop leaves the donor channel first.
    /// `None` if no donor has a guild-capable spare bot.
    fn steal_for_activation(
        &self,
        guard: &mut AllocState,
        exclude_key: &str,
        target_guild: GuildId,
    ) -> Option<usize> {
        // For each candidate donor, the newest non-anchor dedicated speaker whose
        // bot can join the target guild. Pick the donor with the most slack.
        let pick = guard
            .channels
            .iter()
            .filter(|(k, ch)| k.as_str() != exclude_key && ch.puppet_count() > 1)
            .filter_map(|(k, ch)| {
                let donor_anchor = ch.anchor.load(Ordering::Relaxed);
                let victim = ch
                    .assignments
                    .iter()
                    .filter_map(|(vk, a)| match a {
                        Assign::Puppet { idx, seq }
                            if *idx != donor_anchor && self.bot_in_guild(*idx, target_guild) =>
                        {
                            Some((vk.clone(), *idx, *seq))
                        }
                        _ => None,
                    })
                    .max_by_key(|(_, _, seq)| *seq)?;
                Some((k.clone(), ch.puppet_count(), victim))
            })
            .max_by_key(|(_, slack, _)| *slack);

        let (donor_key, _slack, (vkey, vidx, vseq)) = pick?;
        let donor = guard.channels.get_mut(&donor_key)?;

        // Demote the victim into the donor's overflow (route-swap = promotion in
        // reverse). The Matrix side keeps pushing to the same RoutedSink.
        let name = self.slots[vidx]
            .name
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .unwrap_or_default();
        let muted = self.slots[vidx].muted.load(Ordering::Relaxed);
        donor
            .assignments
            .insert(vkey.clone(), Assign::Overflow { name, muted, seq: vseq });
        Self::route_to(&mut donor.routes, &vkey, donor.overflow_mixer.clone());
        self.slots[vidx].mixer.clear();
        *self.slots[vidx].name.lock().unwrap_or_else(PoisonError::into_inner) = None;

        // The donor anchor now hosts ≥1 overflow speaker → relabel + unmute it.
        let donor_ref = &guard.channels[&donor_key];
        self.reconcile_anchor_label(donor_ref);
        self.reconcile_anchor_mute(donor_ref);

        // Send the bot away from the donor; the caller will Assign it elsewhere.
        let _ = self.slots[vidx].cmd_tx.try_send(PuppetCommand::Release);
        tracing::info!(
            donor = %donor_key,
            for_channel = exclude_key,
            idx = vidx,
            demoted = %vkey,
            "stole bot from donor to activate channel"
        );
        Some(vidx)
    }

    /// After a bot frees, activate any channel that has waiting speakers but no
    /// anchor (was left pending because the pool was exhausted): give it the bot
    /// as its anchor, promoting the oldest waiting speaker.
    fn service_pending(&self, guard: &mut AllocState) {
        // Each pending channel needs a free bot *in its own guild*; try each once
        // (activating one channel consumes one bot but creates no new pending).
        let pending: Vec<(String, GuildId)> = guard
            .channels
            .iter()
            .filter(|(_, ch)| {
                ch.anchor.load(Ordering::Relaxed) == NO_ANCHOR && !ch.assignments.is_empty()
            })
            .map(|(k, ch)| (k.clone(), ch.guild_id))
            .collect();
        for (key, guild) in pending {
            if let Some(idx) = self.take_free_bot(guild) {
                self.activate_channel_with(guard, &key, idx);
            }
        }
    }

    /// After a bot frees, upgrade overflow speakers to dedicated puppets across ALL
    /// already-anchored channels — not just one whose own slot freed. A steal demotes
    /// a donor's speaker into overflow without ever freeing a slot *in that channel*,
    /// so without this cross-channel sweep the demoted speaker stays stranded in the
    /// mix even after the stealing channel returns the bot to the pool. FCFS: promote
    /// the globally-oldest waiting overflow speaker first; stop when no guild-capable
    /// free bot remains. Each iteration consumes a free bot or marks a guild
    /// exhausted, so it terminates.
    fn promote_overflow_into_free_bots(&self, guard: &mut AllocState) {
        let mut exhausted: HashSet<GuildId> = HashSet::new();
        loop {
            // The anchored channel (guild not yet exhausted) whose oldest overflow
            // speaker has the lowest seq — global FCFS across channels.
            let pick = guard
                .channels
                .iter()
                .filter(|(_, ch)| {
                    ch.anchor.load(Ordering::Relaxed) != NO_ANCHOR
                        && !exhausted.contains(&ch.guild_id)
                })
                .filter_map(|(k, ch)| {
                    ch.assignments
                        .values()
                        .filter_map(|a| match a {
                            Assign::Overflow { seq, .. } => Some(*seq),
                            _ => None,
                        })
                        .min()
                        .map(|seq| (k.clone(), ch.guild_id, seq))
                })
                .min_by_key(|(_, _, seq)| *seq);
            let Some((key, guild, _)) = pick else { break };
            let Some(idx) = self.take_free_bot(guild) else {
                exhausted.insert(guild);
                continue;
            };
            // Promote that channel's oldest overflow speaker into the freed bot as a
            // dedicated (non-anchor) puppet — mirrors the free-bot path in `assign`.
            let promoted = {
                let Some(ch) = guard.channels.get_mut(&key) else {
                    self.slots[idx].busy.store(false, Ordering::Relaxed);
                    continue;
                };
                let promote = ch
                    .assignments
                    .iter()
                    .filter_map(|(k, a)| match a {
                        Assign::Overflow { name, muted, seq } => {
                            Some((k.clone(), name.clone(), *muted, *seq))
                        }
                        _ => None,
                    })
                    .min_by_key(|(_, _, _, seq)| *seq);
                let Some((pkey, pname, pmuted, pseq)) = promote else {
                    // Raced away (speaker left); return the bot and re-evaluate.
                    self.slots[idx].busy.store(false, Ordering::Relaxed);
                    continue;
                };
                ch.overflow_mixer.remove(&pkey);
                ch.assignments
                    .insert(pkey.clone(), Assign::Puppet { idx, seq: pseq });
                self.slots[idx].mixer.clear();
                Self::route_to(&mut ch.routes, &pkey, self.slots[idx].mixer.clone());
                *self.slots[idx].name.lock().unwrap_or_else(PoisonError::into_inner) =
                    Some(pname.clone());
                self.slots[idx].muted.store(pmuted, Ordering::Relaxed);
                tracing::info!(channel = %key, promoted = %pkey, idx, "promoted overflow speaker into freed pool bot (cross-channel)");
                (self.ctx_for(ch), pname, pmuted)
            };
            let (ctx, name, pmuted) = promoted;
            let _ = self.slots[idx].cmd_tx.try_send(PuppetCommand::Assign {
                ctx: Box::new(ctx),
                display_name: name,
            });
            // A fresh Assign joins unmuted; restore the speaker's mute if it was
            // muted while demoted (the join applies the nickname, not the mute).
            if pmuted {
                let _ = self.slots[idx].cmd_tx.try_send(PuppetCommand::SetMuted(true));
            }
            // Overflow count dropped → the anchor may no longer host overflow.
            let ch = &guard.channels[&key];
            self.reconcile_anchor_label(ch);
            self.reconcile_anchor_mute(ch);
        }
    }

    /// Re-home freed pool capacity after a bot returns to the pool: first give
    /// anchor-less channels a bot (a fully-unbridged channel recovers before we
    /// upgrade an already-working one), then promote overflow speakers into any
    /// remaining free bots. Called wherever a bot frees.
    fn rebalance(&self, guard: &mut AllocState) {
        self.service_pending(guard);
        self.promote_overflow_into_free_bots(guard);
    }

    /// Make bot `idx` the anchor of channel `key`, promoting that channel's oldest
    /// waiting (overflow) speaker into it. Sends the bot Assign + BecomeAnchor.
    fn activate_channel_with(&self, guard: &mut AllocState, key: &str, idx: usize) {
        let ctx = {
            let ch = match guard.channels.get_mut(key) {
                Some(c) => c,
                None => return,
            };
            // Oldest waiting speaker (lowest seq) becomes the anchor's participant.
            let promote = ch
                .assignments
                .iter()
                .filter_map(|(k, a)| match a {
                    Assign::Overflow { name, muted, seq } => {
                        Some((k.clone(), name.clone(), *muted, *seq))
                    }
                    _ => None,
                })
                .min_by_key(|(_, _, _, seq)| *seq);
            let Some((pkey, pname, pmuted, pseq)) = promote else {
                // No waiting speaker after all — return the bot to the pool.
                self.slots[idx].busy.store(false, Ordering::Relaxed);
                return;
            };
            ch.overflow_mixer.remove(&pkey);
            self.slots[idx].mixer.clear();
            Self::route_to(&mut ch.routes, &pkey, self.slots[idx].mixer.clone());
            ch.assignments
                .insert(pkey.clone(), Assign::Puppet { idx, seq: pseq });
            *self.slots[idx].name.lock().unwrap_or_else(PoisonError::into_inner) = Some(pname);
            self.slots[idx].muted.store(pmuted, Ordering::Relaxed);
            ch.anchor.store(idx, Ordering::Relaxed);
            self.ctx_for(ch)
        };
        let name = self.slots[idx]
            .name
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .unwrap_or_default();
        let _ = self.slots[idx].cmd_tx.try_send(PuppetCommand::Assign {
            ctx: Box::new(ctx),
            display_name: name,
        });
        // Anchor activation is deferred to `on_join_succeeded` (sent once the bot's
        // `manager.join` actually completes), so a join timeout can fail over
        // instead of falsely reporting the anchor live.
        // The promoted speaker is the anchor's own participant now; remaining
        // overflow (if any) is reconciled by the caller's normal path.
        let ch = &guard.channels[key];
        self.reconcile_anchor_label(ch);
        self.reconcile_anchor_mute(ch);
        tracing::info!(channel = key, idx, "channel activated — anchor assigned");
    }

    fn assign(&self, bridge: &str, key: &str, display_name: &str) -> Arc<dyn ParticipantSink> {
        let mut guard = self.alloc.lock().unwrap_or_else(PoisonError::into_inner);

        // Idempotent: a re-subscribe for the same participant reuses its route.
        if let Some(ch) = guard.channels.get(bridge) {
            if ch.assignments.contains_key(key) {
                if let Some(r) = ch.routes.get(key) {
                    return Arc::new(RoutedSink { route: r.clone(), key: key.to_owned() });
                }
            }
        }

        // The channel's guild scopes which bots can serve it.
        let guild = match guard.channels.get(bridge) {
            Some(ch) => ch.guild_id,
            None => return Arc::new(NullSink),
        };

        let seq = {
            guard.next_seq += 1;
            guard.next_seq
        };

        // A Matrix participant is (re)joining → cancel any pending idle-release grace
        // on this channel's capture-only anchor.
        Self::cancel_anchor_idle_release(&mut guard, bridge);

        // (Fix 2c) Reuse a capture-only anchor — one held in voice with no
        // participant of its own — for this participant instead of taking a fresh
        // bot. It's already in voice, so relabel + re-point the route; don't send a
        // fresh Assign (which would re-join and churn its SSRC map). Keeps it named
        // and avoids a nameless "[Matrix] " bot beside a freshly-allocated puppet.
        let reuse_anchor = guard.channels.get(bridge).and_then(|ch| {
            let a = ch.anchor.load(Ordering::Relaxed);
            let serves = ch
                .assignments
                .values()
                .any(|x| matches!(x, Assign::Puppet { idx, .. } if *idx == a));
            (a != NO_ANCHOR && !serves).then_some(a)
        });
        if let Some(idx) = reuse_anchor {
            let route = {
                let ch = guard.channels.get_mut(bridge).unwrap();
                ch.assignments
                    .insert(key.to_owned(), Assign::Puppet { idx, seq });
                *self.slots[idx].name.lock().unwrap_or_else(PoisonError::into_inner) =
                    Some(display_name.to_owned());
                self.slots[idx].muted.store(false, Ordering::Relaxed);
                self.slots[idx].mixer.clear();
                Self::route_to(&mut ch.routes, key, self.slots[idx].mixer.clone())
            };
            let ch = &guard.channels[bridge];
            self.reconcile_anchor_label(ch);
            self.reconcile_anchor_mute(ch);
            tracing::info!(bridge, key, display_name, idx, "assigned participant to capture-only anchor (reused, no new bot)");
            return Arc::new(RoutedSink { route, key: key.to_owned() });
        }

        // Try to claim a free bot (in this guild) for a dedicated puppet.
        if let Some(idx) = self.take_free_bot(guild) {
            let ctx = {
                let Some(ch) = guard.channels.get_mut(bridge) else {
                    self.slots[idx].busy.store(false, Ordering::Relaxed);
                    return Arc::new(NullSink);
                };
                ch.assignments
                    .insert(key.to_owned(), Assign::Puppet { idx, seq });
                *self.slots[idx].name.lock().unwrap_or_else(PoisonError::into_inner) =
                    Some(display_name.to_owned());
                self.slots[idx].muted.store(false, Ordering::Relaxed);
                self.slots[idx].mixer.clear();
                Self::route_to(&mut ch.routes, key, self.slots[idx].mixer.clone());
                let elect = ch.anchor.load(Ordering::Relaxed) == NO_ANCHOR;
                if elect {
                    ch.anchor.store(idx, Ordering::Relaxed);
                }
                let ctx = self.ctx_for(ch);
                (ctx, elect)
            };
            let (ctx, elect) = ctx;
            let route = guard.channels[bridge].routes[key].clone();
            let _ = self.slots[idx].cmd_tx.try_send(PuppetCommand::Assign {
                ctx: Box::new(ctx),
                display_name: display_name.to_owned(),
            });
            tracing::info!(bridge, key, display_name, idx, "assigned participant to puppet");
            if elect {
                // Elected, but `BecomeAnchor` is deferred to `on_join_succeeded` so
                // the anchor only goes live after the join actually completes.
                tracing::info!(bridge, idx, "puppet elected capture anchor (activates after join)");
            }
            return Arc::new(RoutedSink { route, key: key.to_owned() });
        }

        // Pool exhausted. If this channel has no anchor yet, it MUST get a bot to
        // function — try to steal one. Otherwise the speaker overflows.
        let has_anchor = guard
            .channels
            .get(bridge)
            .map(|ch| ch.anchor.load(Ordering::Relaxed) != NO_ANCHOR)
            .unwrap_or(false);

        if !has_anchor {
            if let Some(idx) = self.steal_for_activation(&mut guard, bridge, guild) {
                // Bot freed from a donor (Release already sent). Assign it here as
                // the anchor for this participant.
                let ctx = {
                    let Some(ch) = guard.channels.get_mut(bridge) else {
                        self.slots[idx].busy.store(false, Ordering::Relaxed);
                        return Arc::new(NullSink);
                    };
                    ch.assignments
                        .insert(key.to_owned(), Assign::Puppet { idx, seq });
                    *self.slots[idx].name.lock().unwrap_or_else(PoisonError::into_inner) =
                        Some(display_name.to_owned());
                    self.slots[idx].muted.store(false, Ordering::Relaxed);
                    self.slots[idx].mixer.clear();
                    Self::route_to(&mut ch.routes, key, self.slots[idx].mixer.clone());
                    ch.anchor.store(idx, Ordering::Relaxed);
                    self.ctx_for(ch)
                };
                let route = guard.channels[bridge].routes[key].clone();
                let _ = self.slots[idx].cmd_tx.try_send(PuppetCommand::Assign {
                    ctx: Box::new(ctx),
                    display_name: display_name.to_owned(),
                });
                // `BecomeAnchor` deferred to `on_join_succeeded` (see assign above).
                tracing::info!(bridge, key, idx, "stole bot to activate channel anchor");
                return Arc::new(RoutedSink { route, key: key.to_owned() });
            }
            // No guild-capable spare bot anywhere — the channel can't be bridged
            // yet. Record the speaker as overflow (no audio plays until a bot in
            // this guild frees up; `service_pending` will then activate it).
            tracing::warn!(
                bridge,
                key,
                guild = %guild,
                "no puppet available for this guild (pool exhausted / no bot in guild) — \
                 channel waiting for a free bot (Matrix audio not yet playing)"
            );
        }

        // Overflow into this channel's overflow mix (played by its anchor when it
        // has one). On the first overflow speaker of an anchored channel, relabel
        // the anchor and force it unmuted.
        let Some(ch) = guard.channels.get_mut(bridge) else {
            return Arc::new(NullSink);
        };
        let was_overflowing = ch.overflow_count() > 0;
        ch.assignments.insert(
            key.to_owned(),
            Assign::Overflow { name: display_name.to_owned(), muted: false, seq },
        );
        let route = Self::route_to(&mut ch.routes, key, ch.overflow_mixer.clone());
        tracing::info!(bridge, key, display_name, "overflow to anchor mix");
        if !was_overflowing {
            let ch = &guard.channels[bridge];
            self.reconcile_anchor_label(ch);
            self.reconcile_anchor_mute(ch);
        }
        Arc::new(RoutedSink { route, key: key.to_owned() })
    }

    /// Called by a puppet's command loop after `manager.join` SUCCEEDS. If this bot
    /// is the elected anchor of the channel it now serves, activate capture — this
    /// is deferred from the assign paths so a timed-out join never falsely reports
    /// the anchor live.
    fn on_join_succeeded(&self, idx: usize) {
        let guard = self.alloc.lock().unwrap_or_else(PoisonError::into_inner);
        for ch in guard.channels.values() {
            let serves = ch
                .assignments
                .values()
                .any(|a| matches!(a, Assign::Puppet { idx: i, .. } if *i == idx));
            if serves && ch.anchor.load(Ordering::Relaxed) == idx {
                let _ = self.slots[idx].cmd_tx.try_send(PuppetCommand::BecomeAnchor);
                tracing::info!(bridge = %ch.bridge, idx, "puppet joined — activating capture anchor");
                return;
            }
        }
    }

    /// Called by a puppet's command loop after `manager.join` FAILS (e.g. the voice
    /// gateway timed out after a channel hop). Quarantine the stuck bot so the
    /// allocator skips it, then fail the participant over to a healthy free bot in
    /// the same guild — re-pointing its EXISTING route so the Matrix side follows
    /// with no re-subscribe, and re-electing the anchor if the failed bot was it.
    /// If no healthy bot is free, the participant drops to overflow and the channel
    /// degrades to no-anchor with a loud error — an honest state, not a silent
    /// fake-live anchor. The caller schedules a recovery timer (`recover_bot`).
    fn on_join_failed(&self, idx: usize) {
        let mut guard = self.alloc.lock().unwrap_or_else(PoisonError::into_inner);

        // Quarantine the stuck bot regardless (cleared after QUARANTINE_COOLDOWN).
        self.slots[idx].unhealthy.store(true, Ordering::Relaxed);

        // Locate the (bridge, key) this bot was serving.
        let found = guard.channels.iter().find_map(|(bk, ch)| {
            ch.assignments.iter().find_map(|(k, a)| match a {
                Assign::Puppet { idx: i, .. } if *i == idx => Some((bk.clone(), k.clone())),
                _ => None,
            })
        });
        let Some((bridge, key)) = found else {
            // Participant already left during the join — just free the bot.
            self.slots[idx].mixer.clear();
            self.slots[idx].busy.store(false, Ordering::Relaxed);
            *self.slots[idx].name.lock().unwrap_or_else(PoisonError::into_inner) = None;
            tracing::warn!(idx, "puppet join failed; assignment already gone — quarantined");
            return;
        };

        let display_name = self.slots[idx]
            .name
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .unwrap_or_default();
        let (guild, was_anchor) = {
            let ch = &guard.channels[&bridge];
            (ch.guild_id, ch.anchor.load(Ordering::Relaxed) == idx)
        };

        // Free the stuck bot (its route entry is kept — we re-point it below) and
        // tell it to leave voice so its songbird Call is reset.
        self.slots[idx].mixer.clear();
        self.slots[idx].busy.store(false, Ordering::Relaxed);
        *self.slots[idx].name.lock().unwrap_or_else(PoisonError::into_inner) = None;
        let _ = self.slots[idx].cmd_tx.try_send(PuppetCommand::Release);
        if was_anchor {
            guard
                .channels
                .get_mut(&bridge)
                .unwrap()
                .anchor
                .store(NO_ANCHOR, Ordering::Relaxed);
        }
        tracing::warn!(bridge = %bridge, key = %key, idx, was_anchor, "puppet join failed — bot quarantined; failing over");

        let seq = {
            guard.next_seq += 1;
            guard.next_seq
        };

        // Fail over to a healthy free bot in the same guild.
        if let Some(new_idx) = self.take_free_bot(guild) {
            let ctx = {
                let ch = guard.channels.get_mut(&bridge).unwrap();
                ch.assignments
                    .insert(key.clone(), Assign::Puppet { idx: new_idx, seq });
                *self.slots[new_idx].name.lock().unwrap_or_else(PoisonError::into_inner) =
                    Some(display_name.clone());
                self.slots[new_idx].muted.store(false, Ordering::Relaxed);
                self.slots[new_idx].mixer.clear();
                Self::route_to(&mut ch.routes, &key, self.slots[new_idx].mixer.clone());
                if was_anchor {
                    ch.anchor.store(new_idx, Ordering::Relaxed);
                }
                self.ctx_for(ch)
            };
            let _ = self.slots[new_idx].cmd_tx.try_send(PuppetCommand::Assign {
                ctx: Box::new(ctx),
                display_name,
            });
            // BecomeAnchor (if new_idx is the anchor) is deferred to its own
            // on_join_succeeded — same as the normal assign paths.
            tracing::info!(bridge = %bridge, key = %key, old = idx, new = new_idx, "failed over to replacement puppet");
        } else {
            // No healthy bot free: drop the participant to overflow (its anchor, if
            // any, plays the overflow mix) and let service_pending re-anchor once a
            // bot recovers/frees. Degraded but honest.
            let ch = guard.channels.get_mut(&bridge).unwrap();
            ch.assignments.insert(
                key.clone(),
                Assign::Overflow { name: display_name, muted: false, seq },
            );
            Self::route_to(&mut ch.routes, &key, ch.overflow_mixer.clone());
            let ch = &guard.channels[&bridge];
            self.reconcile_anchor_label(ch);
            self.reconcile_anchor_mute(ch);
            self.rebalance(&mut guard);
            tracing::error!(
                bridge = %bridge,
                key = %key,
                guild = %guild,
                "puppet join failed and no healthy pool bot free — channel DEGRADED \
                 (capture anchor down / audio via overflow until a bot recovers)"
            );
        }
    }

    /// Clear a bot's join-timeout quarantine (after `QUARANTINE_COOLDOWN`) and try
    /// to activate any channel left without an anchor while it was unavailable.
    fn recover_bot(&self, idx: usize) {
        self.slots[idx].unhealthy.store(false, Ordering::Relaxed);
        let mut guard = self.alloc.lock().unwrap_or_else(PoisonError::into_inner);
        self.rebalance(&mut guard);
        tracing::info!(idx, "puppet join-timeout quarantine cleared — eligible again");
    }

    fn set_muted(&self, bridge: &str, key: &str, muted: bool) {
        let mut guard = self.alloc.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(ch) = guard.channels.get_mut(bridge) else { return };
        match ch.assignments.get_mut(key) {
            Some(Assign::Puppet { idx, .. }) => {
                let idx = *idx;
                self.slots[idx].muted.store(muted, Ordering::Relaxed);
                if idx == ch.anchor.load(Ordering::Relaxed) {
                    let ch = &guard.channels[bridge];
                    self.reconcile_anchor_mute(ch);
                } else {
                    let _ = self.slots[idx].cmd_tx.try_send(PuppetCommand::SetMuted(muted));
                }
            }
            Some(Assign::Overflow { muted: m, .. }) => *m = muted,
            None => {}
        }
    }

    fn release(self: &Arc<Self>, bridge: &str, key: &str) {
        let mut guard = self.alloc.lock().unwrap_or_else(PoisonError::into_inner);
        // Remove the assignment; act on what it was.
        let removed = match guard.channels.get_mut(bridge) {
            Some(ch) => ch.assignments.remove(key),
            None => return,
        };
        match removed {
            Some(Assign::Puppet { idx, .. }) => {
                {
                    let ch = guard.channels.get_mut(bridge).unwrap();
                    ch.routes.remove(key);
                }
                // Promote the oldest overflow speaker (FCFS) into the freed slot.
                let promote = {
                    let ch = &guard.channels[bridge];
                    ch.assignments
                        .iter()
                        .filter_map(|(k, a)| match a {
                            Assign::Overflow { name, muted, seq } => {
                                Some((k.clone(), name.clone(), *muted, *seq))
                            }
                            _ => None,
                        })
                        .min_by_key(|(_, _, _, seq)| *seq)
                };
                match promote {
                    Some((yk, yname, ymuted, yseq)) => {
                        let ch = guard.channels.get_mut(bridge).unwrap();
                        ch.assignments
                            .insert(yk.clone(), Assign::Puppet { idx, seq: yseq });
                        ch.overflow_mixer.remove(&yk);
                        self.slots[idx].mixer.clear();
                        Self::route_to(&mut ch.routes, &yk, self.slots[idx].mixer.clone());
                        *self.slots[idx].name.lock().unwrap_or_else(PoisonError::into_inner) =
                            Some(yname.clone());
                        self.slots[idx].muted.store(ymuted, Ordering::Relaxed);
                        let is_anchor = idx == ch.anchor.load(Ordering::Relaxed);
                        let overflow_cleared = ch.overflow_count() == 0;
                        tracing::info!(bridge, key, promoted = %yk, idx, "promoted overflow speaker into freed slot");
                        if !is_anchor {
                            let _ = self.slots[idx].cmd_tx.try_send(PuppetCommand::SetNickname(yname));
                            let _ = self.slots[idx].cmd_tx.try_send(PuppetCommand::SetMuted(ymuted));
                        }
                        if overflow_cleared {
                            let ch = &guard.channels[bridge];
                            self.reconcile_anchor_label(ch);
                            self.reconcile_anchor_mute(ch);
                        }
                    }
                    None => {
                        let is_anchor = guard.channels[bridge].anchor.load(Ordering::Relaxed) == idx;

                        // (Fix 2a) If this freed bot was the anchor but OTHER puppets
                        // still serve Matrix participants, move the oldest such
                        // participant onto this anchor (route-swap — the anchor keeps
                        // its songbird session / SSRC map, so no audio churn) and free
                        // the now-spare bot. Otherwise the anchor would linger as a
                        // nameless "[Matrix] " capture-only bot beside the real
                        // puppets, wasting a slot.
                        if is_anchor {
                            let victim = guard.channels[bridge]
                                .assignments
                                .iter()
                                .filter_map(|(k, a)| match a {
                                    Assign::Puppet { idx: vidx, seq } if *vidx != idx => {
                                        Some((k.clone(), *vidx, *seq))
                                    }
                                    _ => None,
                                })
                                .min_by_key(|(_, _, seq)| *seq);
                            if let Some((vkey, vidx, vseq)) = victim {
                                let vname = self.slots[vidx]
                                    .name
                                    .lock()
                                    .unwrap_or_else(PoisonError::into_inner)
                                    .clone()
                                    .unwrap_or_default();
                                let vmuted = self.slots[vidx].muted.load(Ordering::Relaxed);
                                {
                                    let ch = guard.channels.get_mut(bridge).unwrap();
                                    ch.assignments
                                        .insert(vkey.clone(), Assign::Puppet { idx, seq: vseq });
                                    self.slots[idx].mixer.clear();
                                    Self::route_to(&mut ch.routes, &vkey, self.slots[idx].mixer.clone());
                                    *self.slots[idx].name.lock().unwrap_or_else(PoisonError::into_inner) =
                                        Some(vname);
                                    self.slots[idx].muted.store(vmuted, Ordering::Relaxed);
                                }
                                // Free the now-spare bot.
                                self.slots[vidx].mixer.clear();
                                self.slots[vidx].busy.store(false, Ordering::Relaxed);
                                *self.slots[vidx].name.lock().unwrap_or_else(PoisonError::into_inner) =
                                    None;
                                let _ = self.slots[vidx].cmd_tx.try_send(PuppetCommand::Release);
                                let ch = &guard.channels[bridge];
                                self.reconcile_anchor_label(ch);
                                self.reconcile_anchor_mute(ch);
                                tracing::info!(bridge, moved = %vkey, freed = vidx, onto_anchor = idx, "consolidated participant onto capture anchor; freed spare bot");
                                self.rebalance(&mut guard);
                                return;
                            }
                        }

                        // If this freed bot is the capture anchor and Discord
                        // speakers are still present, KEEP it in the voice channel
                        // as a capture-only anchor — briefly. Leaving immediately would
                        // drop the songbird session and reset SSRC→user mappings,
                        // cutting Discord→Matrix audio until each speaker re-onsets —
                        // the churn a spurious Matrix membership flap would cause. But
                        // bound it with a grace timer so no nameless bot idles in voice
                        // once no Matrix user returns (Fix 2b).
                        let speakers_present = !guard.channels[bridge]
                            .user_sinks
                            .read()
                            .unwrap_or_else(PoisonError::into_inner)
                            .is_empty();
                        if is_anchor && speakers_present {
                            // Drop only its playback identity; keep busy + anchor so
                            // it stays un-allocatable and still the elected anchor.
                            self.slots[idx].mixer.clear();
                            *self.slots[idx].name.lock().unwrap_or_else(PoisonError::into_inner) = None;
                            let _ = self.slots[idx].cmd_tx.try_send(PuppetCommand::SetNickname(String::new()));
                            let _ = self.slots[idx].cmd_tx.try_send(PuppetCommand::SetMuted(false));
                            tracing::info!(bridge, key, idx, "last puppet released but Discord speakers remain — keeping capture-only anchor (grace before release)");
                            self.schedule_anchor_idle_release(&mut guard, bridge);
                            return;
                        }
                        // No overflow waiting, no capture need → free the bot.
                        self.slots[idx].mixer.clear();
                        self.slots[idx].busy.store(false, Ordering::Relaxed);
                        *self.slots[idx].name.lock().unwrap_or_else(PoisonError::into_inner) = None;
                        let _ = self.slots[idx].cmd_tx.try_send(PuppetCommand::Release);
                        tracing::info!(bridge, key, idx, "released puppet");
                        // Hand the anchor off to another puppet in this channel.
                        let ch = guard.channels.get_mut(bridge).unwrap();
                        if ch.anchor.load(Ordering::Relaxed) == idx {
                            let next = ch.assignments.values().find_map(|a| match a {
                                Assign::Puppet { idx, .. } => Some(*idx),
                                Assign::Overflow { .. } => None,
                            });
                            match next {
                                Some(new_idx) => {
                                    ch.anchor.store(new_idx, Ordering::Relaxed);
                                    let _ = self.slots[new_idx]
                                        .cmd_tx
                                        .try_send(PuppetCommand::BecomeAnchor);
                                    tracing::info!(bridge, old = idx, new = new_idx, "capture anchor handed off");
                                }
                                None => {
                                    ch.anchor.store(NO_ANCHOR, Ordering::Relaxed);
                                    tracing::info!(bridge, idx, "capture anchor released — no puppets left");
                                }
                            }
                        }
                        // A freed bot can activate a channel waiting for an anchor.
                        self.rebalance(&mut guard);
                    }
                }
            }
            Some(Assign::Overflow { .. }) => {
                let ch = guard.channels.get_mut(bridge).unwrap();
                ch.overflow_mixer.remove(key);
                ch.routes.remove(key);
                tracing::info!(bridge, key, "released overflow participant");
                if ch.overflow_count() == 0 {
                    let ch = &guard.channels[bridge];
                    self.reconcile_anchor_label(ch);
                    self.reconcile_anchor_mute(ch);
                }
            }
            None => {}
        }
    }

    fn release_all(&self, bridge: &str) {
        let mut guard = self.alloc.lock().unwrap_or_else(PoisonError::into_inner);
        {
            let Some(ch) = guard.channels.get_mut(bridge) else { return };
            let anchor = ch.anchor.load(Ordering::Relaxed);
            let mut anchor_freed = false;
            for (_key, assign) in ch.assignments.drain() {
                if let Assign::Puppet { idx, .. } = assign {
                    self.slots[idx].mixer.clear();
                    self.slots[idx].busy.store(false, Ordering::Relaxed);
                    *self.slots[idx].name.lock().unwrap_or_else(PoisonError::into_inner) = None;
                    let _ = self.slots[idx].cmd_tx.try_send(PuppetCommand::Release);
                    if idx == anchor {
                        anchor_freed = true;
                    }
                }
            }
            // A capture-only-held anchor has no Assign::Puppet of its own, so the
            // drain above won't free it — release it explicitly so teardown returns
            // to baseline (Phase 9 guarantee).
            if anchor != NO_ANCHOR && !anchor_freed {
                self.slots[anchor].mixer.clear();
                self.slots[anchor].busy.store(false, Ordering::Relaxed);
                *self.slots[anchor].name.lock().unwrap_or_else(PoisonError::into_inner) = None;
                let _ = self.slots[anchor].cmd_tx.try_send(PuppetCommand::Release);
            }
            ch.routes.clear();
            ch.anchor.store(NO_ANCHOR, Ordering::Relaxed);
            ch.overflow_mixer.clear();
        }
        // The bots just freed can activate channels waiting for an anchor.
        self.rebalance(&mut guard);
    }

    /// Release a capture-only-held anchor once its channel is idle on BOTH sides:
    /// no playback puppet assigned AND no Discord speakers left. Called when the
    /// last Discord speaker leaves (see `drop_ghost`). No-op if there is no held
    /// anchor or either side is still active.
    fn maybe_release_idle_anchor(&self, bridge: &str) {
        let mut guard = self.alloc.lock().unwrap_or_else(PoisonError::into_inner);
        let idx = {
            let Some(ch) = guard.channels.get(bridge) else { return };
            let anchor = ch.anchor.load(Ordering::Relaxed);
            if anchor == NO_ANCHOR {
                return;
            }
            let has_puppet = ch
                .assignments
                .values()
                .any(|a| matches!(a, Assign::Puppet { .. }));
            let speakers = !ch.user_sinks.read().unwrap_or_else(PoisonError::into_inner).is_empty();
            if has_puppet || speakers {
                return; // still in use on one side — keep the anchor
            }
            anchor
        };
        self.slots[idx].mixer.clear();
        self.slots[idx].busy.store(false, Ordering::Relaxed);
        *self.slots[idx].name.lock().unwrap_or_else(PoisonError::into_inner) = None;
        let _ = self.slots[idx].cmd_tx.try_send(PuppetCommand::Release);
        {
            let ch = guard.channels.get_mut(bridge).unwrap();
            ch.anchor.store(NO_ANCHOR, Ordering::Relaxed);
            ch.overflow_mixer.clear();
        }
        tracing::info!(bridge, idx, "channel idle (no puppets, no Discord speakers) — releasing capture-only anchor");
        self.rebalance(&mut guard);
    }

    /// Arm (or re-arm) the grace timer that releases a capture-only anchor if no
    /// Matrix participant returns within [`ANCHOR_IDLE_GRACE`]. Cancels any prior
    /// timer for the channel. Must be called with the `alloc` guard already held
    /// (it stores the token under it); the spawned task re-locks on expiry.
    fn schedule_anchor_idle_release(self: &Arc<Self>, guard: &mut AllocState, bridge: &str) {
        let token = CancellationToken::new();
        let Some(ch) = guard.channels.get_mut(bridge) else { return };
        if let Some(old) = ch.anchor_idle_release.replace(token.clone()) {
            old.cancel();
        }
        let pool = self.clone();
        let bridge = bridge.to_owned();
        tokio::spawn(async move {
            tokio::select! {
                _ = token.cancelled() => {}
                _ = tokio::time::sleep(ANCHOR_IDLE_GRACE) => {
                    pool.release_capture_only_anchor_if_idle(&bridge);
                }
            }
        });
    }

    /// Cancel a pending capture-only-anchor idle-release timer (a Matrix
    /// participant is (re)joining, so the anchor is no longer idle).
    fn cancel_anchor_idle_release(guard: &mut AllocState, bridge: &str) {
        if let Some(ch) = guard.channels.get_mut(bridge) {
            if let Some(tok) = ch.anchor_idle_release.take() {
                tok.cancel();
            }
        }
    }

    /// Grace-timer callback: release the capture-only anchor (leave Discord voice)
    /// iff the channel still has an anchor and **no** Matrix participant — i.e. the
    /// grace elapsed with nobody returning. A no-op if a participant reused the
    /// anchor in the meantime (then it serves someone and must stay).
    fn release_capture_only_anchor_if_idle(&self, bridge: &str) {
        let mut guard = self.alloc.lock().unwrap_or_else(PoisonError::into_inner);
        let idx = {
            let Some(ch) = guard.channels.get(bridge) else { return };
            let anchor = ch.anchor.load(Ordering::Relaxed);
            if anchor == NO_ANCHOR {
                return;
            }
            let any_puppet = ch
                .assignments
                .values()
                .any(|a| matches!(a, Assign::Puppet { .. }));
            if any_puppet {
                return; // a Matrix participant is present — keep the capture path
            }
            anchor
        };
        self.slots[idx].mixer.clear();
        self.slots[idx].busy.store(false, Ordering::Relaxed);
        *self.slots[idx].name.lock().unwrap_or_else(PoisonError::into_inner) = None;
        let _ = self.slots[idx].cmd_tx.try_send(PuppetCommand::Release);
        {
            let ch = guard.channels.get_mut(bridge).unwrap();
            ch.anchor.store(NO_ANCHOR, Ordering::Relaxed);
            ch.overflow_mixer.clear();
            ch.anchor_idle_release = None;
        }
        tracing::info!(bridge, idx, "capture-only anchor idle grace elapsed — leaving Discord voice (no Matrix user present)");
        self.rebalance(&mut guard);
    }

    /// Record that bot `idx` is now a member of `guild` (from `ready`/
    /// `guild_create`), then service any channel in that guild waiting for a bot
    /// (covers the startup race where an assign arrives before membership is known).
    fn note_bot_guild(&self, idx: usize, guild: GuildId) {
        let added = self.slots[idx]
            .guilds
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(guild);
        if added {
            let mut guard = self.alloc.lock().unwrap_or_else(PoisonError::into_inner);
            self.rebalance(&mut guard);
        }
    }

    /// Record that bot `idx` left `guild` (kicked / guild unavailable) so it is no
    /// longer assigned there.
    fn forget_bot_guild(&self, idx: usize, guild: GuildId) {
        self.slots[idx]
            .guilds
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&guild);
    }
}

/// A no-op sink for the (unreachable) case where a channel vanished mid-assign.
struct NullSink;
impl ParticipantSink for NullSink {
    fn push(&self, _mono: &[i16]) {}
}

/// A [`ParticipantSink`] whose target mixer the pool can swap (for promotion/demotion).
struct RoutedSink {
    route: Route,
    key: String,
}

impl ParticipantSink for RoutedSink {
    fn push(&self, mono: &[i16]) {
        let mixer = self.route.lock().unwrap_or_else(PoisonError::into_inner).clone();
        mixer.push(&self.key, mono);
    }
}

/// A per-bridge view into the [`SharedPool`], implementing [`PlaybackRouter`].
/// All calls delegate to the pool scoped to this bridge's channel key.
pub struct ChannelView {
    pool: Arc<SharedPool>,
    key: Arc<str>,
}

impl PlaybackRouter for ChannelView {
    fn assign(&self, key: &str, display_name: &str) -> Arc<dyn ParticipantSink> {
        self.pool.assign(&self.key, key, display_name)
    }
    fn set_muted(&self, key: &str, muted: bool) {
        self.pool.set_muted(&self.key, key, muted)
    }
    fn release(&self, key: &str) {
        self.pool.release(&self.key, key)
    }
    fn release_all(&self) {
        self.pool.release_all(&self.key)
    }
}

// ─── Puppet serenity client ────────────────────────────────────────────────────

/// Per-puppet gateway handler. On `ready` it captures a live [`Context`] and
/// spawns the (channel-agnostic) command loop; it never auto-joins.
struct PuppetHandler {
    pool: Arc<SharedPool>,
    idx: usize,
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
        // Self-heal: if a startup race ghosted this bot before its id was known
        // (e.g. it was lingering in voice from a previous crash), retract it now.
        self.pool.retract_self_ghost(ready.user.id.get());

        // Seed this bot's guild membership from the ready payload so the allocator
        // knows which channels it can serve before any assign arrives.
        for ug in &ready.guilds {
            self.pool.note_bot_guild(self.idx, ug.id);
        }

        // `ready` can fire again on reconnect; only the first run takes the rx.
        let slot = &self.pool.slots[self.idx];
        let Some(rx) = slot.cmd_rx.lock().unwrap_or_else(PoisonError::into_inner).take() else {
            return;
        };

        tokio::spawn(puppet_command_loop(
            ctx,
            self.idx,
            slot.mixer.clone(),
            slot.current_ssrc.clone(),
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
        if self.pool.is_bridge_bot(user_id) {
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
        if self.pool.slots[self.idx].busy.load(Ordering::Relaxed) {
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
}

/// Mint a ghost for a Discord user in the channel if they don't have one yet.
/// Skips bridge-owned puppets and any other bot. Resolves the display name from
/// the supplied `Member` if present, else via a lookup.
async fn ensure_ghost(ctx: &Context, channel: &ChannelCtx, user_id: u64, member: Option<&Member>) {
    // Exclude only OUR OWN puppet/anchor bots (echo prevention, constraint #5).
    // Third-party bots (e.g. a TTS bot) ARE bridged — their audio is a legitimate
    // Discord speaker to mirror into Matrix. Our bots are doubly protected: their
    // SSRCs are also in the capture `excluded` set, registered on DriverConnect.
    if channel
        .bot_ids
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .contains(&user_id)
    {
        return;
    }
    if channel
        .user_sinks
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .contains_key(&user_id)
    {
        return; // already has a ghost
    }

    let (name, avatar_url) = match member {
        Some(m) => (member_display_name(m), m.user.avatar_url()),
        None => match channel.guild_id.member(&ctx.http, UserId::new(user_id)).await {
            Ok(m) => (member_display_name(&m), m.user.avatar_url()),
            Err(e) => {
                tracing::debug!(user_id, error = %e, "member lookup failed; using id as name");
                (user_id.to_string(), None)
            }
        },
    };

    // Commit under the user_sinks write lock, and RE-CHECK bot_ids here: the user's
    // gateway `ready` may have landed during the (awaited) name resolution above,
    // revealing it as one of our OWN puppet bots that was in voice before its id was
    // known (a startup race — see `retract_self_ghost`). Holding the write lock
    // across the re-check and the insert makes this race-free against
    // `retract_self_ghost`, which removes the id under the same lock.
    {
        let mut sinks = channel
            .user_sinks
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        if channel
            .bot_ids
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&user_id)
        {
            return; // our own puppet bot — never a ghost (echo prevention)
        }
        if sinks.contains_key(&user_id) {
            return; // raced with another ensure_ghost for the same user
        }
        let sink = channel
            .capture_router
            .assign(&user_id.to_string(), &name, avatar_url.as_deref());
        sinks.insert(user_id, sink);
    }
    tracing::info!(bridge = %channel.bridge, user_id, display_name = %name, "Discord user present — ghost assigned");
}

/// Retire a user's ghost if they had one (left or moved out of the channel).
/// Remove a Discord speaker's ghost. Returns `true` iff this removed the **last**
/// speaker (so the caller can release a now-idle capture-only anchor).
fn drop_ghost(channel: &ChannelCtx, user_id: u64) -> bool {
    let now_empty = {
        let mut sinks = channel.user_sinks.write().unwrap_or_else(PoisonError::into_inner);
        if sinks.remove(&user_id).is_none() {
            return false; // wasn't a tracked speaker
        }
        sinks.is_empty()
    };
    channel.capture_router.release(&user_id.to_string());
    tracing::info!(bridge = %channel.bridge, user_id, "Discord user left — ghost released");
    now_empty
}

/// Decode a bot's user id from its token. A Discord bot token is
/// `base64(user_id) "." base64(timestamp) "." hmac`; the first segment base64-
/// decodes to the decimal user id. Best-effort — returns `None` on any unexpected
/// shape (the caller then relies on the `ready` event to learn the id).
fn decode_bot_user_id(token: &str) -> Option<u64> {
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
fn member_display_name(m: &Member) -> String {
    m.nick
        .clone()
        .or_else(|| m.user.global_name.clone())
        .unwrap_or_else(|| m.user.name.clone())
}

/// Drain commands for one puppet, driving its voice connection. The loop is
/// channel-agnostic: each `Assign` carries the channel context, which the loop
/// remembers (`current`) for subsequent commands and shutdown.
async fn puppet_command_loop(
    ctx: Context,
    idx: usize,
    mixer: Arc<PlaybackMixer>,
    current_ssrc: Arc<StdMutex<Option<u32>>>,
    nickname_prefix: Arc<str>,
    pool: Arc<SharedPool>,
    mut cmd_rx: mpsc::Receiver<PuppetCommand>,
) {
    let mut current: Option<ChannelCtx> = None;
    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            PuppetCommand::Assign { ctx: chan, display_name } => {
                let chan = *chan;
                let joined =
                    puppet_join(&chan, idx, &ctx, &mixer, &current_ssrc, &nickname_prefix, &display_name)
                        .await;
                current = Some(chan);
                if joined {
                    // Activate this bot as anchor iff the allocator elected it.
                    pool.on_join_succeeded(idx);
                } else {
                    // Quarantine this bot + fail the participant over to another,
                    // then schedule the bot's recovery after the cooldown.
                    pool.on_join_failed(idx);
                    let pool = pool.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(QUARANTINE_COOLDOWN).await;
                        pool.recover_bot(idx);
                    });
                }
            }
            PuppetCommand::BecomeAnchor => {
                if let Some(chan) = &current {
                    puppet_become_anchor(chan, idx, &ctx).await;
                } else {
                    tracing::warn!(idx, "BecomeAnchor with no current channel");
                }
            }
            PuppetCommand::SetMuted(muted) => {
                if let Some(chan) = &current {
                    puppet_set_muted(&ctx, idx, chan.guild_id, muted).await;
                }
            }
            PuppetCommand::SetNickname(name) => {
                if let Some(chan) = &current {
                    let nick = build_nickname(&nickname_prefix, &name);
                    if let Err(e) = chan.guild_id.edit_nickname(&ctx.http, Some(nick.as_str())).await {
                        tracing::warn!(idx, error = %e, "failed to relabel puppet nickname");
                    } else {
                        tracing::info!(idx, nick = %nick, "puppet relabeled");
                    }
                }
            }
            PuppetCommand::Release => {
                if let Some(chan) = current.take() {
                    puppet_leave(&ctx, idx, &chan, &current_ssrc).await;
                }
            }
            PuppetCommand::ReleaseAndAck { done } => {
                if let Some(chan) = current.take() {
                    puppet_leave(&ctx, idx, &chan, &current_ssrc).await;
                }
                let _ = done.send(());
            }
        }
    }
    tracing::info!(idx, "puppet command loop exited");
}

/// Register the Discord→Matrix capture handlers + overflow playback on this bot's
/// voice connection (it has become the anchor), then seed ghosts for members
/// already present in the channel.
async fn puppet_become_anchor(channel: &ChannelCtx, idx: usize, ctx: &Context) {
    let Some(manager) = songbird::get(ctx).await else { return };
    let Some(call_lock) = manager.get(channel.guild_id) else {
        tracing::warn!(idx, "anchor command but puppet not in voice yet");
        return;
    };
    {
        let mut call = call_lock.lock().await;
        call.add_global_event(
            CoreEvent::VoiceTick.into(),
            VoiceReceiver::new(
                channel.bridge.clone(),
                channel.exclude.clone(),
                channel.ssrc_map.clone(),
                channel.user_sinks.clone(),
                channel.capture_sink.clone(),
                channel.anchor.clone(),
                idx,
            ),
        );
        // The anchor also plays the channel's overflow mix (un-slotted speakers).
        let overflow_src = PlaybackSource::new(channel.overflow_mixer.clone());
        let overflow_input =
            Input::from(RawAdapter::new(overflow_src, PLAYBACK_SAMPLE_RATE, PLAYBACK_CHANNELS));
        let _overflow_handle = call.play_input(overflow_input);
    }
    tracing::info!(idx, bridge = %channel.bridge, "anchor capture active — Discord→Matrix demux from this puppet");

    // Seed ghosts for everyone already in the voice channel — voice_state_update
    // only fires on later changes. Read the cached voice states (populated via the
    // GUILD_VOICE_STATES intent); ensure_ghost is idempotent.
    let present: Vec<u64> = match ctx.cache.guild(channel.guild_id) {
        Some(guild) => guild
            .voice_states
            .iter()
            .filter(|(_, vs)| vs.channel_id == Some(channel.channel_id))
            .map(|(uid, _)| uid.get())
            .collect(),
        None => Vec::new(),
    };
    if !present.is_empty() {
        tracing::info!(count = present.len(), bridge = %channel.bridge, "seeding ghosts for existing voice members");
    }
    for uid in present {
        ensure_ghost(ctx, channel, uid, None).await;
    }
}

/// Self-mute/unmute this puppet's voice connection to mirror the Matrix
/// participant's mute state (shows as muted in the Discord client).
async fn puppet_set_muted(ctx: &Context, idx: usize, guild_id: GuildId, muted: bool) {
    let Some(manager) = songbird::get(ctx).await else { return };
    if let Some(call_lock) = manager.get(guild_id) {
        let mut call = call_lock.lock().await;
        if let Err(e) = call.mute(muted).await {
            tracing::warn!(idx, error = %e, muted, "failed to set puppet mute state");
        } else {
            tracing::info!(idx, muted, "puppet mute state mirrored");
        }
    }
}

/// Returns `true` if the bot actually joined the voice channel. `false` on a join
/// failure (e.g. the gateway timed out after a channel hop) — the caller fails the
/// participant over to another pool bot.
async fn puppet_join(
    channel: &ChannelCtx,
    idx: usize,
    ctx: &Context,
    mixer: &Arc<PlaybackMixer>,
    current_ssrc: &Arc<StdMutex<Option<u32>>>,
    nickname_prefix: &str,
    display_name: &str,
) -> bool {
    let Some(manager) = songbird::get(ctx).await else {
        tracing::error!(idx, "songbird not registered on puppet client");
        return false;
    };

    // Register handlers and start playback on the Call *before* connecting: two
    // events fire *during* `manager.join().await` and are missed if a handler is
    // attached after the await — `DriverConnect` (our own SSRC, echo-exclusion)
    // and `SpeakingStateUpdate` (others' SSRC→user; emitted around connect).
    let call_lock = manager.get_or_insert(channel.guild_id);
    {
        let mut call = call_lock.lock().await;
        call.add_global_event(
            CoreEvent::DriverConnect.into(),
            PuppetSsrcRegistrar {
                bridge: channel.bridge.clone(),
                idx,
                exclude: channel.exclude.clone(),
                current_ssrc: current_ssrc.clone(),
            },
        );
        call.add_global_event(
            CoreEvent::SpeakingStateUpdate.into(),
            SpeakingLogger::new(channel.bridge.clone(), channel.ssrc_map.clone()),
        );
        for ev in [
            CoreEvent::DriverReconnect,
            CoreEvent::DriverDisconnect,
            CoreEvent::ClientDisconnect,
        ] {
            call.add_global_event(
                ev.into(),
                ConnectionLogger::new(channel.bridge.clone(), channel.ssrc_map.clone()),
            );
        }
        let source = PlaybackSource::new(mixer.clone());
        let input = Input::from(RawAdapter::new(source, PLAYBACK_SAMPLE_RATE, PLAYBACK_CHANNELS));
        let _handle = call.play_input(input);
    }

    if let Err(e) = manager.join(channel.guild_id, channel.channel_id).await {
        tracing::error!(idx, error = %e, "puppet failed to join voice channel");
        // Reset the (possibly half-open) Call so a later reuse of this bot starts
        // clean once it recovers from quarantine.
        let _ = manager.remove(channel.guild_id).await;
        return false;
    }

    let nick = build_nickname(nickname_prefix, display_name);
    if let Err(e) = channel.guild_id.edit_nickname(&ctx.http, Some(nick.as_str())).await {
        tracing::warn!(idx, error = %e, "failed to set puppet nickname");
    }
    tracing::info!(idx, bridge = %channel.bridge, display_name, "puppet joined voice and assigned");
    true
}

async fn puppet_leave(
    ctx: &Context,
    idx: usize,
    channel: &ChannelCtx,
    current_ssrc: &Arc<StdMutex<Option<u32>>>,
) {
    if let Err(e) = channel.guild_id.edit_nickname(&ctx.http, None).await {
        tracing::warn!(idx, error = %e, "failed to reset puppet nickname");
    }
    if let Some(manager) = songbird::get(ctx).await {
        if let Err(e) = manager.remove(channel.guild_id).await {
            tracing::warn!(idx, error = %e, "puppet failed to leave voice channel");
        }
    }
    // Stop excluding our (now-gone) SSRC so a recycled SSRC isn't wrongly muted.
    let ssrc = current_ssrc.lock().unwrap_or_else(PoisonError::into_inner).take();
    if let Some(s) = ssrc {
        channel
            .exclude
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&s);
        tracing::info!(idx, ssrc = s, "puppet SSRC removed from echo-exclusion");
    }
    tracing::info!(idx, bridge = %channel.bridge, "puppet left voice and released");
}

/// Songbird handler that records a puppet's own SSRC into its channel's
/// echo-exclusion set as soon as its voice driver connects.
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
            *self.current_ssrc.lock().unwrap_or_else(PoisonError::into_inner) = Some(c.ssrc);
            tracing::info!(idx = self.idx, ssrc = c.ssrc, "puppet SSRC registered for echo-exclusion");
        }
        None
    }
}

/// Test-only inspection of allocator state (commands are not consumed in unit
/// tests — `run` is never called — so these assert on the bookkeeping directly).
#[cfg(test)]
impl SharedPool {
    /// Slot index a participant is assigned to, if it has a dedicated puppet.
    fn test_slot_of(&self, bridge: &str, key: &str) -> Option<usize> {
        let g = self.alloc.lock().unwrap();
        match g.channels.get(bridge)?.assignments.get(key)? {
            Assign::Puppet { idx, .. } => Some(*idx),
            Assign::Overflow { .. } => None,
        }
    }
    fn test_is_overflow(&self, bridge: &str, key: &str) -> bool {
        let g = self.alloc.lock().unwrap();
        matches!(
            g.channels.get(bridge).and_then(|c| c.assignments.get(key)),
            Some(Assign::Overflow { .. })
        )
    }
    fn test_anchor(&self, bridge: &str) -> Option<usize> {
        let g = self.alloc.lock().unwrap();
        let a = g.channels.get(bridge)?.anchor.load(Ordering::Relaxed);
        (a != NO_ANCHOR).then_some(a)
    }
    fn test_free_count(&self) -> usize {
        self.slots.iter().filter(|s| !s.busy.load(Ordering::Relaxed)).count()
    }
    fn test_is_unhealthy(&self, idx: usize) -> bool {
        self.slots[idx].unhealthy.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nvb_audio::routing::CaptureSink as GhostSink;

    /// A no-op capture router for allocator tests (no ghosts are minted).
    struct NoopCapture;
    struct NoopCaptureSink;
    impl GhostSink for NoopCaptureSink {
        fn push(&self, _mono: &[i16]) {}
    }
    impl CaptureRouter for NoopCapture {
        fn assign(&self, _k: &str, _n: &str, _a: Option<&str>) -> Arc<dyn GhostSink> {
            Arc::new(NoopCaptureSink)
        }
        fn set_muted(&self, _k: &str, _muted: bool) {}
        fn release(&self, _k: &str) {}
        fn release_all(&self) {}
    }

    /// Build a pool of `n` bots with no channels registered yet.
    fn pool(n: usize) -> Arc<SharedPool> {
        let cfg = PuppetsConfig {
            tokens: (0..n).map(|i| format!("Bot {i}")).collect(),
            nickname_prefix: "[Matrix] ".to_string(),
        };
        SharedPool::new(&cfg)
    }

    /// Register a channel `label` at guild/channel ids derived from `g`/`c`, and
    /// enroll every pool bot in guild `g` (in production this comes from the
    /// gateway `ready`/`guild_create` events, which don't fire in unit tests).
    fn channel(p: &Arc<SharedPool>, label: &str, g: u64, c: u64) {
        p.register_channel(
            Arc::from(label),
            &g.to_string(),
            &c.to_string(),
            Arc::new(NoopCapture),
            None,
        )
        .expect("register channel");
        for slot in &p.slots {
            slot.guilds
                .write()
                .unwrap()
                .insert(GuildId::new(g));
        }
    }

    #[test]
    fn dedicated_until_full_then_overflow() {
        let p = pool(2);
        channel(&p, "A", 1, 10);
        p.assign("A", "alice", "Alice");
        p.assign("A", "bob", "Bob");
        let sa = p.test_slot_of("A", "alice").unwrap();
        let sb = p.test_slot_of("A", "bob").unwrap();
        assert_ne!(sa, sb, "distinct puppets");
        assert_eq!(p.test_anchor("A"), Some(sa), "first assigned is anchor");
        assert_eq!(p.test_free_count(), 0);
        // Pool exhausted → overflow.
        p.assign("A", "carol", "Carol");
        assert!(p.test_is_overflow("A", "carol"));
    }

    #[test]
    fn release_promotes_oldest_overflow_fcfs() {
        let p = pool(2);
        channel(&p, "A", 1, 10);
        p.assign("A", "alice", "Alice");
        p.assign("A", "bob", "Bob");
        p.assign("A", "carol", "Carol"); // overflow, older
        p.assign("A", "dave", "Dave"); // overflow, newer
        let sa = p.test_slot_of("A", "alice").unwrap();
        p.release("A", "alice");
        // Carol (oldest overflow) promoted into the freed slot; Dave still overflow.
        assert_eq!(p.test_slot_of("A", "carol"), Some(sa));
        assert!(p.test_is_overflow("A", "dave"));
    }

    #[test]
    fn second_channel_waits_then_activates_when_bot_frees() {
        let p = pool(1);
        channel(&p, "A", 1, 10);
        channel(&p, "B", 1, 20);
        p.assign("A", "alice", "Alice"); // takes the only bot (A anchor)
        assert_eq!(p.test_free_count(), 0);
        // B has no anchor and no donor has slack (A has just its anchor) → pending.
        p.assign("B", "bob", "Bob");
        assert_eq!(p.test_anchor("B"), None, "B waiting for a bot");
        assert!(p.test_is_overflow("B", "bob"));
        // Free A's bot → B is activated with it.
        p.release("A", "alice");
        assert!(p.test_anchor("B").is_some(), "B activated");
        assert!(p.test_slot_of("B", "bob").is_some(), "bob now has a puppet");
    }

    #[test]
    fn steals_from_donor_with_slack_to_activate_new_channel() {
        let p = pool(3);
        channel(&p, "A", 1, 10);
        channel(&p, "B", 2, 20); // different guild
        p.assign("A", "a1", "A1");
        p.assign("A", "a2", "A2");
        p.assign("A", "a3", "A3"); // A uses all 3 bots
        assert_eq!(p.test_free_count(), 0);
        let anchor_a = p.test_anchor("A").unwrap();
        // B activates by stealing A's newest non-anchor dedicated speaker (a3).
        p.assign("B", "b1", "B1");
        assert!(p.test_anchor("B").is_some(), "B got an anchor");
        assert!(p.test_slot_of("B", "b1").is_some());
        // a3 (newest) was demoted to A's overflow; A keeps its anchor + a2.
        assert!(p.test_is_overflow("A", "a3"), "newest A speaker demoted");
        assert_eq!(p.test_anchor("A"), Some(anchor_a), "A anchor unchanged");
        assert!(p.test_slot_of("A", "a2").is_some());
    }

    #[test]
    fn freed_bot_repromotes_donor_overflow_after_steal() {
        // Reproduces the live "stranded overflow bot" bug: a steal demotes a donor
        // speaker to overflow; when the stealing channel later frees the bot, the
        // donor's demoted speaker must be re-promoted to a dedicated puppet (it was
        // previously left in overflow forever — no cross-channel re-promotion).
        let p = pool(3);
        channel(&p, "A", 1, 10);
        channel(&p, "B", 2, 20); // different guild; all 3 bots are in both
        p.assign("A", "a1", "A1");
        p.assign("A", "a2", "A2");
        p.assign("A", "a3", "A3"); // A uses all 3 bots
        // B activates by stealing A's newest dedicated speaker (a3 → A overflow).
        p.assign("B", "b1", "B1");
        assert!(p.test_is_overflow("A", "a3"), "a3 demoted to overflow by steal");
        assert!(p.test_anchor("B").is_some(), "B got an anchor");
        // B empties → its bot returns to the pool, which must re-promote a3.
        p.release("B", "b1");
        assert!(
            p.test_slot_of("A", "a3").is_some(),
            "a3 re-promoted to a dedicated puppet after the bot freed"
        );
        assert!(!p.test_is_overflow("A", "a3"), "a3 no longer in overflow");
    }

    #[test]
    fn freed_bot_only_repromotes_guild_capable_channel() {
        // A freed bot in guild 1 must NOT be pulled into a guild-2 channel's overflow
        // it can't serve. Partitioned pool: bot 0 ∈ guild 1, bot 1 ∈ guild 2.
        let p = pool(2);
        p.register_channel(Arc::from("A"), "1", "10", Arc::new(NoopCapture), None).unwrap();
        p.register_channel(Arc::from("B"), "2", "20", Arc::new(NoopCapture), None).unwrap();
        p.slots[0].guilds.write().unwrap().insert(GuildId::new(1));
        p.slots[1].guilds.write().unwrap().insert(GuildId::new(2));
        // B (guild 2) anchored on bot 1, plus an overflow speaker it can't promote
        // (no guild-2 bot free).
        p.assign("B", "b1", "B1");
        p.assign("B", "b2", "B2");
        assert!(p.test_is_overflow("B", "b2"), "b2 overflow (no guild-2 spare)");
        // A (guild 1) takes bot 0, then releases it → bot 0 frees.
        p.assign("A", "a1", "A1");
        p.release("A", "a1");
        // The freed guild-1 bot can't serve B (guild 2); b2 stays overflow.
        assert!(p.test_is_overflow("B", "b2"), "b2 still overflow — bot wrong guild");
    }

    #[test]
    fn join_failure_quarantines_and_fails_over_anchor() {
        let p = pool(2);
        channel(&p, "A", 1, 10);
        p.assign("A", "alice", "Alice");
        let sa = p.test_slot_of("A", "alice").unwrap();
        assert_eq!(p.test_anchor("A"), Some(sa));
        // The anchor bot's join times out → fail over to the other pool bot.
        p.on_join_failed(sa);
        assert!(p.test_is_unhealthy(sa), "stuck bot quarantined");
        let sb = p.test_slot_of("A", "alice").expect("alice re-assigned");
        assert_ne!(sb, sa, "moved to a different bot");
        assert_eq!(p.test_anchor("A"), Some(sb), "anchor handed to replacement");
        assert!(!p.test_is_unhealthy(sb));
    }

    #[test]
    fn join_failure_with_no_spare_degrades_then_recovers() {
        let p = pool(1);
        channel(&p, "A", 1, 10);
        p.assign("A", "alice", "Alice");
        let sa = p.test_slot_of("A", "alice").unwrap();
        // Sole bot's join times out, no spare → degrade to overflow + no anchor.
        p.on_join_failed(sa);
        assert!(p.test_is_unhealthy(sa));
        assert!(p.test_is_overflow("A", "alice"), "dropped to overflow");
        assert_eq!(p.test_anchor("A"), None, "channel has no anchor (degraded)");
        // Quarantine clears → service_pending re-activates the channel on that bot.
        p.recover_bot(sa);
        assert!(!p.test_is_unhealthy(sa));
        assert_eq!(p.test_anchor("A"), Some(sa), "re-anchored after recovery");
        assert_eq!(p.test_slot_of("A", "alice"), Some(sa));
    }

    #[test]
    fn allocates_only_guild_capable_bots() {
        // Partitioned pool: bot 0 only in guild 1, bot 1 only in guild 2.
        let p = pool(2);
        p.register_channel(Arc::from("A"), "1", "10", Arc::new(NoopCapture), None).unwrap();
        p.register_channel(Arc::from("B"), "2", "20", Arc::new(NoopCapture), None).unwrap();
        p.slots[0].guilds.write().unwrap().insert(GuildId::new(1));
        p.slots[1].guilds.write().unwrap().insert(GuildId::new(2));

        // Each channel can only use the bot that's in its guild.
        p.assign("A", "a", "A");
        p.assign("B", "b", "B");
        assert_eq!(p.test_slot_of("A", "a"), Some(0), "A uses the guild-1 bot");
        assert_eq!(p.test_slot_of("B", "b"), Some(1), "B uses the guild-2 bot");

        // A second A speaker can't use bot 1 (wrong guild) and B has no slack to
        // steal a guild-1 bot from → overflow.
        p.assign("A", "a2", "A2");
        assert!(p.test_is_overflow("A", "a2"));
    }

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
        let nick = build_nickname("[Matrix] ", &"é".repeat(40));
        assert!(nick.chars().count() <= 32);
        assert!(nick.starts_with("[Matrix] "));
    }

    // Samples a buffer must accumulate before the mixer releases it (priming).
    const PRIME: usize = 150 * 48;

    #[test]
    fn routed_sink_follows_swapped_mixer() {
        // A RoutedSink pushes to whatever mixer its route points at; swapping the
        // route (as promotion/demotion does) redirects subsequent frames with no
        // change on the Matrix side.
        let a = Arc::new(PlaybackMixer::new());
        let b = Arc::new(PlaybackMixer::new());
        let route: Route = Arc::new(StdMutex::new(a.clone()));
        let sink = RoutedSink { route: route.clone(), key: "k".to_owned() };

        sink.push(&vec![100i16; PRIME]);
        assert!(a.pop_10ms_mixed().iter().all(|&s| s == 100), "frames went to A");

        *route.lock().unwrap() = b.clone();
        sink.push(&vec![80i16; PRIME]);
        assert!(b.pop_10ms_mixed().iter().all(|&s| s == 80), "frames follow to B after swap");
    }

    #[test]
    fn note_presence_reports_edges_once() {
        let p = pool(2);
        channel(&p, "A", 1, 10);
        // Voice-state events reach every pool bot; only the first observer of a
        // join sees the 0→1 edge (the one that should mint the ghost).
        assert!(p.note_presence("A", 1001, true), "first join is an edge");
        assert!(!p.note_presence("A", 1001, true), "duplicate join (2nd bot) is not");
        assert!(!p.note_presence("A", 1001, true), "still not");
        // Likewise only the first observer of the matching leave sees the 1→0 edge.
        assert!(p.note_presence("A", 1001, false), "first leave is an edge");
        assert!(!p.note_presence("A", 1001, false), "duplicate leave is not");
        // A leave for a user that was never present is never an edge.
        assert!(!p.note_presence("A", 9999, false));
    }

    #[test]
    fn note_presence_is_per_user_and_per_channel() {
        let p = pool(2);
        channel(&p, "A", 1, 10);
        channel(&p, "B", 1, 20);
        assert!(p.note_presence("A", 1, true));
        assert!(p.note_presence("A", 2, true), "a distinct user is its own edge");
        assert!(p.note_presence("B", 1, true), "same user in another channel edges once");
        // Leaving A leaves the user's B presence untouched.
        assert!(p.note_presence("A", 1, false));
        assert!(!p.note_presence("A", 1, false));
        assert!(p.note_presence("B", 1, false), "B still tracked the user");
        // An unknown bridge is a no-op, never an edge.
        assert!(!p.note_presence("nope", 1, true));
    }

    #[test]
    fn bridge_bots_excluded_from_presence() {
        let p = pool(1);
        // A puppet bot's user id, as recorded on the gateway `ready` event.
        p.bot_ids.write().unwrap().insert(4242);
        assert!(p.is_bridge_bot(4242), "our own bot is recognised");
        assert!(!p.is_bridge_bot(1001), "a human is not");
    }

    #[test]
    fn releasing_anchor_consolidates_a_participant_onto_it() {
        // When the anchor's own participant leaves while OTHER participants remain,
        // the oldest moves onto the anchor (route-swap, SSRC map preserved) and its
        // old bot is freed — no nameless "[Matrix] " capture-only orphan.
        let p = pool(3);
        channel(&p, "A", 1, 10);
        p.assign("A", "alice", "Alice"); // anchor
        p.assign("A", "bob", "Bob"); // oldest non-anchor
        p.assign("A", "carol", "Carol");
        let anchor = p.test_anchor("A").unwrap();
        let bob_old = p.test_slot_of("A", "bob").unwrap();
        assert_ne!(bob_old, anchor);

        p.release("A", "alice");

        assert_eq!(p.test_anchor("A"), Some(anchor), "anchor bot is unchanged");
        assert_eq!(p.test_slot_of("A", "bob"), Some(anchor), "bob consolidated onto the anchor");
        assert!(p.test_slot_of("A", "carol").is_some(), "carol still served");
        assert_eq!(p.test_free_count(), 1, "bob's old bot was freed");
        assert!(p.test_slot_of("A", "alice").is_none(), "alice released");
    }

    #[tokio::test]
    async fn assign_reuses_a_capture_only_anchor() {
        // A lone participant leaving while a Discord speaker remains retains a
        // capture-only anchor; the next participant must REUSE it, not grab a fresh
        // bot (which would leave the anchor as a nameless "[Matrix] " bot).
        let p = pool(2);
        channel(&p, "A", 1, 10);
        p.assign("A", "alice", "Alice");
        let anchor = p.test_anchor("A").unwrap();
        // Mark a Discord speaker present so the anchor is retained on release.
        {
            let guard = p.alloc.lock().unwrap();
            let ch = guard.channels.get("A").unwrap();
            ch.user_sinks
                .write()
                .unwrap()
                .insert(999, Arc::new(NoopCaptureSink));
        }
        p.release("A", "alice"); // no other puppet → capture-only anchor (grace armed)
        assert_eq!(p.test_anchor("A"), Some(anchor), "capture-only anchor retained");
        assert!(p.test_slot_of("A", "alice").is_none());

        p.assign("A", "dave", "Dave");
        assert_eq!(p.test_slot_of("A", "dave"), Some(anchor), "dave reused the anchor bot");
        assert_eq!(p.test_free_count(), 1, "the spare bot stayed free (no fresh allocation)");
    }

    #[test]
    fn decode_bot_user_id_from_token() {
        use base64::Engine;
        let uid = 1515382284960796774u64;
        let seg = base64::engine::general_purpose::STANDARD_NO_PAD.encode(uid.to_string());
        let token = format!("{seg}.AbCdEf.xyz123-hmac");
        assert_eq!(decode_bot_user_id(&token), Some(uid), "decodes a token-shaped string");
        assert_eq!(
            decode_bot_user_id(&format!("Bot {token}")),
            Some(uid),
            "tolerates a 'Bot ' prefix"
        );
        assert_eq!(decode_bot_user_id("not-base64.x.y"), None, "rejects garbage");
    }

    #[test]
    fn retract_self_ghost_clears_presence_and_sink() {
        // A startup race can mint a ghost for our own puppet bot before its id is in
        // `bot_ids`; retract_self_ghost (called from `ready`) must clean it up.
        let p = pool(1);
        channel(&p, "A", 1, 10);
        {
            let mut guard = p.alloc.lock().unwrap();
            let ch = guard.channels.get_mut("A").unwrap();
            ch.presence.insert(4242);
            ch.user_sinks
                .write()
                .unwrap()
                .insert(4242, Arc::new(NoopCaptureSink));
        }
        p.retract_self_ghost(4242);
        let guard = p.alloc.lock().unwrap();
        let ch = guard.channels.get("A").unwrap();
        assert!(!ch.presence.contains(&4242), "presence entry cleared");
        assert!(
            !ch.user_sinks.read().unwrap().contains_key(&4242),
            "ghost sink cleared"
        );
    }
}
