//! Puppet pool — one Discord bot per Matrix call participant (Phase 7+).
//!
//! Each configured puppet token is a **separate** Discord bot. There is no
//! dedicated listener bot: the pool owns the whole Discord side. The Matrix RTC
//! side drives [`PlaybackRouter`] — `assign` on call membership (the bot joins,
//! takes the participant's display name, present even if silent), `release` on
//! leave, `set_muted` to mirror mute. One assigned puppet is elected the
//! **anchor**: it captures all Discord speakers (Discord→Matrix) and plays the
//! shared **overflow** mix. When members exceed the puppet count, the extras are
//! mixed into that overflow (no audio lost) and the anchor is relabeled.
//!
//! Lifecycle is on-demand *per participant*: a puppet joins/leaves voice only
//! when a Matrix user joins/leaves the call, so nickname edits stay well within
//! Discord's rate limits (no per-utterance churn).
//!
//! Echo prevention (constraint #5): each puppet registers its own SSRC into the
//! shared [`SharedExclude`] set on `DriverConnect`, so the anchor — which hears
//! every speaker in the channel, puppets included — never re-captures puppet
//! playback and loops it back to Matrix.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, PoisonError, RwLock};

use anyhow::{anyhow, Result};
use serenity::{
    async_trait,
    model::prelude::*,
    prelude::*,
};
use songbird::{
    driver::{DecodeConfig, DecodeMode},
    input::{Input, RawAdapter},
    CoreEvent, Event, EventContext, EventHandler as SongbirdEventHandler, SerenityInit,
};
use tokio::sync::mpsc;

use nvb_audio::mixer::PlaybackMixer;
use nvb_audio::routing::{CaptureRouter, ParticipantSink, PlaybackRouter};
use nvb_config::DiscordConfig;

use crate::{
    BotIdSet, CaptureSink, ConnectionLogger, PlaybackSource, SpeakingLogger, SsrcMap, UserSinkMap,
    VoiceReceiver, PLAYBACK_CHANNELS, PLAYBACK_SAMPLE_RATE,
};

/// Sentinel for "no anchor currently elected" in [`PuppetPool::anchor`].
const NO_ANCHOR: usize = usize::MAX;

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

/// Shared, mutable set of bridge-owned SSRCs to exclude from capture.
/// Owned by the pool; shared with every puppet.  Reads happen on the anchor's
/// 50 Hz capture tick (one read-lock per tick), writes only on a puppet
/// join/leave — so an `RwLock` keeps the hot path uncontended.
pub type SharedExclude = Arc<RwLock<HashSet<u32>>>;

/// Commands sent from the (sync) pool allocator to a puppet's async command loop.
enum PuppetCommand {
    /// Join voice, start playback from this puppet's mixer, set the nickname.
    Assign { display_name: String },
    /// Become the capture **anchor**: register the Discord→Matrix capture handler
    /// on this puppet's voice connection (it forwards all Discord speakers).
    BecomeAnchor,
    /// Mirror the Matrix participant's mute state onto this bot's voice connection.
    SetMuted(bool),
    /// Set this puppet's nickname (used to relabel the overflow host to/from the
    /// overflow label). The configured prefix is applied by the puppet.
    SetNickname(String),
    /// Reset the nickname, leave voice, drop the SSRC exclusion.
    Release,
}

/// Where a participant's audio is currently routed.
enum Assign {
    /// A dedicated puppet (index into [`PuppetPool::slots`]).
    Puppet(usize),
    /// Overflow — mixed into the shared overflow mix that the anchor plays. Holds
    /// the speaker's name + mute so they can be **promoted** into a freed slot
    /// (their own named bot) later.
    Overflow { name: String, muted: bool },
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
    /// This slot's current participant display name — used to restore the anchor's
    /// nickname after it has been relabeled to the overflow label. Guarded by the
    /// `assignments` lock.
    name: StdMutex<Option<String>>,
    /// The participant's intended mute state (mirrored from Matrix). Tracked even
    /// when not currently *applied* — the anchor stays unmuted while it hosts the
    /// overflow mix, and reapplies this once overflow clears.
    muted: AtomicBool,
    /// True while assigned to a participant.  Only mutated under the
    /// `assignments` lock, so `Relaxed` ordering is sufficient.
    busy: AtomicBool,
}

/// A swappable audio route: the mixer a participant's frames currently flow into.
/// The pool re-points this (under the `assignments` lock) when a participant is
/// promoted from the overflow mix into a freed puppet slot; the held
/// [`RoutedSink`] then pushes to the new mixer with no change on the matrix side.
type Route = Arc<StdMutex<Arc<PlaybackMixer>>>;

/// The puppet pool / playback router.
pub struct PuppetPool {
    /// Shared overflow mix — un-slotted speakers (members beyond the puppet
    /// count). Played by the current anchor's `PlaybackSource`.
    overflow_mixer: Arc<PlaybackMixer>,
    exclude: SharedExclude,
    /// Prefix for puppet nicknames (e.g. `"[Matrix] "`; empty disables).
    nickname_prefix: Arc<str>,
    slots: Vec<PuppetSlot>,
    /// Participant key → current routing.  The single source of truth for
    /// allocation; guarded so `assign`/`release` stay race-free.
    assignments: StdMutex<HashMap<String, Assign>>,
    /// Participant key → its swappable audio route handle. The pool re-points a
    /// route on promotion; the matrix-side [`RoutedSink`] holds the same handle.
    /// Only touched under the `assignments` lock.
    routes: StdMutex<HashMap<String, Route>>,
    /// Slot index of the current capture **anchor** ([`NO_ANCHOR`] if none) — the
    /// one puppet that captures Discord→Matrix. Shared with each anchor's
    /// `VoiceReceiver`, which only forwards while this equals its own slot, so a
    /// handoff is seamless (no doubled or dropped forwarding during the overlap).
    /// Written only under the `assignments` lock.
    anchor: Arc<AtomicUsize>,
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
                    name: StdMutex::new(None),
                    muted: AtomicBool::new(false),
                    busy: AtomicBool::new(false),
                }
            })
            .collect();

        Arc::new(Self {
            overflow_mixer: Arc::new(PlaybackMixer::new()),
            exclude: Arc::new(RwLock::new(HashSet::new())),
            nickname_prefix: Arc::from(cfg.nickname_prefix.as_str()),
            slots,
            assignments: StdMutex::new(HashMap::new()),
            routes: StdMutex::new(HashMap::new()),
            anchor: Arc::new(AtomicUsize::new(NO_ANCHOR)),
        })
    }

    /// Start every puppet's serenity client and drive them to completion.
    /// Returns on the first client error.  Called once from `nvb_discord::run`.
    #[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %bridge))]
    pub async fn run_puppets(
        self: &Arc<Self>,
        guild_id: GuildId,
        channel_id: ChannelId,
        bridge: Arc<str>,
        capture_router: Arc<dyn CaptureRouter>,
        capture_sink: Option<Arc<CaptureSink>>,
    ) -> Result<()> {
        if self.slots.is_empty() {
            tracing::warn!("no puppet tokens configured — Discord side has no bots");
            return Ok(());
        }

        let intents = GatewayIntents::GUILDS | GatewayIntents::GUILD_VOICE_STATES;
        let mut futs = Vec::with_capacity(self.slots.len());

        // Shared Discord→Matrix (ghost) capture state, created once and cloned to
        // every puppet handler: the anchor's `VoiceReceiver` demuxes per speaker
        // using `ssrc_map` + `user_sinks`; the slot-0 voice-state handler drives
        // ghost presence into `user_sinks` (skipping `bot_ids` — our own puppets).
        let ssrc_map: SsrcMap = Arc::new(RwLock::new(HashMap::new()));
        let user_sinks: UserSinkMap = Arc::new(RwLock::new(HashMap::new()));
        let bot_ids: BotIdSet = Arc::new(RwLock::new(HashSet::new()));

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
                capture_router: capture_router.clone(),
                capture_sink: capture_sink.clone(),
                ssrc_map: ssrc_map.clone(),
                user_sinks: user_sinks.clone(),
                bot_ids: bot_ids.clone(),
                anchor: self.anchor.clone(),
                overflow_mixer: self.overflow_mixer.clone(),
                current_ssrc: slot.current_ssrc.clone(),
                cmd_rx: StdMutex::new(Some(rx)),
            };
            let token = slot.token.clone();

            futs.push(async move {
                // Puppets both PLAY their participant and (when elected anchor)
                // CAPTURE Discord speakers, so they need Opus decode / DAVE-receive
                // (`DecodeMode::Decode`) — any puppet can become the anchor.
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

        futures_util::future::try_join_all(futs).await?;
        Ok(())
    }

    /// Get or create the route handle for `key`, re-pointing it at `mixer`.
    fn route_to(
        routes: &mut HashMap<String, Route>,
        key: &str,
        mixer: Arc<PlaybackMixer>,
    ) -> Route {
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
    fn reconcile_anchor_label(&self, map: &HashMap<String, Assign>) {
        let anchor = self.anchor.load(Ordering::Relaxed);
        if anchor == NO_ANCHOR {
            return;
        }
        let name = if overflow_count(map) > 0 {
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
    /// participant's intended mute. (Bug-1 fix: the overflow host never self-mutes.)
    fn reconcile_anchor_mute(&self, map: &HashMap<String, Assign>) {
        let anchor = self.anchor.load(Ordering::Relaxed);
        if anchor == NO_ANCHOR {
            return;
        }
        let applied = overflow_count(map) == 0 && self.slots[anchor].muted.load(Ordering::Relaxed);
        let _ = self.slots[anchor]
            .cmd_tx
            .try_send(PuppetCommand::SetMuted(applied));
    }
}

/// Count overflow participants (those routed to the shared mix, no dedicated puppet).
fn overflow_count(map: &HashMap<String, Assign>) -> usize {
    map.values()
        .filter(|a| matches!(a, Assign::Overflow { .. }))
        .count()
}

/// A [`ParticipantSink`] whose target mixer the pool can swap (for promotion).
struct RoutedSink {
    route: Route,
    key: String,
}

impl ParticipantSink for RoutedSink {
    fn push(&self, mono: &[i16]) {
        let mixer = self
            .route
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        mixer.push(&self.key, mono);
    }
}

impl PlaybackRouter for PuppetPool {
    fn assign(&self, key: &str, display_name: &str) -> Arc<dyn ParticipantSink> {
        let mut map = self.assignments.lock().unwrap_or_else(PoisonError::into_inner);
        let mut routes = self.routes.lock().unwrap_or_else(PoisonError::into_inner);

        // Idempotent: a re-subscribe for the same participant reuses its route.
        if map.contains_key(key) {
            if let Some(r) = routes.get(key) {
                return Arc::new(RoutedSink { route: r.clone(), key: key.to_owned() });
            }
        }

        // Claim the first free puppet (mutated under the lock → Relaxed is fine).
        for (idx, slot) in self.slots.iter().enumerate() {
            if !slot.busy.swap(true, Ordering::Relaxed) {
                map.insert(key.to_owned(), Assign::Puppet(idx));
                *slot.name.lock().unwrap_or_else(PoisonError::into_inner) =
                    Some(display_name.to_owned());
                slot.muted.store(false, Ordering::Relaxed);
                if slot
                    .cmd_tx
                    .try_send(PuppetCommand::Assign { display_name: display_name.to_owned() })
                    .is_err()
                {
                    tracing::warn!(idx, "puppet command channel full/closed on assign");
                }
                tracing::info!(key, display_name, idx, "assigned participant to puppet");
                // Elect this puppet as the capture anchor if there isn't one.
                if self.anchor.load(Ordering::Relaxed) == NO_ANCHOR {
                    self.anchor.store(idx, Ordering::Relaxed);
                    let _ = slot.cmd_tx.try_send(PuppetCommand::BecomeAnchor);
                    tracing::info!(idx, "puppet elected capture anchor");
                }
                let route = Self::route_to(&mut routes, key, slot.mixer.clone());
                return Arc::new(RoutedSink { route, key: key.to_owned() });
            }
        }

        // Pool exhausted → overflow into the shared overflow mix (played by the
        // anchor). On the first overflow speaker, relabel the anchor and force it
        // unmuted (it must keep transmitting the overflow mix).
        let was_overflowing = overflow_count(&map) > 0;
        map.insert(
            key.to_owned(),
            Assign::Overflow { name: display_name.to_owned(), muted: false },
        );
        tracing::info!(key, display_name, "puppet pool exhausted — overflow to anchor mix");
        let route = Self::route_to(&mut routes, key, self.overflow_mixer.clone());
        if !was_overflowing {
            self.reconcile_anchor_label(&map);
            self.reconcile_anchor_mute(&map);
        }
        Arc::new(RoutedSink { route, key: key.to_owned() })
    }

    fn set_muted(&self, key: &str, muted: bool) {
        let mut map = self.assignments.lock().unwrap_or_else(PoisonError::into_inner);
        match map.get_mut(key) {
            Some(Assign::Puppet(idx)) => {
                let idx = *idx;
                self.slots[idx].muted.store(muted, Ordering::Relaxed);
                if idx == self.anchor.load(Ordering::Relaxed) {
                    // The anchor's applied mute depends on whether it hosts overflow.
                    self.reconcile_anchor_mute(&map);
                } else {
                    let _ = self.slots[idx].cmd_tx.try_send(PuppetCommand::SetMuted(muted));
                }
            }
            // Overflow speaker has no dedicated bot yet — just remember the state
            // so a later promotion applies it.
            Some(Assign::Overflow { muted: m, .. }) => *m = muted,
            None => {}
        }
    }

    fn release(&self, key: &str) {
        let mut map = self.assignments.lock().unwrap_or_else(PoisonError::into_inner);
        let mut routes = self.routes.lock().unwrap_or_else(PoisonError::into_inner);
        match map.remove(key) {
            Some(Assign::Puppet(idx)) => {
                routes.remove(key);
                // If an overflow speaker is waiting, PROMOTE them into this freed
                // slot (reuse the bot — no leave/rejoin) instead of releasing it.
                let promote = map.iter().find_map(|(k, a)| match a {
                    Assign::Overflow { name, muted } => Some((k.clone(), name.clone(), *muted)),
                    _ => None,
                });
                match promote {
                    Some((yk, yname, ymuted)) => {
                        map.insert(yk.clone(), Assign::Puppet(idx));
                        // Re-route Y from the overflow mix to this slot's mixer.
                        self.overflow_mixer.remove(&yk);
                        self.slots[idx].mixer.clear();
                        Self::route_to(&mut routes, &yk, self.slots[idx].mixer.clone());
                        *self.slots[idx].name.lock().unwrap_or_else(PoisonError::into_inner) =
                            Some(yname.clone());
                        self.slots[idx].muted.store(ymuted, Ordering::Relaxed);
                        tracing::info!(key, promoted = %yk, idx, "promoted overflow speaker into freed slot");
                        let is_anchor = idx == self.anchor.load(Ordering::Relaxed);
                        let overflow_cleared = overflow_count(&map) == 0;
                        if is_anchor {
                            // The anchor's name/mute are driven by reconcile (it may
                            // still be hosting overflow); avoid a redundant edit.
                            if overflow_cleared {
                                self.reconcile_anchor_label(&map);
                                self.reconcile_anchor_mute(&map);
                            }
                        } else {
                            // A normal named puppet now: set its name + Y's mute.
                            let _ = self.slots[idx].cmd_tx.try_send(PuppetCommand::SetNickname(yname));
                            let _ = self.slots[idx].cmd_tx.try_send(PuppetCommand::SetMuted(ymuted));
                            if overflow_cleared {
                                self.reconcile_anchor_label(&map);
                                self.reconcile_anchor_mute(&map);
                            }
                        }
                    }
                    None => {
                        // No overflow waiting → free the slot.
                        self.slots[idx].mixer.clear();
                        self.slots[idx].busy.store(false, Ordering::Relaxed);
                        *self.slots[idx].name.lock().unwrap_or_else(PoisonError::into_inner) = None;
                        let _ = self.slots[idx].cmd_tx.try_send(PuppetCommand::Release);
                        tracing::info!(key, idx, "released puppet");
                        // Hand the anchor off to another busy puppet if needed.
                        if self.anchor.load(Ordering::Relaxed) == idx {
                            let next = map.values().find_map(|a| match a {
                                Assign::Puppet(i) => Some(*i),
                                Assign::Overflow { .. } => None,
                            });
                            match next {
                                Some(new_idx) => {
                                    self.anchor.store(new_idx, Ordering::Relaxed);
                                    let _ = self.slots[new_idx]
                                        .cmd_tx
                                        .try_send(PuppetCommand::BecomeAnchor);
                                    tracing::info!(old = idx, new = new_idx, "capture anchor handed off");
                                    // Reached only with no overflow (else we'd have
                                    // promoted), so the new anchor keeps its own name
                                    // and already-applied mute — no reconcile needed.
                                }
                                None => {
                                    self.anchor.store(NO_ANCHOR, Ordering::Relaxed);
                                    tracing::info!(idx, "capture anchor released — no puppets left");
                                }
                            }
                        }
                    }
                }
            }
            Some(Assign::Overflow { .. }) => {
                self.overflow_mixer.remove(key);
                routes.remove(key);
                tracing::info!(key, "released overflow participant from overflow mix");
                // Only relabel/unmute the anchor when overflow actually cleared
                // (avoids spurious nickname edits while overflow is still active).
                if overflow_count(&map) == 0 {
                    self.reconcile_anchor_label(&map);
                    self.reconcile_anchor_mute(&map);
                }
            }
            None => {}
        }
    }

    fn release_all(&self) {
        let mut map = self.assignments.lock().unwrap_or_else(PoisonError::into_inner);
        let mut routes = self.routes.lock().unwrap_or_else(PoisonError::into_inner);
        for (_key, assign) in map.drain() {
            if let Assign::Puppet(idx) = assign {
                let slot = &self.slots[idx];
                slot.mixer.clear();
                slot.busy.store(false, Ordering::Relaxed);
                *slot.name.lock().unwrap_or_else(PoisonError::into_inner) = None;
                let _ = slot.cmd_tx.try_send(PuppetCommand::Release);
            }
        }
        routes.clear();
        self.anchor.store(NO_ANCHOR, Ordering::Relaxed);
        self.overflow_mixer.clear();
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
    /// Routes each Discord speaker to their Matrix ghost (Phase 8). The slot-0
    /// handler drives presence (`assign`/`release`); the anchor demuxes audio
    /// into the sinks it returns.
    capture_router: Arc<dyn CaptureRouter>,
    /// Optional WAV capture (used by the anchor's `VoiceReceiver`).
    capture_sink: Option<Arc<CaptureSink>>,
    /// SSRC → Discord user id (anchor demux attribution).
    ssrc_map: SsrcMap,
    /// Discord user id → ghost sink (presence + anchor demux target).
    user_sinks: UserSinkMap,
    /// Our own puppet bot ids — never minted as ghosts.
    bot_ids: BotIdSet,
    /// Shared current-anchor marker (gates this puppet's capture forwarding).
    anchor: Arc<AtomicUsize>,
    /// Shared overflow mix — the anchor also plays this (un-slotted speakers).
    overflow_mixer: Arc<PlaybackMixer>,
    current_ssrc: Arc<StdMutex<Option<u32>>>,
    cmd_rx: StdMutex<Option<mpsc::Receiver<PuppetCommand>>>,
}

#[async_trait]
impl EventHandler for PuppetHandler {
    #[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %self.bridge))]
    async fn ready(&self, ctx: Context, ready: Ready) {
        tracing::info!(idx = self.idx, bot_user = %ready.user.name, "puppet connected");

        // Record this puppet's own user id so it is never minted as a ghost
        // (a bridge-owned bot in the channel is not a Discord human). Safe to
        // re-insert on a reconnect `ready`.
        self.bot_ids
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(ready.user.id.get());

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

        // Startup reset (first `ready` only — must NOT run on reconnect, which
        // could kick an active puppet out of voice): clear any stale guild
        // nickname and lingering voice session left by a previous crash/kill, so
        // restarts don't leave confusing `[Matrix] …` zombie bots behind.
        if let Err(e) = self.guild_id.edit_nickname(&ctx.http, None).await {
            tracing::debug!(idx = self.idx, error = %e, "startup nickname reset failed (may already be clear)");
        }
        if let Some(manager) = songbird::get(&ctx).await {
            let _ = manager.remove(self.guild_id).await;
        }

        tokio::spawn(puppet_command_loop(
            self.bridge.clone(),
            ctx,
            self.idx,
            self.guild_id,
            self.channel_id,
            self.mixer.clone(),
            self.exclude.clone(),
            self.nickname_prefix.clone(),
            self.capture_sink.clone(),
            self.ssrc_map.clone(),
            self.user_sinks.clone(),
            self.anchor.clone(),
            self.overflow_mixer.clone(),
            self.current_ssrc.clone(),
            rx,
        ));
    }

    /// Drive ghost **presence** off Discord voice-channel membership (Phase 8).
    /// Only the slot-0 handler acts (voice-state events reach every puppet's
    /// gateway, but presence must be decided once). A non-bot user joining the
    /// target channel gets a ghost; leaving (or moving away) retires it.
    #[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %self.bridge))]
    async fn voice_state_update(&self, ctx: Context, _old: Option<VoiceState>, new: VoiceState) {
        if self.idx != 0 || new.guild_id != Some(self.guild_id) {
            return;
        }
        let user_id = new.user_id.get();
        if new.channel_id == Some(self.channel_id) {
            self.ensure_ghost(&ctx, user_id, new.member.as_ref()).await;
        } else {
            self.drop_ghost(user_id);
        }
    }

    /// On (re)connect, seed ghosts for everyone **already** in the target voice
    /// channel — `voice_state_update` only fires on later changes, so without this
    /// users present before the bridge started would never get a ghost. Slot-0
    /// only; idempotent (re-runs and reconnects are no-ops via `ensure_ghost`).
    #[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %self.bridge))]
    async fn guild_create(&self, ctx: Context, guild: serenity::model::guild::Guild, _is_new: Option<bool>) {
        if self.idx != 0 || guild.id != self.guild_id {
            return;
        }
        let present: Vec<u64> = guild
            .voice_states
            .iter()
            .filter(|(_, vs)| vs.channel_id == Some(self.channel_id))
            .map(|(uid, _)| uid.get())
            .collect();
        tracing::info!(count = present.len(), "guild_create: seeding ghosts for existing voice members");
        for uid in present {
            // voice_states carries no member object; ensure_ghost resolves the
            // name (and bot check) via a member lookup when needed.
            self.ensure_ghost(&ctx, uid, None).await;
        }
    }
}

impl PuppetHandler {
    /// Mint a ghost for a Discord user in the target channel if they don't have
    /// one yet. Skips bridge-owned puppets and any other bot. Resolves the
    /// display name from the supplied `Member` if present, else via a lookup.
    async fn ensure_ghost(&self, ctx: &Context, user_id: u64, member: Option<&Member>) {
        if self
            .bot_ids
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&user_id)
            || member.is_some_and(|m| m.user.bot)
        {
            return;
        }
        if self
            .user_sinks
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(&user_id)
        {
            return; // already has a ghost
        }

        // Resolve display name + Discord avatar URL from the same Member (the one
        // the event carried, or a lookup when absent — e.g. the guild_create path).
        let (name, avatar_url) = match member {
            Some(m) => (member_display_name(m), m.user.avatar_url()),
            None => match self.guild_id.member(&ctx.http, UserId::new(user_id)).await {
                Ok(m) if m.user.bot => return, // a bot discovered via lookup
                Ok(m) => (member_display_name(&m), m.user.avatar_url()),
                Err(e) => {
                    tracing::debug!(user_id, error = %e, "member lookup failed; using id as name");
                    (user_id.to_string(), None)
                }
            },
        };

        let sink =
            self.capture_router
                .assign(&user_id.to_string(), &name, avatar_url.as_deref());
        self.user_sinks
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(user_id, sink);
        tracing::info!(user_id, display_name = %name, "Discord user present — ghost assigned");
    }

    /// Retire a user's ghost if they had one (left or moved out of the channel).
    fn drop_ghost(&self, user_id: u64) {
        let had = self
            .user_sinks
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&user_id)
            .is_some();
        if had {
            self.capture_router.release(&user_id.to_string());
            tracing::info!(user_id, "Discord user left — ghost released");
        }
    }
}

/// Resolve a Discord member's display name: server nick → global name → username.
fn member_display_name(m: &Member) -> String {
    m.nick
        .clone()
        .or_else(|| m.user.global_name.clone())
        .unwrap_or_else(|| m.user.name.clone())
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
    capture_sink: Option<Arc<CaptureSink>>,
    ssrc_map: SsrcMap,
    user_sinks: UserSinkMap,
    anchor: Arc<AtomicUsize>,
    overflow_mixer: Arc<PlaybackMixer>,
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
                    &ssrc_map,
                    &nickname_prefix,
                    &current_ssrc,
                    &display_name,
                )
                .await;
            }
            PuppetCommand::BecomeAnchor => {
                puppet_become_anchor(
                    &bridge,
                    &ctx,
                    idx,
                    guild_id,
                    &exclude,
                    ssrc_map.clone(),
                    user_sinks.clone(),
                    capture_sink.clone(),
                    anchor.clone(),
                    &overflow_mixer,
                )
                .await;
            }
            PuppetCommand::SetMuted(muted) => {
                puppet_set_muted(&ctx, idx, guild_id, muted).await;
            }
            PuppetCommand::SetNickname(name) => {
                let nick = build_nickname(&nickname_prefix, &name);
                if let Err(e) = guild_id.edit_nickname(&ctx.http, Some(nick.as_str())).await {
                    tracing::warn!(idx, error = %e, "failed to relabel puppet nickname");
                } else {
                    tracing::info!(idx, nick = %nick, "puppet relabeled");
                }
            }
            PuppetCommand::Release => {
                puppet_leave(&ctx, idx, guild_id, &exclude, &current_ssrc).await;
            }
        }
    }
    tracing::info!(idx, "puppet command loop exited");
}

/// Register the Discord→Matrix capture handlers on this puppet's voice connection
/// (it has become the anchor). The `VoiceReceiver` demuxes each non-excluded
/// Discord speaker into their ghost sink; only one puppet holds these at a time.
/// If this puppet hasn't joined voice yet, the command is dropped — the pool only
/// elects an anchor among assigned puppets, which join on `Assign`.
#[allow(clippy::too_many_arguments)]
async fn puppet_become_anchor(
    bridge: &Arc<str>,
    ctx: &Context,
    idx: usize,
    guild_id: GuildId,
    exclude: &SharedExclude,
    ssrc_map: SsrcMap,
    user_sinks: UserSinkMap,
    capture_sink: Option<Arc<CaptureSink>>,
    anchor: Arc<AtomicUsize>,
    overflow_mixer: &Arc<PlaybackMixer>,
) {
    let Some(manager) = songbird::get(ctx).await else {
        return;
    };
    let Some(call_lock) = manager.get(guild_id) else {
        tracing::warn!(idx, "anchor command but puppet not in voice yet");
        return;
    };
    let mut call = call_lock.lock().await;
    // The VoiceReceiver only captures while `anchor == idx`, so even if a previous
    // anchor's handler is still briefly registered during handoff, exactly one
    // puppet captures at a time.
    call.add_global_event(
        CoreEvent::VoiceTick.into(),
        VoiceReceiver::new(
            bridge.clone(),
            exclude.clone(),
            ssrc_map.clone(),
            user_sinks,
            capture_sink,
            anchor,
            idx,
        ),
    );
    // The anchor also plays the shared overflow mix (un-slotted speakers), in
    // addition to its own participant — so overflow audio is never lost. Silent
    // when nobody is overflowed.
    let overflow_src = PlaybackSource::new(overflow_mixer.clone());
    let overflow_input =
        Input::from(RawAdapter::new(overflow_src, PLAYBACK_SAMPLE_RATE, PLAYBACK_CHANNELS));
    let _overflow_handle = call.play_input(overflow_input);
    // NOTE: `SpeakingLogger` (SSRC→user map) and `ConnectionLogger` are registered
    // in `puppet_join` *before* the connection, not here — Discord emits speaking
    // announcements around connect, so a handler attached after join misses them
    // (the same "register before join" lesson as the DriverConnect SSRC registrar).
    tracing::info!(idx, "anchor capture active — Discord→Matrix demux from this puppet");
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
    ssrc_map: &SsrcMap,
    nickname_prefix: &str,
    current_ssrc: &Arc<StdMutex<Option<u32>>>,
    display_name: &str,
) {
    let Some(manager) = songbird::get(ctx).await else {
        tracing::error!(idx, "songbird not registered on puppet client");
        return;
    };

    // Register handlers and start the playback source on the Call *before*
    // connecting. Two events fire *during* `manager.join().await` and are missed
    // if a handler is attached after the await returns:
    //   - `DriverConnect` carries our own SSRC (echo-exclusion) — see Phase 7.
    //   - `SpeakingStateUpdate` carries other users' SSRC→user_id; Discord emits
    //     these around connect, so registering `SpeakingLogger` late loses the
    //     initial batch and a Matrix listener can't hear already-present speakers
    //     until they speak again (Phase 8 live-test fix).
    // Events go to the driver's persistent event task, so pre-registration is
    // safe; the Call from `get_or_insert` is the same one `join` connects. Every
    // assigned puppet registers `SpeakingLogger`/`ConnectionLogger` (all are in
    // the channel; they share `ssrc_map`, redundant writes are harmless).
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
        call.add_global_event(
            CoreEvent::SpeakingStateUpdate.into(),
            SpeakingLogger::new(bridge.clone(), ssrc_map.clone()),
        );
        for ev in [
            CoreEvent::DriverReconnect,
            CoreEvent::DriverDisconnect,
            CoreEvent::ClientDisconnect,
        ] {
            call.add_global_event(ev.into(), ConnectionLogger::new(bridge.clone(), ssrc_map.clone()));
        }
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
    use super::*;

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

    // Samples a buffer must accumulate before the mixer releases it (priming).
    const PRIME: usize = 150 * 48;

    #[test]
    fn routed_sink_follows_swapped_mixer() {
        // A RoutedSink pushes to whatever mixer its route points at; swapping the
        // route (as promotion does) redirects subsequent frames with no change on
        // the matrix side.
        let a = Arc::new(PlaybackMixer::new());
        let b = Arc::new(PlaybackMixer::new());
        let route: Route = Arc::new(StdMutex::new(a.clone()));
        let sink = RoutedSink { route: route.clone(), key: "k".to_owned() };

        sink.push(&vec![100i16; PRIME]);
        assert!(a.pop_10ms_mixed().iter().all(|&s| s == 100), "frames went to A");

        // Promote: swap the route to B. B was empty, so receiving the frames
        // proves the swap redirected them (A keeps its own earlier buffer).
        *route.lock().unwrap() = b.clone();
        sink.push(&vec![80i16; PRIME]);
        assert!(b.pop_10ms_mixed().iter().all(|&s| s == 80), "frames follow to B after swap");
    }
}
