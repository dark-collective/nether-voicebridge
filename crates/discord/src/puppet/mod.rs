//! Shared puppet pool — a process-wide pool of Discord bots, dynamically
//! allocated across all bridged channels, **per guild** (#46).
//!
//! Each configured token is a generic "voice worker" bot that can join any
//! channel on demand. A Discord bot holds one independent voice connection PER
//! GUILD, and the pool exploits that: the allocation unit is `(bot, guild)` —
//! one bot multiplexes engagements in several guilds at once (each its own
//! songbird `Call`, playback mixer, SSRC, and speaking gates), so N bots serve
//! up to N simultaneous puppets per guild across unlimited guilds. Two bridged
//! channels in the SAME guild still need distinct bots (one Call per guild per
//! bot). The Matrix side of each bridge drives a per-channel [`ChannelView`]
//! (a [`PlaybackRouter`]): `assign` on call membership (a bot joins, takes the
//! participant's display name), `release` on leave, `set_muted` to mirror mute.
//! Per active channel, one bot is the **anchor**: it captures all Discord
//! speakers (Discord→Matrix) and plays that channel's shared **overflow** mix.
//! Speakers beyond the bots available to a channel are mixed into that overflow
//! (no audio lost) and the anchor is relabeled.
//!
//! Allocation (single global lock, [`SharedPool::alloc`]; a bot is free for a
//! channel iff it has no session in that channel's guild, is healthy there, and
//! is a guild member):
//! - A channel's first speaker takes a free bot as the anchor. If none is free
//!   in the guild, a spare (non-anchor) bot is **stolen** from the channel with
//!   the most slack — under per-guild allocation effectively always a channel
//!   of the same guild (its most-recently-assigned dedicated speaker is demoted
//!   to overflow); if no channel has slack, the channel waits until a bot frees
//!   (logged).
//! - Within an active channel, extra speakers take free bots (dedicated puppets)
//!   or fall to overflow; freed bots promote the oldest overflow speaker (FCFS).
//! - Voice-session health (join-timeout quarantine, disconnect recovery) is
//!   tracked per `(bot, guild)` too — a broken voice server in one guild never
//!   sidelines the bot's engagements elsewhere.
//!
//! Echo prevention (constraint #5): each puppet registers its own per-connection
//! SSRC into its channel's [`SharedExclude`] set on `DriverConnect`, so the
//! anchor — which hears every speaker including the puppets' own playback —
//! never re-captures it.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, PoisonError, RwLock};
use std::time::Duration;

use anyhow::{anyhow, Result};
use serenity::{model::prelude::*, prelude::*};
use songbird::{
    driver::{DecodeConfig, DecodeMode},
    SerenityInit,
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use nvb_audio::mixer::PlaybackMixer;
use nvb_audio::routing::CaptureRouter;
use nvb_config::{ApprovalOutcome, PuppetsConfig, TimingConfig};

use crate::{BotIdSet, CaptureSink, OptoutSet, SsrcMap, UserSinkMap};

mod alloc;
mod commands;
mod events;
mod ghosts;
mod router;
mod sinks;
mod voice;
#[cfg(test)]
mod tests;

use self::events::{decode_bot_user_id, PuppetHandler};

/// Sentinel for "no anchor currently elected" in [`ChannelState::anchor`].
const NO_ANCHOR: usize = usize::MAX;

// The quarantine backoff, voice-health sweep, and anchor-idle grace timers
// were hardcoded consts here until #38; they now come from the resolved
// `[timing]` config (`nvb_config::TimingConfig`, defaults identical) and live
// as fields on [`SharedPool`].


/// Speaking-gate poll cadence. One Discord tick; well under the mixer's 150 ms
/// jitter prime, so resuming a paused track on speech onset adds at most one tick
/// of latency and never clips the first phoneme.
const GATE_POLL: Duration = Duration::from_millis(20);

/// Silence hold-off before the speaking gate pauses a playback track. Debounces
/// the natural gaps between words so the Discord "speaking" ring doesn't strobe.
/// songbird appends its own ~100 ms (5-frame) silence tail after the track goes
/// quiet, so the ring clears ~350 ms after speech actually stops.
const GATE_HANGOVER: Duration = Duration::from_millis(250);

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
    /// Users who opted out of being bridged (#63) — never ghosted, never
    /// captured (the demux drops them too).
    optout: OptoutSet,
    /// This channel's anchor marker (slot index, or [`NO_ANCHOR`]). The anchor's
    /// `VoiceReceiver` only captures while it equals its own idx.
    anchor: Arc<AtomicUsize>,
    /// This channel's shared overflow mix — played by the anchor.
    overflow_mixer: Arc<PlaybackMixer>,
}

/// Commands sent from the (sync) allocator to a puppet's async command loop.
enum PuppetCommand {
    /// Join `ctx`'s channel, start playback from the session's `mixer`, set the
    /// nickname. The loop stores the engagement under `ctx`'s guild.
    Assign {
        ctx: Box<ChannelCtx>,
        display_name: String,
        /// The [`PuppetSession`]'s playback buffer (routes already point at it).
        mixer: Arc<PlaybackMixer>,
    },
    /// Become the capture **anchor** of the channel this bot serves in `guild`:
    /// register the Discord→Matrix capture handler + overflow playback, and seed
    /// ghosts for members already present.
    BecomeAnchor { guild: GuildId },
    /// Mirror the Matrix participant's mute onto this bot's voice connection in
    /// `guild`.
    SetMuted { guild: GuildId, muted: bool },
    /// Set this bot's nickname in `guild` (relabel the overflow host). Prefix
    /// applied here.
    SetNickname { guild: GuildId, name: String },
    /// Wear the standby label (`[puppets] standby_nickname`, a WHOLE nickname —
    /// no prefix) while idling as a capture-only anchor in `guild` during the
    /// anchor-idle grace window, so Discord users can tell "the bridge is
    /// idling here briefly" from a bridged participant (#59 UX).
    SetStandbyNickname { guild: GuildId },
    /// Reset the nickname, leave voice, drop the SSRC exclusion — for this bot's
    /// engagement in `guild` only.
    Release { guild: GuildId },
    /// Release **every** guild engagement, and fire `done` once the puppet has
    /// actually left voice. Used by graceful shutdown to **await** teardown.
    ReleaseAndAck { done: oneshot::Sender<()> },
    /// An UNEXPECTED voice `DriverDisconnect` fired for this bot's `guild`
    /// connection (sent by [`DriverFaultHandler`]). `epoch` ties it to the join
    /// generation that registered the handler, so a disconnect from a connection
    /// we have already replaced is ignored. The loop rejoins that guild's channel.
    DriverDropped { guild: GuildId, epoch: u64 },
    /// Reconcile probe: if the bot is out of voice in `guild` — or connected to
    /// a channel OTHER than its assigned one (#62: a moderator force-move keeps
    /// the connection alive, so no `DriverDisconnect` fires) — while the
    /// allocator still believes it active there, rejoin the assigned channel.
    /// Sent by the periodic sweep and (delayed a beat, so songbird's view has
    /// settled) by the gateway voice-state handler when it sees one of our OWN
    /// bots change channel. Deliberately carries NO channel snapshot: each
    /// probe re-evaluates songbird's CURRENT state, so a probe queued behind
    /// an in-flight recovery is a harmless no-op instead of replaying a stale
    /// displacement.
    VerifyConnected { guild: GuildId },
}

impl PuppetCommand {
    /// Short label for drop diagnostics (a silently dropped `Release` leaks a
    /// voice presence — make drops visible).
    fn label(&self) -> &'static str {
        match self {
            Self::Assign { .. } => "Assign",
            Self::BecomeAnchor { .. } => "BecomeAnchor",
            Self::SetMuted { .. } => "SetMuted",
            Self::SetNickname { .. } => "SetNickname",
            Self::SetStandbyNickname { .. } => "SetStandbyNickname",
            Self::Release { .. } => "Release",
            Self::ReleaseAndAck { .. } => "ReleaseAndAck",
            Self::DriverDropped { .. } => "DriverDropped",
            Self::VerifyConnected { .. } => "VerifyConnected",
        }
    }
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

/// One puppet bot's gateway-level state. `cmd_tx`/`cmd_rx` connect the sync
/// allocator to the bot's async serenity task. Everything tied to a specific
/// voice engagement (mixer, name, mute) lives in the bot's [`PuppetSession`]
/// inside [`AllocState`], keyed by `(slot index, guild)`.
struct PuppetSlot {
    token: String,
    cmd_tx: mpsc::Sender<PuppetCommand>,
    /// Taken (once) by [`SharedPool::run`] to drive this puppet's client.
    cmd_rx: StdMutex<Option<mpsc::Receiver<PuppetCommand>>>,
    /// Guilds this bot is a member of (learned from `ready`/`guild_create`). The
    /// allocator only assigns/steals a bot for a channel whose guild is in here,
    /// so a partitioned pool (some bots per guild) works and a missing invite
    /// surfaces as a clear "no puppet" warning instead of a silent join failure.
    guilds: RwLock<HashSet<GuildId>>,
    /// Set true once this puppet's gateway `ready` fired and its command loop is
    /// live. The shutdown drain only awaits `ReleaseAndAck` from connected bots.
    /// Gateway-level (per-bot); voice-session health is per-(bot, guild) in
    /// [`AllocState::health`].
    connected: Arc<AtomicBool>,
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

/// A bot's live engagement in one guild: presence of a `(slot, guild)` entry in
/// [`AllocState::sessions`] IS the "busy" state (#46 — the allocation unit is
/// per-(bot, guild), not per-bot). All fields mutated only under the alloc lock.
struct PuppetSession {
    /// This session's playback buffer — fresh per session, so a reclaimed bot can
    /// never replay a previous participant's audio. Routes point here; the bot's
    /// command loop receives it with the `Assign` and plays it on the Call.
    mixer: Arc<PlaybackMixer>,
    /// Display name of the participant this session voices (`None` while
    /// capture-only) — used to restore the anchor's nickname after the overflow
    /// relabel, and to carry the name on a steal-demotion.
    name: Option<String>,
    /// The participant's intended mute (mirrored from Matrix). Tracked even when
    /// not *applied* — the anchor stays unmuted while hosting overflow.
    muted: bool,
}

impl PuppetSession {
    fn fresh() -> Self {
        Self { mixer: Arc::new(PlaybackMixer::new()), name: None, muted: false }
    }
}

/// `(slot index, guild)` → that bot's live session there. See [`PuppetSession`].
type SessionMap = HashMap<(usize, GuildId), PuppetSession>;

/// Voice-session health for one `(bot, guild)` pair (#46). Join timeouts and
/// `DriverDisconnect`s are properties of ONE guild's voice connection, so
/// quarantine/recovery are scoped there — a broken voice server in one guild
/// must not sideline the bot everywhere (that would recreate the cross-guild
/// starvation this issue removes). Entries OUTLIVE sessions (a bot is
/// quarantined precisely while it has none) and are pruned once fully clear.
#[derive(Default)]
struct SessionHealth {
    /// This guild's `manager.join` timed out — the allocator skips the pair
    /// until a recovery timer clears it after the backoff cooldown.
    unhealthy: bool,
    /// Consecutive join failures here with no intervening success. Drives the
    /// exponential quarantine backoff (`quarantine_base * 2^n`, capped at
    /// `quarantine_max`); reset only by a successful join.
    fail_count: u32,
    /// A mid-session voice-disconnect recovery (rejoin) is in flight for this
    /// guild's Call. Debounces repeated `DriverDisconnect`/sweep signals; gated
    /// on (and cleared by) `should_recover_disconnect` / `finish_recovery`.
    recovering: bool,
}

/// The global allocation state, guarded by [`SharedPool::alloc`].
struct AllocState {
    /// Channel key (bridge label) → its state.
    channels: HashMap<String, ChannelState>,
    /// Monotonic arrival counter for FCFS ordering.
    next_seq: u64,
    /// Live per-(bot, guild) sessions — entry present ⇔ that bot is allocated in
    /// that guild.
    sessions: SessionMap,
    /// Per-(bot, guild) voice-session health. See [`SessionHealth`].
    health: HashMap<(usize, GuildId), SessionHealth>,
}

/// A Discord-side `/bridge approve` invocation (#47 two-sided link auth),
/// forwarded to whoever holds the pending-link table (the Matrix management
/// bot, via an adapter in the binary). The invoker's Manage Server permission
/// has already been verified against the interaction payload; `reply` carries
/// the human-readable outcome back for the interaction follow-up.
pub struct LinkApproval {
    /// The code the invoker typed (as-is; matching is the receiver's job).
    pub code: String,
    /// Guild the command was invoked in — the pending link's guild must match.
    pub guild_id: u64,
    /// Display handle of the approving Discord user, for the audit trail and
    /// the Matrix-side notification (`name (id)`).
    pub invoker: String,
}

/// Approval-command wiring (#47), set once before [`SharedPool::run`] when the
/// management bot requires Discord-side approval. Only `bot_index` registers
/// and serves the `/bridge` command (slash commands are per-application, and
/// N pool bots each registering one would show N duplicates).
struct LinkApprovalCfg {
    bot_index: usize,
    tx: mpsc::UnboundedSender<(LinkApproval, oneshot::Sender<ApprovalOutcome>)>,
}

/// The shared puppet pool. Built once per process; every bridge's Discord side is
/// a [`ChannelView`] into it.
pub struct SharedPool {
    /// Prefix for puppet nicknames (e.g. `"[Matrix] "`; empty disables).
    nickname_prefix: Arc<str>,
    /// Whole nickname a capture-only anchor wears during its idle grace (#59).
    standby_nickname: Arc<str>,
    slots: Vec<PuppetSlot>,
    /// All puppet bot user ids — never minted as ghosts (global; a bot is a bot
    /// in every channel).
    bot_ids: BotIdSet,
    /// Bot user id → slot index (#62): lets the gateway voice-state handler
    /// route an event about one of our OWN bots to that bot's command loop.
    /// Seeded from the tokens at build, confirmed at each `ready`.
    bot_index_by_id: RwLock<HashMap<u64, usize>>,
    alloc: StdMutex<AllocState>,
    /// `/bridge approve` wiring (#47) — `None` unless two-sided link auth is on.
    link_approval: std::sync::OnceLock<LinkApprovalCfg>,
    /// Base quarantine cooldown after a bot's `manager.join` times out (skipped
    /// by the allocator until a recovery timer makes it eligible again). The
    /// cooldown grows exponentially per consecutive failure —
    /// `quarantine_base * 2^n`, capped at `quarantine_max` — so a transient
    /// hop-glitch heals fast while a genuinely wedged gateway backs off instead
    /// of thrashing rejoin attempts. The counter (`SessionHealth::fail_count`)
    /// resets on a successful join. Default 15 s (`[timing]`, #38).
    quarantine_base: Duration,
    /// Cap on the exponential quarantine backoff. Default 120 s.
    quarantine_max: Duration,
    /// How often the reconcile sweep probes each active puppet's voice
    /// connection (a backstop for a silent connection death that never fired a
    /// `DriverDisconnect`). The probe is cheap and only acts when a bot the
    /// allocator believes is live is genuinely out of voice
    /// (`current_channel()` is `None`), so the interval can be generous.
    /// Default 15 s.
    voice_health_sweep: Duration,
    /// How long a capture-only anchor (one held in voice with no Matrix
    /// participant of its own, only Discord speakers remaining) lingers before
    /// it leaves the Discord channel. Long enough to absorb a Matrix membership
    /// flap (which would otherwise churn the songbird/SSRC session) but bounded
    /// so no nameless bot idles in voice once the call is genuinely empty of
    /// Matrix users. Default 90 s.
    anchor_idle_grace: Duration,
    /// Users who opted out of being bridged (#63). Kept in lock-step with
    /// `<data_dir>/optout.toml` (loaded at boot via [`enable_optout`], edited
    /// by the `/bridge optout`/`optin` commands).
    optout_set: OptoutSet,
    /// Path of the persisted opt-out list; unset (tests) disables the commands.
    optout_path: std::sync::OnceLock<String>,
    /// Serializes the opt-out file read-modify-write + in-memory `optout_set`
    /// update so two concurrent `/bridge optout|optin` interactions (serenity
    /// dispatches each event on its own task) can't lose an update or diverge
    /// the file from the set (review F5-2). Mirrors the mgmt bot's `mutate_lock`.
    optout_write_lock: tokio::sync::Mutex<()>,
    /// `ready` refires on reconnect; register the `/bridge` command once.
    command_registered: AtomicBool,
}

impl SharedPool {
    /// Build the pool from the shared `[puppets]` config and the resolved
    /// timing profile. Cheap and synchronous — clients are not started until
    /// [`run`](Self::run), so the returned `Arc` and its [`ChannelView`]s can
    /// be handed to the Matrix side up front.
    pub fn new(cfg: &PuppetsConfig, timing: &TimingConfig) -> Arc<Self> {
        let slots = cfg
            .tokens
            .iter()
            .map(|tok| {
                // Sized for multiplexing: one bot can serve several guilds, each
                // producing assign/mute/nickname bursts; a full channel drops
                // commands (try_send), so keep generous headroom.
                let (cmd_tx, cmd_rx) = mpsc::channel(32);
                PuppetSlot {
                    token: tok.clone(),
                    cmd_tx,
                    cmd_rx: StdMutex::new(Some(cmd_rx)),
                    guilds: RwLock::new(HashSet::new()),
                    connected: Arc::new(AtomicBool::new(false)),
                }
            })
            .collect();

        // Pre-populate bot_ids by decoding each puppet's user id from its token, so
        // echo-exclusion is correct BEFORE any gateway event. Otherwise a puppet
        // lingering in a voice channel (e.g. after an ungraceful crash) could be
        // ghosted by another bot's startup seeding before its own `ready` registers
        // it — an echo (constraint #5). Best-effort; `ready` re-confirms each id.
        let mut bot_ids = HashSet::new();
        let mut bot_index_by_id = HashMap::new();
        for (idx, tok) in cfg.tokens.iter().enumerate() {
            match decode_bot_user_id(tok) {
                Some(id) => {
                    bot_ids.insert(id);
                    bot_index_by_id.insert(id, idx);
                }
                None => tracing::warn!("could not decode a puppet bot user id from its token — relying on `ready` (a brief startup echo window is possible)"),
            }
        }

        Arc::new(Self {
            nickname_prefix: Arc::from(cfg.nickname_prefix.as_str()),
            standby_nickname: Arc::from(cfg.standby_nickname.as_str()),
            slots,
            bot_ids: Arc::new(RwLock::new(bot_ids)),
            bot_index_by_id: RwLock::new(bot_index_by_id),
            alloc: StdMutex::new(AllocState {
                channels: HashMap::new(),
                next_seq: 0,
                sessions: HashMap::new(),
                health: HashMap::new(),
            }),
            link_approval: std::sync::OnceLock::new(),
            quarantine_base: timing.quarantine_base(),
            quarantine_max: timing.quarantine_max(),
            voice_health_sweep: timing.voice_health_sweep(),
            anchor_idle_grace: timing.anchor_idle_grace(),
            optout_set: Arc::new(RwLock::new(HashSet::new())),
            optout_path: std::sync::OnceLock::new(),
            optout_write_lock: tokio::sync::Mutex::new(()),
            command_registered: AtomicBool::new(false),
        })
    }

    /// Lock the allocator state, recovering a poisoned mutex rather than cascading
    /// the panic across every bridge (a panicked holder leaves the state
    /// structurally intact). All production allocator access goes through here.
    fn alloc(&self) -> std::sync::MutexGuard<'_, AllocState> {
        self.alloc.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Load the persisted opt-out list (#63) and arm the `/bridge optout`/
    /// `optin` commands. Call once before [`run`](Self::run); a missing file is
    /// an empty list.
    pub fn enable_optout(&self, path: String) -> Result<()> {
        let list = nvb_config::load_optout(&path)
            .map_err(|e| anyhow!("loading opt-out list: {e}"))?;
        {
            let mut set = self.optout_set.write().unwrap_or_else(PoisonError::into_inner);
            for u in &list.users {
                match u.parse::<u64>() {
                    Ok(id) => {
                        set.insert(id);
                    }
                    Err(_) => tracing::warn!(entry = %u, "ignoring non-numeric opt-out entry"),
                }
            }
        }
        let _ = self.optout_path.set(path);
        Ok(())
    }

    /// Apply a fresh opt-out to the live state (#63): retire the user's ghost
    /// in every bridged channel. Their presence records stay (they ARE still in
    /// the channel); with the ghost sink gone the demux has nothing to feed and
    /// `maybe_release_idle_anchor` sees the channel correctly.
    fn apply_optout(&self, user_id: u64) {
        let mut guard = self.alloc();
        for ch in guard.channels.values_mut() {
            let removed = ch
                .user_sinks
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&user_id)
                .is_some();
            if removed {
                ch.capture_router.release(&user_id.to_string());
                tracing::info!(user_id, bridge = %ch.bridge, "ghost retired — user opted out of bridging");
            }
        }
        drop(guard);
        // A lone opted-out speaker may leave a capture-only anchor idle.
        let bridges: Vec<String> = {
            let guard = self.alloc();
            guard.channels.keys().cloned().collect()
        };
        for b in bridges {
            self.maybe_release_idle_anchor(&b);
        }
    }

    /// Turn on the `/bridge approve` command (#47 two-sided link auth): bot
    /// `bot_index` registers the global slash command at `ready` and forwards
    /// each valid invocation (with a oneshot for the outcome text) over `tx`.
    /// Call before [`run`](Self::run); a second call is a no-op.
    pub fn enable_link_approval(
        &self,
        bot_index: usize,
        tx: mpsc::UnboundedSender<(LinkApproval, oneshot::Sender<ApprovalOutcome>)>,
    ) {
        if self.link_approval.set(LinkApprovalCfg { bot_index, tx }).is_err() {
            tracing::warn!("enable_link_approval called twice — keeping the first wiring");
        }
    }

    /// Which bot registers/serves the `/bridge` command: the approval bot when
    /// two-sided auth is wired (#47), else bot 0 (the `optout`/`optin`
    /// subcommands are useful on any instance, #63).
    fn command_bot_index(&self) -> usize {
        self.link_approval.get().map(|c| c.bot_index).unwrap_or(0)
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
        let mut guard = self.alloc();
        // A silently-replaced ChannelState would orphan the old channel's
        // assignments and routes; with runtime registration (#45) a duplicate
        // label is a real possibility, so refuse it.
        if guard.channels.contains_key(bridge.as_ref()) {
            return Err(anyhow!(
                "channel key '{bridge}' is already registered — bridge labels must be unique"
            ));
        }
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

        // Reconcile sweep: periodically nudge each live puppet to verify it is
        // actually in voice and rejoin if a connection died silently (a backstop
        // for a drop that never fired a `DriverDisconnect`). Stops on shutdown.
        let sweeper = {
            let pool = self.clone();
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(pool.voice_health_sweep);
                tick.tick().await; // consume the immediate first tick
                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        _ = tick.tick() => pool.sweep_voice_health(),
                    }
                }
            })
        };

        tokio::select! {
            r = &mut clients => {
                sweeper.abort();
                return r.map(|_| ());
            }
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
        sweeper.abort();
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

    /// Whether at least one puppet bot has learned (via `ready`/`guild_create`)
    /// that it is a member of `guild_id`. Used by the management bot's `link`
    /// preflight. Guild membership is only known after each bot's gateway
    /// `ready`, so shortly after startup this can be a false negative — callers
    /// should treat `false` as a warning, not a hard failure.
    pub fn any_bot_in_guild(&self, guild_id: u64) -> bool {
        // GuildId is a NonZeroU64 and `GuildId::new(0)` panics; 0 can reach us
        // from user input (`is_numeric_id` accepts "0").
        if guild_id == 0 {
            return false;
        }
        let guild = GuildId::new(guild_id);
        self.slots.iter().any(|s| {
            s.guilds
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .contains(&guild)
        })
    }

    /// Runtime counterpart to [`register_channel`](Self::register_channel) (#45):
    /// release every bot serving this channel, cancel its pending idle-anchor
    /// timer, remove its `ChannelState`, and rebalance so freed bots can activate
    /// waiting channels. Idempotent (unknown label is a no-op) and safe to call
    /// after the Matrix side's own teardown already released participants —
    /// gateway handlers tolerate a vanished channel key, and `ChannelCtx` clones
    /// held by in-flight bot commands keep their `Arc`s alive until they finish.
    /// Per-(bot, guild) `health` entries are intentionally kept: they describe the
    /// guild's voice server, not this channel, and prune on their own schedule.
    pub fn unregister_channel(self: &Arc<Self>, bridge: &str) {
        let mut guard = self.alloc();
        let guard = &mut *guard;
        // Full drain (#59): never keep a capture-only anchor here — the channel
        // is going away and teardown must return connections to baseline.
        self.release_all_locked(guard, bridge, false);
        let Some(mut ch) = guard.channels.remove(bridge) else { return };
        if let Some(token) = ch.anchor_idle_release.take() {
            token.cancel();
        }
        tracing::info!(bridge, "channel unregistered from puppet pool");
        // The bots just freed can activate channels waiting for an anchor.
        self.rebalance(guard);
    }
}

/// A per-bridge view into the [`SharedPool`], implementing [`PlaybackRouter`].
/// All calls delegate to the pool scoped to this bridge's channel key.
pub struct ChannelView {
    pool: Arc<SharedPool>,
    key: Arc<str>,
}
