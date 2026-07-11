//! Discord-side fake participants.
//!
//! * [`DiscordSpeaker`] (D1): a hand-created harness bot that joins the test
//!   voice channel and plays a deterministic probe — the "Discord human" the
//!   bridge should mint a Matrix ghost for.
//! * [`DiscordListener`] (D2): a harness bot that RECEIVES in the channel and
//!   records selected users (the bridge's puppets) to mono PCM for the audio
//!   oracle — the far-side ear of the Matrix→Discord direction.
//! * [`DiscordMod`] (D3): the adversarial moderator — a REST-only bot that
//!   voice-kicks / force-moves a bridge puppet to exercise the recovery paths.
//!
//! Both mirror the bridge's own songbird idioms (pinned versions, constraint
//! #7): an explicit `Songbird` manager attached to a serenity client; playback
//! through `RawAdapter` over f32-LE PCM (the path the workspace's symphonia
//! `pcm` feature guarantees — file/WAV decoding is NOT enabled); receive via
//! `DecodeMode::Decode` + `VoiceTick`, exactly like the anchor's capture.

use super::ParticipantDriver;
use anyhow::{Context as _, Result, anyhow, bail};
use async_trait::async_trait;
use serenity::builder::EditMember;
use serenity::client::{Context, EventHandler};
use serenity::model::gateway::{GatewayIntents, Ready};
use serenity::model::id::{ChannelId, GuildId, UserId};
use songbird::input::{Input, RawAdapter};
use songbird::model::payload::Speaking;
use songbird::{CoreEvent, Event, EventContext, EventHandler as SongbirdEventHandler};
use std::collections::{HashMap, HashSet};
use std::io::{Read, Seek, SeekFrom};
use std::sync::{Arc, Mutex as StdMutex, PoisonError};
use std::time::Duration;

/// A finite, seekable `MediaSource` over f32-LE PCM bytes — unlike the
/// bridge's live `PlaybackSource`, this one ENDS (EOF stops the track).
struct ProbeSource {
    bytes: std::io::Cursor<Vec<u8>>,
}

impl ProbeSource {
    /// Mono 48 kHz i16 samples → f32-LE bytes.
    fn from_mono_i16(samples: &[i16]) -> Self {
        let mut bytes = Vec::with_capacity(samples.len() * 4);
        for s in samples {
            bytes.extend_from_slice(&(*s as f32 / 32768.0).to_le_bytes());
        }
        Self { bytes: std::io::Cursor::new(bytes) }
    }
}

impl Read for ProbeSource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.bytes.read(buf)
    }
}
impl Seek for ProbeSource {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.bytes.seek(pos)
    }
}
impl songbird::input::core::io::MediaSource for ProbeSource {
    fn is_seekable(&self) -> bool {
        true
    }
    fn byte_len(&self) -> Option<u64> {
        Some(self.bytes.get_ref().len() as u64)
    }
}

struct SpeakerHandler {
    ready: Arc<std::sync::atomic::AtomicBool>,
}

#[serenity::async_trait]
impl EventHandler for SpeakerHandler {
    async fn ready(&self, _ctx: Context, _ready: Ready) {
        self.ready.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

pub struct DiscordSpeaker {
    guild: GuildId,
    channel: ChannelId,
    songbird: Arc<songbird::Songbird>,
    shard: Arc<serenity::gateway::ShardManager>,
    /// `None` once [`DiscordSpeaker::shutdown`] has taken + awaited it on the
    /// graceful path; `Some` otherwise, so `Drop` can abort it (#72 F6-4).
    client_task: Option<tokio::task::JoinHandle<()>>,
}

impl DiscordSpeaker {
    /// Connect the speaker bot's gateway (does not join voice yet).
    pub async fn connect(token: &str, guild_id: &str, channel_id: &str) -> Result<Self> {
        let intents = GatewayIntents::GUILDS | GatewayIntents::GUILD_VOICE_STATES;
        let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // An explicit manager (instead of `register_songbird`) so the harness
        // holds it directly rather than fishing it out of the TypeMap.
        let songbird = songbird::Songbird::serenity();
        let mut client = serenity::Client::builder(token, intents)
            .event_handler(SpeakerHandler { ready: ready.clone() })
            .voice_manager_arc(songbird.clone())
            .await
            .context("building speaker client")?;
        let shard = client.shard_manager.clone();
        let client_task = tokio::spawn(async move {
            if let Err(e) = client.start().await {
                tracing::warn!(error = %e, "discord speaker client ended");
            }
        });
        crate::observers::await_flag(&ready, Duration::from_secs(30), "speaker gateway").await?;

        Ok(Self {
            guild: GuildId::new(guild_id.parse().context("guild id")?),
            channel: ChannelId::new(channel_id.parse().context("channel id")?),
            songbird,
            shard,
            client_task: Some(client_task),
        })
    }

    /// Play the warm-up lead-in then the seeded deterministic probe (Phase B
    /// synthesis — the same signal the audio oracle aligns against) and wait
    /// out its duration.
    pub async fn speak_probe(&self, seed: u64, dur_ms: u32) -> Result<()> {
        let call = self
            .songbird
            .get(self.guild)
            .ok_or_else(|| anyhow!("speaker is not in voice (call missing)"))?;
        let mut samples = super::warmup_lead_in();
        samples.extend(nvb_audio::probe::synth_probe_mono_48k(seed, dur_ms));
        let playout_ms = (samples.len() / 48) as u64;
        let input = Input::from(RawAdapter::new(ProbeSource::from_mono_i16(&samples), 48_000, 1));
        let handle = {
            let mut call = call.lock().await;
            call.play_input(input)
        };
        // Let the whole lead-in + probe play out (plus scheduler slack), then stop.
        tokio::time::sleep(Duration::from_millis(playout_ms + 500)).await;
        let _ = handle.stop();
        Ok(())
    }

    /// Disconnect the gateway entirely (end of scenario).
    pub async fn shutdown(mut self) {
        let _ = self.leave().await;
        self.shard.shutdown_all().await;
        if let Some(task) = self.client_task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for DiscordSpeaker {
    fn drop(&mut self) {
        // Best-effort: if `shutdown()` never ran (e.g. the scenario was
        // cancelled by the runner's outer timeout mid-await — scenario.rs's
        // `tokio::time::timeout` around `(def.run)(ctx)`), abort the detached
        // gateway task rather than leak the speaker's voice session into the
        // next scenario (#72 F6-4).
        crate::observers::abort_if_present(&mut self.client_task);
    }
}

#[async_trait]
impl ParticipantDriver for DiscordSpeaker {
    async fn join(&mut self) -> Result<()> {
        self.songbird
            .join(self.guild, self.channel)
            .await
            .map_err(|e| anyhow!("speaker failed to join voice: {e}"))?;
        Ok(())
    }

    async fn leave(&mut self) -> Result<()> {
        if self.songbird.get(self.guild).is_some() {
            self.songbird
                .remove(self.guild)
                .await
                .map_err(|e| anyhow!("speaker failed to leave voice: {e}"))?;
        }
        Ok(())
    }
}

// ─── DiscordListener (D2) ────────────────────────────────────────────────────────

/// SSRC → Discord user id map, filled from `SpeakingStateUpdate` (the same
/// trick the bridge's anchor uses to attribute capture).
type SsrcMap = Arc<StdMutex<HashMap<u32, u64>>>;
/// User id → recorded mono 48 kHz samples.
type Recordings = Arc<StdMutex<HashMap<u64, Vec<i16>>>>;

/// Records the SSRC→user mapping from speaking-state updates.
struct ListenerSpeakingMap {
    ssrc_map: SsrcMap,
}

#[serenity::async_trait]
impl SongbirdEventHandler for ListenerSpeakingMap {
    async fn act(&self, ctx: &EventContext<'_>) -> Option<Event> {
        if let EventContext::SpeakingStateUpdate(Speaking { ssrc, user_id: Some(uid), .. }) = ctx {
            self.ssrc_map
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(*ssrc, uid.0);
            tracing::debug!(ssrc, user_id = uid.0, "listener SSRC → user mapping");
        }
        None
    }
}

/// One 20 ms mono tick of digital silence (the zero-fill unit).
const MONO_20MS: usize = 960;

/// Per-`VoiceTick` recorder: appends each targeted speaker's decoded 20 ms
/// frame (downmixed to mono) to their recording buffer, reconstructing a
/// continuous wall-clock timeline on the 50 Hz tick.
///
/// **Zero-fill only on songbird's explicit `silent` set.** A receiver must
/// keep the recording wall-clock-continuous — otherwise the probe's silence
/// windows compress out and the oracle can't align (live-diagnosed
/// 2026-07-07). But the fill source matters: an earlier version filled
/// whenever a target was merely *absent from* `tick.speaking`, which conflates
/// genuine silence with a transient opus/DAVE receive gap (a dropped/coalesced
/// tick). Those phantom fills accumulate — measured ~300 ms of over-length
/// across an 8 s capture, drifting the tail out of alignment (interior cycles
/// collapsing to ncc ~0.2 while the head stayed clean). songbird partitions
/// each tick into `speaking` (sent audio) and `silent` (a tracked SSRC that
/// sent a silence frame); a target in NEITHER is a real packet-loss gap, which
/// we must NOT fill. So: audio when speaking, 20 ms zeros only when explicitly
/// silent, nothing on a loss gap. The bridge puppet plays a continuous stream
/// (silence on underrun, never EOF), so in practice it is in one of the two
/// sets every tick and the timeline stays true.
struct ListenerTick {
    ssrc_map: SsrcMap,
    /// Only these user ids are recorded (pass the bridge's puppet bot ids).
    targets: Arc<HashSet<u64>>,
    recordings: Recordings,
}

#[serenity::async_trait]
impl SongbirdEventHandler for ListenerTick {
    async fn act(&self, ctx: &EventContext<'_>) -> Option<Event> {
        if let EventContext::VoiceTick(tick) = ctx {
            let ssrc_map = self.ssrc_map.lock().unwrap_or_else(PoisonError::into_inner);
            let mut recs = self.recordings.lock().unwrap_or_else(PoisonError::into_inner);
            // Targets that sent audio this tick → append it.
            for (ssrc, data) in &tick.speaking {
                let Some(pcm) = &data.decoded_voice else { continue };
                let Some(uid) = ssrc_map.get(ssrc) else { continue };
                if self.targets.contains(uid) {
                    recs.entry(*uid)
                        .or_default()
                        .extend_from_slice(&nvb_audio::downmix_stereo_to_mono(pcm));
                }
            }
            // Targets explicitly silent this tick → 20 ms of zeros (genuine
            // suppression gap), but only once they've started recording. A
            // target in neither set is a loss gap and is deliberately skipped.
            for ssrc in &tick.silent {
                let Some(uid) = ssrc_map.get(ssrc) else { continue };
                if self.targets.contains(uid) {
                    if let Some(buf) = recs.get_mut(uid) {
                        buf.resize(buf.len() + MONO_20MS, 0i16);
                    }
                }
            }
        }
        None
    }
}

/// A harness bot that sits in the test voice channel and records what selected
/// users (the bridge's puppets) play — receive needs `DecodeMode::Decode`
/// (Opus + DAVE), exactly like the bridge's own anchor.
pub struct DiscordListener {
    guild: GuildId,
    channel: ChannelId,
    songbird: Arc<songbird::Songbird>,
    shard: Arc<serenity::gateway::ShardManager>,
    cache: Arc<serenity::cache::Cache>,
    /// `None` once [`DiscordListener::stop`] has taken + awaited it on the
    /// graceful path; `Some` otherwise, so `Drop` can abort it (#72 F6-4).
    client_task: Option<tokio::task::JoinHandle<()>>,
    ssrc_map: SsrcMap,
    targets: Arc<HashSet<u64>>,
    recordings: Recordings,
}

impl DiscordListener {
    /// Connect the listener bot's gateway (does not join voice yet). `targets`
    /// are the Discord user ids to record — everyone else is ignored.
    pub async fn connect(
        token: &str,
        guild_id: &str,
        channel_id: &str,
        targets: HashSet<u64>,
    ) -> Result<Self> {
        use songbird::driver::{DecodeConfig, DecodeMode};
        let intents = GatewayIntents::GUILDS | GatewayIntents::GUILD_VOICE_STATES;
        let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Receive requires opus decode (and rides DAVE receive underneath).
        let sb_config =
            songbird::Config::default().decode_mode(DecodeMode::Decode(DecodeConfig::default()));
        let songbird = songbird::Songbird::serenity_from_config(sb_config);
        let mut client = serenity::Client::builder(token, intents)
            .event_handler(SpeakerHandler { ready: ready.clone() })
            .voice_manager_arc(songbird.clone())
            .await
            .context("building listener client")?;
        let cache = client.cache.clone();
        let shard = client.shard_manager.clone();
        let client_task = tokio::spawn(async move {
            if let Err(e) = client.start().await {
                tracing::warn!(error = %e, "discord listener client ended");
            }
        });
        crate::observers::await_flag(&ready, Duration::from_secs(30), "listener gateway").await?;

        Ok(Self {
            guild: GuildId::new(guild_id.parse().context("guild id")?),
            channel: ChannelId::new(channel_id.parse().context("channel id")?),
            songbird,
            shard,
            cache,
            client_task: Some(client_task),
            ssrc_map: Arc::new(StdMutex::new(HashMap::new())),
            targets: Arc::new(targets),
            recordings: Arc::new(StdMutex::new(HashMap::new())),
        })
    }

    /// Wait until at least one target user (a bridge puppet) is in THIS voice
    /// channel — the "puppet actually arrived" gate a scenario needs before
    /// playing its probe (audio pushed while the puppet is still joining would
    /// overflow the bridge's playback buffer and drop the probe's head).
    pub async fn await_target_in_voice(&self, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let present = self
                .cache
                .guild(self.guild)
                .map(|g| {
                    g.voice_states.iter().any(|(uid, vs)| {
                        self.targets.contains(&uid.get()) && vs.channel_id == Some(self.channel)
                    })
                })
                .unwrap_or(false);
            if present {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("no target user (bridge puppet) appeared in the voice channel");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Target user ids currently in THIS voice channel, per the gateway
    /// cache. Returns `None` when the guild is not (yet) cached — an UNKNOWN
    /// state, distinct from "cached and empty" (the same contract as
    /// `ConnCountProbe::voice_members_among`, #72 F6-3).
    pub fn targets_in_channel(&self) -> Option<Vec<u64>> {
        let guild = self.cache.guild(self.guild)?;
        Some(
            guild
                .voice_states
                .iter()
                .filter(|(uid, vs)| {
                    self.targets.contains(&uid.get()) && vs.channel_id == Some(self.channel)
                })
                .map(|(uid, _)| uid.get())
                .collect(),
        )
    }

    /// Join voice and start recording the target users.
    pub async fn join(&self) -> Result<()> {
        let call = self
            .songbird
            .join(self.guild, self.channel)
            .await
            .map_err(|e| anyhow!("listener failed to join voice: {e}"))?;
        let mut call = call.lock().await;
        call.add_global_event(
            CoreEvent::SpeakingStateUpdate.into(),
            ListenerSpeakingMap { ssrc_map: self.ssrc_map.clone() },
        );
        call.add_global_event(
            CoreEvent::VoiceTick.into(),
            ListenerTick {
                ssrc_map: self.ssrc_map.clone(),
                targets: self.targets.clone(),
                recordings: self.recordings.clone(),
            },
        );
        tracing::info!(targets = self.targets.len(), "discord listener recording");
        Ok(())
    }

    /// Leave voice, disconnect the gateway, and return the per-user recordings
    /// (mono 48 kHz).
    pub async fn stop(mut self) -> HashMap<u64, Vec<i16>> {
        if self.songbird.get(self.guild).is_some() {
            if let Err(e) = self.songbird.remove(self.guild).await {
                tracing::warn!(error = %e, "listener failed to leave voice");
            }
        }
        self.shard.shutdown_all().await;
        if let Some(task) = self.client_task.take() {
            let _ = task.await;
        }
        let mut recs = self.recordings.lock().unwrap_or_else(PoisonError::into_inner);
        std::mem::take(&mut *recs)
    }
}

impl Drop for DiscordListener {
    fn drop(&mut self) {
        // Best-effort: if `stop()` never ran (e.g. the scenario was cancelled
        // by the runner's outer timeout mid-await), abort the detached
        // gateway task rather than leak the listener's voice session into the
        // next scenario (#72 F6-4).
        crate::observers::abort_if_present(&mut self.client_task);
    }
}

// ─── DiscordMod (D3) ─────────────────────────────────────────────────────────────

/// The adversarial moderator (D3): the `nvb-test-mod` application (Move
/// Members + Connect, no admin) voice-kicking or force-moving a bridge puppet
/// to exercise the recovery paths (#46 external disconnect, #62 force-move
/// displacement). REST-only — no gateway, no voice, no cache — so it acts
/// out-of-band while the listener/probe actors keep their live view.
pub struct DiscordMod {
    http: serenity::http::Http,
    guild: GuildId,
}

impl DiscordMod {
    /// Build the REST client and validate the token up front (a dead token
    /// would otherwise only surface as a mid-scenario 401 — the same reason
    /// the run recipe validates every bot token with `GET /users/@me`).
    pub async fn connect(token: &str, guild_id: &str) -> Result<Self> {
        let http = serenity::http::Http::new(token);
        let me = http
            .get_current_user()
            .await
            .context("validating mod bot token (GET /users/@me)")?;
        tracing::info!(user = %me.name, "discord mod bot ready");
        Ok(Self { http, guild: GuildId::new(guild_id.parse().context("guild id")?) })
    }

    /// Force-move `user` into `channel` (needs Move Members) — the #62 shape:
    /// the voice connection survives, so no `DriverDisconnect` fires and only
    /// the bridge's own voice-state probe can notice the displacement.
    pub async fn force_move(&self, user: u64, channel: u64) -> Result<()> {
        self.http
            .edit_member(
                self.guild,
                UserId::new(user),
                &EditMember::new().voice_channel(ChannelId::new(channel)),
                Some("nvb-e2e force-move"),
            )
            .await
            .context("mod force-move failed")?;
        Ok(())
    }

    /// Kick `user` out of voice entirely (`channel_id: null`) — the external
    /// disconnect the #46 recovery must rejoin from.
    pub async fn disconnect(&self, user: u64) -> Result<()> {
        self.http
            .edit_member(
                self.guild,
                UserId::new(user),
                &EditMember::new().disconnect_member(),
                Some("nvb-e2e voice-kick"),
            )
            .await
            .context("mod voice-disconnect failed")?;
        Ok(())
    }
}
