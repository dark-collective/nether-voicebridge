use anyhow::Result;
use serenity::{
    async_trait,
    model::prelude::*,
    prelude::*,
};
use songbird::{
    driver::{DecodeConfig, DecodeMode},
    input::{core::io::MediaSource, Input, RawAdapter},
    model::payload::Speaking,
    CoreEvent, Event, EventContext, EventHandler as SongbirdEventHandler,
    SerenityInit,
};
use std::{
    io::{self, Read, Seek, SeekFrom},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, PoisonError,
    },
};
use tokio::sync::{mpsc, Mutex};

pub mod capture;
pub use capture::CaptureSink;

pub mod puppet;
pub use puppet::{PuppetPool, SharedExclude};

use nvb_audio::{self as audio, mixer::PlaybackMixer};
use nvb_config::DiscordConfig;

/// Output sample rate / channel layout for the Discord playback track. We hand
/// songbird mono 48 kHz f32 PCM and let it upmix to stereo and Opus-encode.
const PLAYBACK_SAMPLE_RATE: u32 = 48_000;
const PLAYBACK_CHANNELS: u32 = 1;

// ─── Voice playback source (Matrix → Discord) ──────────────────────────────────

/// A live, never-ending `MediaSource` that songbird's mixer reads to play Matrix
/// audio into the voice channel.  It pulls one mixed 10 ms mono frame at a time
/// from the [`PlaybackMixer`], converts i16 → interleaved f32-LE bytes (what
/// [`RawAdapter`] expects), and **fills the whole read buffer every call** —
/// returning `0` would signal EOF and end the track, so on mixer underrun the
/// frame is silence rather than a short read.  songbird paces playout by only
/// requesting what each 20 ms tick needs; this source never blocks.
struct PlaybackSource {
    mixer: Arc<PlaybackMixer>,
    /// f32-LE bytes produced but not yet handed to songbird.
    leftover: Vec<u8>,
    /// Read cursor into `leftover`.
    pos: usize,
    /// Health: number of 10 ms frames pulled by songbird so far.
    refills: u64,
    /// Health: peak level (dBFS) of what we've sent since the last log.
    peak_db: f32,
    /// Health: min/max jitter-buffer depth (samples) seen since the last log —
    /// reveals whether the cushion holds against songbird's burst reads.
    min_depth: usize,
    max_depth: usize,
}

impl PlaybackSource {
    fn new(mixer: Arc<PlaybackMixer>) -> Self {
        Self {
            mixer,
            leftover: Vec::new(),
            pos: 0,
            refills: 0,
            peak_db: f32::NEG_INFINITY,
            min_depth: usize::MAX,
            max_depth: 0,
        }
    }

    /// Pull the next mixed 10 ms frame and re-encode it as f32-LE bytes.
    fn refill(&mut self) {
        // Sample buffer depth *before* the pop (the cushion songbird is drawing
        // from). min over the window is the key health signal: if it stays well
        // above zero, the cushion is absorbing the burst reads (no crackle).
        let depth = self.mixer.max_depth_samples();
        self.min_depth = self.min_depth.min(depth);
        self.max_depth = self.max_depth.max(depth);

        let frame = self.mixer.pop_10ms_mixed(); // 480 i16, silence when idle
        // Health: confirm songbird is reading (refills climb at ~100/s); track
        // peak level, underruns, and cushion depth over each window. Underruns
        // should be ~0 during continuous speech once primed.
        self.refills += 1;
        self.peak_db = self.peak_db.max(audio::rms_dbfs(&frame));
        if self.refills == 1 || self.refills.is_multiple_of(500) {
            tracing::debug!(
                refills = self.refills,
                peak_db = format_args!("{:.1}", self.peak_db),
                underruns = self.mixer.take_underruns(),
                depth_min_ms = self.min_depth / 48,
                depth_max_ms = self.max_depth / 48,
                "playback source level (peak over window)"
            );
            self.peak_db = f32::NEG_INFINITY;
            self.min_depth = usize::MAX;
            self.max_depth = 0;
        }
        let mut bytes = Vec::with_capacity(frame.len() * 4);
        for s in frame {
            let f = s as f32 / 32768.0;
            bytes.extend_from_slice(&f.to_le_bytes());
        }
        self.leftover = bytes;
        self.pos = 0;
    }
}

impl Read for PlaybackSource {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut written = 0;
        while written < buf.len() {
            if self.pos >= self.leftover.len() {
                self.refill();
            }
            let avail = &self.leftover[self.pos..];
            let n = avail.len().min(buf.len() - written);
            buf[written..written + n].copy_from_slice(&avail[..n]);
            self.pos += n;
            written += n;
        }
        Ok(written)
    }
}

impl Seek for PlaybackSource {
    fn seek(&mut self, _pos: SeekFrom) -> io::Result<u64> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "playback source is a live stream",
        ))
    }
}

impl MediaSource for PlaybackSource {
    fn is_seekable(&self) -> bool {
        false
    }
    fn byte_len(&self) -> Option<u64> {
        None
    }
}

// ─── Voice capture handler ────────────────────────────────────────────────────

/// Songbird event handler: at each 20 ms `VoiceTick`, mixes all non-excluded
/// Discord speakers (songbird delivers them already time-aligned for the tick)
/// into one mono frame and forwards it to the Matrix publish path.  Optionally
/// also writes per-SSRC WAV files for debugging.
struct VoiceReceiver {
    /// Bridge label for per-bridge log attribution (songbird calls `act` from its
    /// own driver task, outside the bridge span).
    bridge: Arc<str>,
    /// Forward channel to the RTC audio consumer.  20 ms mono frames (960 i16).
    audio_tx: mpsc::Sender<Vec<i16>>,
    /// Bridge-owned SSRCs to exclude from capture (echo prevention).  Populated
    /// at runtime as puppets join voice (Phase 7): the listener hears the
    /// puppets replaying Matrix audio and must not re-capture and loop it back.
    exclude: SharedExclude,
    /// Optional Phase 3 WAV capture, enabled via `[discord] capture_wav`.
    capture_sink: Option<Arc<CaptureSink>>,
    /// Count of frames dropped because the forward channel was full (logged
    /// periodically rather than per-drop to avoid spamming the audio path).
    dropped: AtomicU64,
    /// Total VoiceTicks seen — logged periodically with speaking/silent sizes so
    /// we can tell "ticks stopped" (connection dead) from "ticks fire but all
    /// silent" (decode dead) when capture goes quiet.
    ticks: AtomicU64,
}

impl VoiceReceiver {
    /// Build the mixed 20 ms mono frame for this tick from the present,
    /// non-excluded speakers.  Returns silence (zeros) when nobody is speaking
    /// so the downstream clock/jitter buffer keeps ticking at a steady 50 Hz.
    fn mix_tick(&self, tick: &songbird::events::context_data::VoiceTick) -> Vec<i16> {
        let mut monos: Vec<Vec<i16>> = Vec::new();
        // One read-lock for the whole tick; writers (puppet join/leave) are rare.
        let excluded = self.exclude.read().unwrap_or_else(PoisonError::into_inner);
        for (ssrc, data) in &tick.speaking {
            if excluded.contains(ssrc) {
                continue;
            }
            if let Some(pcm) = &data.decoded_voice {
                if let Some(sink) = &self.capture_sink {
                    sink.write_frame(*ssrc, pcm);
                }
                monos.push(audio::downmix_stereo_to_mono(pcm));
            }
        }

        if monos.is_empty() {
            return vec![0i16; audio::SAMPLES_20MS];
        }
        let refs: Vec<&[i16]> = monos.iter().map(|m| m.as_slice()).collect();
        audio::mix_mono(&refs)
    }
}

#[async_trait]
impl SongbirdEventHandler for VoiceReceiver {
    #[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %self.bridge))]
    async fn act(&self, ctx: &EventContext<'_>) -> Option<Event> {
        if let EventContext::VoiceTick(tick) = ctx {
            // Tick-health: distinguishes "ticks stopped" from "ticks fire but
            // everyone is silent" (decode dead) when capture goes quiet.
            let n = self.ticks.fetch_add(1, Ordering::Relaxed) + 1;
            if n.is_multiple_of(250) {
                tracing::info!(
                    ticks = n,
                    speaking = tick.speaking.len(),
                    silent = tick.silent.len(),
                    "voicetick health"
                );
            }

            let frame = self.mix_tick(tick);
            // Never block or panic on the audio path: drop on a full channel
            // (audio is real-time; a stale backlog is worse than a gap).
            if self.audio_tx.try_send(frame).is_err() {
                let n = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
                if n.is_multiple_of(50) {
                    tracing::warn!(dropped = n, "audio forward channel full — dropping frames");
                }
            }
        }
        None
    }
}

/// Logs the SSRC → UserId mapping whenever a participant sends a speaking-state
/// update.  This lets you correlate `capture_<ssrc>.wav` with real Discord users
/// without any additional tooling.
struct SpeakingLogger {
    bridge: Arc<str>,
}

#[async_trait]
impl SongbirdEventHandler for SpeakingLogger {
    #[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %self.bridge))]
    async fn act(&self, ctx: &EventContext<'_>) -> Option<Event> {
        if let EventContext::SpeakingStateUpdate(Speaking { ssrc, user_id: Some(uid), .. }) = ctx {
            tracing::info!(ssrc, user_id = uid.0, "SSRC → Discord user mapping");
        }
        None
    }
}

/// Logs songbird voice-connection lifecycle events (DriverConnect/Reconnect/
/// Disconnect) and other users leaving the channel (ClientDisconnect).
/// Diagnostic instrumentation: lets us see whether a capture-death coincides
/// with a voice-connection drop vs. happening with the connection still up.
/// (songbird 0.6 has no ClientConnect event — joins come via the serenity
/// `voice_state_update` handler.)
struct ConnectionLogger {
    bridge: Arc<str>,
}

#[async_trait]
impl SongbirdEventHandler for ConnectionLogger {
    #[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %self.bridge))]
    async fn act(&self, ctx: &EventContext<'_>) -> Option<Event> {
        match ctx {
            EventContext::DriverConnect(c) => tracing::info!(
                channel_id = ?c.channel_id, ssrc = c.ssrc, server = c.server,
                "songbird DriverConnect"
            ),
            EventContext::DriverReconnect(c) => tracing::warn!(
                channel_id = ?c.channel_id, ssrc = c.ssrc, server = c.server,
                "songbird DriverReconnect"
            ),
            EventContext::DriverDisconnect(d) => tracing::warn!(
                kind = ?d.kind, reason = ?d.reason, channel_id = ?d.channel_id,
                "songbird DriverDisconnect"
            ),
            EventContext::ClientDisconnect(cd) => tracing::info!(
                user_id = cd.user_id.0, "songbird ClientDisconnect (user left voice)"
            ),
            _ => {}
        }
        None
    }
}

// ─── Serenity gateway handler ─────────────────────────────────────────────────

struct Handler {
    /// Bridge label for per-bridge log attribution (multi-bridge spans). Serenity
    /// invokes these callbacks from its own gateway task, which does not inherit
    /// the bridge span set up in `main`, so each callback re-establishes it.
    bridge: Arc<str>,
    guild_id: GuildId,
    channel_id: ChannelId,
    /// The listener bot's own UserId — seeded in `ready`, used to filter its
    /// own voice-state events (echo-exclusion seed for future phases).
    bridge_user_ids: Arc<Mutex<Vec<UserId>>>,
    /// Forward channel to the RTC audio consumer.
    audio_tx: mpsc::Sender<Vec<i16>>,
    /// Bridge-owned SSRCs to exclude from capture (puppet SSRCs, Phase 7).
    exclude: SharedExclude,
    /// Optional WAV capture, enabled via `[discord] capture_wav`.
    capture_sink: Option<Arc<CaptureSink>>,
    /// Inbound Matrix audio to play into the voice channel: the shared listener
    /// mix (overflow / un-attributed speakers in Phase 7).
    playback: Arc<PlaybackMixer>,
}

#[async_trait]
impl EventHandler for Handler {
    #[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %self.bridge))]
    async fn ready(&self, ctx: Context, ready: Ready) {
        tracing::info!(bot_user = %ready.user.name, "Discord listener connected");

        // Register ourselves so join/leave events for the bot are filtered out.
        self.bridge_user_ids.lock().await.push(ready.user.id);

        let manager = songbird::get(&ctx).await.expect("songbird not registered");
        match manager.join(self.guild_id, self.channel_id).await {
            Ok(call) => {
                tracing::info!(channel_id = %self.channel_id, "joined voice channel");

                // Register the capture and speaking-logger handlers.
                let mut call = call.lock().await;
                call.add_global_event(
                    CoreEvent::VoiceTick.into(),
                    VoiceReceiver {
                        bridge: self.bridge.clone(),
                        audio_tx: self.audio_tx.clone(),
                        exclude: Arc::clone(&self.exclude),
                        capture_sink: self.capture_sink.clone(),
                        dropped: AtomicU64::new(0),
                        ticks: AtomicU64::new(0),
                    },
                );
                call.add_global_event(
                    CoreEvent::SpeakingStateUpdate.into(),
                    SpeakingLogger { bridge: self.bridge.clone() },
                );

                // Diagnostic: log voice-connection lifecycle + client disconnects.
                for ev in [
                    CoreEvent::DriverConnect,
                    CoreEvent::DriverReconnect,
                    CoreEvent::DriverDisconnect,
                    CoreEvent::ClientDisconnect,
                ] {
                    call.add_global_event(ev.into(), ConnectionLogger { bridge: self.bridge.clone() });
                }

                // Start the inbound playback track (Matrix → Discord). songbird
                // reads this live source forever, even while the mixer is idle
                // (silence), so the track stays open for the whole connection.
                let source = PlaybackSource::new(Arc::clone(&self.playback));
                let input = Input::from(RawAdapter::new(
                    source,
                    PLAYBACK_SAMPLE_RATE,
                    PLAYBACK_CHANNELS,
                ));
                let _handle = call.play_input(input);
                tracing::info!("playback track started — Matrix audio will play into Discord");

                tracing::info!("audio capture active — talk in Discord to bridge into Element Call");
            }
            Err(e) => tracing::error!(error = %e, "failed to join voice channel"),
        }
    }

    #[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %self.bridge))]
    async fn voice_state_update(&self, _ctx: Context, old: Option<VoiceState>, new: VoiceState) {
        let was_here = old
            .and_then(|s| s.channel_id)
            .map(|c| c == self.channel_id)
            .unwrap_or(false);
        let now_here = new
            .channel_id
            .map(|c| c == self.channel_id)
            .unwrap_or(false);

        {
            let ids = self.bridge_user_ids.lock().await;
            if ids.contains(&new.user_id) {
                return;
            }
        }

        match (was_here, now_here) {
            (false, true) => tracing::info!(
                user_id = %new.user_id,
                channel_id = %self.channel_id,
                "user joined voice channel"
            ),
            (true, false) => tracing::info!(
                user_id = %new.user_id,
                channel_id = %self.channel_id,
                "user left voice channel"
            ),
            _ => {}
        }
    }
}

// ─── Entry point ─────────────────────────────────────────────────────────────

#[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %bridge))]
pub async fn run(
    cfg: &DiscordConfig,
    audio_tx: mpsc::Sender<Vec<i16>>,
    capture_sink: Option<Arc<CaptureSink>>,
    pool: Arc<PuppetPool>,
    bridge: Arc<str>,
) -> Result<()> {
    let guild_id: u64 = cfg
        .guild_id
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid guild_id: {}", cfg.guild_id))?;
    let channel_id: u64 = cfg
        .channel_id
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid channel_id: {}", cfg.channel_id))?;
    let guild_id = GuildId::new(guild_id);
    let channel_id = ChannelId::new(channel_id);

    let handler = Handler {
        bridge: bridge.clone(),
        guild_id,
        channel_id,
        bridge_user_ids: Arc::new(Mutex::new(Vec::new())),
        audio_tx,
        // The listener and every puppet share one exclude set + listener mix.
        exclude: pool.exclude(),
        capture_sink,
        playback: pool.listener_mixer(),
    };

    let intents = GatewayIntents::GUILDS | GatewayIntents::GUILD_VOICE_STATES;

    // Configure songbird to DAVE-decrypt and Opus-decode incoming packets.
    // DecodeConfig::default() = stereo / 48 kHz, matching the WAV spec.
    let sb_config = songbird::Config::default()
        .decode_mode(DecodeMode::Decode(DecodeConfig::default()));

    let mut client = Client::builder(&cfg.listener_token, intents)
        .event_handler(handler)
        .register_songbird_from_config(sb_config)
        .await
        .map_err(|e| anyhow::anyhow!("failed to build Discord client: {e}"))?;

    tracing::info!("starting Discord gateway (listener + puppet pool)");
    // Run the listener client and every puppet client concurrently; the first
    // to error aborts the bridge.
    tokio::try_join!(
        async {
            client
                .start()
                .await
                .map_err(|e| anyhow::anyhow!("Discord listener client error: {e}"))
        },
        pool.run_puppets(guild_id, channel_id, bridge.clone()),
    )?;
    Ok(())
}

#[cfg(test)]
mod playback_tests {
    use super::*;
    use songbird::input::codecs::{get_codec_registry, get_probe};

    // Reproduce songbird's promotion of our live RawAdapter input in isolation
    // (no Discord, no DAVE) to surface the exact error if it won't become playable.
    #[tokio::test]
    async fn playback_source_is_promotable() {
        let mixer = Arc::new(PlaybackMixer::new());
        let source = PlaybackSource::new(mixer);
        let input = Input::from(RawAdapter::new(source, PLAYBACK_SAMPLE_RATE, PLAYBACK_CHANNELS));

        let result = input
            .make_playable_async(get_codec_registry(), get_probe())
            .await;

        let ok = result.is_ok();
        match &result {
            Ok(i) => println!("PROMOTE OK; is_playable={}", i.is_playable()),
            Err(e) => println!("PROMOTE ERR: {e:?}"),
        }
        assert!(ok, "playback input failed to promote");
    }
}
