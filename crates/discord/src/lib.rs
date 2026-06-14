use anyhow::Result;
use serenity::{async_trait, model::prelude::*};
use songbird::{
    input::core::io::MediaSource,
    model::payload::Speaking,
    Event, EventContext, EventHandler as SongbirdEventHandler,
};
use std::{
    io::{self, Read, Seek, SeekFrom},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, PoisonError,
    },
};
use tokio::sync::mpsc;

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
    /// Shared current-anchor marker and this puppet's own slot index. This
    /// handler only forwards to Matrix while `anchor == idx`, so capture moves
    /// cleanly between puppets on handoff (no doubled or dropped forwarding).
    anchor: Arc<std::sync::atomic::AtomicUsize>,
    idx: usize,
    /// Count of frames dropped because the forward channel was full (logged
    /// periodically rather than per-drop to avoid spamming the audio path).
    dropped: AtomicU64,
    /// Total VoiceTicks seen — logged periodically with speaking/silent sizes so
    /// we can tell "ticks stopped" (connection dead) from "ticks fire but all
    /// silent" (decode dead) when capture goes quiet.
    ticks: AtomicU64,
}

impl VoiceReceiver {
    /// Construct the capture handler for the anchor puppet.
    pub(crate) fn new(
        bridge: Arc<str>,
        audio_tx: mpsc::Sender<Vec<i16>>,
        exclude: SharedExclude,
        capture_sink: Option<Arc<CaptureSink>>,
        anchor: Arc<std::sync::atomic::AtomicUsize>,
        idx: usize,
    ) -> Self {
        Self {
            bridge,
            audio_tx,
            exclude,
            capture_sink,
            anchor,
            idx,
            dropped: AtomicU64::new(0),
            ticks: AtomicU64::new(0),
        }
    }

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
            // Only the current anchor forwards; a stale/overlapping handler that
            // is no longer the anchor goes silent (handoff is seamless).
            if self.anchor.load(Ordering::Relaxed) != self.idx {
                return None;
            }
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

impl SpeakingLogger {
    pub(crate) fn new(bridge: Arc<str>) -> Self {
        Self { bridge }
    }
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

impl ConnectionLogger {
    pub(crate) fn new(bridge: Arc<str>) -> Self {
        Self { bridge }
    }
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

// The songbird capture handlers above (`VoiceReceiver` / `SpeakingLogger` /
// `ConnectionLogger`) are now registered by the puppet pool's **anchor** puppet
// (see `puppet.rs`), not by a dedicated listener. There is no listener `Handler`.

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

    // There is no dedicated listener bot anymore: the puppet pool owns the whole
    // Discord side. One puppet is elected the "anchor" and captures Discord→Matrix
    // audio; the rest just play their participant. `audio_tx` + `capture_sink` go
    // to the pool so the anchor can forward.
    tracing::info!("starting Discord gateway (puppet pool)");
    pool.run_puppets(guild_id, channel_id, bridge, audio_tx, capture_sink)
        .await?;
    Ok(())
}

#[cfg(test)]
mod playback_tests {
    use super::*;
    use songbird::input::codecs::{get_codec_registry, get_probe};
    use songbird::input::{Input, RawAdapter};

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
