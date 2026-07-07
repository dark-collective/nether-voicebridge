use serenity::async_trait;
use songbird::{
    input::core::io::MediaSource,
    model::payload::Speaking,
    Event, EventContext, EventHandler as SongbirdEventHandler,
};
use std::{
    collections::{HashMap, HashSet},
    io::{self, Read, Seek, SeekFrom},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, PoisonError, RwLock,
    },
};

pub mod capture;
pub use capture::CaptureSink;

pub mod puppet;
pub use puppet::{ChannelView, LinkApproval, SharedExclude, SharedPool};

// The Matrix-side ghost sink trait (one per Discord speaker). Aliased so it
// doesn't clash with this crate's WAV-writing `capture::CaptureSink`.
use nvb_audio::routing::CaptureSink as GhostSink;
use nvb_audio::{self as audio, mixer::PlaybackMixer};

// ─── Discord → Matrix (ghost) capture routing shared state (Phase 8) ───────────

/// SSRC → Discord user id. Populated from `SpeakingStateUpdate` (which carries
/// both), read by the anchor's capture demux to attribute each speaker's audio
/// to their ghost. An `RwLock`: written rarely (a speaker (re)announces), read on
/// every 50 Hz capture tick.
pub type SsrcMap = Arc<RwLock<HashMap<u32, u64>>>;

/// Discord user id → that speaker's ghost sink. Populated by the voice-state
/// handler on a member joining the channel, read by the capture demux.
pub type UserSinkMap = Arc<RwLock<HashMap<u64, Arc<dyn GhostSink>>>>;

/// Bridge-owned puppet bot user ids — excluded from ghost minting (a puppet in
/// the channel is not a Discord human). Filled from each puppet's `ready`.
pub type BotIdSet = Arc<RwLock<HashSet<u64>>>;

/// Discord user ids that opted out of being bridged (#63): never forwarded
/// Discord→Matrix and never ghosted. Shared between the pool (command handler,
/// ghost gate) and every anchor's [`VoiceReceiver`] (capture gate).
pub type OptoutSet = Arc<RwLock<HashSet<u64>>>;

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

/// Songbird event handler: at each 20 ms `VoiceTick`, **demuxes** each
/// non-excluded Discord speaker to their own Matrix ghost (Phase 8). songbird
/// delivers all speakers time-aligned for the tick; rather than mix them, this
/// pushes each speaker's mono frame into that speaker's [`GhostSink`] so each
/// Discord user publishes their own attributed LiveKit track. Optionally also
/// writes per-SSRC WAV files for debugging.
struct VoiceReceiver {
    /// Bridge label for per-bridge log attribution (songbird calls `act` from its
    /// own driver task, outside the bridge span).
    bridge: Arc<str>,
    /// Bridge-owned SSRCs to exclude from capture (echo prevention).  Populated
    /// at runtime as puppets join voice (Phase 7): the anchor hears the puppets
    /// replaying Matrix audio and must not re-capture and loop it back.
    exclude: SharedExclude,
    /// SSRC → Discord user id, to attribute each captured stream to a ghost.
    ssrc_map: SsrcMap,
    /// Discord user id → that speaker's ghost sink (the demux target).
    user_sinks: UserSinkMap,
    /// Optional Phase 3 WAV capture, enabled via `[discord] capture_wav`.
    capture_sink: Option<Arc<CaptureSink>>,
    /// Users who opted out of being bridged (#63) — their audio is dropped at
    /// this demux even if a ghost sink somehow exists.
    optout: OptoutSet,
    /// Shared current-anchor marker and this puppet's own slot index. This
    /// handler only captures while `anchor == idx`, so capture moves cleanly
    /// between puppets on handoff (no doubled or dropped capture).
    anchor: Arc<std::sync::atomic::AtomicUsize>,
    idx: usize,
    /// Count of speaker frames dropped because no ghost sink was resolvable yet
    /// (unknown SSRC→user, or the user has no ghost). Logged periodically.
    dropped: AtomicU64,
    /// Total VoiceTicks seen — logged periodically with speaking/silent sizes so
    /// we can tell "ticks stopped" (connection dead) from "ticks fire but all
    /// silent" (decode dead) when capture goes quiet.
    ticks: AtomicU64,
    /// Count of decoded frames whose length wasn't the expected stereo-20 ms 1920
    /// samples (issue #32 diagnostic): songbird can hand back short PLC frames on
    /// packet loss, a candidate source of glitching. Logged rate-limited.
    shape_warns: AtomicU64,
}

/// Expected decoded length of one VoiceTick: 48 kHz × 20 ms × 2 channels.
const STEREO_20MS_SAMPLES: usize = 1920;

impl VoiceReceiver {
    /// Construct the capture handler for the anchor puppet.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        bridge: Arc<str>,
        exclude: SharedExclude,
        ssrc_map: SsrcMap,
        user_sinks: UserSinkMap,
        capture_sink: Option<Arc<CaptureSink>>,
        optout: OptoutSet,
        anchor: Arc<std::sync::atomic::AtomicUsize>,
        idx: usize,
    ) -> Self {
        Self {
            bridge,
            exclude,
            ssrc_map,
            user_sinks,
            capture_sink,
            optout,
            anchor,
            idx,
            dropped: AtomicU64::new(0),
            ticks: AtomicU64::new(0),
            shape_warns: AtomicU64::new(0),
        }
    }

    /// Push each present, non-excluded speaker's 20 ms mono frame to their ghost
    /// sink. Returns the number of speaker frames dropped for want of a ghost.
    fn demux_tick(&self, tick: &songbird::events::context_data::VoiceTick) -> u64 {
        // One read-lock each per tick; writers (puppet join/leave, speaker
        // announce, ghost assign) are all rare relative to the 50 Hz tick.
        let excluded = self.exclude.read().unwrap_or_else(PoisonError::into_inner);
        let ssrc_map = self.ssrc_map.read().unwrap_or_else(PoisonError::into_inner);
        let sinks = self.user_sinks.read().unwrap_or_else(PoisonError::into_inner);
        let optout = self.optout.read().unwrap_or_else(PoisonError::into_inner);
        let mut dropped = 0u64;
        for (ssrc, data) in &tick.speaking {
            if excluded.contains(ssrc) {
                continue;
            }
            let Some(pcm) = &data.decoded_voice else { continue };
            // Shape diagnostic (#32): a non-1920 decoded frame (short PLC on packet
            // loss, or a format surprise) is a candidate corruption source. Warn
            // rate-limited rather than silently downmixing an odd-length frame.
            if pcm.len() != STEREO_20MS_SAMPLES {
                let n = self.shape_warns.fetch_add(1, Ordering::Relaxed) + 1;
                if n.is_multiple_of(50) {
                    tracing::warn!(
                        ssrc = *ssrc,
                        len = pcm.len(),
                        expected = STEREO_20MS_SAMPLES,
                        count = n,
                        "decoded Discord frame not stereo-20ms — possible PLC/short frame"
                    );
                }
            }
            if let Some(sink) = &self.capture_sink {
                sink.write_frame(*ssrc, pcm);
            }
            // Attribute SSRC → user → ghost. A speaker with no mapping yet (their
            // SpeakingStateUpdate or ghost assign hasn't landed) is dropped this
            // tick; the next tick (or their next utterance) resolves it.
            // Opted-out users (#63) are dropped HERE, unconditionally — even if
            // a ghost sink still exists (e.g. mid-retirement), their audio never
            // crosses to Matrix. This is the single Discord→Matrix choke point.
            match ssrc_map.get(ssrc) {
                Some(uid) if optout.contains(uid) => {}
                Some(uid) => match sinks.get(uid) {
                    Some(sink) => sink.push(&audio::downmix_stereo_to_mono(pcm)),
                    None => dropped += 1,
                },
                None => dropped += 1,
            }
        }
        dropped
    }
}

#[async_trait]
impl SongbirdEventHandler for VoiceReceiver {
    #[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %self.bridge))]
    async fn act(&self, ctx: &EventContext<'_>) -> Option<Event> {
        if let EventContext::VoiceTick(tick) = ctx {
            // Only the current anchor captures; a stale/overlapping handler that
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

            let dropped = self.demux_tick(tick);
            if dropped > 0 {
                let total = self.dropped.fetch_add(dropped, Ordering::Relaxed) + dropped;
                if total.is_multiple_of(50) {
                    tracing::warn!(dropped = total, "speaker frames dropped — no ghost sink yet");
                }
            }
        }
        None
    }
}

/// Records the SSRC → Discord user id mapping whenever a participant sends a
/// speaking-state update — the key that lets the anchor attribute each captured
/// stream to a ghost (and correlate `capture_<ssrc>.wav` with a real user).
struct SpeakingLogger {
    bridge: Arc<str>,
    ssrc_map: SsrcMap,
}

impl SpeakingLogger {
    pub(crate) fn new(bridge: Arc<str>, ssrc_map: SsrcMap) -> Self {
        Self { bridge, ssrc_map }
    }
}

#[async_trait]
impl SongbirdEventHandler for SpeakingLogger {
    #[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %self.bridge))]
    async fn act(&self, ctx: &EventContext<'_>) -> Option<Event> {
        if let EventContext::SpeakingStateUpdate(Speaking { ssrc, user_id, speaking, .. }) = ctx {
            match user_id {
                Some(uid) => {
                    self.ssrc_map
                        .write()
                        .unwrap_or_else(PoisonError::into_inner)
                        .insert(*ssrc, uid.0);
                    tracing::info!(ssrc, user_id = uid.0, ?speaking, "SSRC → Discord user mapping");
                }
                // Diagnostic: an update with no user_id can't be attributed. Seeing
                // these (vs. nothing) tells us the event fires but lacks the id.
                None => tracing::debug!(ssrc, ?speaking, "speaking-state update without user_id"),
            }
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
    /// Drop a departed user's SSRC mappings so the map doesn't grow unbounded
    /// over a long call (a recycled SSRC is also re-announced on next speech).
    ssrc_map: SsrcMap,
}

impl ConnectionLogger {
    pub(crate) fn new(bridge: Arc<str>, ssrc_map: SsrcMap) -> Self {
        Self { bridge, ssrc_map }
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
            EventContext::ClientDisconnect(cd) => {
                let uid = cd.user_id.0;
                self.ssrc_map
                    .write()
                    .unwrap_or_else(PoisonError::into_inner)
                    .retain(|_ssrc, mapped| *mapped != uid);
                tracing::info!(user_id = uid, "songbird ClientDisconnect (user left voice)");
            }
            _ => {}
        }
        None
    }
}

// The songbird capture handlers above (`VoiceReceiver` / `SpeakingLogger` /
// `ConnectionLogger`) are registered by a channel's **anchor** puppet (see
// `puppet.rs`), not by a dedicated listener. There is no listener `Handler`.
//
// The Discord side has no per-bridge entry point anymore: the shared
// [`SharedPool`] owns every puppet gateway and is started once from `main`
// (`SharedPool::run`); each bridge registers its channel via
// `SharedPool::register_channel` and drives playback through the returned
// [`ChannelView`].

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
