//! Multi-guild concurrent voice spike (#46) — GO/NO-GO gate for per-guild
//! puppet multiplexing.
//!
//! One Discord bot (one serenity client, one songbird manager) joins voice in
//! **two different guilds at once** and proves, live, everything the allocator
//! refactor depends on but unit tests can't cover:
//!
//! 1. Both `manager.join`s complete and establish DAVE sessions — watch for two
//!    `DriverConnect` lines with **distinct SSRCs**.
//! 2. `VoiceTick` capture runs concurrently on both Calls and demuxes cleanly:
//!    speaking in guild A produces WAVs only under A's label. Speak in both
//!    simultaneously and listen to the per-guild `spike_<label>_ssrc<n>.wav`
//!    files for cross-talk or corruption.
//! 3. Independent playback: guild A hears a 440 Hz tone, guild B 660 Hz.
//! 4. Soak: leave it running ≥10 min; a status line logs per-guild tick counts
//!    every minute (ticks advancing ≈50/s on both = both drivers healthy).
//! 5. Clean teardown on Ctrl-C: leaves both channels, finalises WAVs.
//!
//! Run with:
//!
//! ```text
//! NVB_SPIKE_TOKEN=<bot token> \
//! NVB_SPIKE_GUILD_A=<id> NVB_SPIKE_CHANNEL_A=<id> \
//! NVB_SPIKE_GUILD_B=<id> NVB_SPIKE_CHANNEL_B=<id> \
//! cargo run -p nvb-discord --example multiguild_spike
//! ```
//!
//! The bot must be invited to both guilds with Connect/Speak/Use Voice Activity.

use std::{
    collections::HashMap,
    env,
    fs::File,
    io::{self, BufWriter, Read, Seek, SeekFrom},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, PoisonError,
    },
    time::{Duration, Instant},
};

use anyhow::{bail, Context as _, Result};
use hound::{SampleFormat, WavSpec, WavWriter};
use serenity::{async_trait, model::prelude::*, prelude::*};
use songbird::{
    driver::{DecodeConfig, DecodeMode},
    events::context_data::VoiceTick,
    input::{core::io::MediaSource, Input, RawAdapter},
    model::payload::Speaking,
    CoreEvent, Event, EventContext, EventHandler as SongbirdEventHandler, SerenityInit, Songbird,
};
use tracing::{error, info, warn};

/// Capture WAV format: what songbird's decoder hands us per VoiceTick.
const WAV_SPEC: WavSpec = WavSpec {
    channels: 2,
    sample_rate: 48_000,
    bits_per_sample: 16,
    sample_format: SampleFormat::Int,
};

// ─── Per-guild spike state ──────────────────────────────────────────────────────

struct WavState {
    writer: WavWriter<BufWriter<File>>,
    frames: u32,
}

/// Everything the spike tracks for one guild's voice session.
struct GuildTarget {
    label: &'static str,
    guild: GuildId,
    channel: ChannelId,
    tone_hz: f32,
    /// Our own SSRC on this connection (from `DriverConnect`); 0 = not yet up.
    own_ssrc: AtomicU64,
    /// VoiceTicks seen on this Call (should advance ~50/s while connected).
    ticks: AtomicU64,
    /// SSRC → Discord user id, from `SpeakingStateUpdate` on this Call.
    ssrc_users: Mutex<HashMap<u32, u64>>,
    /// Per-SSRC capture WAVs (`spike_<label>_ssrc<n>.wav`).
    wavs: Mutex<HashMap<u32, WavState>>,
}

impl GuildTarget {
    fn from_env(label: &'static str, guild_var: &str, channel_var: &str, tone_hz: f32) -> Result<Arc<Self>> {
        let parse = |var: &str| -> Result<u64> {
            env::var(var)
                .with_context(|| format!("missing env {var}"))?
                .trim()
                .parse::<u64>()
                .with_context(|| format!("{var} is not a numeric id"))
        };
        Ok(Arc::new(Self {
            label,
            guild: GuildId::new(parse(guild_var)?),
            channel: ChannelId::new(parse(channel_var)?),
            tone_hz,
            own_ssrc: AtomicU64::new(0),
            ticks: AtomicU64::new(0),
            ssrc_users: Mutex::new(HashMap::new()),
            wavs: Mutex::new(HashMap::new()),
        }))
    }

    fn on_tick(&self, tick: &VoiceTick) {
        let n = self.ticks.fetch_add(1, Ordering::Relaxed) + 1;
        if n.is_multiple_of(250) {
            info!(
                guild = self.label,
                ticks = n,
                speaking = tick.speaking.len(),
                silent = tick.silent.len(),
                "voicetick health"
            );
        }
        for (ssrc, data) in &tick.speaking {
            let Some(pcm) = &data.decoded_voice else { continue };
            self.write_wav(*ssrc, pcm);
        }
    }

    fn write_wav(&self, ssrc: u32, pcm: &[i16]) {
        let mut wavs = self.wavs.lock().unwrap_or_else(PoisonError::into_inner);
        let state = match wavs.entry(ssrc) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(v) => {
                let path = format!("spike_{}_ssrc{}.wav", self.label, ssrc);
                match File::create(&path).map_err(hound::Error::IoError).and_then(|f| {
                    WavWriter::new(BufWriter::new(f), WAV_SPEC)
                }) {
                    Ok(writer) => {
                        info!(guild = self.label, ssrc, path, "capturing speaker → WAV");
                        v.insert(WavState { writer, frames: 0 })
                    }
                    Err(e) => {
                        warn!(guild = self.label, ssrc, error = %e, "failed to open capture WAV");
                        return;
                    }
                }
            }
        };
        for &s in pcm {
            if state.writer.write_sample(s).is_err() {
                break;
            }
        }
        state.frames += 1;
        if state.frames.is_multiple_of(250) {
            info!(
                guild = self.label,
                ssrc,
                frames = state.frames,
                rms_db = format_args!("{:.1}", rms_dbfs(pcm)),
                "capture level"
            );
        }
    }

    fn finalize_wavs(&self) {
        let mut wavs = self.wavs.lock().unwrap_or_else(PoisonError::into_inner);
        for (ssrc, state) in wavs.drain() {
            let frames = state.frames;
            match state.writer.finalize() {
                Ok(()) => info!(guild = self.label, ssrc, frames, "WAV finalised"),
                Err(e) => warn!(guild = self.label, ssrc, error = %e, "WAV finalise failed"),
            }
        }
    }
}

fn rms_dbfs(pcm: &[i16]) -> f32 {
    if pcm.is_empty() {
        return -96.0;
    }
    let mean_sq: f64 = pcm
        .iter()
        .map(|&s| {
            let f = s as f64 / i16::MAX as f64;
            f * f
        })
        .sum::<f64>()
        / pcm.len() as f64;
    if mean_sq <= 0.0 {
        return -96.0;
    }
    (10.0 * mean_sq.log10()) as f32
}

// ─── Songbird event handler (one clone per registered event, shared state) ─────

#[derive(Clone)]
struct SpikeEvents {
    t: Arc<GuildTarget>,
}

#[async_trait]
impl SongbirdEventHandler for SpikeEvents {
    async fn act(&self, ctx: &EventContext<'_>) -> Option<Event> {
        match ctx {
            EventContext::DriverConnect(c) => {
                self.t.own_ssrc.store(c.ssrc as u64, Ordering::Relaxed);
                info!(
                    guild = self.t.label,
                    ssrc = c.ssrc,
                    channel_id = ?c.channel_id,
                    server = c.server,
                    "DriverConnect — voice session up"
                );
            }
            EventContext::DriverReconnect(c) => {
                warn!(guild = self.t.label, ssrc = c.ssrc, "DriverReconnect");
            }
            EventContext::DriverDisconnect(d) => {
                warn!(guild = self.t.label, kind = ?d.kind, reason = ?d.reason, "DriverDisconnect");
            }
            EventContext::SpeakingStateUpdate(Speaking {
                ssrc,
                user_id: Some(uid),
                speaking,
                ..
            }) => {
                self.t
                    .ssrc_users
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(*ssrc, uid.0);
                info!(guild = self.t.label, ssrc, user_id = uid.0, ?speaking, "SSRC → user");
            }
            EventContext::VoiceTick(tick) => self.t.on_tick(tick),
            EventContext::ClientDisconnect(cd) => {
                info!(guild = self.t.label, user_id = cd.user_id.0, "user left voice");
            }
            _ => {}
        }
        None
    }
}

// ─── Tone playback source (distinct pitch per guild) ───────────────────────────

/// Endless mono 48 kHz f32 sine, generated 10 ms at a time (mirrors the shape of
/// the production `PlaybackSource` so playback exercises the same songbird path).
struct ToneSource {
    hz: f32,
    sample_idx: u64,
    leftover: Vec<u8>,
    pos: usize,
}

impl ToneSource {
    const AMPLITUDE: f32 = 0.15;

    fn new(hz: f32) -> Self {
        Self { hz, sample_idx: 0, leftover: Vec::new(), pos: 0 }
    }

    fn refill(&mut self) {
        let mut bytes = Vec::with_capacity(480 * 4);
        for _ in 0..480 {
            let t = self.sample_idx as f32 / 48_000.0;
            let s = Self::AMPLITUDE * (2.0 * std::f32::consts::PI * self.hz * t).sin();
            bytes.extend_from_slice(&s.to_le_bytes());
            self.sample_idx += 1;
        }
        self.leftover = bytes;
        self.pos = 0;
    }
}

impl Read for ToneSource {
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

impl Seek for ToneSource {
    fn seek(&mut self, _pos: SeekFrom) -> io::Result<u64> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "tone source is a live stream"))
    }
}

impl MediaSource for ToneSource {
    fn is_seekable(&self) -> bool {
        false
    }
    fn byte_len(&self) -> Option<u64> {
        None
    }
}

// ─── Serenity gateway handler: join both guilds on ready ───────────────────────

struct SpikeReady {
    manager: Arc<Songbird>,
    targets: [Arc<GuildTarget>; 2],
    joined: AtomicBool,
}

#[async_trait]
impl serenity::prelude::EventHandler for SpikeReady {
    async fn ready(&self, _ctx: Context, ready: Ready) {
        info!(user = %ready.user.name, "gateway ready");
        if self.joined.swap(true, Ordering::SeqCst) {
            return; // session resume — don't re-join
        }
        for t in &self.targets {
            let call_lock = self.manager.get_or_insert(t.guild);
            {
                let mut call = call_lock.lock().await;
                // Register before joining: DriverConnect and the first
                // SpeakingStateUpdates fire during the join await.
                for ev in [
                    CoreEvent::DriverConnect,
                    CoreEvent::DriverReconnect,
                    CoreEvent::DriverDisconnect,
                    CoreEvent::SpeakingStateUpdate,
                    CoreEvent::VoiceTick,
                    CoreEvent::ClientDisconnect,
                ] {
                    call.add_global_event(ev.into(), SpikeEvents { t: t.clone() });
                }
                let input = Input::from(RawAdapter::new(ToneSource::new(t.tone_hz), 48_000, 1));
                let _handle = call.play_input(input);
            }
            match self.manager.join(t.guild, t.channel).await {
                Ok(_) => info!(
                    guild = t.label,
                    guild_id = t.guild.get(),
                    channel_id = t.channel.get(),
                    tone_hz = t.tone_hz,
                    "joined voice"
                ),
                Err(e) => error!(guild = t.label, error = %e, "voice join FAILED"),
            }
        }
        let a = self.targets[0].own_ssrc.load(Ordering::Relaxed);
        let b = self.targets[1].own_ssrc.load(Ordering::Relaxed);
        info!(
            ssrc_a = a,
            ssrc_b = b,
            distinct = a != 0 && b != 0 && a != b,
            "SPIKE: both joins attempted — two DriverConnects with distinct SSRCs = GO"
        );
    }
}

// ─── Main ───────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,serenity=warn,songbird=info".into()),
        )
        .init();

    let token = env::var("NVB_SPIKE_TOKEN").context("missing env NVB_SPIKE_TOKEN")?;
    let a = GuildTarget::from_env("A", "NVB_SPIKE_GUILD_A", "NVB_SPIKE_CHANNEL_A", 440.0)?;
    let b = GuildTarget::from_env("B", "NVB_SPIKE_GUILD_B", "NVB_SPIKE_CHANNEL_B", 660.0)?;
    if a.guild == b.guild {
        bail!("the spike needs two DIFFERENT guilds (one Call per guild per bot)");
    }

    // Same voice config as production (`SharedPool::run`): Opus decode on
    // (DAVE-receive) so capture works.
    let sb_config = songbird::Config::default().decode_mode(DecodeMode::Decode(DecodeConfig::default()));
    let manager = Songbird::serenity_from_config(sb_config);

    let intents = GatewayIntents::GUILDS | GatewayIntents::GUILD_VOICE_STATES;
    let mut client = Client::builder(&token, intents)
        .event_handler(SpikeReady {
            manager: manager.clone(),
            targets: [a.clone(), b.clone()],
            joined: AtomicBool::new(false),
        })
        .register_songbird_with(manager.clone())
        .await
        .context("failed to build spike client")?;
    let shard_manager = client.shard_manager.clone();

    let gateway = tokio::spawn(async move { client.start().await });

    // Soak status: per-guild tick counts every minute. Both advancing ≈3000/min
    // means both drivers are alive and decoding.
    let started = Instant::now();
    let status = {
        let (a, b) = (a.clone(), b.clone());
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            tick.tick().await;
            loop {
                tick.tick().await;
                info!(
                    elapsed_min = started.elapsed().as_secs() / 60,
                    ticks_a = a.ticks.load(Ordering::Relaxed),
                    ticks_b = b.ticks.load(Ordering::Relaxed),
                    ssrc_a = a.own_ssrc.load(Ordering::Relaxed),
                    ssrc_b = b.own_ssrc.load(Ordering::Relaxed),
                    "soak status"
                );
            }
        })
    };

    info!("spike running — speak in both channels, listen for the per-guild tones; Ctrl-C to stop");
    tokio::signal::ctrl_c().await.context("ctrl-c handler")?;
    info!("Ctrl-C — tearing down");
    status.abort();

    for t in [&a, &b] {
        match manager.remove(t.guild).await {
            Ok(()) => info!(guild = t.label, "left voice"),
            Err(e) => warn!(guild = t.label, error = %e, "voice leave failed"),
        }
    }
    a.finalize_wavs();
    b.finalize_wavs();
    shard_manager.shutdown_all().await;
    let _ = gateway.await;

    let (sa, sb) = (a.own_ssrc.load(Ordering::Relaxed), b.own_ssrc.load(Ordering::Relaxed));
    info!(
        elapsed_min = started.elapsed().as_secs() / 60,
        ssrc_a = sa,
        ssrc_b = sb,
        distinct_ssrcs = sa != 0 && sb != 0 && sa != sb,
        ticks_a = a.ticks.load(Ordering::Relaxed),
        ticks_b = b.ticks.load(Ordering::Relaxed),
        "SPIKE SUMMARY — GO requires: distinct_ssrcs=true, both tick counts advanced, clean WAVs"
    );
    Ok(())
}
