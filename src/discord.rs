use anyhow::Result;
use serenity::{
    async_trait,
    model::prelude::*,
    prelude::*,
};
use songbird::{
    driver::{DecodeConfig, DecodeMode},
    model::payload::Speaking,
    CoreEvent, Event, EventContext, EventHandler as SongbirdEventHandler,
    SerenityInit,
};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::sync::{mpsc, Mutex};

use crate::{audio, capture::CaptureSink, config::DiscordConfig};

// ─── Voice capture handler ────────────────────────────────────────────────────

/// Songbird event handler: at each 20 ms `VoiceTick`, mixes all non-excluded
/// Discord speakers (songbird delivers them already time-aligned for the tick)
/// into one mono frame and forwards it to the Matrix publish path.  Optionally
/// also writes per-SSRC WAV files for debugging.
struct VoiceReceiver {
    /// Forward channel to the RTC audio consumer.  20 ms mono frames (960 i16).
    audio_tx: mpsc::Sender<Vec<i16>>,
    /// Bridge-owned SSRCs to exclude from capture (echo prevention).  Empty in
    /// Phase 5; populated in Phase 7 once puppets replay Matrix audio that the
    /// listener would otherwise re-capture.
    exclude: Arc<HashSet<u32>>,
    /// Optional Phase 3 WAV capture, enabled via `[discord] capture_wav`.
    capture_sink: Option<Arc<CaptureSink>>,
    /// Count of frames dropped because the forward channel was full (logged
    /// periodically rather than per-drop to avoid spamming the audio path).
    dropped: AtomicU64,
}

impl VoiceReceiver {
    /// Build the mixed 20 ms mono frame for this tick from the present,
    /// non-excluded speakers.  Returns silence (zeros) when nobody is speaking
    /// so the downstream clock/jitter buffer keeps ticking at a steady 50 Hz.
    fn mix_tick(&self, tick: &songbird::events::context_data::VoiceTick) -> Vec<i16> {
        let mut monos: Vec<Vec<i16>> = Vec::new();
        for (ssrc, data) in &tick.speaking {
            if self.exclude.contains(ssrc) {
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
    async fn act(&self, ctx: &EventContext<'_>) -> Option<Event> {
        if let EventContext::VoiceTick(tick) = ctx {
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
struct SpeakingLogger;

#[async_trait]
impl SongbirdEventHandler for SpeakingLogger {
    async fn act(&self, ctx: &EventContext<'_>) -> Option<Event> {
        if let EventContext::SpeakingStateUpdate(Speaking { ssrc, user_id: Some(uid), .. }) = ctx {
            tracing::info!(ssrc, user_id = uid.0, "SSRC → Discord user mapping");
        }
        None
    }
}

// ─── Serenity gateway handler ─────────────────────────────────────────────────

struct Handler {
    guild_id: GuildId,
    channel_id: ChannelId,
    /// The listener bot's own UserId — seeded in `ready`, used to filter its
    /// own voice-state events (echo-exclusion seed for future phases).
    bridge_user_ids: Arc<Mutex<Vec<UserId>>>,
    /// Forward channel to the RTC audio consumer.
    audio_tx: mpsc::Sender<Vec<i16>>,
    /// Bridge-owned SSRCs to exclude from capture (empty in Phase 5).
    exclude: Arc<HashSet<u32>>,
    /// Optional WAV capture, enabled via `[discord] capture_wav`.
    capture_sink: Option<Arc<CaptureSink>>,
}

#[async_trait]
impl EventHandler for Handler {
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
                        audio_tx: self.audio_tx.clone(),
                        exclude: Arc::clone(&self.exclude),
                        capture_sink: self.capture_sink.clone(),
                        dropped: AtomicU64::new(0),
                    },
                );
                call.add_global_event(
                    CoreEvent::SpeakingStateUpdate.into(),
                    SpeakingLogger,
                );

                tracing::info!("audio capture active — talk in Discord to bridge into Element Call");
            }
            Err(e) => tracing::error!(error = %e, "failed to join voice channel"),
        }
    }

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

pub async fn run(
    cfg: &DiscordConfig,
    audio_tx: mpsc::Sender<Vec<i16>>,
    capture_sink: Option<Arc<CaptureSink>>,
) -> Result<()> {
    let guild_id: u64 = cfg
        .guild_id
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid guild_id: {}", cfg.guild_id))?;
    let channel_id: u64 = cfg
        .channel_id
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid channel_id: {}", cfg.channel_id))?;

    let handler = Handler {
        guild_id: GuildId::new(guild_id),
        channel_id: ChannelId::new(channel_id),
        bridge_user_ids: Arc::new(Mutex::new(Vec::new())),
        audio_tx,
        exclude: Arc::new(HashSet::new()),
        capture_sink,
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

    tracing::info!("starting Discord gateway");
    client
        .start()
        .await
        .map_err(|e| anyhow::anyhow!("Discord client error: {e}"))
}
