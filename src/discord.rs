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
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::{capture::CaptureSink, config::DiscordConfig};

// ─── Voice capture handler ────────────────────────────────────────────────────

/// Songbird event handler: buffers decoded, DAVE-decrypted PCM into the
/// per-SSRC WAV files via `CaptureSink`.
struct VoiceReceiver {
    sink: Arc<CaptureSink>,
}

#[async_trait]
impl SongbirdEventHandler for VoiceReceiver {
    async fn act(&self, ctx: &EventContext<'_>) -> Option<Event> {
        if let EventContext::VoiceTick(tick) = ctx {
            for (ssrc, data) in &tick.speaking {
                if let Some(pcm) = &data.decoded_voice {
                    self.sink.write_frame(*ssrc, pcm);
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
        if let EventContext::SpeakingStateUpdate(Speaking { ssrc, user_id, .. }) = ctx {
            if let Some(uid) = user_id {
                tracing::info!(ssrc, user_id = uid.0, "SSRC → Discord user mapping");
            }
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
    sink: Arc<CaptureSink>,
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
                    VoiceReceiver { sink: Arc::clone(&self.sink) },
                );
                call.add_global_event(
                    CoreEvent::SpeakingStateUpdate.into(),
                    SpeakingLogger,
                );

                tracing::info!("audio capture active — talk in Discord to generate .wav files");
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

pub async fn run(cfg: &DiscordConfig, sink: Arc<CaptureSink>) -> Result<()> {
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
        sink,
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
