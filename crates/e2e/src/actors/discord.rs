//! DiscordSpeaker: a hand-created harness bot that joins the test voice
//! channel and plays a deterministic probe — the "Discord human" the bridge
//! should mint a Matrix ghost for.
//!
//! Mirrors the bridge's own songbird idioms (pinned versions, constraint #7):
//! an explicit `Songbird` manager attached to a serenity client, and playback
//! through `RawAdapter` over f32-LE PCM (the path the workspace's symphonia
//! `pcm` feature guarantees — file/WAV decoding is NOT enabled).

use super::ParticipantDriver;
use anyhow::{Context as _, Result, anyhow};
use async_trait::async_trait;
use serenity::client::{Context, EventHandler};
use serenity::model::gateway::{GatewayIntents, Ready};
use serenity::model::id::{ChannelId, GuildId};
use songbird::input::{Input, RawAdapter};
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
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
    client_task: tokio::task::JoinHandle<()>,
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
            client_task,
        })
    }

    /// Play the seeded deterministic probe (Phase B synthesis — the same
    /// signal the audio oracle aligns against) and wait out its duration.
    pub async fn speak_probe(&self, seed: u64, dur_ms: u32) -> Result<()> {
        let call = self
            .songbird
            .get(self.guild)
            .ok_or_else(|| anyhow!("speaker is not in voice (call missing)"))?;
        let samples = nvb_audio::probe::synth_probe_mono_48k(seed, dur_ms);
        let input = Input::from(RawAdapter::new(ProbeSource::from_mono_i16(&samples), 48_000, 1));
        let handle = {
            let mut call = call.lock().await;
            call.play_input(input)
        };
        // Let the whole probe play out (plus scheduler slack), then stop.
        tokio::time::sleep(Duration::from_millis(dur_ms as u64 + 500)).await;
        let _ = handle.stop();
        Ok(())
    }

    /// Disconnect the gateway entirely (end of scenario).
    pub async fn shutdown(mut self) {
        let _ = self.leave().await;
        self.shard.shutdown_all().await;
        let _ = self.client_task.await;
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
