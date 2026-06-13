use anyhow::Result;
use serenity::{
    async_trait,
    model::prelude::*,
    prelude::*,
};
use songbird::SerenityInit;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::config::DiscordConfig;

struct Handler {
    guild_id: GuildId,
    channel_id: ChannelId,
    // listener bot's own UserId — excluded from join/leave logs (echo exclusion seed)
    bridge_user_ids: Arc<Mutex<Vec<UserId>>>,
}

#[async_trait]
impl EventHandler for Handler {
    async fn ready(&self, ctx: Context, ready: Ready) {
        tracing::info!(bot_user = %ready.user.name, "Discord listener connected");

        // Register ourselves so join/leave events for the bot are filtered out
        self.bridge_user_ids.lock().await.push(ready.user.id);

        let manager = songbird::get(&ctx).await.expect("songbird not registered");
        match manager.join(self.guild_id, self.channel_id).await {
            Ok(_) => tracing::info!(channel_id = %self.channel_id, "joined voice channel"),
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

pub async fn run(cfg: &DiscordConfig) -> Result<()> {
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
    };

    let intents = GatewayIntents::GUILDS | GatewayIntents::GUILD_VOICE_STATES;

    let mut client = Client::builder(&cfg.listener_token, intents)
        .event_handler(handler)
        .register_songbird()
        .await
        .map_err(|e| anyhow::anyhow!("failed to build Discord client: {e}"))?;

    tracing::info!("starting Discord gateway");
    client
        .start()
        .await
        .map_err(|e| anyhow::anyhow!("Discord client error: {e}"))
}
