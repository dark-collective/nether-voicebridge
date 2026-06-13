#![allow(dead_code)] // secret fields are read in later phases

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file '{path}': {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub discord: DiscordConfig,
    pub matrix: MatrixConfig,
    pub livekit: LiveKitConfig,
}

#[derive(Debug, Deserialize)]
pub struct DiscordConfig {
    pub guild_id: String,
    pub channel_id: String,
    pub listener_token: String,
    pub puppet_tokens: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct MatrixConfig {
    pub homeserver_url: String,
    pub room_id: String,
    pub bot_user_id: String,
    pub access_token: String,
    pub appservice_id: String,
    pub appservice_token: String,
}

#[derive(Debug, Deserialize)]
pub struct LiveKitConfig {
    pub lk_jwt_url: String,
    pub url: String,
    pub api_key: String,
    pub api_secret: String,
}

impl Config {
    pub fn load(path: &str) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_string(),
            source,
        })?;
        toml::from_str(&content).map_err(ConfigError::Parse)
    }

    pub fn log_redacted_summary(&self) {
        tracing::info!(
            discord_guild_id = %self.discord.guild_id,
            discord_channel_id = %self.discord.channel_id,
            discord_listener_token = "[redacted]",
            discord_puppet_count = self.discord.puppet_tokens.len(),
            matrix_homeserver_url = %self.matrix.homeserver_url,
            matrix_room_id = %self.matrix.room_id,
            matrix_bot_user_id = %self.matrix.bot_user_id,
            matrix_access_token = "[redacted]",
            matrix_appservice_id = %self.matrix.appservice_id,
            matrix_appservice_token = "[redacted]",
            livekit_lk_jwt_url = %self.livekit.lk_jwt_url,
            livekit_url = %self.livekit.url,
            livekit_api_key = %self.livekit.api_key,
            livekit_api_secret = "[redacted]",
            "config loaded"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_config() {
        let raw = r#"
[discord]
guild_id = "111"
channel_id = "222"
listener_token = "Bot abc"
puppet_tokens = ["Bot def", "Bot ghi"]

[matrix]
homeserver_url = "https://nether.im"
room_id = "!room:nether.im"
bot_user_id = "@bridge:nether.im"
access_token = "syt_tok"
appservice_id = "voicebridge"
appservice_token = "hs_tok"

[livekit]
lk_jwt_url = "https://lk-jwt.nether.im"
url = "wss://livekit.nether.im"
api_key = "key"
api_secret = "secret"
"#;
        let cfg: Config = toml::from_str(raw).expect("parse failed");
        assert_eq!(cfg.discord.puppet_tokens.len(), 2);
        assert_eq!(cfg.matrix.homeserver_url, "https://nether.im");
        assert_eq!(cfg.livekit.api_key, "key");
    }

    #[test]
    fn rejects_missing_section() {
        let raw = r#"
[discord]
guild_id = "111"
channel_id = "222"
listener_token = "Bot abc"
puppet_tokens = []
"#;
        assert!(toml::from_str::<Config>(raw).is_err());
    }
}
