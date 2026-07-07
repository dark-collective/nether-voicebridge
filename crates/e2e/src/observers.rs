//! Out-of-band observers: what the harness sees WITHOUT trusting the bridge's
//! own logs — Matrix room state over the raw CS API, and Discord voice
//! presence via a gateway cache. Baseline pre/postconditions are built from
//! these.

use crate::config::E2eConfig;
use anyhow::{Context, Result, bail};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

/// Who a `m.call.member` entry belongs to, classified by sender MXID prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SenderClass {
    /// The bridge bot itself (`{sender_localpart}_bot`).
    Bot,
    /// A bridge ghost (`{sender_localpart}_<suffix>` other than `_bot`).
    Ghost,
    /// Anyone else (a real user, a harness fake user).
    Other,
}

pub fn sender_class(sender: &str, ghost_prefix: &str, bot_user_id: &str) -> SenderClass {
    if sender == bot_user_id {
        return SenderClass::Bot;
    }
    let localpart = sender.strip_prefix('@').and_then(|r| r.split(':').next()).unwrap_or("");
    if localpart.starts_with(ghost_prefix) {
        SenderClass::Ghost
    } else {
        SenderClass::Other
    }
}

/// One `org.matrix.msc3401.call.member` state entry.
#[derive(Debug, Clone)]
pub struct CallMemberEntry {
    #[allow(dead_code)] // read by the D2 reaper scenarios
    pub state_key: String,
    pub sender: String,
    /// Non-empty content = an asserted (possibly expired) membership; the
    /// bridge's own liveness rules are NOT re-implemented here — scenarios
    /// assert on presence/absence transitions, which empty-content marks.
    pub active: bool,
    pub class: SenderClass,
}

/// Polls room state over the raw CS API with the appservice token. A browser
/// UA is set explicitly: the standing homeserver sits behind Cloudflare,
/// which 403s the default library UAs.
pub struct RoomStatePoller {
    http: reqwest::Client,
    base: String,
    as_token: String,
    ghost_prefix: String,
    bot_user_id: String,
}

impl RoomStatePoller {
    pub fn new(cfg: &E2eConfig) -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .use_rustls_tls()
                .user_agent("Mozilla/5.0 (compatible; nvb-e2e)")
                .build()
                .context("building poller http client")?,
            base: cfg.homeserver_url.trim_end_matches('/').to_owned(),
            as_token: cfg.appservice_token.clone(),
            ghost_prefix: cfg.ghost_prefix(),
            bot_user_id: cfg.bot_user_id(),
        })
    }

    /// Current `m.call.member` entries in the room.
    pub async fn call_members(&self, room_id: &str) -> Result<Vec<CallMemberEntry>> {
        let url = format!(
            "{}/_matrix/client/v3/rooms/{}/state",
            self.base,
            urlencode(room_id)
        );
        let resp = self
            .http
            .get(url)
            .bearer_auth(&self.as_token)
            .send()
            .await
            .context("room state GET")?;
        if !resp.status().is_success() {
            bail!("room state GET failed: {}", resp.status());
        }
        let state: Vec<Value> = resp.json().await.context("room state JSON")?;
        Ok(state
            .iter()
            .filter(|ev| ev["type"] == "org.matrix.msc3401.call.member")
            .map(|ev| {
                let sender = ev["sender"].as_str().unwrap_or_default().to_owned();
                let class = sender_class(&sender, &self.ghost_prefix, &self.bot_user_id);
                CallMemberEntry {
                    state_key: ev["state_key"].as_str().unwrap_or_default().to_owned(),
                    active: ev["content"].as_object().is_some_and(|c| !c.is_empty()),
                    sender,
                    class,
                }
            })
            .collect())
    }

    /// Poll (1 s cadence) until the room's call members satisfy `pred`.
    pub async fn await_members(
        &self,
        room_id: &str,
        timeout: Duration,
        what: &str,
        pred: impl Fn(&[CallMemberEntry]) -> bool,
    ) -> Result<Vec<CallMemberEntry>> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let members = self.call_members(room_id).await?;
            if pred(&members) {
                return Ok(members);
            }
            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "timed out waiting for {what}; current entries: {:?}",
                    members
                        .iter()
                        .map(|m| format!("{:?} {} active={}", m.class, m.sender, m.active))
                        .collect::<Vec<_>>()
                );
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

fn urlencode(s: &str) -> String {
    // Room ids only need `!`/`:` escaping for path segments; cover the general
    // reserved set conservatively.
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// Discord-side connection-count probe: one harness bot's gateway cache of
/// the test guild's voice states. The baseline assert is "no bridge-owned bot
/// is in voice" before and after a scenario.
pub struct ConnCountProbe {
    cache: Arc<serenity::cache::Cache>,
    guild: serenity::model::id::GuildId,
    shard: Arc<serenity::gateway::ShardManager>,
    client_task: tokio::task::JoinHandle<()>,
}

struct ProbeHandler {
    ready: Arc<std::sync::atomic::AtomicBool>,
}

#[serenity::async_trait]
impl serenity::client::EventHandler for ProbeHandler {
    async fn ready(&self, _ctx: serenity::client::Context, _ready: serenity::model::gateway::Ready) {
        self.ready.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Poll an atomic ready flag — immune to the set-before-await race a Notify
/// wakeup has (READY can beat the first poll of a `Notified`).
pub(crate) async fn await_flag(
    flag: &std::sync::atomic::AtomicBool,
    timeout: Duration,
    what: &str,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    while !flag.load(std::sync::atomic::Ordering::SeqCst) {
        if tokio::time::Instant::now() >= deadline {
            bail!("{what} never became ready");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Ok(())
}

impl ConnCountProbe {
    /// Connect with `token` (a harness speaker/mod bot — it must be a member
    /// of the test guild). Resolves once the gateway is ready and the guild's
    /// voice states are cached.
    pub async fn connect(token: &str, guild_id: &str) -> Result<Self> {
        use serenity::model::gateway::GatewayIntents;
        let intents = GatewayIntents::GUILDS | GatewayIntents::GUILD_VOICE_STATES;
        let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut client = serenity::Client::builder(token, intents)
            .event_handler(ProbeHandler { ready: ready.clone() })
            .await
            .context("building probe client")?;
        let cache = client.cache.clone();
        let shard = client.shard_manager.clone();
        let client_task = tokio::spawn(async move {
            if let Err(e) = client.start().await {
                tracing::warn!(error = %e, "conn-count probe client ended");
            }
        });
        await_flag(&ready, Duration::from_secs(30), "probe gateway").await?;
        // GUILD_CREATE (with voice states) follows READY; give the cache a beat.
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok(Self {
            cache,
            guild: serenity::model::id::GuildId::new(guild_id.parse().context("guild id")?),
            shard,
            client_task,
        })
    }

    /// User ids currently in ANY voice channel of the test guild, filtered to
    /// `ids` (pass the bridge's puppet bot ids for the baseline check).
    pub fn voice_members_among(&self, ids: &HashSet<u64>) -> Vec<u64> {
        let Some(guild) = self.cache.guild(self.guild) else {
            return Vec::new();
        };
        guild
            .voice_states
            .iter()
            .filter(|(uid, vs)| ids.contains(&uid.get()) && vs.channel_id.is_some())
            .map(|(uid, _)| uid.get())
            .collect()
    }

    /// Poll until none of `ids` is in voice (or fail at `timeout`).
    pub async fn await_baseline(&self, ids: &HashSet<u64>, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let present = self.voice_members_among(ids);
            if present.is_empty() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("bridge bots still in voice at baseline check: {present:?}");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    pub async fn shutdown(self) {
        self.shard.shutdown_all().await;
        let _ = self.client_task.await;
    }
}

/// Decode a Discord bot's user id from its token (first dot-separated segment
/// is base64(user id)) — the same trick the puppet pool uses for pre-gateway
/// echo exclusion. Best-effort: `None` on foreign token shapes.
pub fn decode_bot_user_id(token: &str) -> Option<u64> {
    use base64::Engine as _;
    let seg = token.trim_start_matches("Bot ").split('.').next()?;
    let bytes = base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(seg.trim_end_matches('='))
        .or_else(|_| base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(seg.trim_end_matches('=')))
        .ok()?;
    String::from_utf8(bytes).ok()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sender_classification() {
        let gp = "nether-voicebridge_";
        let bot = "@nether-voicebridge_bot:nether.im";
        assert_eq!(sender_class(bot, gp, bot), SenderClass::Bot);
        assert_eq!(
            sender_class("@nether-voicebridge_123:nether.im", gp, bot),
            SenderClass::Ghost
        );
        assert_eq!(sender_class("@dark:nether.im", gp, bot), SenderClass::Other);
    }

    #[test]
    fn token_user_id_roundtrip() {
        use base64::Engine as _;
        // Mirrors the puppet.rs test: base64(user id string) as first segment.
        let uid = 1515382284960796774u64;
        let seg = base64::engine::general_purpose::STANDARD_NO_PAD.encode(uid.to_string());
        let token = format!("{seg}.AbCdEf.xyz");
        assert_eq!(decode_bot_user_id(&token), Some(uid));
        assert_eq!(decode_bot_user_id("garbage!!.x.y"), None);
    }

    #[test]
    fn urlencode_room_id() {
        assert_eq!(urlencode("!a b:nether.im"), "%21a%20b%3Anether.im");
    }
}
