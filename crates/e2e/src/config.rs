//! Harness configuration: where the bridge binary lives, which homeserver /
//! appservice / Discord assets the standing test environment provides.
//!
//! Resolution is **env-first with an `e2e.toml` fallback** so CI passes
//! everything through Forgejo Actions secrets while a developer keeps a local
//! file. Every env var is `NVB_E2E_*`; the file uses the same key names minus
//! the prefix, lowercased (e.g. `NVB_E2E_ROOM_A` ↔ `room_a`).
//!
//! Secrets (appservice token, bot tokens) follow the repo rule: never logged,
//! never echoed into artifacts.

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::path::PathBuf;

/// Timing profile for a run. `Compressed` starts the bridge-under-test with
/// `NVB_TIMING=compressed` (the #38 C1 preset — graces in seconds);
/// `Real` leaves timing at production defaults for the nightly real-timing
/// subset (reap grace, leave-grace debounce).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    Compressed,
    Real,
}

impl Profile {
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "compressed" => Ok(Self::Compressed),
            "real" => Ok(Self::Real),
            other => bail!("unknown profile '{other}' (expected 'compressed' or 'real')"),
        }
    }

    /// Ceiling for a single scenario; generous because live runs ride real
    /// gateway/HTTP round-trips.
    pub fn scenario_timeout(self) -> std::time::Duration {
        match self {
            Self::Compressed => std::time::Duration::from_secs(300),
            Self::Real => std::time::Duration::from_secs(900),
        }
    }
}

/// The `e2e.toml` shape — every field optional; env wins over file.
#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    bridge_bin: Option<String>,
    homeserver_url: Option<String>,
    matrix_domain: Option<String>,
    appservice_id: Option<String>,
    appservice_token: Option<String>,
    sender_localpart: Option<String>,
    puppet_tokens: Option<Vec<String>>,
    speaker_tokens: Option<Vec<String>>,
    guild_id: Option<String>,
    channel_a: Option<String>,
    room_a: Option<String>,
    channel_b: Option<String>,
    room_b: Option<String>,
    channel_c: Option<String>,
    room_c: Option<String>,
    test_as_token: Option<String>,
    mod_token: Option<String>,
    artifacts_dir: Option<String>,
}

/// Fully-resolved harness configuration.
#[derive(Debug, Clone)]
pub struct E2eConfig {
    /// Path to the real `nether-voicebridge` binary under test.
    pub bridge_bin: PathBuf,
    // ── Matrix side (the bridge's own appservice identity — [server.test]) ──
    pub homeserver_url: String,
    pub matrix_domain: Option<String>,
    pub appservice_id: String,
    pub appservice_token: String,
    pub sender_localpart: String,
    // ── Discord side ──
    /// Puppet pool handed to the bridge-under-test. NEVER prod's tokens while
    /// prod is running — one token in two processes = fighting voice sessions.
    pub puppet_tokens: Vec<String>,
    /// Harness-owned speaker bots (hand-created, invited to the test guild).
    pub speaker_tokens: Vec<String>,
    pub guild_id: String,
    /// Auto Test 01: unencrypted room + its voice channel.
    pub channel_a: String,
    pub room_a: String,
    /// Auto Test 02: ENCRYPTED room + its voice channel. Optional — only the
    /// `audio_*_enc` scenarios need them (they bail with a clear message when
    /// absent, so unencrypted-only runs need no AT02 provisioning).
    pub channel_b: Option<String>,
    pub room_b: Option<String>,
    /// Auto Test 03: a SECOND unencrypted room + its voice channel. Optional —
    /// only scenarios needing a distinct-from-AT01 unencrypted target (e.g. the
    /// force-move and multi-bridge-isolation scenarios) require them; they bail
    /// with a clear message when absent.
    pub channel_c: Option<String>,
    pub room_c: Option<String>,
    /// The TEST appservice's `as_token` (the `nvbtest` registration) — the
    /// fake Matrix participants (D2 testkit) masquerade under it. Optional:
    /// only scenarios with Matrix actors need it.
    pub test_as_token: Option<String>,
    /// A Discord bot token with Move Members + Connect (the `nvb-test-mod`
    /// application) used by the `DiscordMod` REST actor to force-move /
    /// disconnect a puppet. Optional — only the mod-driven scenarios need it.
    pub mod_token: Option<String>,
    /// Where per-run artifacts (bridge log, report.json, WAVs) land.
    pub artifacts_dir: PathBuf,
}

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

impl E2eConfig {
    /// Load from `NVB_E2E_*` env vars with `e2e.toml` fallback (path override
    /// via `NVB_E2E_CONFIG`). Missing required fields are reported together.
    pub fn load() -> Result<Self> {
        let file_path = env_nonempty("NVB_E2E_CONFIG").unwrap_or_else(|| "e2e.toml".into());
        let file: FileConfig = match std::fs::read_to_string(&file_path) {
            Ok(s) => toml::from_str(&s).with_context(|| format!("parsing {file_path}"))?,
            Err(_) => FileConfig::default(),
        };

        let pick = |env: &str, file_val: Option<String>| env_nonempty(env).or(file_val);
        let pick_list = |env: &str, file_val: Option<Vec<String>>| {
            env_nonempty(env)
                .map(|v| v.split(',').map(|s| s.trim().to_owned()).filter(|s| !s.is_empty()).collect())
                .or(file_val)
                .unwrap_or_default()
        };

        let mut missing: Vec<&str> = Vec::new();
        let mut req = |env: &'static str, v: Option<String>| {
            v.unwrap_or_else(|| {
                missing.push(env);
                String::new()
            })
        };

        // The bridge child runs with its workdir as cwd (so diagnostic WAV taps
        // land in the artifacts), so a relative binary path must be resolved
        // to absolute here. A missing binary keeps the raw path — the spawn
        // path reports it with a clear error.
        let bridge_bin_raw: PathBuf = pick("NVB_E2E_BRIDGE_BIN", file.bridge_bin)
            .unwrap_or_else(|| "target/debug/nether-voicebridge".into())
            .into();
        let cfg = Self {
            bridge_bin: std::fs::canonicalize(&bridge_bin_raw).unwrap_or(bridge_bin_raw),
            homeserver_url: req("NVB_E2E_HOMESERVER_URL", pick("NVB_E2E_HOMESERVER_URL", file.homeserver_url)),
            matrix_domain: pick("NVB_E2E_MATRIX_DOMAIN", file.matrix_domain),
            appservice_id: req("NVB_E2E_APPSERVICE_ID", pick("NVB_E2E_APPSERVICE_ID", file.appservice_id)),
            appservice_token: req(
                "NVB_E2E_APPSERVICE_TOKEN",
                pick("NVB_E2E_APPSERVICE_TOKEN", file.appservice_token),
            ),
            sender_localpart: req(
                "NVB_E2E_SENDER_LOCALPART",
                pick("NVB_E2E_SENDER_LOCALPART", file.sender_localpart),
            ),
            puppet_tokens: pick_list("NVB_E2E_PUPPET_TOKENS", file.puppet_tokens),
            speaker_tokens: pick_list("NVB_E2E_SPEAKER_TOKENS", file.speaker_tokens),
            guild_id: req("NVB_E2E_GUILD_ID", pick("NVB_E2E_GUILD_ID", file.guild_id)),
            channel_a: req("NVB_E2E_CHANNEL_A", pick("NVB_E2E_CHANNEL_A", file.channel_a)),
            room_a: req("NVB_E2E_ROOM_A", pick("NVB_E2E_ROOM_A", file.room_a)),
            channel_b: pick("NVB_E2E_CHANNEL_B", file.channel_b),
            room_b: pick("NVB_E2E_ROOM_B", file.room_b),
            channel_c: pick("NVB_E2E_CHANNEL_C", file.channel_c),
            room_c: pick("NVB_E2E_ROOM_C", file.room_c),
            test_as_token: pick("NVB_E2E_TEST_AS_TOKEN", file.test_as_token),
            mod_token: pick("NVB_E2E_MOD_TOKEN", file.mod_token),
            artifacts_dir: pick("NVB_E2E_ARTIFACTS", file.artifacts_dir)
                .unwrap_or_else(|| "e2e-artifacts".into())
                .into(),
        };

        if !missing.is_empty() {
            bail!(
                "missing required e2e configuration: {} (set the env vars or e2e.toml keys)",
                missing.join(", ")
            );
        }
        if cfg.puppet_tokens.is_empty() {
            bail!("NVB_E2E_PUPPET_TOKENS must list at least one dedicated harness puppet token");
        }
        if cfg.speaker_tokens.is_empty() {
            bail!("NVB_E2E_SPEAKER_TOKENS must list at least one speaker bot token");
        }
        Ok(cfg)
    }

    /// The AT02 (encrypted) room + channel pair, or a clear error naming the
    /// missing keys — used by the `audio_*_enc` scenarios.
    pub fn encrypted_pair(&self) -> Result<(&str, &str)> {
        match (self.room_b.as_deref(), self.channel_b.as_deref()) {
            (Some(r), Some(c)) => Ok((r, c)),
            _ => bail!(
                "this scenario needs the encrypted test room: set NVB_E2E_ROOM_B and \
                 NVB_E2E_CHANNEL_B (or room_b/channel_b in e2e.toml) to Auto Test 02's ids"
            ),
        }
    }

    /// The test appservice token, or a clear error — required by every
    /// scenario that drives fake Matrix participants.
    pub fn test_as_token(&self) -> Result<&str> {
        self.test_as_token.as_deref().ok_or_else(|| {
            anyhow::anyhow!(
                "this scenario needs the test appservice: set NVB_E2E_TEST_AS_TOKEN \
                 (or test_as_token in e2e.toml) to the nvbtest registration's as_token"
            )
        })
    }

    /// The AT03 (second unencrypted) room + channel pair, or a clear error
    /// naming the missing keys — used by the force-move / multi-bridge scenarios.
    pub fn at03_pair(&self) -> Result<(&str, &str)> {
        match (self.room_c.as_deref(), self.channel_c.as_deref()) {
            (Some(r), Some(c)) => Ok((r, c)),
            _ => bail!(
                "this scenario needs the second unencrypted test room: set \
                 NVB_E2E_ROOM_C and NVB_E2E_CHANNEL_C (or room_c/channel_c in \
                 e2e.toml) to Auto Test 03's ids"
            ),
        }
    }

    /// The `nvb-test-mod` Discord token, or a clear error — required by the
    /// mod-driven scenarios (force-move / disconnect).
    pub fn mod_token(&self) -> Result<&str> {
        self.mod_token.as_deref().ok_or_else(|| {
            anyhow::anyhow!(
                "this scenario needs the mod bot: set NVB_E2E_MOD_TOKEN \
                 (or mod_token in e2e.toml) to the nvb-test-mod application's token"
            )
        })
    }

    /// The bridge bot's full MXID, derived the same way the bridge derives it
    /// (`{sender_localpart}_bot` on the server domain — see
    /// `nvb_config::Config::resolve_bot_user_ids`).
    pub fn bot_user_id(&self) -> String {
        format!("@{}_bot:{}", self.sender_localpart, self.server_domain())
    }

    /// Ghost localparts start with this (`{sender_localpart}_`).
    pub fn ghost_prefix(&self) -> String {
        format!("{}_", self.sender_localpart)
    }

    /// Server domain: explicit `matrix_domain` wins, else the host of
    /// `homeserver_url` (mirrors the bridge's own resolution).
    pub fn server_domain(&self) -> String {
        if let Some(d) = &self.matrix_domain {
            return d.clone();
        }
        self.homeserver_url
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .split(['/', ':'])
            .next()
            .unwrap_or("")
            .to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_domain_prefers_matrix_domain_then_url_host() {
        let mut cfg = E2eConfig {
            bridge_bin: "x".into(),
            homeserver_url: "http://100.64.0.21:8008".into(),
            matrix_domain: None,
            appservice_id: "as".into(),
            appservice_token: "t".into(),
            sender_localpart: "nether-voicebridge".into(),
            puppet_tokens: vec!["a".into()],
            speaker_tokens: vec!["b".into()],
            guild_id: "1".into(),
            channel_a: "2".into(),
            room_a: "!r:nether.im".into(),
            channel_b: None,
            room_b: None,
            channel_c: None,
            room_c: None,
            test_as_token: None,
            mod_token: None,
            artifacts_dir: "art".into(),
        };
        assert_eq!(cfg.server_domain(), "100.64.0.21");
        cfg.matrix_domain = Some("nether.im".into());
        assert_eq!(cfg.server_domain(), "nether.im");
        assert_eq!(cfg.bot_user_id(), "@nether-voicebridge_bot:nether.im");
        assert_eq!(cfg.ghost_prefix(), "nether-voicebridge_");
    }

    #[test]
    fn profile_parses_and_rejects() {
        assert_eq!(Profile::parse("compressed").unwrap(), Profile::Compressed);
        assert_eq!(Profile::parse("real").unwrap(), Profile::Real);
        assert!(Profile::parse("fast").is_err());
    }
}
