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

/// One appservice-authenticated homeserver endpoint the harness mints fake
/// Matrix users on — the primary (`nvbtest` on the bridge's own homeserver)
/// or the #109 federation fixture (`autotest` on nether.autos). Actors that
/// take an `AsHomeserver` can impersonate users on either side of a
/// federated exchange.
#[derive(Debug, Clone)]
pub struct AsHomeserver {
    /// Client-API base URL, no trailing slash.
    pub base_url: String,
    /// The appservice registration's `as_token` (secret — never logged).
    pub as_token: String,
    /// The server_name minted users live on.
    pub domain: String,
    /// Localpart stem of the registration's exclusive namespace, without the
    /// trailing underscore (`nvbtest` / `autotest`).
    pub user_prefix: String,
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
    fed_homeserver_url: Option<String>,
    fed_as_token: Option<String>,
    fed_domain: Option<String>,
    fed_user_prefix: Option<String>,
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
    /// The #109 federation fixture (nether.autos + its `autotest`
    /// registration). Optional — the `fed_*` scenarios report `skipped` when
    /// absent, so the suite still runs anywhere. A PARTIAL group fails the
    /// load loudly (a typo must never demote a run to silently-skipped).
    pub federation: Option<AsHomeserver>,
    /// Where per-run artifacts (bridge log, report.json, WAVs) land.
    pub artifacts_dir: PathBuf,
}

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

/// Host component of an http(s) URL (the same rule the bridge applies to
/// derive `matrix_domain` from `homeserver_url`).
fn url_host(url: &str) -> String {
    url.trim_start_matches("https://")
        .trim_start_matches("http://")
        .split(['/', ':'])
        .next()
        .unwrap_or("")
        .to_owned()
}

/// Resolve the optional `fed_*` group: all keys absent → `None` (the `fed_*`
/// scenarios skip); url + as_token present → the fixture endpoint (domain
/// defaults to the url host, prefix to `autotest`); anything partial → a loud
/// error, so a typo can't demote a federated run to silently-skipped.
fn resolve_federation(
    url: Option<String>,
    as_token: Option<String>,
    domain: Option<String>,
    user_prefix: Option<String>,
) -> Result<Option<AsHomeserver>> {
    match (url, as_token) {
        (Some(url), Some(as_token)) => {
            let base_url = url.trim_end_matches('/').to_owned();
            let domain = domain.unwrap_or_else(|| url_host(&base_url));
            Ok(Some(AsHomeserver {
                base_url,
                as_token,
                domain,
                user_prefix: user_prefix.unwrap_or_else(|| "autotest".into()),
            }))
        }
        (None, None) if domain.is_none() && user_prefix.is_none() => Ok(None),
        _ => bail!(
            "federation fixture config is incomplete: NVB_E2E_FED_HOMESERVER_URL and \
             NVB_E2E_FED_AS_TOKEN (fed_homeserver_url/fed_as_token in e2e.toml) must both \
             be set when any fed_* key is present"
        ),
    }
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
            federation: resolve_federation(
                pick("NVB_E2E_FED_HOMESERVER_URL", file.fed_homeserver_url),
                pick("NVB_E2E_FED_AS_TOKEN", file.fed_as_token),
                pick("NVB_E2E_FED_DOMAIN", file.fed_domain),
                pick("NVB_E2E_FED_USER_PREFIX", file.fed_user_prefix),
            )?,
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

    /// The PRIMARY appservice endpoint (the `nvbtest` registration on the
    /// bridge's own homeserver) as an [`AsHomeserver`] — what every actor
    /// used before the #109 fixture existed, now as a value the fixture
    /// variant can substitute. Fail-loud like [`Self::test_as_token`].
    pub fn primary_as(&self) -> Result<AsHomeserver> {
        Ok(AsHomeserver {
            base_url: self.homeserver_url.trim_end_matches('/').to_owned(),
            as_token: self.test_as_token()?.to_owned(),
            domain: self.server_domain(),
            user_prefix: "nvbtest".into(),
        })
    }

    /// The #109 federation fixture endpoint, or a [`crate::scenario::Skip`]
    /// error — the `fed_*` scenarios' one-line gate:
    /// `let fed = ctx.cfg.federation_or_skip()?;`
    pub fn federation_or_skip(&self) -> Result<&AsHomeserver> {
        self.federation.as_ref().ok_or_else(|| {
            crate::scenario::Skip(
                "no federation fixture configured: set NVB_E2E_FED_HOMESERVER_URL + \
                 NVB_E2E_FED_AS_TOKEN (fed_homeserver_url/fed_as_token in e2e.toml) \
                 to run the fed_* scenarios (see docs/e2e-homeserver.md)"
                    .into(),
            )
            .into()
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
        url_host(&self.homeserver_url)
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
            federation: None,
            artifacts_dir: "art".into(),
        };
        assert_eq!(cfg.server_domain(), "100.64.0.21");
        cfg.matrix_domain = Some("nether.im".into());
        assert_eq!(cfg.server_domain(), "nether.im");
        assert_eq!(cfg.bot_user_id(), "@nether-voicebridge_bot:nether.im");
        assert_eq!(cfg.ghost_prefix(), "nether-voicebridge_");

        // primary_as() is fail-loud without the test appservice, and mirrors
        // the url/domain resolution once it's present.
        assert!(cfg.primary_as().is_err());
        cfg.test_as_token = Some("tok".into());
        let primary = cfg.primary_as().unwrap();
        assert_eq!(primary.base_url, "http://100.64.0.21:8008");
        assert_eq!(primary.domain, "nether.im");
        assert_eq!(primary.user_prefix, "nvbtest");

        // federation_or_skip() without the fixture is a Skip, not a failure.
        let err = cfg.federation_or_skip().unwrap_err();
        assert!(err.is::<crate::scenario::Skip>());
        cfg.federation = Some(AsHomeserver {
            base_url: "https://nether.autos".into(),
            as_token: "t".into(),
            domain: "nether.autos".into(),
            user_prefix: "autotest".into(),
        });
        assert_eq!(cfg.federation_or_skip().unwrap().domain, "nether.autos");
    }

    #[test]
    fn federation_group_resolves_or_fails_loudly() {
        // Absent → None (scenarios skip).
        assert!(resolve_federation(None, None, None, None).unwrap().is_none());
        // Complete → defaults derived from the url.
        let fed = resolve_federation(
            Some("https://nether.autos/".into()),
            Some("tok".into()),
            None,
            None,
        )
        .unwrap()
        .unwrap();
        assert_eq!(fed.base_url, "https://nether.autos");
        assert_eq!(fed.domain, "nether.autos");
        assert_eq!(fed.user_prefix, "autotest");
        // Partial → loud error, never a silent skip.
        assert!(resolve_federation(Some("https://nether.autos".into()), None, None, None).is_err());
        assert!(resolve_federation(None, Some("tok".into()), None, None).is_err());
        assert!(resolve_federation(None, None, Some("nether.autos".into()), None).is_err());
    }

    #[test]
    fn profile_parses_and_rejects() {
        assert_eq!(Profile::parse("compressed").unwrap(), Profile::Compressed);
        assert_eq!(Profile::parse("real").unwrap(), Profile::Real);
        assert!(Profile::parse("fast").is_err());
    }
}
