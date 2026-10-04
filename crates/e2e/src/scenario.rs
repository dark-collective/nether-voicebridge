//! Scenario runner: registry entries, per-scenario artifacts, `report.json`,
//! and outcome accounting (a KnownFail that PASSES is surfaced too — it means
//! the gap it tracks got fixed and the expectation should be flipped).

use crate::config::{E2eConfig, Profile};
use anyhow::Result;
use serde::Serialize;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

/// What the catalogue expects of a scenario. `KnownFail` documents a filed
/// gap: the scenario runs, its failure is recorded as `known_fail` and does
/// NOT fail the suite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expectation {
    Pass,
    /// A filed, not-yet-fixed gap. Live user: `mgmt_dm_pending_reap` (#99),
    /// tracking the #97 pending-invite reap — alpha.25's exemption reads the
    /// SDK store's `invited_members_count()`, which lags/omits the fresh
    /// invite, so the reap still fires (live 2026-07-12, post-fix). Flip to
    /// `Pass` when the exemption is rebuilt on a non-lagging source. (First
    /// design target, the #62 force-move gap, was fixed before ever shipping
    /// a KnownFail.)
    KnownFail { issue: &'static str },
}

/// A scenario that cannot run in THIS environment — an optional fixture's
/// config group is absent (e.g. the #109 federation fixture's `fed_*` keys).
/// Returned through `anyhow` and intercepted by the runner as outcome
/// `skipped`: never fails the suite, never counts as `known_fail` /
/// `unexpected_pass`. Distinct from the `bail!`-ing accessors
/// (`encrypted_pair` &c.), which stay fail-loud: on the standing env those
/// assets are always provisioned, so their absence is a misconfiguration.
#[derive(Debug)]
pub struct Skip(pub String);

impl std::fmt::Display for Skip {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "skipped: {}", self.0)
    }
}

impl std::error::Error for Skip {}

/// Everything a scenario gets to work with.
pub struct Ctx {
    pub cfg: E2eConfig,
    pub profile: Profile,
    /// This scenario's own artifacts directory (bridge workdir, WAVs, …).
    pub artifacts: PathBuf,
}

use std::future::Future;
pub type ScenarioFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

pub struct ScenarioDef {
    pub name: &'static str,
    pub expectation: Expectation,
    pub run: fn(Arc<Ctx>) -> ScenarioFuture,
}

#[derive(Debug, Serialize)]
pub struct ScenarioReport {
    pub name: String,
    pub expectation: String,
    /// `pass` | `fail` | `known_fail` | `unexpected_pass` | `timeout` | `skipped`
    pub outcome: String,
    pub duration_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub artifacts_dir: String,
    /// Attempts that failed before the recorded one (the #124 retry), each
    /// with its preserved artifacts dir — so a retried failure keeps the
    /// evidence of BOTH attempts (nightly #501 lost attempt 1's).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub earlier_attempts: Vec<EarlierAttempt>,
}

#[derive(Debug, Serialize)]
pub struct EarlierAttempt {
    pub attempt: u32,
    pub error: String,
    pub artifacts_dir: String,
}

#[derive(Debug, Serialize)]
pub struct RunReport {
    pub profile: String,
    pub scenarios: Vec<ScenarioReport>,
}

impl RunReport {
    /// Suite verdict: false when any `Pass`-expected scenario failed. An
    /// `unexpected_pass` is loud in the summary but does not fail the run.
    pub fn ok(&self) -> bool {
        !self.scenarios.iter().any(|s| s.outcome == "fail" || s.outcome == "timeout")
    }
}

/// Run `defs` sequentially (live scenarios share real rooms/bots — parallel
/// runs would fight over them), writing `report.json` + a summary to stdout.
pub async fn run_scenarios(
    defs: &[&ScenarioDef],
    cfg: E2eConfig,
    profile: Profile,
) -> Result<RunReport> {
    let run_root = cfg.artifacts_dir.clone();
    tokio::fs::create_dir_all(&run_root).await?;

    let mut reports = Vec::new();
    for def in defs {
        let artifacts = run_root.join(def.name);

        eprintln!("── scenario {} ──", def.name);
        let started = std::time::Instant::now();

        // This is a Tier-3 LIVE suite against real Discord/Matrix/Cloudflare, so
        // Pass-expected scenarios fail intermittently on transient infra blips —
        // homeserver 502s dropping a phantom's membership-heartbeat mid-run,
        // Discord voice-state propagation lag at the tail of a long suite, etc.
        // (root-caused in #124). Retry a Pass-expected hard failure ONCE before
        // recording it: each scenario re-sweeps stale test memberships on entry,
        // so the retry starts clean, and a genuine regression fails BOTH attempts
        // — this masks flakiness without masking bugs. A KnownFail never retries
        // (its failure is the expected outcome), and a Skip is terminal (the
        // fixture is absent — retrying can't conjure it).
        let max_attempts = match def.expectation {
            Expectation::Pass => 2,
            Expectation::KnownFail { .. } => 1,
        };
        let mut outcome = None;
        let mut earlier_attempts = Vec::new();
        for attempt in 1..=max_attempts {
            // The recorded (last) attempt always uses `<run>/<name>`; a failed
            // attempt that gets retried is moved aside to `<name>.attemptN`
            // below, so this dir starts fresh every time.
            tokio::fs::create_dir_all(&artifacts).await?;
            let ctx = Arc::new(Ctx { cfg: cfg.clone(), profile, artifacts: artifacts.clone() });
            let res = tokio::time::timeout(profile.scenario_timeout(), (def.run)(ctx)).await;
            // Terminal on success or Skip; a hard failure/timeout is retryable.
            let retryable = match &res {
                Ok(Ok(())) => false,
                Ok(Err(e)) => !e.is::<Skip>(),
                Err(_) => true,
            };
            let last = !retryable || attempt == max_attempts;
            if last {
                outcome = Some(res);
                break;
            }
            let kept = run_root.join(format!("{}.attempt{attempt}", def.name));
            let _ = tokio::fs::remove_dir_all(&kept).await;
            tokio::fs::rename(&artifacts, &kept).await?;
            earlier_attempts.push(EarlierAttempt {
                attempt,
                error: match res {
                    Ok(Err(e)) => format!("{e:#}"),
                    _ => "scenario timed out".into(),
                },
                artifacts_dir: kept.display().to_string(),
            });
            eprintln!("   attempt {attempt} failed — retrying once (transient-infra tolerance, #124)");
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
        // `max_attempts >= 1`, so the loop body always ran and set `outcome`.
        let outcome = outcome.expect("scenario loop runs at least once");
        let duration_ms = started.elapsed().as_millis();

        let (outcome, error) = match (outcome, def.expectation) {
            // A Skip is environmental, not behavioral — it wins over the
            // expectation (a skipping KnownFail is `skipped`, not `known_fail`).
            (Ok(Err(e)), _) if e.is::<Skip>() => ("skipped", Some(format!("{e}"))),
            (Ok(Ok(())), Expectation::Pass) => ("pass", None),
            (Ok(Ok(())), Expectation::KnownFail { issue }) => (
                "unexpected_pass",
                Some(format!("expected to fail (tracked as {issue}) but passed — flip the expectation?")),
            ),
            (Ok(Err(e)), Expectation::Pass) => ("fail", Some(format!("{e:#}"))),
            (Ok(Err(e)), Expectation::KnownFail { issue }) => {
                ("known_fail", Some(format!("(tracked as {issue}) {e:#}")))
            }
            (Err(_), Expectation::Pass) => ("timeout", Some("scenario timed out".into())),
            (Err(_), Expectation::KnownFail { issue }) => {
                ("known_fail", Some(format!("(tracked as {issue}) scenario timed out")))
            }
        };
        eprintln!(
            "   {} in {:.1}s{}",
            outcome,
            duration_ms as f64 / 1000.0,
            error.as_deref().map(|e| format!(" — {e}")).unwrap_or_default()
        );

        reports.push(ScenarioReport {
            name: def.name.to_owned(),
            expectation: match def.expectation {
                Expectation::Pass => "pass".into(),
                Expectation::KnownFail { issue } => format!("known_fail({issue})"),
            },
            outcome: outcome.to_owned(),
            duration_ms,
            error,
            artifacts_dir: artifacts.display().to_string(),
            earlier_attempts,
        });
    }

    let report = RunReport {
        profile: format!("{profile:?}").to_lowercase(),
        scenarios: reports,
    };
    let json = serde_json::to_string_pretty(&report)?;
    tokio::fs::write(run_root.join("report.json"), &json).await?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_cfg(artifacts: PathBuf) -> E2eConfig {
        E2eConfig {
            bridge_bin: "nonexistent".into(),
            homeserver_url: "https://x".into(),
            matrix_domain: None,
            appservice_id: "a".into(),
            appservice_token: "t".into(),
            sender_localpart: "s".into(),
            puppet_tokens: vec!["p".into()],
            speaker_tokens: vec!["s".into()],
            guild_id: "1".into(),
            channel_a: "2".into(),
            room_a: "!r:x".into(),
            channel_b: None,
            room_b: None,
            channel_c: None,
            room_c: None,
            test_as_token: None,
            mod_token: None,
            federation: None,
            artifacts_dir: artifacts,
        }
    }

    fn passing(_: Arc<Ctx>) -> super::ScenarioFuture {
        Box::pin(async { Ok(()) })
    }
    fn failing(_: Arc<Ctx>) -> super::ScenarioFuture {
        Box::pin(async { anyhow::bail!("nope") })
    }
    fn skipping(_: Arc<Ctx>) -> super::ScenarioFuture {
        Box::pin(async { Err(Skip("fixture absent".into()).into()) })
    }

    static OK: ScenarioDef =
        ScenarioDef { name: "ok", expectation: Expectation::Pass, run: passing };
    static BAD: ScenarioDef =
        ScenarioDef { name: "bad", expectation: Expectation::Pass, run: failing };
    static GAP: ScenarioDef =
        ScenarioDef { name: "gap", expectation: Expectation::KnownFail { issue: "#62" }, run: failing };
    static FIXED_GAP: ScenarioDef = ScenarioDef {
        name: "fixed_gap",
        expectation: Expectation::KnownFail { issue: "#62" },
        run: passing,
    };
    static SKIPPED: ScenarioDef =
        ScenarioDef { name: "skipped", expectation: Expectation::Pass, run: skipping };
    static SKIPPED_GAP: ScenarioDef = ScenarioDef {
        name: "skipped_gap",
        expectation: Expectation::KnownFail { issue: "#62" },
        run: skipping,
    };

    // A Pass-expected scenario that fails its FIRST attempt then succeeds — the
    // runner's single retry (transient-infra tolerance, #124) must turn it green.
    static FLAKY_ATTEMPTS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    fn flaky_then_pass(_: Arc<Ctx>) -> super::ScenarioFuture {
        Box::pin(async {
            let n = FLAKY_ATTEMPTS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n == 0 { anyhow::bail!("transient blip") } else { Ok(()) }
        })
    }
    static FLAKY: ScenarioDef =
        ScenarioDef { name: "flaky", expectation: Expectation::Pass, run: flaky_then_pass };

    // A KnownFail must NOT be retried (its failure is the expected outcome, and a
    // retry would just double the run time). This one asserts single invocation.
    static KF_ATTEMPTS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    fn count_then_fail(_: Arc<Ctx>) -> super::ScenarioFuture {
        Box::pin(async {
            KF_ATTEMPTS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            anyhow::bail!("expected gap")
        })
    }
    static KF_COUNTED: ScenarioDef = ScenarioDef {
        name: "kf_counted",
        expectation: Expectation::KnownFail { issue: "#124" },
        run: count_then_fail,
    };

    #[tokio::test(start_paused = true)]
    async fn retry_turns_a_transient_failure_green_and_spares_knownfail() {
        let dir = std::env::temp_dir().join(format!("nvb-e2e-retry-{}", std::process::id()));
        FLAKY_ATTEMPTS.store(0, std::sync::atomic::Ordering::SeqCst);
        KF_ATTEMPTS.store(0, std::sync::atomic::Ordering::SeqCst);
        let defs = [&FLAKY, &KF_COUNTED];
        let report = run_scenarios(&defs, dummy_cfg(dir.clone()), Profile::Compressed)
            .await
            .expect("runner completes");
        let by_name: std::collections::HashMap<_, _> =
            report.scenarios.iter().map(|s| (s.name.as_str(), s.outcome.as_str())).collect();
        // First attempt failed, retry passed → recorded as pass, and it ran twice.
        assert_eq!(by_name["flaky"], "pass");
        assert_eq!(FLAKY_ATTEMPTS.load(std::sync::atomic::Ordering::SeqCst), 2);
        // The failed first attempt's artifacts survive the retry, aside.
        let flaky = report.scenarios.iter().find(|s| s.name == "flaky").unwrap();
        assert_eq!(flaky.earlier_attempts.len(), 1);
        assert_eq!(flaky.earlier_attempts[0].error, "transient blip");
        assert!(dir.join("flaky.attempt1").is_dir());
        assert!(dir.join("flaky").is_dir());
        // KnownFail ran exactly once (no retry).
        assert_eq!(by_name["kf_counted"], "known_fail");
        assert_eq!(KF_ATTEMPTS.load(std::sync::atomic::Ordering::SeqCst), 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test(start_paused = true)]
    async fn outcomes_and_report_json() {
        let dir = std::env::temp_dir().join(format!("nvb-e2e-report-{}", std::process::id()));
        let defs = [&OK, &BAD, &GAP, &FIXED_GAP, &SKIPPED, &SKIPPED_GAP];
        let report = run_scenarios(&defs, dummy_cfg(dir.clone()), Profile::Compressed)
            .await
            .expect("runner completes");
        let by_name: std::collections::HashMap<_, _> =
            report.scenarios.iter().map(|s| (s.name.as_str(), s.outcome.as_str())).collect();
        assert_eq!(by_name["ok"], "pass");
        assert_eq!(by_name["bad"], "fail");
        assert_eq!(by_name["gap"], "known_fail");
        assert_eq!(by_name["fixed_gap"], "unexpected_pass");
        // A Skip is environmental: it wins over the expectation on both kinds.
        assert_eq!(by_name["skipped"], "skipped");
        assert_eq!(by_name["skipped_gap"], "skipped");
        assert!(!report.ok(), "a Pass-expected failure fails the suite");
        assert!(dir.join("report.json").exists());
        // known_fail / skipped alone must not fail the suite.
        let report2 =
            run_scenarios(&[&GAP, &SKIPPED, &SKIPPED_GAP], dummy_cfg(dir.clone()), Profile::Compressed)
                .await
                .unwrap();
        assert!(report2.ok());
        std::fs::remove_dir_all(&dir).ok();
    }
}
