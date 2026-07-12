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
    /// tracking the #97 pending-invite reap — flip it to `Pass` when the fix
    /// (alpha.25) merges. (Its first design target, the #62 force-move gap,
    /// was fixed in v0.3.0-alpha.11 before ever shipping a KnownFail.)
    KnownFail { issue: &'static str },
}

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
    /// `pass` | `fail` | `known_fail` | `unexpected_pass` | `timeout`
    pub outcome: String,
    pub duration_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
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
        tokio::fs::create_dir_all(&artifacts).await?;
        let ctx = Arc::new(Ctx { cfg: cfg.clone(), profile, artifacts: artifacts.clone() });

        eprintln!("── scenario {} ──", def.name);
        let started = std::time::Instant::now();
        let outcome = tokio::time::timeout(profile.scenario_timeout(), (def.run)(ctx)).await;
        let duration_ms = started.elapsed().as_millis();

        let (outcome, error) = match (outcome, def.expectation) {
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
            artifacts_dir: artifacts,
        }
    }

    fn passing(_: Arc<Ctx>) -> super::ScenarioFuture {
        Box::pin(async { Ok(()) })
    }
    fn failing(_: Arc<Ctx>) -> super::ScenarioFuture {
        Box::pin(async { anyhow::bail!("nope") })
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

    #[tokio::test]
    async fn outcomes_and_report_json() {
        let dir = std::env::temp_dir().join(format!("nvb-e2e-report-{}", std::process::id()));
        let defs = [&OK, &BAD, &GAP, &FIXED_GAP];
        let report = run_scenarios(&defs, dummy_cfg(dir.clone()), Profile::Compressed)
            .await
            .expect("runner completes");
        let by_name: std::collections::HashMap<_, _> =
            report.scenarios.iter().map(|s| (s.name.as_str(), s.outcome.as_str())).collect();
        assert_eq!(by_name["ok"], "pass");
        assert_eq!(by_name["bad"], "fail");
        assert_eq!(by_name["gap"], "known_fail");
        assert_eq!(by_name["fixed_gap"], "unexpected_pass");
        assert!(!report.ok(), "a Pass-expected failure fails the suite");
        assert!(dir.join("report.json").exists());
        // known_fail alone must not fail the suite.
        let report2 = run_scenarios(&[&GAP], dummy_cfg(dir.clone()), Profile::Compressed)
            .await
            .unwrap();
        assert!(report2.ok());
        std::fs::remove_dir_all(&dir).ok();
    }
}
