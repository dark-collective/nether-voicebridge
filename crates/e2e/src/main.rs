//! nvb-e2e — the #38 Tier-3 live regression harness.
//!
//! ```text
//! nvb-e2e list
//! nvb-e2e run [--all] [--scenario NAME]... [--profile compressed|real] [--artifacts DIR]
//! ```
//!
//! Configuration comes from `NVB_E2E_*` env vars (CI secrets) with an
//! `e2e.toml` fallback — see `config.rs` for the key list. The harness drives
//! the REAL bridge binary against the standing test environment (Auto Test
//! guild + rooms); it must never share puppet tokens with a running prod
//! instance.

// clippy ≥ 1.99 (Rust 1.99.0, 2026-09-28) flags the `#[must_use]` that the
// `async_trait` macro attaches to its generated boxed-future methods
// (double_must_use). The attribute is the macro's, not ours; allowed crate-wide
// so every `#[async_trait]` item here stays lint-clean under `-D warnings`.
#![allow(clippy::double_must_use)]
// The #99 crypto DM actor tokio::spawns matrix-sdk's deeply-nested sync
// future; the trait solver needs the same headroom nvb-matrix-rtc grants.
#![recursion_limit = "256"]

mod actors;
mod bridge_proc;
mod config;
mod logscrape;
mod observers;
mod scenario;
mod scenarios;

use anyhow::{Result, bail};
use config::Profile;

#[tokio::main]
async fn main() -> Result<()> {
    // Same dual-provider situation as the bridge: pick ring before any TLS.
    rustls::crypto::ring::default_provider().install_default().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("nvb_e2e=info")),
        )
        .init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("list") => {
            for def in scenarios::all() {
                let exp = match def.expectation {
                    scenario::Expectation::Pass => "pass".to_string(),
                    scenario::Expectation::KnownFail { issue } => format!("known-fail ({issue})"),
                };
                println!("{:<32} expected: {exp}", def.name);
            }
            Ok(())
        }
        Some("run") => run(&args[1..]).await,
        _ => {
            eprintln!(
                "usage: nvb-e2e list\n       nvb-e2e run [--all] [--scenario NAME]... \
                 [--profile compressed|real] [--artifacts DIR]"
            );
            std::process::exit(2);
        }
    }
}

async fn run(args: &[String]) -> Result<()> {
    let mut selected: Vec<String> = Vec::new();
    let mut all = false;
    let mut profile = Profile::Compressed;
    let mut artifacts: Option<String> = None;

    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--all" => all = true,
            "--scenario" => match it.next() {
                Some(name) => selected.push(name.clone()),
                None => bail!("--scenario needs a name"),
            },
            "--profile" => match it.next() {
                Some(p) => profile = Profile::parse(p)?,
                None => bail!("--profile needs a value"),
            },
            "--artifacts" => match it.next() {
                Some(dir) => artifacts = Some(dir.clone()),
                None => bail!("--artifacts needs a directory"),
            },
            other => bail!("unknown argument '{other}'"),
        }
    }
    if all && !selected.is_empty() {
        bail!("--all and --scenario are mutually exclusive");
    }
    if !all && selected.is_empty() {
        bail!("pass --all or at least one --scenario NAME");
    }

    let mut cfg = config::E2eConfig::load()?;
    if let Some(dir) = artifacts {
        cfg.artifacts_dir = dir.into();
    }

    let defs: Vec<&scenario::ScenarioDef> = if all {
        scenarios::all()
    } else {
        selected
            .iter()
            .map(|n| {
                scenarios::by_name(n)
                    .ok_or_else(|| anyhow::anyhow!("unknown scenario '{n}' (see `nvb-e2e list`)"))
            })
            .collect::<Result<_>>()?
    };

    let report = scenario::run_scenarios(&defs, cfg, profile).await?;
    let (mut pass, mut fail, mut known, mut unexpected, mut skipped) = (0, 0, 0, 0, 0);
    for s in &report.scenarios {
        match s.outcome.as_str() {
            "pass" => pass += 1,
            "known_fail" => known += 1,
            "unexpected_pass" => unexpected += 1,
            "skipped" => skipped += 1,
            _ => fail += 1,
        }
    }
    eprintln!(
        "== {} pass, {} fail, {} known-fail, {} unexpected-pass, {} skipped — report.json in artifacts ==",
        pass, fail, known, unexpected, skipped
    );
    if !report.ok() {
        std::process::exit(1);
    }
    Ok(())
}
