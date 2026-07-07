mod supervisor;

use anyhow::{Context, Result};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use nvb_discord::SharedPool;
use supervisor::Supervisor;

#[tokio::main]
async fn main() -> Result<()> {
    // Minimal arg scan (the binary has no other CLI): `--check-tenants [PATH]`
    // validates the static config + tenants file and exits — the pre-flight an
    // operator runs before HUPing a reload (#45). Unknown flags fail loudly
    // instead of silently starting the daemon on a typo.
    let args: Vec<String> = std::env::args().skip(1).collect();
    let check_tenants_path: Option<Option<String>> = match args.as_slice() {
        [] => None,
        [flag] if flag == "--check-tenants" => Some(None),
        [flag, path] if flag == "--check-tenants" => Some(Some(path.clone())),
        _ => {
            eprintln!("usage: nether-voicebridge [--check-tenants [PATH]]");
            std::process::exit(2);
        }
    };

    // Two transitive deps (matrix-sdk via reqwest, serenity via tokio-tungstenite) each
    // pull in a different rustls crypto provider (aws-lc-rs and ring). rustls panics if
    // no default is installed before the first TLS connection. Pick ring explicitly.
    // Process-global: installed once here, never inside the per-bridge loop.
    rustls::crypto::ring::default_provider().install_default().ok();

    init_tracing();

    // Raise the open-file (nofile) soft limit. Each ghost (one per Discord speaker)
    // is its own matrix-sdk client with a SQLite crypto store (db + -wal + -shm) plus
    // connection sockets, so a crowd of speakers exhausts the default 1024 soft limit
    // → "unable to open database file" → that ghost's crypto sync dies and the speaker
    // is silently inaudible in an encrypted call. Raise it here so EVERY launch path
    // is covered — systemd's LimitNOFILE only helps systemd; Docker and manual runs
    // need this. Best-effort: raises soft toward the hard cap, never lowers it.
    match rlimit::increase_nofile_limit(65536) {
        Ok(n) => tracing::info!(nofile_soft = n, "raised open-file (nofile) soft limit"),
        Err(e) => tracing::warn!(
            error = %e,
            "could not raise nofile limit — many simultaneous speakers may exhaust file descriptors"
        ),
    }

    let cfg = load_config()?;

    // Pre-flight mode: validate the tenants file against this config and exit.
    if let Some(path_override) = check_tenants_path {
        std::process::exit(check_tenants(&cfg, path_override.as_deref()));
    }

    // Boot-time tenants (#45): load BEFORE validation so a hosting deployment
    // whose bridges all come from tenants.toml (zero static [[bridge]]s) can
    // pass. An invalid tenants file at boot fails startup with the same rigor
    // as config.toml — only the SIGHUP reload path is forgiving.
    let tenants = nvb_config::load_tenants(
        &nvb_config::tenants_path(&cfg.data_dir),
        &cfg.servers,
        &cfg.data_dir,
    )
    .context("loading tenants.toml")?;
    if cfg.bridges.is_empty() && (!tenants.is_empty() || cfg.management.is_some()) {
        // Bridges may all come from tenants.toml — or, with the management bot
        // configured (#4), from future `link` commands: an empty boot is a
        // legitimate starting state for a hosted deployment.
        cfg.validate_allowing_no_bridges()
            .context("config validation failed")?;
    } else {
        // Also the no-bridges-anywhere case: validate() then errors with the
        // usual "must define at least one [[bridge]]".
        cfg.validate().context("config validation failed")?;
    }
    cfg.log_redacted_summary();

    // The static bridges' labels + uniqueness keys, captured before the configs
    // move: tenants are validated against these (at boot and on every reload),
    // and they stay reserved even if a static bridge later dies.
    let static_labels: std::collections::HashSet<String> = cfg
        .bridges
        .iter()
        .enumerate()
        .map(|(i, b)| b.name.clone().unwrap_or_else(|| format!("bridge-{i}")))
        .collect();
    let mut static_keys = nvb_config::LiveKeys::from_bridges(&cfg.bridges);
    if let Some(m) = &cfg.management {
        // The management bot's store/device are reserved like a static bridge's:
        // no tenant may ever claim them.
        static_keys.reserve_management(m);
    }

    let nvb_config::Config {
        puppets,
        bridges,
        servers,
        data_dir,
        management,
        limits,
        timing,
        ..
    } = cfg;

    // Loud guard (#38): a non-default [timing]/NVB_TIMING profile compresses
    // the lifecycle grace timers — meant ONLY for the test harness. Make it
    // impossible to miss on a production deployment.
    if !timing.is_default() {
        tracing::warn!(
            ?timing,
            "NON-DEFAULT TIMING PROFILE ACTIVE — grace timers differ from production \
             defaults; this is a test-harness feature, do not run real traffic with it"
        );
    }

    // Captured before `servers`/`data_dir` move into the supervisor's ctx: the
    // management bot only needs the profile NAMES (for `link`'s server
    // argument) and the tenants path.
    let mgmt_inputs = management.map(|m| {
        let mut names: Vec<String> = servers.keys().cloned().collect();
        names.sort();
        (m, names, data_dir.clone())
    });

    // The Discord puppet bots are a SHARED, process-wide pool: one set of gateway
    // clients started once (`pool.run`), dynamically allocated across every
    // bridge's channel. Each bridge registers its channel and gets a `ChannelView`
    // (its `PlaybackRouter`). The Matrix/RTC side stays per-bridge.
    let pool = SharedPool::new(&puppets, &timing);
    // Per-user bridge opt-out (#63): load the persisted list and arm the
    // `/bridge optout`/`optin` commands. Refusing to start on an unreadable
    // list would be worse than starting without it — log loudly instead (the
    // demux/ghost gates then run on an empty set until an operator fixes it).
    if let Err(e) = pool.enable_optout(nvb_config::optout_path(&data_dir)) {
        tracing::error!(error = %e, "opt-out list unreadable — starting with opt-outs NOT enforced");
    }
    // The pool's own shutdown root: Ctrl-C cancels it to drain every puppet.
    let pool_shutdown = CancellationToken::new();

    let mgmt_approval_rx =
        wire_mgmt_pool_adapter(&pool, mgmt_inputs.as_ref().map(|(m, _, _)| m));

    // The supervisor owns every running bridge (and drives the pool's future):
    // one independent Matrix/RTC pipeline per bridge, all `!Send`, all driven on
    // this task. Each bridge future swallows its own error (logging it), so one
    // bridge failing ends *that* bridge only — the rest keep running.
    let mut supervisor = Supervisor::new(
        pool,
        pool_shutdown.clone(),
        supervisor::TenantsCtx {
            servers,
            data_dir,
            static_keys,
            static_labels,
            limits,
            timing,
        },
    );

    for (idx, bridge) in bridges.into_iter().enumerate() {
        // Shared bridge label: drives the `bridge{name=…}` span, is the shared
        // pool's channel key, AND is passed into each crate so its internal
        // spawned tasks / library callbacks re-establish per-bridge attribution.
        let label: Arc<str> = Arc::from(
            bridge.name.clone().unwrap_or_else(|| format!("bridge-{idx}")).as_str(),
        );
        supervisor
            .spawn_bridge(label.clone(), bridge, supervisor::Origin::Static)
            .with_context(|| format!("starting bridge '{label}'"))?;
    }
    supervisor
        .spawn_boot_tenants(tenants)
        .context("starting tenants from tenants.toml")?;

    // Management bot (#4): a plain tokio task (its future is Send, unlike the
    // bridge futures) talking to the supervisor over the control channel. It
    // shares the pool's shutdown token so the drain stops it too. A bot that
    // fails (bad token, homeserver down) logs and stays down — bridges are
    // unaffected; restart the process to retry.
    if let Some((mgmt_cfg, server_names, data_dir)) = mgmt_inputs {
        let control = supervisor.control_handle();
        let shutdown = pool_shutdown.clone();
        tokio::spawn(async move {
            if let Err(e) = nvb_matrix_rtc::mgmt::run(
                mgmt_cfg,
                server_names,
                data_dir,
                limits,
                control,
                mgmt_approval_rx,
                shutdown,
            )
            .await
            {
                tracing::error!(error = %e, "management bot exited with error");
            }
        });
    }


    // Drive everything. On SIGINT/SIGTERM the supervisor cancels every bridge
    // root and *awaits* (bounded by a timeout) that the bridge futures unwind —
    // each side leaves voice, resets nicknames, retracts ghost m.call.member,
    // and closes LiveKit before we return. WAV finalize happens per bridge as
    // it completes; SIGHUP reconciles the tenant set against tenants.toml.
    supervisor.run().await;

    Ok(())
}

/// Install the process-wide `tracing` subscriber.
///
/// The default filter enables info for every workspace crate. The split moved
/// the log targets from `nether_voicebridge` to per-crate names (`nvb_audio`,
/// `nvb_discord`, `nvb_matrix_rtc`, `nvb_config`), so RUST_LOG now targets
/// those — e.g. `RUST_LOG=nvb_discord=debug,nvb_matrix_rtc=debug`.
///
/// `NVB_LOG_JSON=1` switches stdout to newline-delimited JSON events — the
/// machine-readable feed the #38 e2e harness scrapes (`nvb-e2e`'s LogScraper).
/// Anything else (unset, "0", …) keeps the human format, so production output
/// is unchanged.
fn init_tracing() {
    let env_filter = || {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            EnvFilter::new(
                "nether_voicebridge=info,nvb_audio=info,nvb_discord=info,\
                 nvb_matrix_rtc=info,nvb_config=info",
            )
        })
    };
    if std::env::var("NVB_LOG_JSON").is_ok_and(|v| v == "1") {
        tracing_subscriber::fmt().json().with_env_filter(env_filter()).init();
    } else {
        tracing_subscriber::fmt().with_env_filter(env_filter()).init();
    }
}

/// Two-sided link auth (#47): when the management bot requires Discord-side
/// approval, wire the pool's `/bridge approve` command to the bot. The two
/// crates don't know each other, so an adapter task converts the pool's
/// `LinkApproval` into the bot's `DiscordApproval` (the reply oneshot passes
/// through untouched). Returns the receiver the management bot reads approvals
/// from, or `None` when approval isn't required.
fn wire_mgmt_pool_adapter(
    pool: &SharedPool,
    mgmt: Option<&nvb_config::ManagementConfig>,
) -> Option<tokio::sync::mpsc::UnboundedReceiver<nvb_matrix_rtc::mgmt::DiscordApproval>> {
    let m = mgmt?;
    if !m.require_discord_approval {
        return None;
    }
    let (pool_tx, mut pool_rx) = tokio::sync::mpsc::unbounded_channel();
    let (mgmt_tx, rx) = tokio::sync::mpsc::unbounded_channel();
    pool.enable_link_approval(m.approval_bot_index, pool_tx);
    tokio::spawn(async move {
        while let Some((a, reply)) = pool_rx.recv().await {
            let _ = mgmt_tx.send(nvb_matrix_rtc::mgmt::DiscordApproval {
                code: a.code,
                guild_id: a.guild_id,
                invoker: a.invoker,
                reply,
            });
        }
    });
    Some(rx)
}

/// Load the static config: CONFIG_PATH (default `config.toml`), falling back to
/// `NETHER_*` environment variables when no file is present (the Docker image
/// bakes CONFIG_PATH=/app/config.toml whether or not one is mounted).
fn load_config() -> Result<nvb_config::Config> {
    match std::env::var("CONFIG_PATH") {
        Ok(path) => match nvb_config::Config::load(&path) {
            Ok(cfg) => Ok(cfg),
            // File *not found*: CONFIG_PATH may be baked into the image as a default
            // location (e.g. /app/config.toml) but no file was mounted — fall back to
            // env vars just like the no-file path below. A genuine read error
            // (permission denied, EISDIR, I/O failure) must NOT be silently swallowed
            // into an env fallback — surface it (review F5-4).
            Err(nvb_config::ConfigError::Io { ref source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                tracing::info!(
                    "config file not found at {path}, \
                     loading config from environment variables"
                );
                nvb_config::Config::from_env()
                    .context("config file not found; supply all NETHER_* environment variables")
            }
            Err(e) => Err(e).with_context(|| format!("loading config from {path}")),
        },
        Err(_) => match nvb_config::Config::load("config.toml") {
            Ok(cfg) => Ok(cfg),
            Err(nvb_config::ConfigError::Io { ref source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                tracing::info!("no config.toml found, loading config from environment variables");
                nvb_config::Config::from_env().context(
                    "no config.toml found; set CONFIG_PATH or supply all NETHER_* environment variables",
                )
            }
            Err(e) => Err(e).context("loading config from config.toml"),
        },
    }
}

/// `--check-tenants [PATH]`: validate the tenants file against the static config
/// — the same gate the boot path and a SIGHUP reload apply — printing a verdict
/// per entry. Returns the process exit code: 0 all-OK, 1 any failure. Unlike the
/// reload's all-or-nothing reject, this keeps going after an error so one run
/// reports every problem.
fn check_tenants(cfg: &nvb_config::Config, path_override: Option<&str>) -> i32 {
    let default_path = nvb_config::tenants_path(&cfg.data_dir);
    let path = path_override.unwrap_or(&default_path);

    if let Err(e) = cfg.validate_allowing_no_bridges() {
        println!("static config: ERROR {e}");
        return 1;
    }
    // Mirror the boot/reload gate: the management bot's store/device are
    // reserved exactly like a static bridge's.
    let mut live = nvb_config::LiveKeys::from_bridges(&cfg.bridges);
    if let Some(m) = &cfg.management {
        live.reserve_management(m);
    }
    let tenants = match nvb_config::load_tenants(path, &cfg.servers, &cfg.data_dir) {
        Ok(t) => t,
        Err(e) => {
            println!("{path}: ERROR {e}");
            return 1;
        }
    };
    if tenants.is_empty() {
        println!("{path}: no tenants (missing or empty file) — nothing to check");
        return 0;
    }
    // Admission budget (#47): mirror the boot/reload gate's max_tenants check.
    if let Some(max) = cfg.limits.max_tenants {
        if tenants.len() > max {
            println!(
                "{path}: ERROR {} tenants but [limits] max_tenants = {max}",
                tenants.len()
            );
            return 1;
        }
    }

    let static_labels: std::collections::HashSet<String> = cfg
        .bridges
        .iter()
        .enumerate()
        .map(|(i, b)| b.name.clone().unwrap_or_else(|| format!("bridge-{i}")))
        .collect();
    let mut failed = false;
    for t in &tenants {
        // The loader guarantees a name.
        let name = t.name.as_deref().unwrap_or_default();
        if static_labels.contains(name) {
            println!("tenant '{name}': ERROR collides with a static [[bridge]] of the same label");
            failed = true;
            continue;
        }
        match nvb_config::validate_bridge_against(t, &cfg.servers, &live) {
            Ok(()) => {
                println!("tenant '{name}': OK");
                // Only accepted entries claim keys, mirroring the reload gate.
                live.insert(t);
            }
            Err(e) => {
                println!("tenant '{name}': ERROR {e}");
                failed = true;
            }
        }
    }
    if failed { 1 } else { 0 }
}
