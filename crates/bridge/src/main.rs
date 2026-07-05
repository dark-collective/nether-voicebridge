mod supervisor;

use anyhow::{Context, Result};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use nvb_discord::SharedPool;
use supervisor::Supervisor;

#[tokio::main]
async fn main() -> Result<()> {
    // Two transitive deps (matrix-sdk via reqwest, serenity via tokio-tungstenite) each
    // pull in a different rustls crypto provider (aws-lc-rs and ring). rustls panics if
    // no default is installed before the first TLS connection. Pick ring explicitly.
    // Process-global: installed once here, never inside the per-bridge loop.
    rustls::crypto::ring::default_provider().install_default().ok();

    // Default filter enables info for every workspace crate. The split moved the
    // log targets from `nether_voicebridge` to per-crate names (`nvb_audio`,
    // `nvb_discord`, `nvb_matrix_rtc`, `nvb_config`), so RUST_LOG now targets
    // those — e.g. `RUST_LOG=nvb_discord=debug,nvb_matrix_rtc=debug`.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            EnvFilter::new(
                "nether_voicebridge=info,nvb_audio=info,nvb_discord=info,\
                 nvb_matrix_rtc=info,nvb_config=info",
            )
        }))
        .init();

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

    let cfg = match std::env::var("CONFIG_PATH") {
        Ok(path) => match nvb_config::Config::load(&path) {
            Ok(cfg) => cfg,
            // File not found: CONFIG_PATH may be baked into the image as a default
            // location (e.g. /app/config.toml) but no file was mounted — fall back to
            // env vars just like the no-file path below.
            Err(nvb_config::ConfigError::Io { .. }) => {
                tracing::info!(
                    "config file not found at {path}, \
                     loading config from environment variables"
                );
                nvb_config::Config::from_env().context(
                    "config file not found; supply all NETHER_* environment variables",
                )?
            }
            Err(e) => return Err(e).with_context(|| format!("loading config from {path}")),
        },
        Err(_) => match nvb_config::Config::load("config.toml") {
            Ok(cfg) => cfg,
            Err(nvb_config::ConfigError::Io { .. }) => {
                tracing::info!("no config.toml found, loading config from environment variables");
                nvb_config::Config::from_env().context(
                    "no config.toml found; set CONFIG_PATH or supply all NETHER_* environment variables",
                )?
            }
            Err(e) => return Err(e).context("loading config from config.toml"),
        },
    };
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
    if cfg.bridges.is_empty() && !tenants.is_empty() {
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
    let static_keys = nvb_config::LiveKeys::from_bridges(&cfg.bridges);

    let nvb_config::Config {
        puppets,
        bridges,
        servers,
        data_dir,
    } = cfg;

    // The Discord puppet bots are a SHARED, process-wide pool: one set of gateway
    // clients started once (`pool.run`), dynamically allocated across every
    // bridge's channel. Each bridge registers its channel and gets a `ChannelView`
    // (its `PlaybackRouter`). The Matrix/RTC side stays per-bridge.
    let pool = SharedPool::new(&puppets);
    // The pool's own shutdown root: Ctrl-C cancels it to drain every puppet.
    let pool_shutdown = CancellationToken::new();

    // The supervisor owns every running bridge (and drives the pool's future):
    // one independent Matrix/RTC pipeline per bridge, all `!Send`, all driven on
    // this task. Each bridge future swallows its own error (logging it), so one
    // bridge failing ends *that* bridge only — the rest keep running.
    let mut supervisor = Supervisor::new(
        pool,
        pool_shutdown,
        supervisor::TenantsCtx {
            servers,
            data_dir,
            static_keys,
            static_labels,
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

    // Drive everything. On SIGINT/SIGTERM the supervisor cancels every bridge
    // root and *awaits* (bounded by a timeout) that the bridge futures unwind —
    // each side leaves voice, resets nicknames, retracts ghost m.call.member,
    // and closes LiveKit before we return. WAV finalize happens per bridge as
    // it completes; SIGHUP reconciles the tenant set against tenants.toml.
    supervisor.run().await;

    Ok(())
}
