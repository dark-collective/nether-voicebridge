use anyhow::{Context, Result};
use futures_util::future::{join_all, FutureExt};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing_subscriber::EnvFilter;

use nvb_audio::routing::{CaptureRouter, PlaybackRouter};
use nvb_discord::{CaptureSink, SharedPool};
use nvb_matrix_rtc::GhostPool;

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
    cfg.validate().context("config validation failed")?;
    cfg.log_redacted_summary();
    // `data_dir` was already consumed by `load` to resolve each bridge's
    // `store_path`; only the pool and bridges are needed past this point.
    let nvb_config::Config {
        puppets, bridges, ..
    } = cfg;

    // The Discord puppet bots are a SHARED, process-wide pool: one set of gateway
    // clients started once (`pool.run`), dynamically allocated across every
    // bridge's channel. Each bridge registers its channel and gets a `ChannelView`
    // (its `PlaybackRouter`). The Matrix/RTC side stays per-bridge.
    let pool = SharedPool::new(&puppets);
    // The pool's own shutdown root: Ctrl-C cancels it to drain every puppet.
    let pool_shutdown = CancellationToken::new();

    // One independent Matrix/RTC pipeline per bridge. The per-bridge `run` futures
    // are `!Send` (they hold `tracing` span guards across awaits), so they cannot
    // be `tokio::spawn`ed; instead we drive them concurrently on this task with
    // `join_all`. Each future swallows its own error (logging it), so one bridge
    // failing ends *that* bridge only — the rest keep running.
    let mut bridge_futs: Vec<futures_util::future::LocalBoxFuture<'static, ()>> = Vec::new();
    // Capture sinks (when `capture_wav` is on) are collected so the Ctrl-C
    // handler can finalise every bridge's WAV headers on shutdown.
    let mut capture_sinks: Vec<Arc<CaptureSink>> = Vec::new();
    // One cancellation root per bridge. Ctrl-C cancels all of them; each bridge's
    // crates select! on it to leave voice / retract memberships / close LiveKit,
    // and we then *await* (with a timeout) that the bridge futures unwind to
    // baseline — replacing the old fire-and-forget release_all + fixed sleep.
    let mut bridge_roots: Vec<CancellationToken> = Vec::new();

    for (idx, bridge) in bridges.into_iter().enumerate() {
        let bridge = Arc::new(bridge);
        // Shared bridge label: drives the `bridge{name=…}` span here, is the
        // shared pool's channel key, AND is passed into each crate so its internal
        // spawned tasks / library callbacks re-establish per-bridge attribution.
        let label: Arc<str> = Arc::from(
            bridge.name.clone().unwrap_or_else(|| format!("bridge-{idx}")).as_str(),
        );

        // Optional WAV capture (Phase 3 debugging artifact), off unless configured.
        let capture_sink: Option<Arc<CaptureSink>> =
            bridge.discord.capture_wav.then(|| Arc::new(CaptureSink::new()));
        if let Some(sink) = &capture_sink {
            capture_sinks.push(sink.clone());
        }

        // Discord → Matrix attribution (Phase 8). The ghost pool mints one Matrix
        // appservice ghost per Discord speaker. Created once and shared: handed to
        // the shared pool as this channel's `CaptureRouter` (the anchor pushes
        // per-speaker audio into it), and to the Matrix side (`run`) to be driven.
        let ghost_pool = GhostPool::new(&bridge.matrix, &bridge.livekit)
            .context("building ghost pool")?;
        let capture_router: Arc<dyn CaptureRouter> = ghost_pool.clone();

        // Register this bridge's channel with the shared pool → its PlaybackRouter.
        let router: Arc<dyn PlaybackRouter> = pool
            .register_channel(
                label.clone(),
                &bridge.discord.guild_id,
                &bridge.discord.channel_id,
                capture_router,
                capture_sink,
            )
            .with_context(|| format!("registering channel for bridge '{label}'"))?;

        // Per-bridge shutdown root, threaded into the Matrix side.
        let bridge_root = CancellationToken::new();
        bridge_roots.push(bridge_root.clone());

        let span = tracing::info_span!("bridge", name = %label);
        let bridge_task = bridge.clone();
        let fut = async move {
            let result = nvb_matrix_rtc::run(
                &bridge_task.matrix,
                &bridge_task.livekit,
                router,
                ghost_pool,
                label.clone(),
                bridge_root.clone(),
            )
            .await;
            match result {
                // No reconnection yet (Phase 10); a clean exit is unexpected.
                Ok(_) => tracing::warn!("bridge exited cleanly (no reconnect yet)"),
                Err(e) => tracing::error!(error = %e, "bridge failed"),
            }
        }
        .instrument(span);
        bridge_futs.push(fut.boxed_local());
    }

    // Drive the shared puppet pool alongside the bridges. It's `!Send` (serenity
    // clients hold non-Send state across awaits) so it runs `boxed_local` on this
    // task too. On its `pool_shutdown`, it drains every puppet (leave voice, reset
    // nicknames) before returning.
    {
        let pool = pool.clone();
        let pool_shutdown = pool_shutdown.clone();
        let fut = async move {
            if let Err(e) = pool.run(pool_shutdown).await {
                tracing::error!(error = %e, "puppet pool exited with error");
            }
        };
        bridge_futs.push(fut.boxed_local());
    }

    // Drive all bridges. On Ctrl-C, cancel every bridge root and *await* (bounded
    // by a timeout) that the bridge futures unwind — each side leaves voice,
    // resets nicknames, retracts ghost m.call.member, and closes LiveKit before
    // we return.
    let mut drain = join_all(bridge_futs);
    tokio::select! {
        // All bridges ended on their own (errors already logged per bridge).
        _ = &mut drain => {
            tracing::warn!("all bridges have exited");
        }
        // Operator shutdown. Race SIGINT (Ctrl-C) *and* SIGTERM so the graceful
        // drain runs no matter how we're stopped: `systemctl stop` and `docker
        // stop` both send SIGTERM, and without this they'd skip teardown and leave
        // puppets in voice / stale ghost `m.call.member` behind on every restart.
        signal = shutdown_signal() => {
            tracing::info!(via = signal, "shutting down — cancelling bridges and draining to baseline");
            for root in &bridge_roots {
                root.cancel();
            }
            // Drain the shared puppet pool (every bot leaves voice + resets nick).
            pool_shutdown.cancel();
            // Await graceful teardown, but never hang: a wedged puppet/ghost
            // can't keep the process alive past the timeout.
            match tokio::time::timeout(Duration::from_secs(10), &mut drain).await {
                Ok(_) => tracing::info!("all bridges drained cleanly"),
                Err(_) => tracing::warn!(
                    "teardown timed out after 10 s — exiting (some bots/ghosts may linger)"
                ),
            }
        }
    }

    // Finalise WAV headers after teardown (no-op unless capture_wav is on).
    for sink in &capture_sinks {
        tracing::info!("finalising WAV files");
        sink.finalize_all();
    }

    Ok(())
}

/// Resolve once either SIGINT (Ctrl-C, interactive) or SIGTERM (the default
/// `systemctl stop` / `docker stop` signal) arrives, returning which one fired.
/// Both must trigger the same graceful drain — see the shutdown `select!` above.
async fn shutdown_signal() -> &'static str {
    use tokio::signal::unix::{signal, SignalKind};

    // If SIGTERM can't be hooked (extremely unlikely on a Unix daemon), fall back
    // to SIGINT-only rather than aborting startup.
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, "could not install SIGTERM handler — Ctrl-C only");
            if let Err(e) = tokio::signal::ctrl_c().await {
                tracing::warn!(error = %e, "ctrl_c signal handler failed");
            }
            return "SIGINT";
        }
    };

    tokio::select! {
        r = tokio::signal::ctrl_c() => {
            if let Err(e) = r {
                tracing::warn!(error = %e, "ctrl_c signal handler failed");
            }
            "SIGINT"
        }
        _ = sigterm.recv() => "SIGTERM",
    }
}
