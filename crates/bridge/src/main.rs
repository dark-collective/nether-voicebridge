use anyhow::{Context, Result};
use futures_util::future::{join_all, FutureExt};
use std::sync::Arc;
use tracing::Instrument;
use tracing_subscriber::EnvFilter;

use nvb_audio::routing::PlaybackRouter;
use nvb_discord::{CaptureSink, PuppetPool};

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

    let config_path =
        std::env::var("CONFIG_PATH").unwrap_or_else(|_| "config.toml".to_string());

    let cfg = nvb_config::Config::load(&config_path)
        .with_context(|| format!("loading config from {config_path}"))?;
    cfg.validate().context("config validation failed")?;
    cfg.log_redacted_summary();

    // One independent pipeline per bridge. Each owns its Discord/Matrix clients,
    // tokens, room, and audio plumbing — nothing is shared across bridges except
    // the process-global rustls/tracing init above. The per-bridge `run` futures
    // are `!Send` (they hold `tracing` span guards across awaits), so they cannot
    // be `tokio::spawn`ed; instead we drive them concurrently on this task with
    // `join_all`. Each future swallows its own error (logging it), so one bridge
    // failing ends *that* bridge only — the rest keep running. The real work
    // (serenity clients, audio consumers) lives in their own internal spawned
    // tasks and still runs across the threadpool.
    let mut bridge_futs: Vec<futures_util::future::LocalBoxFuture<'static, ()>> = Vec::new();
    // Capture sinks (when `capture_wav` is on) are collected so the Ctrl-C
    // handler can finalise every bridge's WAV headers on shutdown.
    let mut capture_sinks: Vec<Arc<CaptureSink>> = Vec::new();

    for (idx, bridge) in cfg.bridges.into_iter().enumerate() {
        // Owned + `'static` so it can move into the spawned task (unlike the old
        // `try_join!`, `JoinSet::spawn` requires `'static`).
        let bridge = Arc::new(bridge);
        let label = bridge.name.clone().unwrap_or_else(|| format!("bridge-{idx}"));

        // Optional WAV capture (Phase 3 debugging artifact), off unless configured.
        let capture_sink: Option<Arc<CaptureSink>> =
            bridge.discord.capture_wav.then(|| Arc::new(CaptureSink::new()));
        if let Some(sink) = &capture_sink {
            capture_sinks.push(sink.clone());
        }

        // Audio forward path: Discord capture → RTC publish. 20 ms mono frames.
        // Bounded; the Discord side drops on a full channel (real-time audio).
        let (audio_tx, audio_rx) = tokio::sync::mpsc::channel::<Vec<i16>>(50);

        // Reverse path: Matrix (LiveKit) → Discord playback. The puppet pool routes
        // each Matrix speaker to a dedicated, named puppet bot (Phase 7); overflow
        // falls back to the shared listener mix. It is both the `PlaybackRouter` the
        // Matrix side pushes into and the owner of the Discord-side puppet clients.
        let pool = PuppetPool::new(&bridge.discord);
        let router: Arc<dyn PlaybackRouter> = pool.clone();

        let span = tracing::info_span!("bridge", name = %label);
        let bridge_task = bridge.clone();
        let fut = async move {
            // try_join! *inside* the bridge future: a failure on either side ends
            // this bridge only. Cross-bridge isolation is `join_all`'s job.
            let result = tokio::try_join!(
                nvb_matrix_rtc::run(
                    &bridge_task.matrix,
                    &bridge_task.livekit,
                    audio_rx,
                    router,
                ),
                nvb_discord::run(&bridge_task.discord, audio_tx, capture_sink, pool),
            );
            match result {
                // No reconnection yet (Phase 10); a clean exit is unexpected.
                Ok(_) => tracing::warn!("bridge exited cleanly (no reconnect yet)"),
                Err(e) => tracing::error!(error = %e, "bridge failed"),
            }
        }
        .instrument(span);
        bridge_futs.push(fut.boxed_local());
    }

    // Ctrl-C handler: finalise every bridge's WAV files (if capturing) so headers
    // are correct, then exit the whole process.
    {
        let capture_sinks = capture_sinks.clone();
        tokio::spawn(async move {
            if let Err(e) = tokio::signal::ctrl_c().await {
                tracing::warn!(error = %e, "ctrl_c signal handler failed");
                return;
            }
            tracing::info!("shutting down");
            for sink in &capture_sinks {
                tracing::info!("finalising WAV files");
                sink.finalize_all();
            }
            std::process::exit(0);
        });
    }

    // Stay alive while any bridge runs; returns once all have ended.
    join_all(bridge_futs).await;
    tracing::warn!("all bridges have exited");

    Ok(())
}
