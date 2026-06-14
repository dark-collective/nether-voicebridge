use anyhow::{Context, Result};
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

mod audio;
mod capture;
mod config;
mod discord;
mod matrix;
mod rtc;

#[tokio::main]
async fn main() -> Result<()> {
    // Two transitive deps (matrix-sdk via reqwest, serenity via tokio-tungstenite) each
    // pull in a different rustls crypto provider (aws-lc-rs and ring). rustls panics if
    // no default is installed before the first TLS connection. Pick ring explicitly.
    rustls::crypto::ring::default_provider().install_default().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("nether_voicebridge=info")),
        )
        .init();

    let config_path =
        std::env::var("CONFIG_PATH").unwrap_or_else(|_| "config.toml".to_string());

    let cfg = config::Config::load(&config_path)
        .with_context(|| format!("loading config from {config_path}"))?;

    cfg.log_redacted_summary();

    // Optional WAV capture (Phase 3 debugging artifact), off unless configured.
    // When enabled it is shared between the Discord task and the Ctrl-C handler.
    let capture_sink: Option<Arc<capture::CaptureSink>> = cfg
        .discord
        .capture_wav
        .then(|| Arc::new(capture::CaptureSink::new()));

    // Build the Matrix client once; clone it into the sync task and the RTC task.
    // matrix_sdk::Client is internally Arc-backed, so cloning is cheap.
    let matrix_client = matrix::build_client(&cfg.matrix).await?;

    // Watch channel: matrix::run signals when the call becomes active / inactive.
    // rtc::run subscribes and joins / leaves accordingly.
    let (call_active_tx, call_active_rx) = tokio::sync::watch::channel(false);

    // Audio forward path: Discord capture → RTC publish. 20 ms mono frames.
    // Bounded; the Discord side drops on a full channel (real-time audio).
    let (audio_tx, audio_rx) = tokio::sync::mpsc::channel::<Vec<i16>>(50);

    // Ctrl-C handler: finalise WAV files (if capturing) so headers are correct.
    {
        let capture_sink = capture_sink.clone();
        tokio::spawn(async move {
            if let Err(e) = tokio::signal::ctrl_c().await {
                tracing::warn!(error = %e, "ctrl_c signal handler failed");
                return;
            }
            tracing::info!("shutting down");
            if let Some(sink) = &capture_sink {
                tracing::info!("finalising WAV files");
                sink.finalize_all();
            }
            std::process::exit(0);
        });
    }

    // Run all three gateway connections concurrently; propagate the first error.
    tokio::try_join!(
        matrix::run(matrix_client.clone(), &cfg.matrix, call_active_tx),
        rtc::run(matrix_client, &cfg.livekit, &cfg.matrix, call_active_rx, audio_rx),
        discord::run(&cfg.discord, audio_tx, capture_sink.clone()),
    )?;

    Ok(())
}
