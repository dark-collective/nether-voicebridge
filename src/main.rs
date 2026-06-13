use anyhow::{Context, Result};
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

mod capture;
mod config;
mod discord;
mod matrix;

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

    // The CaptureSink is shared between the Discord task and the Ctrl-C handler.
    let sink = Arc::new(capture::CaptureSink::new());

    // Ctrl-C handler: finalise all WAV files so headers are correct.
    {
        let sink = Arc::clone(&sink);
        tokio::spawn(async move {
            if let Err(e) = tokio::signal::ctrl_c().await {
                tracing::warn!(error = %e, "ctrl_c signal handler failed");
                return;
            }
            tracing::info!("shutting down — finalising WAV files");
            sink.finalize_all();
            std::process::exit(0);
        });
    }

    // Run both gateway connections concurrently; propagate the first error.
    tokio::try_join!(
        matrix::run(&cfg.matrix),
        discord::run(&cfg.discord, Arc::clone(&sink)),
    )?;

    Ok(())
}
