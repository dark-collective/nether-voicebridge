use anyhow::{Context, Result};
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

use nvb_audio::mixer::PlaybackMixer;
use nvb_discord::CaptureSink;

#[tokio::main]
async fn main() -> Result<()> {
    // Two transitive deps (matrix-sdk via reqwest, serenity via tokio-tungstenite) each
    // pull in a different rustls crypto provider (aws-lc-rs and ring). rustls panics if
    // no default is installed before the first TLS connection. Pick ring explicitly.
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

    cfg.log_redacted_summary();

    // Optional WAV capture (Phase 3 debugging artifact), off unless configured.
    // When enabled it is shared between the Discord task and the Ctrl-C handler.
    let capture_sink: Option<Arc<CaptureSink>> =
        cfg.discord.capture_wav.then(|| Arc::new(CaptureSink::new()));

    // Audio forward path: Discord capture → RTC publish. 20 ms mono frames.
    // Bounded; the Discord side drops on a full channel (real-time audio).
    let (audio_tx, audio_rx) = tokio::sync::mpsc::channel::<Vec<i16>>(50);

    // Reverse path: Matrix (LiveKit) → Discord playback. Per-participant jitter
    // buffers mixed to one mono stream; shared by the RTC consumers (producers)
    // and the Discord playback source (consumer).
    let playback = Arc::new(PlaybackMixer::new());

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

    // Run the Matrix side (sync + RTC) and the Discord side concurrently;
    // propagate the first error.
    tokio::try_join!(
        nvb_matrix_rtc::run(&cfg.matrix, &cfg.livekit, audio_rx, playback.clone()),
        nvb_discord::run(&cfg.discord, audio_tx, capture_sink.clone(), playback),
    )?;

    Ok(())
}
