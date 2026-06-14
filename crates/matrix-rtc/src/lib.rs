//! The Matrix side of the bridge.
//!
//! Two internal modules: [`matrix`] (matrix-sdk client login/sync and
//! call-membership detection) and [`rtc`] (MatrixRTC/LiveKit session join,
//! key exchange, publish, and inbound-track playback). They are wired together
//! by the single public [`run`] entry point; the `call_active` signal between
//! them never leaves this crate.

mod matrix;
mod rtc;

use std::sync::Arc;

use anyhow::Result;
use tokio::sync::{mpsc, watch};

use nvb_audio::routing::PlaybackRouter;
use nvb_config::{LiveKitConfig, MatrixConfig};

/// Run the Matrix side of the bridge to completion.
///
/// Builds the matrix-sdk client, then drives the sync loop (which raises
/// call-active transitions over an internal watch channel) and the RTC
/// participation loop concurrently. The caller supplies only the cross-side
/// plumbing: `audio_rx` (Discord → Matrix frames to publish) and the
/// `router` (Matrix → Discord audio, routed per-participant to puppets).
pub async fn run(
    matrix_cfg: &MatrixConfig,
    livekit_cfg: &LiveKitConfig,
    audio_rx: mpsc::Receiver<Vec<i16>>,
    router: Arc<dyn PlaybackRouter>,
    bridge: Arc<str>,
) -> Result<()> {
    // Build the client first (surfaces login/restore errors before the loops).
    let client = matrix::build_client(matrix_cfg, &bridge).await?;

    // The call-active watch lives entirely between the sync loop (sender) and
    // the RTC loop (receiver) — created here, never exposed.
    let (call_active_tx, call_active_rx) = watch::channel(false);

    tokio::try_join!(
        matrix::run(client.clone(), matrix_cfg, call_active_tx, bridge.clone()),
        rtc::run(
            client,
            livekit_cfg,
            matrix_cfg,
            call_active_rx,
            audio_rx,
            router,
            bridge.clone(),
        ),
    )?;

    Ok(())
}
