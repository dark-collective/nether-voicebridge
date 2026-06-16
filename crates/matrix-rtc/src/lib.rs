// matrix-sdk's `Client::sync` future is deeply nested; proving `Send` for it (so a
// per-ghost crypto sync can be `tokio::spawn`ed) overflows the default 128-deep
// trait-recursion limit. The compiler itself suggests raising it.
#![recursion_limit = "256"]

//! The Matrix side of the bridge.
//!
//! Two internal modules: [`matrix`] (matrix-sdk client login/sync and
//! call-membership detection) and [`rtc`] (MatrixRTC/LiveKit session join,
//! key exchange, subscribe/playback, and the ghost pool). They are wired
//! together by the single public [`run`] entry point; the `call_active` signal
//! between sync and the subscriber never leaves this crate.
//!
//! [`GhostPool`] is re-exported so the binary can construct it once and hand it
//! to both sides — to the Discord side as a `CaptureRouter`, and back here to be
//! driven — mirroring how the Discord `PuppetPool` is shared.

mod matrix;
mod rtc;

use std::sync::Arc;

use anyhow::Result;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use nvb_audio::routing::PlaybackRouter;
use nvb_config::{LiveKitConfig, MatrixConfig};

pub use rtc::ghost::GhostPool;

/// Run the Matrix side of the bridge to completion.
///
/// Builds the matrix-sdk client, then drives the sync loop (which raises
/// call-active transitions over an internal watch channel), the RTC subscriber
/// loop, and the ghost-pool driver concurrently. The caller supplies the
/// cross-side plumbing: the `router` (Matrix → Discord audio, routed
/// per-participant to puppets) and the `ghost_pool` (Discord → Matrix audio,
/// one ghost per Discord speaker — also handed to the Discord side as the
/// `CaptureRouter`).
pub async fn run(
    matrix_cfg: &MatrixConfig,
    livekit_cfg: &LiveKitConfig,
    router: Arc<dyn PlaybackRouter>,
    ghost_pool: Arc<GhostPool>,
    bridge: Arc<str>,
    shutdown: CancellationToken,
) -> Result<()> {
    // Build the client first (surfaces login/restore errors before the loops).
    // Pass media_e2ee so the client gets a persistent Olm crypto store when the
    // bridged room is encrypted.
    let client = matrix::build_client(matrix_cfg, livekit_cfg.media_e2ee, &bridge).await?;

    // Clear any ghost m.call.member left by a prior crashed run before the
    // subscriber joins, so Element Call starts from a clean roster (the bridge
    // bot's own stale membership is retracted separately in `rtc::run`). Reads
    // state with the now-synced client; best-effort.
    if let Err(e) = ghost_pool.sweep_stale_ghosts(&client).await {
        tracing::warn!(error = %e, "stale-ghost sweep failed (continuing)");
    }

    // The call-active watch lives entirely between the sync loop (sender) and
    // the RTC loop (receiver) — created here, never exposed.
    let (call_active_tx, call_active_rx) = watch::channel(false);

    // Membership-generation watch: the sync loop bumps it whenever a new human
    // joins the call; each ghost's key-distribution loop waits on it to (re-)send
    // its E2EE media key to the new member. A monotonic counter so a missed edge is
    // still observed via the changed value (vs. a Notify's lost-wakeup risk).
    let (members_gen_tx, members_gen_rx) = watch::channel(0u64);

    tokio::try_join!(
        matrix::run(
            client.clone(),
            matrix_cfg,
            call_active_tx,
            members_gen_tx,
            router.clone(),
            bridge.clone(),
            shutdown.clone(),
        ),
        rtc::run(
            client.clone(),
            livekit_cfg,
            matrix_cfg,
            call_active_rx,
            router,
            bridge.clone(),
            shutdown.clone(),
        ),
        ghost_pool.run(client, members_gen_rx, bridge.clone(), shutdown.clone()),
    )?;

    Ok(())
}
