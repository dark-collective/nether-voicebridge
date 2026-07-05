//! Bridge supervisor (#45): owns the running bridge tasks.
//!
//! The per-bridge `nvb_matrix_rtc::run` futures are `!Send` (they hold `tracing`
//! span guards across awaits), so they cannot be `tokio::spawn`ed. The original
//! startup drove a fixed `Vec` of them with `join_all`, which is closed once
//! polled — nothing can be added or removed at runtime. The supervisor replaces
//! that with a [`FuturesUnordered`] driven on the main task: bridges can be
//! spawned into it while it runs, and each future resolves to its own label so
//! completion (voluntary exit, error, or cancellation) is attributed and cleaned
//! up (`pool.unregister_channel`, WAV finalize).
//!
//! The shared puppet pool's future — also `!Send` — is driven alongside but
//! deliberately OUTSIDE the bridge set: it only ends on `pool_shutdown` (or a
//! fatal client error), and mixing it in would make "all bridges done" and the
//! shutdown drain wait on it by mistake.

use anyhow::{Context, Result};
use futures_util::future::{FutureExt, LocalBoxFuture};
use futures_util::stream::{FuturesUnordered, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use nvb_audio::routing::{CaptureRouter, PlaybackRouter};
use nvb_config::BridgeConfig;
use nvb_discord::{CaptureSink, SharedPool};
use nvb_matrix_rtc::GhostPool;

/// How long a cancelled bridge gets to unwind (leave voice, retract ghost
/// memberships, close LiveKit) before its future is force-dropped. The
/// per-bridge version of the process-exit drain budget.
const TEARDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// Book-keeping for one live bridge, keyed by label in [`Supervisor::running`].
struct RunningBridge {
    /// Resolved config — the fingerprint reconcile compares against.
    #[allow(dead_code)] // read by the tenants reconcile (next commit)
    cfg: Arc<BridgeConfig>,
    /// Cancellation root threaded into the bridge's Matrix side.
    root: CancellationToken,
    /// This bridge's WAV capture sink (when `capture_wav` is on), finalized when
    /// the bridge completes so a runtime-stopped bridge gets valid headers
    /// without waiting for process exit.
    capture_sink: Option<Arc<CaptureSink>>,
}

/// Owns every running bridge and drives them (plus the shared puppet pool) on
/// the current task. Built once in `main`; static bridges are spawned before
/// [`run`](Self::run) starts the loop.
pub struct Supervisor {
    pool: Arc<SharedPool>,
    pool_shutdown: CancellationToken,
    running: HashMap<String, RunningBridge>,
    /// The live bridge futures; each resolves to its bridge's label.
    bridges: FuturesUnordered<LocalBoxFuture<'static, Arc<str>>>,
}

impl Supervisor {
    pub fn new(pool: Arc<SharedPool>, pool_shutdown: CancellationToken) -> Self {
        Self {
            pool,
            pool_shutdown,
            running: HashMap::new(),
            bridges: FuturesUnordered::new(),
        }
    }

    /// Build and start one bridge: ghost pool, channel registration in the shared
    /// puppet pool, cancellation root, and the labelled `!Send` future pushed into
    /// the live set. Fails without side effects if the ghost pool or channel
    /// registration fails (a failed spawn registers nothing).
    pub fn spawn_bridge(&mut self, label: Arc<str>, bridge: BridgeConfig) -> Result<()> {
        let bridge = Arc::new(bridge);

        // Optional WAV capture (Phase 3 debugging artifact), off unless configured.
        let capture_sink: Option<Arc<CaptureSink>> =
            bridge.discord.capture_wav.then(|| Arc::new(CaptureSink::new()));

        // Discord → Matrix attribution (Phase 8). The ghost pool mints one Matrix
        // appservice ghost per Discord speaker. Created once and shared: handed to
        // the shared pool as this channel's `CaptureRouter` (the anchor pushes
        // per-speaker audio into it), and to the Matrix side (`run`) to be driven.
        let ghost_pool =
            GhostPool::new(&bridge.matrix, &bridge.livekit, bridge.discord.capture_wav)
                .context("building ghost pool")?;
        let capture_router: Arc<dyn CaptureRouter> = ghost_pool.clone();

        // Register this bridge's channel with the shared pool → its PlaybackRouter.
        let router: Arc<dyn PlaybackRouter> = self
            .pool
            .register_channel(
                label.clone(),
                &bridge.discord.guild_id,
                &bridge.discord.channel_id,
                capture_router,
                capture_sink.clone(),
            )
            .with_context(|| format!("registering channel for bridge '{label}'"))?;

        // Per-bridge shutdown root, threaded into the Matrix side.
        let bridge_root = CancellationToken::new();
        self.running.insert(
            label.to_string(),
            RunningBridge {
                cfg: bridge.clone(),
                root: bridge_root.clone(),
                capture_sink,
            },
        );

        let span = tracing::info_span!("bridge", name = %label);
        let fut_label = label.clone();
        let fut = async move {
            tokio::select! {
                biased;
                result = nvb_matrix_rtc::run(
                    &bridge.matrix,
                    &bridge.livekit,
                    router,
                    ghost_pool,
                    fut_label.clone(),
                    bridge_root.clone(),
                ) => {
                    match result {
                        // No reconnection yet (Phase 10); a clean exit is unexpected.
                        Ok(_) => tracing::warn!("bridge exited cleanly (no reconnect yet)"),
                        Err(e) => tracing::error!(error = %e, "bridge failed"),
                    }
                }
                // Bounded teardown: once cancelled, the bridge gets
                // TEARDOWN_TIMEOUT to unwind on its own; a wedged puppet/ghost
                // can't hold the slot (or the process-exit drain) hostage.
                _ = async {
                    bridge_root.cancelled().await;
                    tokio::time::sleep(TEARDOWN_TIMEOUT).await;
                } => {
                    tracing::warn!(
                        "bridge teardown timed out after {}s — force-dropping",
                        TEARDOWN_TIMEOUT.as_secs()
                    );
                }
            }
            fut_label
        }
        .instrument(span);
        self.bridges.push(fut.boxed_local());
        Ok(())
    }

    /// A bridge's future resolved (voluntary exit, error, or cancellation):
    /// remove its channel from the puppet pool, finalize its WAV sink, and drop
    /// its book-keeping.
    fn on_bridge_complete(&mut self, label: &str) {
        self.pool.unregister_channel(label);
        if let Some(rb) = self.running.remove(label) {
            if let Some(sink) = &rb.capture_sink {
                tracing::info!(bridge = label, "finalising WAV files");
                sink.finalize_all();
            }
        }
    }

    /// Drive every bridge and the shared puppet pool until either all bridges
    /// (and the pool) have ended on their own, or an operator signal triggers the
    /// graceful drain. Mirrors the pre-supervisor `join_all` + `select!` shape.
    pub async fn run(mut self) {
        // The pool is `!Send` (serenity clients hold non-Send state across
        // awaits) so it runs boxed on this task too — but outside `bridges`:
        // it only ends on `pool_shutdown`, and treating it as a bridge would
        // stall "all bridges done" and the shutdown drain on it.
        let mut pool_fut = {
            let pool = self.pool.clone();
            let shutdown = self.pool_shutdown.clone();
            (async move {
                if let Err(e) = pool.run(shutdown).await {
                    tracing::error!(error = %e, "puppet pool exited with error");
                }
            })
            .boxed_local()
        };
        let mut pool_done = false;

        loop {
            tokio::select! {
                maybe_label = self.bridges.next(), if !self.bridges.is_empty() => {
                    if let Some(label) = maybe_label {
                        self.on_bridge_complete(&label);
                    }
                }
                _ = &mut pool_fut, if !pool_done => {
                    pool_done = true;
                }
                // Operator shutdown. Race SIGINT (Ctrl-C) *and* SIGTERM so the
                // graceful drain runs no matter how we're stopped: `systemctl
                // stop` and `docker stop` both send SIGTERM, and without this
                // they'd skip teardown and leave puppets in voice / stale ghost
                // `m.call.member` behind on every restart.
                signal = shutdown_signal() => {
                    tracing::info!(
                        via = signal,
                        "shutting down — cancelling bridges and draining to baseline"
                    );
                    self.drain(pool_fut, pool_done).await;
                    return;
                }
            }
            // All work ended on its own (errors already logged per bridge).
            if self.bridges.is_empty() && pool_done {
                tracing::warn!("all bridges have exited");
                return;
            }
        }
    }

    /// Cancel every bridge and the pool, then await graceful teardown — bounded,
    /// so a wedged puppet/ghost can't keep the process alive past the timeout.
    async fn drain(
        &mut self,
        pool_fut: LocalBoxFuture<'static, ()>,
        pool_done: bool,
    ) {
        for rb in self.running.values() {
            rb.root.cancel();
        }
        // Drain the shared puppet pool (every bot leaves voice + resets nick).
        self.pool_shutdown.cancel();
        let drained = tokio::time::timeout(TEARDOWN_TIMEOUT, async {
            while let Some(label) = self.bridges.next().await {
                self.on_bridge_complete(&label);
            }
            if !pool_done {
                pool_fut.await;
            }
        })
        .await;
        match drained {
            Ok(_) => tracing::info!("all bridges drained cleanly"),
            Err(_) => tracing::warn!(
                "teardown timed out after {}s — exiting (some bots/ghosts may linger)",
                TEARDOWN_TIMEOUT.as_secs()
            ),
        }
        // Finalize WAV headers for anything the drain didn't complete.
        for (label, rb) in &self.running {
            if let Some(sink) = &rb.capture_sink {
                tracing::info!(bridge = %label, "finalising WAV files");
                sink.finalize_all();
            }
        }
    }
}

/// Resolve once either SIGINT (Ctrl-C, interactive) or SIGTERM (the default
/// `systemctl stop` / `docker stop` signal) arrives, returning which one fired.
/// Both must trigger the same graceful drain — see the shutdown `select!` above.
async fn shutdown_signal() -> &'static str {
    use tokio::signal::unix::{SignalKind, signal};

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
