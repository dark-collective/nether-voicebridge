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
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use nvb_audio::routing::{CaptureRouter, PlaybackRouter};
use nvb_config::{
    BridgeConfig, LimitsConfig, LiveKeys, ServerProfile, TimingConfig, validate_bridge_against,
};
use nvb_discord::{CaptureSink, SharedPool};
use nvb_matrix_rtc::GhostPool;
use nvb_matrix_rtc::mgmt::{BridgeStatus, ControlMsg, ControlTx, ReconcileSummary};

/// How long a cancelled bridge gets to unwind (leave voice, retract ghost
/// memberships, close LiveKit) before its future is force-dropped. The
/// per-bridge version of the process-exit drain budget.
const TEARDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// Best-effort human-readable text from a caught panic payload (the common
/// `&str` / `String` cases; anything else is opaque).
fn panic_message(payload: &(dyn std::any::Any + Send)) -> &str {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        s
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.as_str()
    } else {
        "<non-string panic payload>"
    }
}

/// Contain a panic in one bridge future so it can't unwind the shared supervisor
/// task — every bridge future *and* the puppet pool are polled on this one task
/// (they're `!Send`), so an uncaught panic in any of them would take down every
/// other tenant plus the pool. On panic we log and resolve to the bridge's label,
/// exactly as a normal exit does, so `on_bridge_complete` tears that one tenant
/// down (`unregister_channel`, WAV finalize) and its neighbours keep running.
///
/// `AssertUnwindSafe` is required because the bridge future closes over
/// `!UnwindSafe` state (Arc<Mutex<…>>, routers); it is sound here because we drop
/// the future on panic rather than resuming it, and the pool's std mutexes already
/// recover from poisoning via `PoisonError::into_inner`.
///
/// NOTE: this relies on the default `panic = "unwind"` strategy — `Cargo.toml` has
/// no `panic = "abort"`. Under `abort` the process dies before `catch_unwind` runs
/// and this boundary silently becomes a no-op.
async fn guard_bridge_panic<F>(label: Arc<str>, fut: F) -> Arc<str>
where
    F: Future<Output = Arc<str>>,
{
    match AssertUnwindSafe(fut).catch_unwind().await {
        Ok(l) => l,
        Err(payload) => {
            tracing::error!(
                bridge = %label,
                panic = %panic_message(payload.as_ref()),
                "bridge task PANICKED — contained; this tenant is torn down, others unaffected"
            );
            label
        }
    }
}

/// Where a bridge came from — static `config.toml` bridges are never touched by
/// the tenants reconcile; `tenants.toml` bridges are owned by it.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    Static,
    Tenant,
}

/// Everything the tenants reconcile needs from the static config, captured once
/// at boot. `static_keys`/`static_labels` stay fixed even if a static bridge
/// later dies — its identity (store, device, label) stays reserved rather than
/// becoming silently claimable by a tenant.
pub struct TenantsCtx {
    pub servers: HashMap<String, ServerProfile>,
    pub data_dir: String,
    /// Uniqueness keys of the static bridges (tenants are validated against
    /// these plus each other).
    pub static_keys: LiveKeys,
    /// Labels of the static bridges (`name` or the positional `bridge-{i}`) —
    /// the supervisor registry / puppet-pool channel key namespace.
    pub static_labels: HashSet<String>,
    /// Admission budgets (`[limits]`, #47): `max_tenants` gates every tenant
    /// admission path through [`validate_desired`]; `max_ghosts_per_bridge` is
    /// threaded into each bridge's `GhostPool`.
    pub limits: LimitsConfig,
    /// Resolved `[timing]` grace-timer profile (#38), passed to every bridge's
    /// Matrix/RTC side at spawn. Defaults match the historical hardcoded values.
    pub timing: TimingConfig,
}

/// Book-keeping for one live bridge, keyed by label in [`Supervisor::running`].
struct RunningBridge {
    /// Resolved config — the fingerprint reconcile compares against.
    cfg: Arc<BridgeConfig>,
    /// Cancellation root threaded into the bridge's Matrix side.
    root: CancellationToken,
    /// This bridge's WAV capture sink (when `capture_wav` is on), finalized when
    /// the bridge completes so a runtime-stopped bridge gets valid headers
    /// without waiting for process exit.
    capture_sink: Option<Arc<CaptureSink>>,
    origin: Origin,
    /// Set by a reconcile "changed" entry: the replacement config, spawned ONLY
    /// from the completion handler once the old future has fully dropped (store
    /// handles closed, pool channel key freed). Never spawn a replacement inline.
    pending_respawn: Option<BridgeConfig>,
}

impl RunningBridge {
    /// The fingerprint reconcile diffs against: the queued replacement when one
    /// is pending (so a second SIGHUP sees the NEW config and doesn't
    /// double-stop), else the running config.
    fn effective_cfg(&self) -> &BridgeConfig {
        self.pending_respawn.as_ref().unwrap_or(&self.cfg)
    }
}

/// Owns every running bridge and drives them (plus the shared puppet pool) on
/// the current task. Built once in `main`; static bridges and boot-time tenants
/// are spawned before [`run`](Self::run) starts the loop. At runtime, SIGHUP
/// re-reads `tenants.toml` and reconciles the running tenant set against it.
pub struct Supervisor {
    pool: Arc<SharedPool>,
    pool_shutdown: CancellationToken,
    running: HashMap<String, RunningBridge>,
    /// The live bridge futures; each resolves to its bridge's label.
    bridges: FuturesUnordered<LocalBoxFuture<'static, Arc<str>>>,
    ctx: TenantsCtx,
    /// Once the operator shutdown begins, completions must not respawn pending
    /// replacements — a fresh bridge mid-drain would hang the drain.
    shutting_down: bool,
    /// Control-plane requests from the management bot (#4), selected in
    /// [`run`](Self::run) alongside the signals. `None` when no frontend was
    /// attached, or after the frontend hung up.
    control_rx: Option<tokio::sync::mpsc::UnboundedReceiver<ControlMsg>>,
}

impl Supervisor {
    pub fn new(pool: Arc<SharedPool>, pool_shutdown: CancellationToken, ctx: TenantsCtx) -> Self {
        Self {
            pool,
            pool_shutdown,
            running: HashMap::new(),
            bridges: FuturesUnordered::new(),
            ctx,
            shutting_down: false,
            control_rx: None,
        }
    }

    /// Attach a control frontend (the management bot): creates the channel,
    /// keeps the receiver for [`run`](Self::run)'s select loop, and returns the
    /// sender for the frontend task.
    pub fn control_handle(&mut self) -> ControlTx {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        self.control_rx = Some(rx);
        tx
    }

    /// Build and start one bridge: ghost pool, channel registration in the shared
    /// puppet pool, cancellation root, and the labelled `!Send` future pushed into
    /// the live set. Fails without side effects if the ghost pool or channel
    /// registration fails (a failed spawn registers nothing).
    pub fn spawn_bridge(
        &mut self,
        label: Arc<str>,
        bridge: BridgeConfig,
        origin: Origin,
    ) -> Result<()> {
        let bridge = Arc::new(bridge);

        // Optional WAV capture (Phase 3 debugging artifact), off unless configured.
        let capture_sink: Option<Arc<CaptureSink>> =
            bridge.discord.capture_wav.then(|| Arc::new(CaptureSink::new()));

        // Discord → Matrix attribution (Phase 8). The ghost pool mints one Matrix
        // appservice ghost per Discord speaker. Created once and shared: handed to
        // the shared pool as this channel's `CaptureRouter` (the anchor pushes
        // per-speaker audio into it), and to the Matrix side (`run`) to be driven.
        let ghost_pool = GhostPool::new(
            &bridge.matrix,
            &bridge.livekit,
            bridge.discord.capture_wav,
            self.ctx.limits.max_ghosts_per_bridge,
        )
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
                origin,
                pending_respawn: None,
            },
        );

        let span = tracing::info_span!("bridge", name = %label);
        let fut_label = label.clone();
        // Copied out of `self.ctx` so the `'static` bridge future doesn't borrow it.
        let timing = self.ctx.timing;
        let fut = async move {
            tokio::select! {
                biased;
                result = nvb_matrix_rtc::run(
                    &bridge.matrix,
                    &bridge.livekit,
                    timing,
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
        // Wrap in a panic boundary so a panic in this one bridge is contained to
        // this bridge instead of unwinding the shared supervisor task (which drives
        // every tenant + the pool). A caught panic resolves to the label, so the
        // normal `on_bridge_complete` teardown runs.
        self.bridges
            .push(guard_bridge_panic(label.clone(), fut).boxed_local());
        Ok(())
    }

    /// Validate and start the boot-time tenant set (loaded from `tenants.toml`
    /// by `main`). Runs the same all-or-nothing gate as a reload, but failures
    /// here are fatal — an operator-written tenants file that is wrong at boot
    /// should fail startup with the same rigor as `config.toml`, not be
    /// silently skipped; only the SIGHUP path is forgiving.
    pub fn spawn_boot_tenants(&mut self, tenants: Vec<BridgeConfig>) -> Result<()> {
        if tenants.is_empty() {
            return Ok(());
        }
        validate_desired(&tenants, &self.ctx).map_err(|e| anyhow::anyhow!(e))?;
        for cfg in tenants {
            let label: Arc<str> = Arc::from(cfg.name.clone().unwrap_or_default().as_str());
            self.spawn_bridge(label.clone(), cfg, Origin::Tenant)
                .with_context(|| format!("starting tenant '{label}'"))?;
        }
        Ok(())
    }

    /// Cancel a running bridge, optionally queueing a replacement config that is
    /// spawned once (and only once) its future has fully unwound — see
    /// [`RunningBridge::pending_respawn`].
    fn stop_bridge(&mut self, label: &str, respawn: Option<BridgeConfig>) {
        let Some(rb) = self.running.get_mut(label) else { return };
        rb.pending_respawn = respawn;
        rb.root.cancel();
    }

    /// A bridge's future resolved (voluntary exit, error, or cancellation):
    /// remove its channel from the puppet pool, finalize its WAV sink, drop its
    /// book-keeping, and spawn the queued replacement if a reconcile left one.
    fn on_bridge_complete(&mut self, label: &str) {
        self.pool.unregister_channel(label);
        let Some(rb) = self.running.remove(label) else { return };
        if let Some(sink) = &rb.capture_sink {
            tracing::info!(bridge = label, "finalising WAV files");
            sink.finalize_all();
        }
        if let Some(cfg) = rb.pending_respawn {
            if self.shutting_down {
                return; // never spawn into a draining process
            }
            tracing::info!(bridge = label, "respawning bridge with updated config");
            if let Err(e) = self.spawn_bridge(Arc::from(label), cfg, rb.origin) {
                tracing::error!(bridge = label, error = %e,
                    "respawn failed — bridge stays down until the next tenants reload");
            }
        }
    }

    /// Re-read `tenants.toml` and reconcile the running tenant bridges against
    /// it — all-or-nothing. A load or validation failure rejects the whole
    /// reload and leaves the running set untouched (partial application would
    /// make the desired-state file lie); per-entry spawn failures after a plan
    /// is accepted are reported in the summary AND logged, the file being the
    /// retry vehicle. Shared by SIGHUP and the control channel (#4) — the only
    /// difference is whether the caller can relay the outcome.
    fn reconcile_tenants(&mut self) -> Result<ReconcileSummary, String> {
        let path = nvb_config::tenants_path(&self.ctx.data_dir);
        let desired = nvb_config::load_tenants(&path, &self.ctx.servers, &self.ctx.data_dir)
            .map_err(|e| {
                tracing::error!(error = %e, path, "tenants reload REJECTED — running set unchanged");
                e.to_string()
            })?;
        validate_desired(&desired, &self.ctx).map_err(|e| {
            tracing::error!(error = %e, path, "tenants reload REJECTED — running set unchanged");
            e
        })?;

        // Diff desired against the running tenant subset (static bridges are
        // never in `desired`, hence never stopped here).
        let running: HashMap<String, BridgeConfig> = self
            .running
            .iter()
            .filter(|(_, rb)| rb.origin == Origin::Tenant)
            .map(|(l, rb)| (l.clone(), rb.effective_cfg().clone()))
            .collect();
        let plan = diff_tenants(&running, desired);
        let mut summary = ReconcileSummary {
            unchanged: running.len() - plan.remove.len() - plan.replace.len(),
            ..ReconcileSummary::default()
        };
        tracing::info!(
            add = plan.add.len(),
            remove = plan.remove.len(),
            replace = plan.replace.len(),
            unchanged = summary.unchanged,
            "tenants reload accepted — applying"
        );
        for label in &plan.remove {
            tracing::info!(bridge = %label, "tenant removed — stopping bridge");
            self.stop_bridge(label, None);
            summary.removed.push(label.clone());
        }
        for cfg in plan.replace {
            // Loader guarantees every tenant has a name.
            let label = cfg.name.clone().unwrap_or_default();
            tracing::info!(bridge = %label, "tenant changed — stopping for respawn");
            self.stop_bridge(&label, Some(cfg));
            summary.replaced.push(label);
        }
        for cfg in plan.add {
            let label: Arc<str> = Arc::from(cfg.name.clone().unwrap_or_default().as_str());
            tracing::info!(bridge = %label, "tenant added — starting bridge");
            if let Err(e) = self.spawn_bridge(label.clone(), cfg, Origin::Tenant) {
                tracing::error!(bridge = %label, error = %e,
                    "failed to start added tenant — fix and reload");
                summary.failed.push((label.to_string(), format!("{e:#}")));
            } else {
                summary.added.push(label.to_string());
            }
        }
        Ok(summary)
    }

    /// Serve one control-plane request (#4).
    fn handle_control(&mut self, msg: ControlMsg) {
        match msg {
            ControlMsg::Reconcile { reply } => {
                tracing::info!("control: reconciling tenants against tenants.toml");
                // A dropped reply just means the frontend gave up waiting.
                let _ = reply.send(self.reconcile_tenants());
            }
            ControlMsg::Status { reply } => {
                let mut out: Vec<BridgeStatus> = self
                    .running
                    .iter()
                    .map(|(label, rb)| {
                        let cfg = rb.effective_cfg();
                        BridgeStatus {
                            label: label.clone(),
                            is_tenant: rb.origin == Origin::Tenant,
                            room_id: cfg.matrix.room_id.clone(),
                            guild_id: cfg.discord.guild_id.clone(),
                            channel_id: cfg.discord.channel_id.clone(),
                            owner: cfg.owner.clone(),
                        }
                    })
                    .collect();
                out.sort_by(|a, b| a.label.cmp(&b.label));
                let _ = reply.send(out);
            }
            ControlMsg::GuildCoverage { guild_id, reply } => {
                let _ = reply.send(self.pool.any_bot_in_guild(guild_id));
            }
        }
    }

    /// Drive every bridge and the shared puppet pool until either all bridges
    /// (and the pool) have ended on their own, or an operator signal triggers the
    /// graceful drain. SIGHUP reconciles tenants; SIGINT/SIGTERM shut down.
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

        // Install all signal streams up front, once. SIGHUP's default disposition
        // KILLS the process, so its handler must exist before the first reload is
        // ever sent; and persistent streams buffer a signal that arrives while
        // the loop is busy (e.g. mid-reconcile), where a per-iteration future
        // could drop it.
        let mut sigint = hook_signal(SignalKind::interrupt(), "SIGINT");
        let mut sigterm = hook_signal(SignalKind::terminate(), "SIGTERM");
        let mut sighup = hook_signal(SignalKind::hangup(), "SIGHUP");

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
                _ = recv_signal(&mut sigint) => {
                    tracing::info!(via = "SIGINT",
                        "shutting down — cancelling bridges and draining to baseline");
                    self.drain(pool_fut, pool_done).await;
                    return;
                }
                _ = recv_signal(&mut sigterm) => {
                    tracing::info!(via = "SIGTERM",
                        "shutting down — cancelling bridges and draining to baseline");
                    self.drain(pool_fut, pool_done).await;
                    return;
                }
                // Operator reload: reconcile the running tenants against
                // tenants.toml (systemd: ExecReload=/bin/kill -HUP $MAINPID).
                _ = recv_signal(&mut sighup) => {
                    tracing::info!("SIGHUP — reconciling tenants against tenants.toml");
                    // Outcome (including rejection) is logged inside.
                    let _ = self.reconcile_tenants();
                }
                // Management-bot request (#4). A closed channel (frontend died)
                // detaches the receiver so this arm stops firing.
                msg = recv_control(&mut self.control_rx) => {
                    match msg {
                        Some(msg) => self.handle_control(msg),
                        None => {
                            tracing::warn!("control frontend disconnected");
                            self.control_rx = None;
                        }
                    }
                }
            }
            // All work ended on its own (errors already logged per bridge). Note
            // this requires the pool future to have ended too — while the pool
            // runs (the normal state), a tenants-only deployment whose last
            // tenant was removed keeps serving reloads rather than exiting.
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
        self.shutting_down = true;
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

/// Install one signal stream. If hooking fails (extremely unlikely on a Unix
/// daemon), warn and return `None` — [`recv_signal`] then never resolves for it
/// rather than aborting startup.
fn hook_signal(kind: SignalKind, name: &'static str) -> Option<Signal> {
    match signal(kind) {
        Ok(s) => Some(s),
        Err(e) => {
            tracing::warn!(error = %e, "could not install {name} handler");
            None
        }
    }
}

/// Await the next delivery on a hooked signal stream; pends forever on `None`
/// (the hook-failed fallback) so its `select!` arm simply never fires.
async fn recv_signal(sig: &mut Option<Signal>) {
    match sig {
        Some(s) => {
            s.recv().await;
        }
        None => std::future::pending().await,
    }
}

/// Await the next control message; pends forever when no frontend is attached
/// (or after it hung up and the receiver was detached).
async fn recv_control(
    rx: &mut Option<tokio::sync::mpsc::UnboundedReceiver<ControlMsg>>,
) -> Option<ControlMsg> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// Validate a desired tenant set against the static bridges and each other —
/// the all-or-nothing gate of a reload. Pure over `ctx`.
fn validate_desired(desired: &[BridgeConfig], ctx: &TenantsCtx) -> Result<(), String> {
    // Admission budget (#47): the whole desired set is rejected beyond the cap,
    // matching the all-or-nothing reload contract. Checked here so every
    // admission path (boot, SIGHUP, mgmt link's reconcile, --check-tenants)
    // shares one gate; the mgmt flow rolls tenants.toml back on rejection.
    if let Some(max) = ctx.limits.max_tenants {
        if desired.len() > max {
            return Err(format!(
                "tenant limit reached: {} tenants requested but [limits] max_tenants = {max}",
                desired.len()
            ));
        }
    }
    let mut live = ctx.static_keys.clone();
    for d in desired {
        // The loader guarantees a name; the label namespace is shared with the
        // static bridges (supervisor registry + puppet-pool channel key).
        let name = d.name.as_deref().unwrap_or_default();
        if ctx.static_labels.contains(name) {
            return Err(format!(
                "tenant '{name}' collides with a static [[bridge]] of the same label"
            ));
        }
        validate_bridge_against(d, &ctx.servers, &live).map_err(|e| e.to_string())?;
        live.insert(d);
    }
    Ok(())
}

/// The applied result of a tenants diff. Fields ordered as applied: removals
/// and replacements free resources before additions claim them.
struct ReconcilePlan {
    add: Vec<BridgeConfig>,
    remove: Vec<String>,
    replace: Vec<BridgeConfig>,
}

/// Pure reconcile diff: desired tenants vs the running tenant subset (label →
/// effective fingerprint). Same label + equal resolved config = no-op; a
/// changed config is a replace (stop-then-respawn); labels only in `running`
/// are removals; labels only in `desired` are additions.
fn diff_tenants(
    running: &HashMap<String, BridgeConfig>,
    desired: Vec<BridgeConfig>,
) -> ReconcilePlan {
    let mut plan = ReconcilePlan {
        add: Vec::new(),
        remove: Vec::new(),
        replace: Vec::new(),
    };
    let mut desired_names: HashSet<String> = HashSet::new();
    for d in desired {
        let name = d.name.clone().unwrap_or_default();
        desired_names.insert(name.clone());
        match running.get(&name) {
            None => plan.add.push(d),
            Some(current) if *current != d => plan.replace.push(d),
            Some(_) => {} // unchanged
        }
    }
    plan.remove = running
        .keys()
        .filter(|l| !desired_names.contains(*l))
        .cloned()
        .collect();
    plan
}

#[cfg(test)]
mod tests {
    use super::*;
    use nvb_config::{DiscordConfig, LiveKitConfig, MatrixConfig};

    /// A minimal resolved tenant config (the shape `load_tenants` produces).
    fn tenant(name: &str, room: &str) -> BridgeConfig {
        BridgeConfig {
            name: Some(name.to_string()),
            server: None,
            room_id: String::new(),
            discord: DiscordConfig {
                guild_id: "111".into(),
                channel_id: format!("{}00", name.len()),
                capture_wav: false,
            },
            matrix: MatrixConfig {
                homeserver_url: "https://nether.im".into(),
                room_id: room.into(),
                bot_user_id: "@bridge:nether.im".into(),
                device_id: format!("dev-{name}"),
                appservice_id: "voicebridge".into(),
                appservice_token: "tok".into(),
                store_path: format!("/data/{name}"),
                ..MatrixConfig::default()
            },
            livekit: LiveKitConfig::default(),
            owner: None,
            created: None,
        }
    }

    fn running(bridges: &[&BridgeConfig]) -> HashMap<String, BridgeConfig> {
        bridges
            .iter()
            .map(|b| (b.name.clone().unwrap(), (*b).clone()))
            .collect()
    }

    #[test]
    fn diff_add_remove_change_and_noop() {
        let a = tenant("a", "!a:x");
        let b = tenant("b", "!b:x");
        let c = tenant("c", "!c:x");
        // Deep-field change on b: livekit override differs.
        let mut b2 = b.clone();
        b2.livekit.media_e2ee = Some(true);
        let run = running(&[&a, &b, &c]);
        // Desired: a unchanged, b changed, c gone, d new.
        let d = tenant("d", "!d:x");
        let plan = diff_tenants(&run, vec![a.clone(), b2.clone(), d.clone()]);
        assert_eq!(plan.add.len(), 1);
        assert_eq!(plan.add[0].name.as_deref(), Some("d"));
        assert_eq!(plan.remove, vec!["c".to_string()]);
        assert_eq!(plan.replace.len(), 1);
        assert_eq!(plan.replace[0].livekit.media_e2ee, Some(true));
    }

    #[test]
    fn diff_empty_desired_removes_everything() {
        let a = tenant("a", "!a:x");
        let plan = diff_tenants(&running(&[&a]), vec![]);
        assert!(plan.add.is_empty() && plan.replace.is_empty());
        assert_eq!(plan.remove, vec!["a".to_string()]);
    }

    #[test]
    fn diff_uses_effective_fingerprint_semantics() {
        // A pending respawn counts as its NEW config (the caller passes
        // effective_cfg): a second reload with the same file must see it as
        // unchanged, not double-stop it.
        let b_new = {
            let mut b = tenant("b", "!b:x");
            b.livekit.media_e2ee = Some(true);
            b
        };
        let run = running(&[&b_new]); // effective view after first reload
        let plan = diff_tenants(&run, vec![b_new.clone()]);
        assert!(plan.add.is_empty() && plan.remove.is_empty() && plan.replace.is_empty());
    }

    #[test]
    fn validate_desired_rejects_static_label_collision_and_key_dupes() {
        let stat = tenant("Main Voice", "!m:x");
        let ctx = TenantsCtx {
            servers: HashMap::new(),
            data_dir: "/data".into(),
            static_keys: LiveKeys::from_bridges([&stat]),
            static_labels: HashSet::from(["Main Voice".to_string()]),
            limits: LimitsConfig::default(),
            timing: TimingConfig::default(),
        };
        // Label collision with a static bridge.
        let t = tenant("Main Voice", "!t:x");
        let err = validate_desired(&[t], &ctx).unwrap_err();
        assert!(err.contains("collides with a static"), "unexpected: {err}");
        // Store collision with a static bridge (whole reload rejected).
        let mut t2 = tenant("Other", "!o:x");
        t2.matrix.store_path = stat.matrix.store_path.clone();
        let err = validate_desired(&[t2], &ctx).unwrap_err();
        assert!(err.contains("duplicate matrix store_path"), "unexpected: {err}");
        // Two desired tenants colliding with each other.
        let t3 = tenant("x", "!x1:x");
        let mut t4 = tenant("y", "!y:x");
        t4.matrix.store_path = t3.matrix.store_path.clone();
        let err = validate_desired(&[t3, t4], &ctx).unwrap_err();
        assert!(err.contains("duplicate matrix store_path"), "unexpected: {err}");
    }

    #[test]
    fn validate_desired_accepts_disjoint_tenants() {
        let ctx = TenantsCtx {
            servers: HashMap::new(),
            data_dir: "/data".into(),
            static_keys: LiveKeys::default(),
            static_labels: HashSet::new(),
            limits: LimitsConfig::default(),
            timing: TimingConfig::default(),
        };
        let a = tenant("a", "!a:x");
        let b = tenant("b", "!b:x");
        validate_desired(&[a, b], &ctx).expect("disjoint tenants accepted");
    }

    #[test]
    fn validate_desired_enforces_max_tenants() {
        let mut ctx = TenantsCtx {
            servers: HashMap::new(),
            data_dir: "/data".into(),
            static_keys: LiveKeys::default(),
            static_labels: HashSet::new(),
            limits: LimitsConfig { max_tenants: Some(2), ..Default::default() },
            timing: TimingConfig::default(),
        };
        let a = tenant("a", "!a:x");
        let b = tenant("b", "!b:x");
        let c = tenant("c", "!c:x");
        // At the cap: accepted.
        validate_desired(&[a.clone(), b.clone()], &ctx).expect("at-cap set accepted");
        // Beyond the cap: whole set rejected with the friendly limit message.
        let err = validate_desired(&[a.clone(), b.clone(), c.clone()], &ctx).unwrap_err();
        assert!(err.contains("max_tenants = 2"), "unexpected: {err}");
        // No cap configured ⇒ unlimited (pre-limits behavior).
        ctx.limits.max_tenants = None;
        validate_desired(&[a, b, c], &ctx).expect("uncapped set accepted");
    }

    /// A bridge future that panics mid-poll. Explicit `Arc<str>` return type so the
    /// `!` from `panic!` coerces cleanly (no unreachable-tail lint).
    async fn panicking_bridge() -> Arc<str> {
        panic!("simulated bridge panic")
    }

    #[tokio::test]
    async fn guard_passes_through_normal_completion() {
        let label: Arc<str> = Arc::from("ok-bridge");
        let out = guard_bridge_panic(label.clone(), async move { label }).await;
        assert_eq!(&*out, "ok-bridge");
    }

    #[tokio::test]
    async fn guard_contains_panic_and_returns_label() {
        // The inner future panics; the guard must catch it (this test must NOT
        // unwind) and resolve to the label so on_bridge_complete can tear it down.
        let label: Arc<str> = Arc::from("boom-bridge");
        let out = guard_bridge_panic(label.clone(), panicking_bridge()).await;
        assert_eq!(&*out, "boom-bridge");
    }

    #[tokio::test]
    async fn guard_lets_the_driving_loop_survive_a_panicking_bridge() {
        // Mirror run()'s FuturesUnordered<LocalBoxFuture<Arc<str>>>: a panicking
        // bridge and a healthy one both drain, and draining the panicker does not
        // abort the set.
        let mut set: FuturesUnordered<LocalBoxFuture<'static, Arc<str>>> =
            FuturesUnordered::new();
        let bad: Arc<str> = Arc::from("bad");
        let good: Arc<str> = Arc::from("good");
        set.push(guard_bridge_panic(bad.clone(), panicking_bridge()).boxed_local());
        set.push(guard_bridge_panic(good.clone(), async move { good }).boxed_local());

        let mut done: Vec<String> = Vec::new();
        while let Some(label) = set.next().await {
            done.push(label.to_string());
        }
        done.sort();
        assert_eq!(done, vec!["bad".to_string(), "good".to_string()]);
    }
}
