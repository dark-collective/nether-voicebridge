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

use crate::roster;
use nvb_audio::routing::{CaptureRouter, PlaybackRouter};
use nvb_config::{
    BridgeConfig, LimitsConfig, LiveKeys, ServerProfile, TimingConfig, validate_bridge_against,
};
use nvb_discord::{CaptureSink, SharedPool};
use nvb_matrix_rtc::GhostPool;
use nvb_matrix_rtc::mgmt::{BotFootprint, BridgeStatus, ControlMsg, ControlTx, ReconcileSummary};

/// How long a cancelled bridge gets to unwind (leave voice, retract ghost
/// memberships, close LiveKit) before its future is force-dropped. The
/// per-bridge version of the process-exit drain budget.
const TEARDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// How often `<data_dir>/bridges.json` is rewritten even when nothing changes,
/// so a stalled supervisor shows up as a stale `updated` stamp (#131). Well
/// inside the monitor's 5-minute staleness threshold.
const ROSTER_REFRESH: Duration = Duration::from_secs(60);

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
async fn guard_bridge_panic<F>(label: Arc<str>, fut: F) -> (Arc<str>, BridgeOutcome)
where
    F: Future<Output = (Arc<str>, BridgeOutcome)>,
{
    match AssertUnwindSafe(fut).catch_unwind().await {
        Ok(out) => out,
        Err(payload) => {
            tracing::error!(
                bridge = %label,
                panic = %panic_message(payload.as_ref()),
                "bridge task PANICKED — contained; this tenant is torn down, others unaffected"
            );
            // A panic is a failure like any other: it earns a backoff restart
            // rather than leaving the bridge silently dead (#130).
            (label, BridgeOutcome::Failed)
        }
    }
}

/// How a bridge future ended — the input to the restart decision (#130).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BridgeOutcome {
    /// The bridge task returned an error, or panicked. Eligible for a backoff
    /// restart unless it was stopped deliberately or we're shutting down.
    Failed,
    /// A clean return, or a cancellation-driven teardown. Never restarted —
    /// cancellation is always something we asked for.
    Ended,
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
    /// Linked identities (ADR 0001): the instance-wide pairing/approval stores
    /// and the registry of live boards (one per bridge, created at spawn).
    pub links: nvb_config::links::LinkShared,
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
    /// Set by [`stop_bridge`]: this bridge is being torn down deliberately
    /// (operator stop, reconcile remove/replace), so its completion must NOT be
    /// treated as a failure and auto-restarted (#130).
    operator_stopped: bool,
    /// Set by a reconcile "changed" entry: the replacement config, spawned ONLY
    /// from the completion handler once the old future has fully dropped (store
    /// handles closed, pool channel key freed). Never spawn a replacement inline.
    pending_respawn: Option<BridgeConfig>,
    /// When this bridge future was spawned — feeds `uptime_secs` in the
    /// operator `stats` command (#94).
    spawned_at: std::time::Instant,
    /// While a restart backoff is being served, when it ends (#130). `None` for
    /// a normally-spawned bridge. Only used to report `restarting` vs `alive`
    /// in the roster the ops monitor reads.
    backoff_until: Option<std::time::Instant>,
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
    /// The live bridge futures; each resolves to its bridge's label and how it
    /// ended (a failure is what arms the restart backoff — #130).
    bridges: FuturesUnordered<LocalBoxFuture<'static, (Arc<str>, BridgeOutcome)>>,
    /// Consecutive failures per bridge label, driving the restart backoff.
    /// Cleared once a bridge has been up longer than `bridge_restart_max`.
    restart_failures: HashMap<String, u32>,
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
            restart_failures: HashMap::new(),
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
        self.spawn_bridge_delayed(label, bridge, origin, Duration::ZERO)
    }

    /// [`spawn_bridge`](Self::spawn_bridge), but the Matrix side only starts
    /// after `start_delay` — the restart backoff (#130).
    ///
    /// The delay deliberately sits *inside* the bridge future, after the pool
    /// channel and store/device identity have been claimed: a bridge waiting to
    /// restart still owns its slot, so a concurrent reconcile can neither
    /// double-spawn the same store nor hand the channel to someone else. It is
    /// cancellable, so an operator stop during backoff takes effect at once.
    pub fn spawn_bridge_delayed(
        &mut self,
        label: Arc<str>,
        bridge: BridgeConfig,
        origin: Origin,
        start_delay: Duration,
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
            self.ctx.timing.session_rotate_min_interval(),
        )
        .context("building ghost pool")?;
        let capture_router: Arc<dyn CaptureRouter> = ghost_pool.clone();

        // Linked identities (ADR 0001): one board per bridge. Its dispatcher
        // routes Discord-side transitions into the pool and Matrix-side ones to
        // this bridge's Matrix task (over `link_tx`).
        let links = self.ctx.links.board_for(
            &bridge.matrix.room_id,
            &bridge.excluded_discord_user_ids,
            &bridge.excluded_matrix_user_ids,
        );
        let (link_tx, link_rx) = tokio::sync::mpsc::unbounded_channel::<nvb_config::links::LinkEvent>();
        {
            let pool = self.pool.clone();
            let label_for_pool = label.to_string();
            links.set_dispatch(Box::new(move |ev| match ev {
                nvb_config::links::LinkEvent::DiscordExcluded(_)
                | nvb_config::links::LinkEvent::DiscordIncluded(_) => {
                    pool.apply_discord_link_event(&label_for_pool, ev)
                }
                other => {
                    let _ = link_tx.send(other);
                }
            }));
        }
        self.ctx.links.registry.register(&label, links.clone());

        // Register this bridge's channel with the shared pool → its PlaybackRouter.
        let router: Arc<dyn PlaybackRouter> = self
            .pool
            .register_channel(
                label.clone(),
                &bridge.discord.guild_id,
                &bridge.discord.channel_id,
                capture_router,
                capture_sink.clone(),
                links.clone(),
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
                operator_stopped: false,
                pending_respawn: None,
                spawned_at: std::time::Instant::now(),
                backoff_until: (!start_delay.is_zero())
                    .then(|| std::time::Instant::now() + start_delay),
            },
        );

        let span = tracing::info_span!("bridge", name = %label);
        let fut_label = label.clone();
        // Copied out of `self.ctx` so the `'static` bridge future doesn't borrow it.
        let timing = self.ctx.timing;
        let fut = async move {
            let mut outcome = BridgeOutcome::Ended;
            // Restart backoff (#130): the slot is already reserved above — store,
            // device and pool channel stay claimed — so nothing else can take
            // this identity while we wait, and an operator stop lands instantly
            // because the sleep races the fresh root.
            if !start_delay.is_zero() {
                tracing::info!(
                    delay_secs = start_delay.as_secs(),
                    "bridge restart backoff — waiting before respawn"
                );
                tokio::select! {
                    _ = bridge_root.cancelled() => return (fut_label, BridgeOutcome::Ended),
                    _ = tokio::time::sleep(start_delay) => {}
                }
            }
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
                    links,
                    link_rx,
                ) => {
                    match result {
                        // No reconnection yet (Phase 10); a clean exit is unexpected.
                        Ok(_) => {
                            tracing::warn!("bridge exited cleanly (no reconnect yet)");
                            outcome = BridgeOutcome::Ended;
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "bridge failed");
                            outcome = BridgeOutcome::Failed;
                        }
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
            (fut_label, outcome)
        }
        .instrument(span);
        // Wrap in a panic boundary so a panic in this one bridge is contained to
        // this bridge instead of unwinding the shared supervisor task (which drives
        // every tenant + the pool). A caught panic resolves to the label, so the
        // normal `on_bridge_complete` teardown runs.
        self.bridges
            .push(guard_bridge_panic(label.clone(), fut).boxed_local());
        self.publish_roster();
        Ok(())
    }

    /// Build the current roster: every running bridge, plus every *static*
    /// bridge that is configured but absent.
    ///
    /// The absent ones are the point (#131). A tenant that disappears has
    /// usually been removed on purpose, but a `config.toml` bridge can only be
    /// missing because it died — the exact condition that stayed invisible for
    /// three days in the 2026-08-11 outage.
    fn roster_entries(&self) -> Vec<roster::RosterEntry> {
        let now = std::time::Instant::now();
        let now_secs = roster::now_secs();
        let mut entries: Vec<roster::RosterEntry> = self
            .running
            .iter()
            .map(|(label, rb)| roster::RosterEntry {
                name: label.clone(),
                origin: match rb.origin {
                    Origin::Static => "static",
                    Origin::Tenant => "tenant",
                },
                state: match rb.backoff_until {
                    Some(until) if until > now => roster::BridgeState::Restarting,
                    _ => roster::BridgeState::Alive,
                },
                since: now_secs.saturating_sub(rb.spawned_at.elapsed().as_secs()),
                consecutive_failures: self.restart_failures.get(label).copied().unwrap_or(0),
            })
            .collect();
        for label in &self.ctx.static_labels {
            if !self.running.contains_key(label) {
                entries.push(roster::RosterEntry {
                    name: label.clone(),
                    origin: "static",
                    state: roster::BridgeState::Down,
                    since: now_secs,
                    consecutive_failures: self.restart_failures.get(label).copied().unwrap_or(0),
                });
            }
        }
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        entries
    }

    /// Publish the roster to `<data_dir>/bridges.json` for the ops monitor.
    /// Best-effort: a monitoring write must never be able to affect bridges.
    fn publish_roster(&self) {
        let entries = self.roster_entries();
        let doc = roster::render(roster::now_secs(), std::process::id(), &entries);
        roster::write(&self.ctx.data_dir, &doc);
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
        // Deliberate teardown — never auto-restarted as a failure (#130).
        rb.operator_stopped = true;
        rb.root.cancel();
    }

    /// A bridge's future resolved (voluntary exit, error, or cancellation):
    /// remove its channel from the puppet pool, finalize its WAV sink, drop its
    /// book-keeping, and spawn the queued replacement if a reconcile left one.
    fn on_bridge_complete(&mut self, label: &str, outcome: BridgeOutcome) {
        self.on_bridge_complete_inner(label, outcome);
        // One publish covering every exit path of the handler above, so the
        // roster the monitor reads can never lag a death or a restart (#131).
        self.publish_roster();
    }

    fn on_bridge_complete_inner(&mut self, label: &str, outcome: BridgeOutcome) {
        self.pool.unregister_channel(label);
        self.ctx.links.registry.unregister(label);
        let Some(rb) = self.running.remove(label) else { return };
        let uptime = rb.spawned_at.elapsed();
        // Cancel the root even on a voluntary/error exit (not just an operator
        // stop): the Matrix side already tears itself down before returning (#67),
        // but this is the belt-and-suspenders that guarantees any task still
        // holding a clone of this root — a detached ghost, a key loop, a
        // membership-retry — is signalled to stop, and it can never touch a
        // respawn (that gets a fresh root in `spawn_bridge`).
        rb.root.cancel();
        if let Some(sink) = &rb.capture_sink {
            tracing::info!(bridge = label, "finalising WAV files");
            sink.finalize_all();
        }
        if let Some(cfg) = rb.pending_respawn {
            if self.shutting_down {
                return; // never spawn into a draining process
            }
            // The queued config is either a same-label "replace" or a renamed
            // addition deferred behind this removal (#71). Spawn under the
            // CONFIG's own label — not the completing bridge's — so a rename
            // (remove "a" + add "b" reusing a's store/device/channel) lands as
            // "b", now that "a"'s store handle and pool channel key are freed.
            let respawn_label: Arc<str> = Arc::from(cfg.name.clone().unwrap_or_default().as_str());
            tracing::info!(bridge = %respawn_label, "spawning queued bridge config");
            if let Err(e) = self.spawn_bridge(respawn_label.clone(), cfg, rb.origin) {
                tracing::error!(bridge = %respawn_label, error = %e,
                    "respawn failed — bridge stays down until the next tenants reload");
            }
            return;
        }

        // A bridge that FAILED (error return or panic) gets restarted with
        // backoff (#130). Before this, any failure — including a transient
        // `/sync` blip misread as fatal auth, or a startup login 502 after a
        // host reboot — left the bridge dead for the process lifetime, with
        // only tenant reconciles able to accidentally resurrect one.
        if outcome != BridgeOutcome::Failed || self.shutting_down || rb.operator_stopped {
            if outcome != BridgeOutcome::Failed {
                self.restart_failures.remove(label);
            }
            return;
        }

        // A bridge that stayed up past the backoff cap counts as recovered, so
        // an outage next week starts from the short delay again rather than
        // inheriting an old streak.
        let fails = self.restart_failures.entry(label.to_string()).or_insert(0);
        if uptime >= self.ctx.timing.bridge_restart_max() {
            *fails = 0;
        }
        let delay = self.ctx.timing.bridge_restart_delay(*fails);
        *fails += 1;
        let attempt = *fails;

        tracing::warn!(
            bridge = label,
            attempt,
            delay_secs = delay.as_secs(),
            uptime_secs = uptime.as_secs(),
            "bridge failed — scheduling automatic restart"
        );
        let restart_label: Arc<str> = Arc::from(label);
        let cfg = (*rb.cfg).clone();
        if let Err(e) = self.spawn_bridge_delayed(restart_label.clone(), cfg, rb.origin, delay) {
            tracing::error!(bridge = %restart_label, error = %e,
                "automatic restart could not be spawned — bridge stays down");
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
        // Snapshot the removed bridges' effective configs BEFORE cancelling them,
        // then decide which additions must queue behind a removal (#71): an add
        // that reuses a removed tenant's store/device/channel may not spawn until
        // that bridge has fully drained (store handle dropped, pool channel key
        // freed), or the two sessions race on one SQLite store / device id. Such
        // an add rides the removed bridge's `pending_respawn` — the same
        // bounded-teardown-then-respawn path the "replace" case uses.
        let removed_cfgs: HashMap<String, BridgeConfig> = plan
            .remove
            .iter()
            .filter_map(|l| self.running.get(l).map(|rb| (l.clone(), rb.effective_cfg().clone())))
            .collect();
        let (mut deferred, inline_adds) = plan_add_ordering(plan.add, &removed_cfgs);

        for label in &plan.remove {
            match deferred.remove(label) {
                Some(add_cfg) => {
                    let add_label = add_cfg.name.clone().unwrap_or_default();
                    tracing::info!(bridge = %label, replacement = %add_label,
                        "tenant removed — stopping; renamed replacement spawns after teardown");
                    // Accepted: it will spawn from the completion handler (a
                    // deferred-spawn failure surfaces in the logs, as on replace).
                    summary.added.push(add_label);
                    self.stop_bridge(label, Some(add_cfg));
                }
                None => {
                    tracing::info!(bridge = %label, "tenant removed — stopping bridge");
                    self.stop_bridge(label, None);
                }
            }
            summary.removed.push(label.clone());
        }
        for cfg in plan.replace {
            // Loader guarantees every tenant has a name.
            let label = cfg.name.clone().unwrap_or_default();
            tracing::info!(bridge = %label, "tenant changed — stopping for respawn");
            self.stop_bridge(&label, Some(cfg));
            summary.replaced.push(label);
        }
        for cfg in inline_adds {
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
                            created: cfg.created,
                            uptime_secs: rb.spawned_at.elapsed().as_secs(),
                        }
                    })
                    .collect();
                out.sort_by(|a, b| a.label.cmp(&b.label));
                let _ = reply.send(out);
            }
            ControlMsg::GuildCoverage { guild_id, reply } => {
                let _ = reply.send(self.pool.any_bot_in_guild(guild_id));
            }
            ControlMsg::PoolFootprint { reply } => {
                let out = self
                    .pool
                    .guild_footprint()
                    .into_iter()
                    .map(|(bot, connected, guilds)| BotFootprint { bot, connected, guilds })
                    .collect();
                let _ = reply.send(out);
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

        // Refresh `bridges.json` on a timer as well as on every transition, so
        // its `updated` stamp doubles as a dead-man's switch: a roster that
        // stops advancing means this loop is wedged, which the monitor reports
        // rather than trusting stale contents (#131).
        let mut roster_tick = tokio::time::interval(ROSTER_REFRESH);
        roster_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        self.publish_roster();

        loop {
            tokio::select! {
                _ = roster_tick.tick() => {
                    self.publish_roster();
                }
                maybe_done = self.bridges.next(), if !self.bridges.is_empty() => {
                    if let Some((label, outcome)) = maybe_done {
                        self.on_bridge_complete(&label, outcome);
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
            while let Some((label, outcome)) = self.bridges.next().await {
                self.on_bridge_complete(&label, outcome);
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

/// Does `cfg` reuse a hard resource (matrix device / SQLite store) or the
/// Discord `(guild, channel)` of any still-draining bridge? A rename (remove
/// "a" + add "b" sharing a's store/device/channel) must not let "b" open the
/// store/device "a" is still unwinding on (#71). Uses [`LiveKeys`] so "collision"
/// matches `validate_desired`'s notion exactly.
fn collides_with(cfg: &BridgeConfig, draining: &LiveKeys) -> bool {
    let m = &cfg.matrix;
    draining.devices.contains(&(
        m.homeserver_url.clone(),
        m.bot_user_id.clone(),
        m.device_id.clone(),
    )) || draining.stores.contains(&m.store_path)
        || draining
            .channels
            .contains(&(cfg.discord.guild_id.clone(), cfg.discord.channel_id.clone()))
}

/// Split a reconcile's additions into those that must wait for a concurrent
/// removal to finish tearing down and those that may spawn immediately (#71).
/// An addition colliding with a removed bridge is queued behind it (returned in
/// the `removed-label -> add cfg` map, later attached as that bridge's
/// `pending_respawn`); the rest spawn inline. Pure, so the ordering contract is
/// unit-testable. `validate_desired` already rejects add-vs-add and add-vs-static
/// collisions, so only add-vs-removed remains — the case its checks can't see
/// (a removed tenant is in neither the static keys nor the desired set).
fn plan_add_ordering(
    adds: Vec<BridgeConfig>,
    removed_cfgs: &HashMap<String, BridgeConfig>,
) -> (HashMap<String, BridgeConfig>, Vec<BridgeConfig>) {
    let mut deferred: HashMap<String, BridgeConfig> = HashMap::new();
    let mut inline: Vec<BridgeConfig> = Vec::new();
    for cfg in adds {
        // At most one add defers behind a given removal (a second add colliding
        // with the same removal would also collide with the first → rejected by
        // validate_desired for hard keys; a soft channel dupe falls to inline).
        let behind = removed_cfgs
            .iter()
            .find(|(label, rc)| {
                !deferred.contains_key(label.as_str())
                    && collides_with(&cfg, &LiveKeys::from_bridges([*rc]))
            })
            .map(|(label, _)| label.clone());
        match behind {
            Some(label) => {
                deferred.insert(label, cfg);
            }
            None => inline.push(cfg),
        }
    }
    (deferred, inline)
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
            excluded_discord_user_ids: Vec::new(),
            excluded_matrix_user_ids: Vec::new(),
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

    /// Apply a reconcile plan to the registry the way `reconcile_tenants`'
    /// bookkeeping does: removals drop, replacements re-key to the NEW config
    /// (the pending-respawn effective view), additions insert.
    fn apply_plan(run: &mut HashMap<String, BridgeConfig>, plan: ReconcilePlan) {
        for l in plan.remove {
            run.remove(&l);
        }
        for b in plan.replace.into_iter().chain(plan.add) {
            run.insert(b.name.clone().unwrap(), b);
        }
    }

    /// Tier-2 named scenario (#38 Phase C2): three consecutive reconciles —
    /// add, replace, remove — with the registry carried forward, asserting the
    /// CUMULATIVE state after each step and that a repeat of any file is a
    /// no-op (the SIGHUP-twice case).
    #[test]
    fn scenario_reconcile_sequence_add_replace_remove() {
        let a = tenant("a", "!a:x");
        let b = tenant("b", "!b:x");
        let mut run: HashMap<String, BridgeConfig> = HashMap::new();

        // Reconcile 1: empty → {a, b}.
        let plan = diff_tenants(&run, vec![a.clone(), b.clone()]);
        assert_eq!(plan.add.len(), 2);
        assert!(plan.remove.is_empty() && plan.replace.is_empty());
        apply_plan(&mut run, plan);
        assert_eq!(run.len(), 2);

        // Reconcile 2: a changes (deep field) → replace a only, b untouched.
        let mut a2 = a.clone();
        a2.livekit.media_e2ee = Some(true);
        let plan = diff_tenants(&run, vec![a2.clone(), b.clone()]);
        assert!(plan.add.is_empty() && plan.remove.is_empty());
        assert_eq!(plan.replace.len(), 1);
        assert_eq!(plan.replace[0].name.as_deref(), Some("a"));
        apply_plan(&mut run, plan);
        assert_eq!(run.len(), 2);
        assert_eq!(run["a"].livekit.media_e2ee, Some(true), "registry holds the NEW a");
        assert_eq!(run["b"], b, "b never churned");

        // A second SIGHUP with the same file: complete no-op (effective-
        // fingerprint semantics — must not double-stop the replaced bridge).
        let plan = diff_tenants(&run, vec![a2.clone(), b.clone()]);
        assert!(plan.add.is_empty() && plan.remove.is_empty() && plan.replace.is_empty());

        // Reconcile 3: a2 unlinked → only b remains.
        let plan = diff_tenants(&run, vec![b.clone()]);
        assert_eq!(plan.remove, vec!["a".to_string()]);
        assert!(plan.add.is_empty() && plan.replace.is_empty());
        apply_plan(&mut run, plan);
        assert_eq!(run.len(), 1);
        assert!(run.contains_key("b"));
    }

    #[test]
    fn add_reusing_removed_resources_defers_behind_it() {
        // "b" renames "a": new label, but SAME store + device + channel — the
        // add must queue behind "a"'s teardown, not spawn inline (which would
        // race two sessions on one SQLite store / device id).
        let a = tenant("a", "!a:x");
        let mut b = tenant("b", "!b:x");
        b.matrix.store_path = a.matrix.store_path.clone();
        b.matrix.device_id = a.matrix.device_id.clone();
        b.discord.channel_id = a.discord.channel_id.clone();
        let removed: HashMap<String, BridgeConfig> = HashMap::from([("a".to_string(), a)]);
        let (deferred, inline) = plan_add_ordering(vec![b], &removed);
        assert!(inline.is_empty(), "a colliding add must not spawn inline");
        assert_eq!(deferred.len(), 1);
        assert_eq!(deferred["a"].name.as_deref(), Some("b"), "queued behind the removal it reuses");
    }

    #[test]
    fn add_with_disjoint_resources_spawns_inline() {
        // Disjoint store/device (derived from the name) AND a distinct channel,
        // so nothing a removal is draining is reused → spawn immediately.
        let a = tenant("a", "!a:x");
        let mut c = tenant("c", "!c:x");
        c.discord.channel_id = "999".into();
        let removed: HashMap<String, BridgeConfig> = HashMap::from([("a".to_string(), a)]);
        let (deferred, inline) = plan_add_ordering(vec![c], &removed);
        assert!(deferred.is_empty(), "a disjoint add is never deferred");
        assert_eq!(inline.len(), 1);
        assert_eq!(inline[0].name.as_deref(), Some("c"));
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
            links: nvb_config::links::LinkShared::in_memory(),
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
            links: nvb_config::links::LinkShared::in_memory(),
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
            links: nvb_config::links::LinkShared::in_memory(),
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

    /// #67: a bridge future resolving on its own (voluntary exit, error, or a
    /// contained panic) must cancel its cancellation root, so any task still
    /// holding a clone of it — a detached ghost, a key-distribution loop, a
    /// membership-retry — is torn down instead of leaking (phantom Element Call
    /// participants, key heartbeats to a dead call).
    #[test]
    fn on_bridge_complete_cancels_root() {
        use nvb_config::PuppetsConfig;
        let pool = SharedPool::new(&PuppetsConfig::default(), &TimingConfig::default());
        let ctx = TenantsCtx {
            servers: HashMap::new(),
            data_dir: "/data".into(),
            static_keys: LiveKeys::default(),
            static_labels: HashSet::new(),
            limits: LimitsConfig::default(),
            timing: TimingConfig::default(),
            links: nvb_config::links::LinkShared::in_memory(),
        };
        let mut sup = Supervisor::new(pool, CancellationToken::new(), ctx);

        let root = CancellationToken::new();
        sup.running.insert(
            "b".to_string(),
            RunningBridge {
                cfg: Arc::new(tenant("b", "!b:x")),
                root: root.clone(),
                capture_sink: None,
                origin: Origin::Static,
                operator_stopped: false,
                pending_respawn: None,
                spawned_at: std::time::Instant::now(),
                backoff_until: None,
            },
        );

        assert!(!root.is_cancelled());
        sup.on_bridge_complete("b", BridgeOutcome::Ended);
        assert!(
            root.is_cancelled(),
            "a completed bridge's root must be cancelled so detached ghost tasks are torn down"
        );
        assert!(!sup.running.contains_key("b"), "book-keeping dropped on completion");
    }

    /// A bridge future that panics mid-poll. Explicit return type so the `!`
    /// from `panic!` coerces cleanly (no unreachable-tail lint).
    async fn panicking_bridge() -> (Arc<str>, BridgeOutcome) {
        panic!("simulated bridge panic")
    }

    #[tokio::test]
    async fn guard_passes_through_normal_completion() {
        let label: Arc<str> = Arc::from("ok-bridge");
        let (out, outcome) =
            guard_bridge_panic(label.clone(), async move { (label, BridgeOutcome::Ended) }).await;
        assert_eq!(&*out, "ok-bridge");
        assert_eq!(outcome, BridgeOutcome::Ended);
    }

    #[tokio::test]
    async fn guard_contains_panic_and_reports_failure() {
        // The inner future panics; the guard must catch it (this test must NOT
        // unwind), resolve to the label so on_bridge_complete can tear it down,
        // and report Failed so the panicking bridge earns a restart (#130).
        let label: Arc<str> = Arc::from("boom-bridge");
        let (out, outcome) = guard_bridge_panic(label.clone(), panicking_bridge()).await;
        assert_eq!(&*out, "boom-bridge");
        assert_eq!(
            outcome,
            BridgeOutcome::Failed,
            "a panicked bridge must be restartable, not silently dead"
        );
    }

    #[tokio::test]
    async fn guard_lets_the_driving_loop_survive_a_panicking_bridge() {
        // Mirror run()'s FuturesUnordered: a panicking bridge and a healthy one
        // both drain, and draining the panicker does not abort the set.
        let mut set: FuturesUnordered<LocalBoxFuture<'static, (Arc<str>, BridgeOutcome)>> =
            FuturesUnordered::new();
        let bad: Arc<str> = Arc::from("bad");
        let good: Arc<str> = Arc::from("good");
        set.push(guard_bridge_panic(bad.clone(), panicking_bridge()).boxed_local());
        set.push(
            guard_bridge_panic(good.clone(), async move { (good, BridgeOutcome::Ended) })
                .boxed_local(),
        );

        let mut done: Vec<String> = Vec::new();
        while let Some((label, _)) = set.next().await {
            done.push(label.to_string());
        }
        done.sort();
        assert_eq!(done, vec!["bad".to_string(), "good".to_string()]);
    }

    // ── #130: a failed bridge restarts itself ────────────────────────────────

    /// Build a supervisor with one running bridge, ready to complete.
    fn sup_with_running(label: &str, operator_stopped: bool) -> Supervisor {
        use nvb_config::PuppetsConfig;
        let pool = SharedPool::new(&PuppetsConfig::default(), &TimingConfig::default());
        let ctx = TenantsCtx {
            servers: HashMap::new(),
            // Deliberately unwritable: the roster publish is best-effort and
            // must not affect any of these outcomes.
            data_dir: "/nonexistent-nvb-test".into(),
            static_keys: LiveKeys::default(),
            static_labels: HashSet::new(),
            limits: LimitsConfig::default(),
            timing: TimingConfig::default(),
            links: nvb_config::links::LinkShared::in_memory(),
        };
        let mut sup = Supervisor::new(pool, CancellationToken::new(), ctx);
        sup.running.insert(
            label.to_string(),
            RunningBridge {
                cfg: Arc::new(tenant(label, "!b:x")),
                root: CancellationToken::new(),
                capture_sink: None,
                origin: Origin::Static,
                operator_stopped,
                pending_respawn: None,
                spawned_at: std::time::Instant::now(),
                backoff_until: None,
            },
        );
        sup
    }

    /// THE #130 REGRESSION, supervisor half. A bridge whose task returned an
    /// error used to be gone for the process lifetime — three of them stayed
    /// dead for three days. It must now come back on a backoff.
    #[test]
    fn a_failed_bridge_is_rescheduled() {
        let mut sup = sup_with_running("b", false);
        sup.on_bridge_complete("b", BridgeOutcome::Failed);
        assert!(
            sup.running.contains_key("b"),
            "a failed bridge must be re-spawned, not left dead"
        );
        assert_eq!(sup.restart_failures.get("b").copied(), Some(1));
        let rb = &sup.running["b"];
        assert!(rb.backoff_until.is_some(), "the respawn must serve a backoff first");
        assert!(!rb.operator_stopped, "the fresh bridge starts un-stopped");
    }

    #[test]
    fn an_operator_stopped_bridge_is_not_restarted() {
        let mut sup = sup_with_running("b", true);
        sup.on_bridge_complete("b", BridgeOutcome::Failed);
        assert!(
            !sup.running.contains_key("b"),
            "a deliberate stop must stay stopped"
        );
    }

    #[test]
    fn a_clean_exit_is_not_restarted_and_clears_the_streak() {
        let mut sup = sup_with_running("b", false);
        sup.restart_failures.insert("b".to_string(), 3);
        sup.on_bridge_complete("b", BridgeOutcome::Ended);
        assert!(!sup.running.contains_key("b"));
        assert_eq!(sup.restart_failures.get("b"), None);
    }

    #[test]
    fn shutdown_never_respawns() {
        let mut sup = sup_with_running("b", false);
        sup.shutting_down = true;
        sup.on_bridge_complete("b", BridgeOutcome::Failed);
        assert!(
            !sup.running.contains_key("b"),
            "a fresh bridge mid-drain would hang the drain"
        );
    }

    #[test]
    fn backoff_grows_with_consecutive_failures_and_is_capped() {
        let t = TimingConfig::default();
        assert_eq!(t.bridge_restart_delay(0), Duration::from_secs(5));
        assert_eq!(t.bridge_restart_delay(1), Duration::from_secs(10));
        assert_eq!(t.bridge_restart_delay(2), Duration::from_secs(20));
        // Capped, and a long streak must not overflow the shift.
        assert_eq!(t.bridge_restart_delay(20), t.bridge_restart_max());
        assert_eq!(t.bridge_restart_delay(u32::MAX), t.bridge_restart_max());
    }

    /// A bridge that ran fine for a long time before dying starts its backoff
    /// from scratch, rather than inheriting a streak from an old outage.
    #[test]
    fn long_uptime_resets_the_failure_streak() {
        let mut sup = sup_with_running("b", false);
        sup.restart_failures.insert("b".to_string(), 6);
        let stable = sup.ctx.timing.bridge_restart_max() + Duration::from_secs(1);
        if let Some(rb) = sup.running.get_mut("b") {
            rb.spawned_at = std::time::Instant::now() - stable;
        }
        sup.on_bridge_complete("b", BridgeOutcome::Failed);
        assert_eq!(
            sup.restart_failures.get("b").copied(),
            Some(1),
            "streak resets to a first failure after a long healthy run"
        );
    }

    /// #131: the roster must show a configured-but-absent static bridge as
    /// `down` — the state that was invisible for three days.
    #[test]
    fn roster_reports_missing_static_bridges_as_down() {
        let mut sup = sup_with_running("alive-one", false);
        sup.ctx.static_labels = HashSet::from(["alive-one".to_string(), "dead-one".to_string()]);
        let entries = sup.roster_entries();
        let dead = entries.iter().find(|e| e.name == "dead-one").expect("listed");
        assert_eq!(dead.state, roster::BridgeState::Down);
        let alive = entries.iter().find(|e| e.name == "alive-one").expect("listed");
        assert_eq!(alive.state, roster::BridgeState::Alive);
    }

    #[test]
    fn roster_reports_a_backing_off_bridge_as_restarting() {
        let mut sup = sup_with_running("b", false);
        sup.on_bridge_complete("b", BridgeOutcome::Failed);
        let entries = sup.roster_entries();
        assert_eq!(entries[0].state, roster::BridgeState::Restarting);
        assert_eq!(entries[0].consecutive_failures, 1);
    }
}
