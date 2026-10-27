// clippy ≥ 1.99 (Rust 1.99.0, 2026-09-28) flags the `#[must_use]` that the
// `async_trait` macro attaches to its generated boxed-future methods
// (double_must_use). The attribute is the macro's, not ours; allowed crate-wide
// so every `#[async_trait]` item here stays lint-clean under `-D warnings`.
#![allow(clippy::double_must_use)]
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

mod auth_error;
pub mod cross_signing;
mod device_prune;
mod matrix;
pub mod links_notice;
pub mod mgmt;
mod rtc;

use std::sync::Arc;

use anyhow::{Context, Result};
use matrix_sdk::ruma::RoomId;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::Instrument as _;

use nvb_audio::routing::PlaybackRouter;
use nvb_config::{LiveKitConfig, MatrixConfig, TimingConfig};

pub use rtc::ghost::GhostPool;
pub use device_prune::delete_device_best_effort;
pub use matrix::RoomUnavailable;

// #38 e2e testkit: fake Matrix call participants for the nvb-e2e harness.
// Feature-gated — a default build compiles none of it (see `rtc/testkit.rs`).
#[cfg(feature = "testkit")]
pub use rtc::ghost::testkit;

/// Run the Matrix side of the bridge to completion.
///
/// Builds the matrix-sdk client, then drives the sync loop (which raises
/// call-active transitions over an internal watch channel), the RTC subscriber
/// loop, and the ghost-pool driver concurrently. The caller supplies the
/// cross-side plumbing: the `router` (Matrix → Discord audio, routed
/// per-participant to puppets) and the `ghost_pool` (Discord → Matrix audio,
/// one ghost per Discord speaker — also handed to the Discord side as the
/// `CaptureRouter`).
#[allow(clippy::too_many_arguments)]
pub async fn run(
    matrix_cfg: &MatrixConfig,
    livekit_cfg: &LiveKitConfig,
    // Resolved `[timing]` grace-timer profile (#38): the leave-grace debounce
    // and the stale-membership reaper cadence. Defaults match the historical
    // hardcoded values; the supervisor resolves it once per process.
    timing: TimingConfig,
    router: Arc<dyn PlaybackRouter>,
    ghost_pool: Arc<GhostPool>,
    bridge: Arc<str>,
    shutdown: CancellationToken,
    // Linked identities (ADR 0001): this bridge's board, and the channel on which
    // the Discord side delivers Matrix-side transitions.
    links: Arc<nvb_config::links::LinkBoard>,
    link_rx: tokio::sync::mpsc::UnboundedReceiver<nvb_config::links::LinkEvent>,
) -> Result<()> {
    // Pair the bot's Matrix device identity with its crypto store (#30): a wiped store
    // must NOT reuse the configured (bridge-name-derived) device_id with fresh keys —
    // peers reject that as a device-identity change and never deliver the call's media
    // key to the subscriber (Matrix→Discord goes silent on a fresh store).
    // `resolve_store_device_id` mints a unique id for a fresh store and reuses the
    // persisted one otherwise. Shadow `matrix_cfg` so build_client, the sync loop, and
    // the subscriber membership all assert the SAME effective device_id.
    let mut owned_cfg = matrix_cfg.clone();
    let resolved = matrix::resolve_store_device_id(&matrix_cfg.store_path, &matrix_cfg.device_id);
    owned_cfg.device_id = resolved.id;

    // Deletion self-heal (#53): a reused id whose device keys have vanished server-side
    // must NOT be resurrected — clients that saw the deletion never re-learn a re-upload
    // with unchanged keys and silently withhold the call media key (the 2026-07-03
    // outage). Only a reused id can be affected (a fresh mint has no server keys yet),
    // and an indeterminate answer fails open: never churn the identity on a flake.
    if resolved.reused {
        let http = reqwest::Client::builder().use_rustls_tls().build().ok();
        if let Some(http) = http {
            let present = device_prune::query_device_keys_present(
                &http,
                &matrix_cfg.homeserver_url,
                &matrix_cfg.appservice_token,
                &matrix_cfg.bot_user_id,
                &owned_cfg.device_id,
            )
            .await;
            if present == Some(false) {
                let dead = owned_cfg.device_id.clone();
                owned_cfg.device_id =
                    matrix::remint_bot_device(&matrix_cfg.store_path, &matrix_cfg.device_id, &dead);
            }
        }
    }
    let matrix_cfg = &owned_cfg;

    // Build the client first (surfaces login/restore errors before the loops). It
    // always opens the persistent store and does the initial sync, so room state
    // (incl. `m.room.encryption`) is local by the time it returns.
    let client = matrix::build_client(matrix_cfg, &bridge).await?;

    // Device hygiene (#53): delete ids this store itself retired (history-scoped, so
    // another deployment's devices are structurally untouchable). After build_client so
    // the current device exists server-side with keys; spawned so startup never blocks
    // on it. Failures stay in the history file for the next start.
    if matrix_cfg.prune_stale_devices.unwrap_or(true) {
        let retired = device_prune::read_retired(&matrix_cfg.store_path);
        if !retired.is_empty() {
            let homeserver_url = matrix_cfg.homeserver_url.clone();
            let as_token = matrix_cfg.appservice_token.clone();
            let user_mxid = matrix_cfg.bot_user_id.clone();
            let store_path = matrix_cfg.store_path.clone();
            let span = tracing::info_span!(parent: None, "bridge", name = %bridge);
            tokio::spawn(
                async move {
                    let Ok(http) = reqwest::Client::builder().use_rustls_tls().build() else {
                        return;
                    };
                    let pending = device_prune::prune_retired(
                        &http,
                        &homeserver_url,
                        &as_token,
                        &user_mxid,
                        retired,
                    )
                    .await;
                    device_prune::write_retired(&store_path, &pending);
                }
                .instrument(span),
            );
        }
    }

    // Resolve media-E2EE for this bridge (#14): auto-detect from the room's
    // `m.room.encryption` state, unless the config pins an override. An encrypted
    // room also defaults the key transport to Olm (true E2EE). These resolved
    // values — not the raw config Options — drive every E2EE decision below.
    let room_id = RoomId::parse(&matrix_cfg.room_id)
        .with_context(|| format!("parsing room_id '{}'", matrix_cfg.room_id))?;
    let detected = matrix::detect_room_encryption(&client, &room_id)
        .await
        .context("detecting room encryption")?;
    let media_e2ee = livekit_cfg.resolved_media_e2ee(detected);
    let key_transport = livekit_cfg.resolved_key_transport(media_e2ee);
    // `%room_id` makes this unambiguous even if a span guard held across an await
    // elsewhere bleeds another bridge's span onto this line (concurrent !Send runs).
    tracing::info!(
        %room_id,
        detected_encrypted = detected,
        media_e2ee,
        ?key_transport,
        override_media_e2ee = ?livekit_cfg.media_e2ee,
        "resolved media E2EE for bridge"
    );
    // The bot is a visible roster member (named + avatared) only with E2EE on, so
    // EC/Commet can target it with media keys. Done here now that detection has run.
    if media_e2ee {
        matrix::apply_bot_profile(&client, matrix_cfg).await;
    }

    // Clear any ghost m.call.member left by a prior crashed run before the
    // subscriber joins, so Element Call starts from a clean roster (the bridge
    // bot's own stale membership is retracted separately in `rtc::run`). Reads
    // state with the now-synced client; best-effort.
    if let Err(e) = ghost_pool.sweep_stale_ghosts(&client).await {
        tracing::warn!(error = %e, "stale-ghost sweep failed (continuing)");
    }

    // Companion stream devices (ADR 0004 §5): this bridge serves `stream:attach` leases for its room while it runs.
    let _stream_provider = nvb_companion::stream::register_provider(&matrix_cfg.room_id, ghost_pool.clone());

    // The call-active watch is fed by the sync loop (sender) and consumed by the
    // RTC loop AND the ghost pool: a ghost connects/publishes (and delivers its
    // E2EE key) as soon as a Matrix human is present, so audio is ready before the
    // Discord user speaks. `watch::Receiver` is cloneable; clone for the ghost pool
    // before `rtc::run` below moves the original.
    let (call_active_tx, call_active_rx) = watch::channel(false);
    let call_active_rx_ghost = call_active_rx.clone();

    // Membership-generation watch: the sync loop bumps it whenever a new human
    // joins the call; each ghost's key-distribution loop waits on it to (re-)send
    // its E2EE media key to the new member. A monotonic counter so a missed edge is
    // still observed via the changed value (vs. a Notify's lost-wakeup risk).
    let (members_gen_tx, members_gen_rx) = watch::channel(0u64);
    // The RTC loop also watches it: a joining human may publish on an SFU the
    // bridge isn't subscribed to yet (multi-SFU fan-in, `rtc::focus`).
    let members_gen_rx_rtc = members_gen_rx.clone();
    // Call-focus election (MSC4195): the RTC loop publishes the SFU the oldest
    // active membership elects; ghosts publish there (and migrate when it moves)
    // so legacy `oldest_membership` clients hear Discord speakers.
    let (call_focus_tx, call_focus_rx) = watch::channel(None);

    // Drive the sync loop, the RTC subscriber, and the ghost-pool driver to
    // completion. A bare `tokio::try_join!` would DROP the sibling futures the
    // instant one returned Err — WITHOUT cancelling `shutdown` — so a mid-call
    // failure (e.g. matrix::run hitting a fatal 401/403 sync error, or a state
    // read failing) detached every live ghost task: their `m.call.member` is
    // never retracted (phantom users linger in Element Call), key-distribution
    // loops heartbeat to a dead call, and the RTC subscriber keeps retrying (#67).
    // Instead, `guard_sibling` cancels `shutdown` the moment ANY of the three
    // exits with an error, and `join3` then keeps polling the others so they
    // observe the cancellation and run their bounded teardown drains — the ghost
    // pool cancels and awaits every ghost's leave PUT — before `run` returns the
    // error. On the happy path (`shutdown` cancelled by the supervisor) nothing
    // errors, the guard never fires, and all three drain exactly as before. The
    // whole teardown is bounded by the supervisor's TEARDOWN_TIMEOUT force-drop,
    // armed the instant `shutdown` fires.
    let (matrix_res, rtc_res, ghost_res) = futures_util::future::join3(
        guard_sibling(
            matrix::run(
                client.clone(),
                matrix_cfg,
                media_e2ee,
                timing.leave_grace(),
                timing.paired_arrival_grace(),
                call_active_tx,
                members_gen_tx,
                router.clone(),
                bridge.clone(),
                shutdown.clone(),
                links.clone(),
                link_rx,
            ),
            &shutdown,
        ),
        guard_sibling(
            rtc::run(
                client.clone(),
                livekit_cfg,
                matrix_cfg,
                media_e2ee,
                timing.reap_interval(),
                timing.reap_grace(),
                call_active_rx,
                members_gen_rx_rtc,
                call_focus_tx,
                router,
                bridge.clone(),
                shutdown.clone(),
                links.clone(),
            ),
            &shutdown,
        ),
        guard_sibling(
            ghost_pool.run(
                client,
                media_e2ee,
                key_transport,
                members_gen_rx,
                call_active_rx_ghost,
                call_focus_rx,
                bridge.clone(),
                shutdown.clone(),
            ),
            &shutdown,
        ),
    )
    .await;
    // Surface the first error (positionally; the siblings return Ok once they
    // drain on the cancelled `shutdown`, so this is the originating failure).
    matrix_res.and(rtc_res).and(ghost_res)?;

    Ok(())
}

/// Drive one of [`run`]'s three top-level futures, cancelling `shutdown` if it
/// exits with an error so the siblings observe the failure and run their
/// (bounded) teardown drains before `run` returns — see the `join3` call in
/// [`run`] (#67). On the happy path `shutdown` is cancelled externally and the
/// inner future returns Ok, so the re-cancel never fires.
async fn guard_sibling(
    fut: impl std::future::Future<Output = Result<()>>,
    shutdown: &CancellationToken,
) -> Result<()> {
    let result = fut.await;
    if result.is_err() {
        shutdown.cancel();
    }
    result
}
