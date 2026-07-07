//! The ghost pool driver: [`GhostPool`]'s `new`/`sweep_stale_ghosts`/`run` and the
//! [`CaptureRouter`] surface the Discord side drives.

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex as StdMutex, PoisonError};

use anyhow::{anyhow, Context, Result};
use matrix_sdk::{
    deserialized_responses::SyncOrStrippedState,
    ruma::{
        events::{SyncStateEvent, call::member::CallMemberEventContent},
        OwnedUserId, RoomId,
    },
    Client,
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use nvb_audio::routing::{CaptureRouter, CaptureSink};
use nvb_config::{KeyTransport as KeyTransportMode, LiveKitConfig, MatrixConfig};

use super::super::key_transport::GhostCryptoCache;
use super::super::well_known::{self, LivekitFocusInfo};
use super::appservice::retract_membership_raw;
use super::identity::{ghost_localpart_prefix, is_ghost_identity, is_legacy_ghost_identity};
use super::lifecycle::{run_ghost, GhostKeyCache};
use super::{GhostCommand, GhostConfig, GhostHandle, GhostPool, GhostSink};

/// Bound on a ghost's frame channel. Real-time: drop on a full channel rather
/// than let a stale backlog build (matches the rest of the audio path).
const FRAME_CHANNEL_DEPTH: usize = 50;

impl GhostPool {
    /// Build the pool. Cheap and synchronous (like `PuppetPool::new`) so the
    /// returned `Arc` can be handed to the Discord side as a [`CaptureRouter`]
    /// and to the Matrix side to drive, both up front.
    pub fn new(
        matrix_cfg: &MatrixConfig,
        livekit_cfg: &LiveKitConfig,
        capture_wav: bool,
        max_ghosts: Option<usize>,
    ) -> Result<Arc<Self>> {
        let bot_user: OwnedUserId = matrix_cfg
            .bot_user_id
            .parse()
            .with_context(|| format!("parsing bot_user_id '{}'", matrix_cfg.bot_user_id))?;
        let http = reqwest::Client::builder()
            .use_rustls_tls()
            .build()
            .context("building ghost HTTP client")?;
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        Ok(Arc::new(Self {
            cmd_tx,
            cmd_rx: StdMutex::new(Some(cmd_rx)),
            sinks: StdMutex::new(HashMap::new()),
            cfg: GhostConfig {
                http,
                homeserver_url: matrix_cfg.homeserver_url.trim_end_matches('/').to_owned(),
                well_known_base_url: matrix_cfg.well_known_base_url(),
                room_id: matrix_cfg.room_id.clone(),
                as_token: matrix_cfg.appservice_token.clone(),
                server_name: bot_user.server_name().as_str().to_owned(),
                bot_user_id: bot_user.as_str().to_owned(),
                // Placeholders. The real media_e2ee / key_transport are resolved
                // (auto-detected from room encryption) only after the client syncs,
                // which happens after this synchronous `new`. `GhostPool::run`
                // overwrites both per ghost from its resolved arguments (#14).
                media_e2ee: false,
                key_transport: KeyTransportMode::Plaintext,
                // Resolve once here (not per-ghost) so concurrent ghost spawns never race
                // on the marker file; rides on `cfg`, cloned per assign.
                ghost_device_salt: crate::matrix::resolve_ghost_device_salt(&matrix_cfg.store_path),
                store_path: matrix_cfg.store_path.clone(),
                lk_jwt_url: livekit_cfg.lk_jwt_url.clone(),
                lk_url: livekit_cfg.url.clone(),
                capture_wav,
                ghost_localpart_prefix: ghost_localpart_prefix(matrix_cfg.sender_localpart.as_deref()),
                prune_stale_devices: matrix_cfg.prune_stale_devices.unwrap_or(true),
                rotate_ghost_sessions: matrix_cfg.rotate_ghost_sessions.unwrap_or(true),
                max_ghosts,
            },
        }))
    }

    /// Retract any **ghost** `m.call.member` left in the room by a prior crashed
    /// run, so Element Call starts from a clean roster before new ghosts mint.
    /// The mirror of the bridge bot's `retract_self_call_membership`. Current
    /// `@nether-voicebridge_*` ghosts are retracted via the appservice masquerade;
    /// stale legacy `@discord_*` ghosts (pre-rename, namespace no longer ours) are
    /// retracted by writing Empty as the bot, with a masquerade fallback.
    ///
    /// Reads room state with the bridge bot's joined client (the appservice
    /// sender is not necessarily in the room, so a raw `as_token` GET could
    /// 403); writes the retraction masqueraded as each ghost. Best-effort: logs
    /// and continues so startup is never blocked.
    pub async fn sweep_stale_ghosts(&self, client: &Client) -> Result<()> {
        let room_id = RoomId::parse(&self.cfg.room_id)
            .with_context(|| format!("parsing room_id '{}'", self.cfg.room_id))?;
        let Some(room) = client.get_room(&room_id) else {
            tracing::warn!("ghost sweep: room not in local state — skipping");
            return Ok(());
        };
        let raws = room
            .get_state_events_static::<CallMemberEventContent>()
            .await
            .context("reading call memberships for ghost sweep")?;
        let mut retracted = 0usize;
        for raw in raws {
            let Ok(ev) = raw.deserialize() else { continue };
            let orig = match ev {
                SyncOrStrippedState::Sync(SyncStateEvent::Original(o)) => o,
                _ => continue,
            };
            // Already retracted — nothing to do.
            if matches!(orig.content, CallMemberEventContent::Empty(_)) {
                continue;
            }
            if is_ghost_identity(orig.sender.as_str(), &self.cfg.ghost_localpart_prefix) {
                // Current-prefix ghost: masquerade-retract (we own this namespace).
                let state_key: &str = orig.state_key.as_ref();
                match retract_membership_raw(&self.cfg, orig.sender.as_str(), state_key).await {
                    Ok(()) => {
                        retracted += 1;
                        tracing::info!(ghost = %orig.sender, state_key, "retracted stale ghost call membership");
                    }
                    Err(e) => tracing::warn!(ghost = %orig.sender, error = %e, "ghost sweep: retract failed"),
                }
            } else if is_legacy_ghost_identity(orig.sender.as_str()) {
                // Legacy `@discord_*` leftover (pre-rename). We gave that namespace
                // up, so we can't masquerade — retract by writing Empty as the bot
                // (the underscore-format state_key is writable by a sufficiently
                // powered member unless the homeserver enforces owned-state). Fall
                // back to masquerade, which only works if `@discord_*` was reclaimed.
                let empty = CallMemberEventContent::new_empty(None);
                match room.send_state_event_for_key(&orig.state_key, empty).await {
                    Ok(_) => {
                        retracted += 1;
                        tracing::info!(ghost = %orig.sender, "retracted stale legacy ghost membership (bot write)");
                    }
                    Err(e) => {
                        let state_key: &str = orig.state_key.as_ref();
                        match retract_membership_raw(&self.cfg, orig.sender.as_str(), state_key).await {
                            Ok(()) => {
                                retracted += 1;
                                tracing::info!(ghost = %orig.sender, "retracted stale legacy ghost membership (masquerade)");
                            }
                            Err(e2) => tracing::warn!(
                                ghost = %orig.sender, bot_write_error = %e, masquerade_error = %e2,
                                "legacy ghost sweep: retract failed — reclaim the @discord_* namespace or redact via homeserver admin"
                            ),
                        }
                    }
                }
            }
            // else: bridge bot or a real user — leave untouched.
        }
        if retracted > 0 {
            tracing::info!(retracted, "stale-ghost sweep complete");
        }
        Ok(())
    }

    /// Drive the ghost lifecycle. Spawned/awaited from `lib::run` alongside the
    /// Matrix sync and RTC loops. Returns when the command channel closes.
    ///
    /// `client` is the bridge bot's matrix-sdk client, used (E2EE only) to
    /// enumerate the call's human members so each ghost can deliver its media key
    /// to them — see [`distribute_key_loop`]. `members_gen_rx` is bumped by the
    /// sync loop when a new human joins, waking those loops to re-key the joiner.
    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %bridge))]
    pub async fn run(
        self: Arc<Self>,
        client: Client,
        // Resolved E2EE settings (#14: auto-detected/overridden in `lib::run`),
        // applied to every ghost minted below. Overrides the `new`-time placeholders
        // in `self.cfg` because room encryption is only known after the client syncs.
        media_e2ee: bool,
        key_transport: KeyTransportMode,
        members_gen_rx: watch::Receiver<u64>,
        // True while a Matrix human is in the call. A ghost connects + publishes
        // (and delivers its E2EE key) as soon as this is true, rather than waiting
        // for its Discord user to speak — so audio is instant on first utterance.
        call_active_rx: watch::Receiver<bool>,
        bridge: Arc<str>,
        shutdown: CancellationToken,
    ) -> Result<()> {
        let mut cmd_rx = self
            .cmd_rx
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .ok_or_else(|| anyhow!("GhostPool::run already called"))?;

        // LiveKit focus is the same for every ghost; discover once, lazily, on
        // the first assign (avoids a startup round-trip when no Discord users
        // are present).
        let mut focus: Option<LivekitFocusInfo> = None;
        let mut ghosts: HashMap<String, GhostHandle> = HashMap::new();
        // Per-ghost crypto reused across rejoins for this bridge's process lifetime
        // (keeps the Olm device stable so a rejoiner stays audible) — see
        // [`GhostCryptoCache`].
        let crypto_cache: GhostCryptoCache = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        // Per-ghost media key reused across respawns — see [`GhostKeyCache`].
        let key_cache: GhostKeyCache = Arc::new(tokio::sync::Mutex::new(HashMap::new()));

        loop {
            let cmd = tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    // Drain every ghost to baseline: cancel + await teardown so all
                    // `post_membership(leave)` PUTs land before the driver returns.
                    tracing::info!(count = ghosts.len(), "ghost pool draining (shutdown)");
                    let draining: Vec<_> = ghosts.drain().collect();
                    for (_key, handle) in &draining {
                        handle.cancel.cancel();
                    }
                    futures_util::future::join_all(
                        draining.into_iter().map(|(_key, handle)| handle.join),
                    )
                    .await;
                    tracing::info!("ghost pool drained — teardown complete");
                    return Ok(());
                }
                cmd = cmd_rx.recv() => match cmd {
                    Some(cmd) => cmd,
                    None => break,
                },
            };
            match cmd {
                GhostCommand::Assign { key, display_name, avatar_url, frame_rx } => {
                    // Race fix: if a ghost for this key is still tearing down
                    // (rapid leave→rejoin), cancel it and **await** its teardown
                    // — including `post_membership(leave)` — before minting the
                    // replacement. The deterministic state_key (`D{key}`) is
                    // shared, so a stale leave PUT landing after the fresh join
                    // would otherwise hide the rejoined ghost from Element Call.
                    // The driver is single-task, so awaiting here serializes
                    // teardown-vs-respawn for free.
                    if let Some(old) = ghosts.remove(&key) {
                        old.cancel.cancel();
                        let _ = old.join.await;
                    }
                    if focus.is_none() {
                        match well_known::discover_livekit_focus(
                            &self.cfg.http,
                            &self.cfg.well_known_base_url,
                            &self.cfg.room_id,
                        )
                        .await
                        {
                            Ok(f) => focus = Some(f),
                            Err(e) => {
                                tracing::error!(error = %e, "ghost: focus discovery failed — skipping assign");
                                continue;
                            }
                        }
                    }
                    let cancel = CancellationToken::new();
                    let (mute_tx, mute_rx) = watch::channel(false);
                    let mut cfg = self.cfg.clone();
                    // Apply the resolved E2EE settings (the `new`-time fields are
                    // placeholders — see `GhostPool::new`).
                    cfg.media_e2ee = media_e2ee;
                    cfg.key_transport = key_transport;
                    let focus = focus.clone().expect("focus set above");
                    let join = tokio::spawn(
                        run_ghost(cfg, focus, client.clone(), members_gen_rx.clone(), call_active_rx.clone(), key.clone(), display_name, avatar_url, frame_rx, mute_rx, cancel.clone(), crypto_cache.clone(), key_cache.clone())
                            .instrument(tracing::info_span!(parent: None, "bridge", name = %bridge)),
                    );
                    ghosts.insert(key, GhostHandle { cancel, join, mute_tx });
                }
                GhostCommand::SetMuted { key, muted } => {
                    // `watch` keeps the latest value even if the ghost is still
                    // connecting (the receiver lives for the task's life). Use
                    // `send_if_modified` so the receiver only wakes — and the track
                    // only re-mutes / logs — on an ACTUAL change: Discord re-sends
                    // unchanged voice state, so `set_muted` is called on every
                    // update, but we want one toggle = one effect.
                    if let Some(handle) = ghosts.get(&key) {
                        handle.mute_tx.send_if_modified(|cur| {
                            if *cur == muted {
                                false
                            } else {
                                *cur = muted;
                                true
                            }
                        });
                    }
                }
                GhostCommand::Release { key } => {
                    // Await the teardown inline so the leave PUT is guaranteed to
                    // land before the entry is forgotten (the driver is the sole
                    // owner; blocking it briefly on a control-plane release is
                    // fine and keeps releases ordered).
                    if let Some(handle) = ghosts.remove(&key) {
                        handle.cancel.cancel();
                        let _ = handle.join.await;
                    }
                }
                GhostCommand::ReleaseAll => {
                    let draining: Vec<_> = ghosts.drain().collect();
                    for (_key, handle) in &draining {
                        handle.cancel.cancel();
                    }
                    // Await every ghost's teardown so all leave PUTs land before
                    // the driver moves on (used by shutdown drain).
                    futures_util::future::join_all(
                        draining.into_iter().map(|(_key, handle)| handle.join),
                    )
                    .await;
                }
            }
        }
        tracing::info!("ghost pool driver exiting");
        Ok(())
    }
}

/// The sink a speaker gets when the bridge is at its ghost cap (`[limits]
/// max_ghosts_per_bridge`, #47): frames are silently discarded and no ghost
/// exists. Never registered in `sinks`, so a later re-`assign` (speaker
/// re-join) re-checks the cap and can promote to a real ghost once a slot
/// frees.
struct NullCaptureSink;

impl CaptureSink for NullCaptureSink {
    fn push(&self, _mono: &[i16]) {}
}

impl CaptureRouter for GhostPool {
    fn assign(&self, key: &str, display_name: &str, avatar_url: Option<&str>) -> Arc<dyn CaptureSink> {
        let mut sinks = self.sinks.lock().unwrap_or_else(PoisonError::into_inner);
        // Idempotent: a re-join for the same Discord user reuses its sink/ghost.
        if let Some(sink) = sinks.get(key) {
            return sink.clone();
        }
        // Admission cap (#47): at the cap a NEW speaker is simply not bridged —
        // one WARN per assign attempt (assign is called once per speaker
        // appearance, so this doesn't spam), existing ghosts unaffected, FCFS
        // like puppet overflow. `sinks` holds only real ghosts, so a release
        // frees the slot for the next assign.
        if let Some(max) = self.cfg.max_ghosts {
            if sinks.len() >= max {
                tracing::warn!(
                    key,
                    display_name,
                    max_ghosts = max,
                    "ghost cap reached — speaker NOT bridged to Matrix until a slot frees"
                );
                return Arc::new(NullCaptureSink);
            }
        }
        let (frame_tx, frame_rx) = mpsc::channel::<Vec<i16>>(FRAME_CHANNEL_DEPTH);
        let sink = Arc::new(GhostSink {
            frame_tx,
            key: key.to_owned(),
            dropped_full: AtomicU64::new(0),
        });
        sinks.insert(key.to_owned(), sink.clone());
        if self
            .cmd_tx
            .send(GhostCommand::Assign {
                key: key.to_owned(),
                display_name: display_name.to_owned(),
                avatar_url: avatar_url.map(str::to_owned),
                frame_rx,
            })
            .is_err()
        {
            tracing::warn!(key, "ghost driver gone — cannot assign");
        } else {
            tracing::info!(key, display_name, "ghost assigned");
        }
        sink
    }

    fn set_muted(&self, key: &str, muted: bool) {
        // Only forward for a speaker that actually has a ghost — keeps a stray
        // mute update for a non-ghost (or already-released) user a cheap no-op.
        let known = self
            .sinks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(key);
        if known {
            let _ = self
                .cmd_tx
                .send(GhostCommand::SetMuted { key: key.to_owned(), muted });
        }
    }

    fn release(&self, key: &str) {
        let removed = self
            .sinks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(key)
            .is_some();
        if removed {
            let _ = self.cmd_tx.send(GhostCommand::Release { key: key.to_owned() });
            tracing::info!(key, "ghost released");
        }
    }

    fn release_all(&self) {
        self.sinks.lock().unwrap_or_else(PoisonError::into_inner).clear();
        let _ = self.cmd_tx.send(GhostCommand::ReleaseAll);
    }
}
