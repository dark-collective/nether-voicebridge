//! Ghost pool — one appservice Matrix user per Discord speaker (Phase 8).
//!
//! The mirror of the Discord side's `PuppetPool`: where a puppet gives a Matrix
//! speaker a named Discord voice, a **ghost** gives a Discord speaker a named
//! Matrix presence. Each ghost is an appservice user
//! `@nether-voicebridge_<id>:server` that
//! joins the MatrixRTC/LiveKit call as its own participant and publishes only
//! that Discord speaker's audio, so Element Call shows each Discord user as a
//! distinct, correctly-named participant.
//!
//! [`GhostPool`] implements [`CaptureRouter`]: the Discord side calls
//! `assign`/`release` (driven by voice-channel membership) and `push`es a single
//! speaker's 20 ms mono frames into the returned [`CaptureSink`]. `assign`/
//! `release` are synchronous (the trait contract) so they only enqueue onto a
//! command channel; the async per-ghost lifecycle runs in [`GhostPool::run`]'s
//! driver task — exactly like `PuppetPool`'s command-loop pattern.
//!
//! ## Appservice mechanics
//!
//! matrix-sdk 0.18 has no appservice support, so ghost operations go over the
//! raw client-server API with the appservice `as_token`, masquerading as the
//! ghost via `?user_id=`: register, set displayname, join the room, fetch an
//! OpenID token, and post `m.call.member`. The OpenID token is then exchanged
//! at lk-jwt-service for a LiveKit JWT exactly as the bridge bot does — the key
//! live risk (architecture open-Q #3) is whether lk-jwt accepts a ghost's
//! OpenID token; the lk-jwt status body is logged so a 401 is diagnosable.
//!
//! ## Echo prevention (constraint #5)
//!
//! Ghosts publish into the same room the bridge bot subscribes to. The bridge
//! bot's subscriber skips ghost identities ([`is_ghost_identity`]) so it never
//! re-bridges a ghost's audio back to Discord.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex as StdMutex, PoisonError};

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use livekit::e2ee::key_provider::{KeyDerivationAlgorithm, KeyProvider, KeyProviderOptions};
use livekit::e2ee::{E2eeOptions, EncryptionType};
use livekit::id::ParticipantIdentity;
use livekit::track::LocalAudioTrack;
use livekit::webrtc::audio_frame::AudioFrame;
use livekit::webrtc::audio_source::native::NativeAudioSource;
use livekit::{Room, RoomOptions};
use matrix_sdk::{
    authentication::{matrix::MatrixSession, SessionTokens},
    config::SyncSettings,
    deserialized_responses::SyncOrStrippedState,
    ruma::{
        events::{
            SyncStateEvent,
            call::member::{
                ActiveFocus, ActiveLivekitFocus, Application, CallApplicationContent,
                CallMemberEventContent, CallMemberStateKey, CallScope, Focus, LivekitFocus,
            },
        },
        OwnedDeviceId, OwnedUserId, RoomId, UserId,
    },
    Client, SessionMeta,
};
use reqwest::Url;
use std::borrow::Cow;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use nvb_audio::jitter::JitterBuffer;
use nvb_audio::routing::{CaptureRouter, CaptureSink};
use nvb_config::{KeyTransport as KeyTransportMode, LiveKitConfig, MatrixConfig};

use super::key_transport::{KeyTransport, OlmTransport, PlaintextTransport};
use super::keys;
use super::lk_jwt;
use super::openid::OpenIdToken;
use super::publish;
use super::session::decode_jwt_claims;
use super::well_known::{self, LivekitFocusInfo};

/// Localpart prefix for ghost MXIDs (`@nether-voicebridge_<discord_user_id>:server`).
/// Must match the appservice namespace regex (`@nether-voicebridge_.*`). Note: this
/// is deliberately NOT `discord_` — that namespace is owned by mautrix-discord, and
/// sharing it on the same homeserver would collide.
const GHOST_LOCALPART_PREFIX: &str = "nether-voicebridge_";

/// MatrixRTC call-membership state event type (MSC3401). Matches what
/// `rtc::membership` posts via ruma's typed `send_state_event_for_key`; here we
/// post over the raw CS-API (masquerade) so the type is spelled out.
const CALL_MEMBER_EVENT_TYPE: &str = "org.matrix.msc3401.call.member";

/// Jitter buffer sizing for each ghost's outbound (Discord → LiveKit) stream —
/// mirrors the bridge bot's former single-stream consumer.
const JITTER_TARGET_MS: u32 = 60;
const JITTER_MAX_MS: u32 = 120;
const SAMPLE_RATE: u32 = 48_000;
const SAMPLES_10MS: u32 = 480;

/// Bound on a ghost's frame channel. Real-time: drop on a full channel rather
/// than let a stale backlog build (matches the rest of the audio path).
const FRAME_CHANNEL_DEPTH: usize = 50;

/// Is this LiveKit participant identity one of our ghosts? Used by the bridge
/// bot's subscriber to skip ghost tracks (echo prevention, constraint #5). The
/// identity is `@nether-voicebridge_<id>:server[:device]`; the localpart is the
/// segment between `@` and the first `:`.
pub(crate) fn is_ghost_identity(identity: &str) -> bool {
    identity
        .strip_prefix('@')
        .and_then(|rest| rest.split(':').next())
        .is_some_and(|localpart| localpart.starts_with(GHOST_LOCALPART_PREFIX))
}

/// The prefix this bridge minted ghosts under *before* the rename to
/// [`GHOST_LOCALPART_PREFIX`]. It collided with mautrix-discord, so we no longer
/// own it — but stale `@discord_<id>` ghosts may linger in old room state.
const LEGACY_GHOST_PREFIX: &str = "discord_";

/// Is this identity a *legacy* ghost (`@discord_<digits>`) we minted before the
/// rename? Matched only as `discord_` followed by an all-digits localpart (the
/// Discord-user-id form we used) so a human `@discord_alice` is never caught.
///
/// Used for **exclusion only** ([`is_bridge_excluded_identity`]) — never for the
/// appservice masquerade paths (register/key-send), which we no longer own for
/// this namespace. mautrix-discord's own `@discord_<id>` ghosts match too, which
/// is fine: they must never be counted as our call participants, and they never
/// post `m.call.member` anyway.
pub(crate) fn is_legacy_ghost_identity(identity: &str) -> bool {
    identity
        .strip_prefix('@')
        .and_then(|rest| rest.split(':').next())
        .and_then(|localpart| localpart.strip_prefix(LEGACY_GHOST_PREFIX))
        .is_some_and(|rest| !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit()))
}

/// Should this identity be excluded from call-participant counting, key recipients,
/// and capture/forward? True for our current ghosts and for stale legacy ghosts.
/// (Constraint #5 echo-prevention plus the post-rename leftover cleanup.)
pub(crate) fn is_bridge_excluded_identity(identity: &str) -> bool {
    is_ghost_identity(identity) || is_legacy_ghost_identity(identity)
}

/// A [`CaptureSink`] for one ghost: pushes a Discord speaker's 20 ms mono frames
/// onto the ghost's frame channel. The per-ghost lifecycle task drains the
/// channel through a jitter buffer into the ghost's LiveKit source.
struct GhostSink {
    frame_tx: mpsc::Sender<Vec<i16>>,
}

impl CaptureSink for GhostSink {
    fn push(&self, mono: &[i16]) {
        // Never block on the capture hot path; drop on a full/closed channel.
        let _ = self.frame_tx.try_send(mono.to_vec());
    }
}

/// Commands from the (sync) [`CaptureRouter`] surface to the async driver.
enum GhostCommand {
    /// Mint a ghost for `key` and start publishing frames arriving on `frame_rx`.
    Assign {
        key: String,
        display_name: String,
        avatar_url: Option<String>,
        frame_rx: mpsc::Receiver<Vec<i16>>,
    },
    /// Mirror the speaker's mute state onto their ghost's LiveKit track.
    SetMuted { key: String, muted: bool },
    /// Retire the ghost for `key`.
    Release { key: String },
    /// Retire every ghost (call teardown).
    ReleaseAll,
}

/// A running ghost's control handle, held by the driver. `cancel` signals the
/// `run_ghost` task to tear down (retract membership, close LiveKit); `join`
/// lets the driver **await** that teardown so a late `post_membership(leave)`
/// can never clobber a same-key re-assign or outlive a shutdown drain.
struct GhostHandle {
    cancel: CancellationToken,
    join: tokio::task::JoinHandle<()>,
    /// Latest mute state for the ghost's track. A `watch` so a `SetMuted` that
    /// arrives before the ghost finishes connecting is applied once the track is
    /// live (also covers a user who joins already muted).
    mute_tx: watch::Sender<bool>,
}

/// The ghost pool / capture router. Cheap to construct; the appservice work
/// happens in the driver started by [`GhostPool::run`].
pub struct GhostPool {
    cmd_tx: mpsc::UnboundedSender<GhostCommand>,
    /// Taken once by [`GhostPool::run`].
    cmd_rx: StdMutex<Option<mpsc::UnboundedReceiver<GhostCommand>>>,
    /// Live per-speaker sinks. The single source of truth for which speakers
    /// have a ghost; guards `assign` idempotence and `release`.
    sinks: StdMutex<HashMap<String, Arc<GhostSink>>>,
    cfg: GhostConfig,
}

/// Immutable per-bridge config the driver needs to mint ghosts.
#[derive(Clone)]
struct GhostConfig {
    http: reqwest::Client,
    homeserver_url: String,
    room_id: String,
    /// Appservice `as_token` (config `matrix.appservice_token`).
    as_token: String,
    /// MXID server part (e.g. `nether.im`), derived from the bridge bot's MXID.
    server_name: String,
    /// The bridge bot's full MXID (the subscriber). Excluded from ghost media-key
    /// recipients: the subscriber skips ghost tracks (constraint #5) so it never
    /// needs to decrypt them.
    bot_user_id: String,
    media_e2ee: bool,
    /// How ghosts deliver their media key when `media_e2ee` is on (plaintext Path 2
    /// or Olm Path 3). Mirrors `livekit.key_transport`.
    key_transport: KeyTransportMode,
    /// Base directory of the bridge's matrix-sdk store (`matrix.store_path`). Each
    /// Olm-transport ghost nests its own persistent crypto store under
    /// `<store_path>/ghosts/<localpart>` so Olm sessions + uploaded keys survive
    /// rejoins (no OTK re-upload churn).
    store_path: String,
    /// Optional config overrides, mirroring the subscriber path. `None` ⇒ derive
    /// the lk-jwt endpoint from the focus and the SFU ws URL from the lk-jwt
    /// response. Set only to override (non-Nether deploy / no `rtc_foci`).
    lk_jwt_url: Option<String>,
    lk_url: Option<String>,
}

impl GhostPool {
    /// Build the pool. Cheap and synchronous (like `PuppetPool::new`) so the
    /// returned `Arc` can be handed to the Discord side as a [`CaptureRouter`]
    /// and to the Matrix side to drive, both up front.
    pub fn new(matrix_cfg: &MatrixConfig, livekit_cfg: &LiveKitConfig) -> Result<Arc<Self>> {
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
                store_path: matrix_cfg.store_path.clone(),
                lk_jwt_url: livekit_cfg.lk_jwt_url.clone(),
                lk_url: livekit_cfg.url.clone(),
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
            if is_ghost_identity(orig.sender.as_str()) {
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
                            &self.cfg.homeserver_url,
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
                        run_ghost(cfg, focus, client.clone(), members_gen_rx.clone(), call_active_rx.clone(), key.clone(), display_name, avatar_url, frame_rx, mute_rx, cancel.clone())
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

impl CaptureRouter for GhostPool {
    fn assign(&self, key: &str, display_name: &str, avatar_url: Option<&str>) -> Arc<dyn CaptureSink> {
        let mut sinks = self.sinks.lock().unwrap_or_else(PoisonError::into_inner);
        // Idempotent: a re-join for the same Discord user reuses its sink/ghost.
        if let Some(sink) = sinks.get(key) {
            return sink.clone();
        }
        let (frame_tx, frame_rx) = mpsc::channel::<Vec<i16>>(FRAME_CHANNEL_DEPTH);
        let sink = Arc::new(GhostSink { frame_tx });
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

/// One ghost's full lifecycle: register/join/connect/publish, post call
/// membership, then publish the speaker's audio until cancelled, retracting
/// membership and disconnecting on the way out.
#[allow(clippy::too_many_arguments)]
async fn run_ghost(
    cfg: GhostConfig,
    focus: LivekitFocusInfo,
    client: Client,
    members_gen_rx: watch::Receiver<u64>,
    mut call_active_rx: watch::Receiver<bool>,
    key: String,
    display_name: String,
    avatar_url: Option<String>,
    frame_rx: mpsc::Receiver<Vec<i16>>,
    mute_rx: watch::Receiver<bool>,
    cancel: CancellationToken,
) {
    let ghost_mxid = format!("@{GHOST_LOCALPART_PREFIX}{key}:{}", cfg.server_name);
    let localpart = format!("{GHOST_LOCALPART_PREFIX}{key}");
    // Stable per-ghost device id; must agree between the lk-jwt identity and the
    // m.call.member content so EC maps the ghost's media to its membership.
    let device_id = format!("D{key}");

    let span = tracing::info_span!("ghost", mxid = %ghost_mxid);
    async {
        // 1. Register (idempotent) and set the display name.
        if let Err(e) = register_ghost(&cfg, &localpart).await {
            tracing::error!(error = %e, "ghost register failed — skipping");
            return;
        }
        if let Err(e) = set_displayname(&cfg, &ghost_mxid, &display_name).await {
            tracing::warn!(error = %e, "ghost set_displayname failed (continuing)");
        }
        // Mirror the Discord avatar onto the ghost's Matrix profile (best-effort;
        // never blocks the ghost going live). Re-set on each assign — cheap, and
        // it tracks avatar changes across rejoins.
        if let Some(url) = avatar_url.as_deref() {
            if let Err(e) = set_avatar(&cfg, &ghost_mxid, url).await {
                tracing::warn!(error = %e, "ghost set_avatar failed (continuing)");
            }
        }
        // 2. Invite the ghost (as the bridge bot, already a room member) so it can
        // join a *restricted* room — a bare /join is refused there without an invite
        // (MSC3083). Best-effort: a public/non-restricted room may not need (or may
        // forbid) the invite, and the bare join below would still succeed — so a
        // failed invite must not block the ghost. `join_room` is the sole fatal gate:
        // if the room really was restricted, it fails there with a clear 403.
        if let Err(e) = invite_ghost(&client, &cfg.room_id, &ghost_mxid).await {
            tracing::warn!(
                error = %e,
                bot = %cfg.bot_user_id,
                room = %cfg.room_id,
                "ghost invite failed — trying join anyway (restricted rooms need the bot to have invite power)"
            );
        }
        // 3. Join the room (required before posting m.call.member).
        if let Err(e) = join_room(&cfg, &ghost_mxid).await {
            tracing::error!(error = %e, "ghost join_room failed — skipping");
            return;
        }
        // 4. Presence phase — ONLY while no Matrix human is in the call yet. Post
        // m.call.member (no media) so Element Call shows "a Discord user is waiting
        // here", then wait for a human. Skipped entirely when a human is already
        // present (e.g. a Discord user rejoining mid-call), so we go straight to the
        // publish-first path below — see step 5 for why ordering matters.
        let mut presence_membership: Option<tokio::task::JoinHandle<()>> = None;
        if !*call_active_rx.borrow_and_update() {
            presence_membership = Some(tokio::spawn(
                post_membership_until_joined(
                    cfg.clone(),
                    ghost_mxid.clone(),
                    device_id.clone(),
                    focus.clone(),
                    cancel.clone(),
                )
                .instrument(tracing::Span::current()),
            ));
            tracing::info!(display_name = %display_name, "ghost present (membership posting; no media until a Matrix user joins the call)");
            loop {
                if *call_active_rx.borrow_and_update() {
                    break;
                }
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => {
                        // Cancelled (Discord user left) before any Matrix human —
                        // retract and retire without ever touching LiveKit.
                        if let Some(h) = presence_membership.take() {
                            h.abort();
                        }
                        if let Err(e) = post_membership(&cfg, &ghost_mxid, &device_id, &focus, true).await {
                            tracing::warn!(error = %e, "ghost post_membership(leave) failed");
                        }
                        tracing::info!("ghost retired (was presence-only)");
                        return;
                    }
                    // Sender drops only on shutdown, which also cancels (handled above
                    // by the biased arm); ignore the result and re-check.
                    _ = call_active_rx.changed() => {}
                }
            }
        }

        // 5. Active phase — a Matrix human is present. Publish the LiveKit track
        // FIRST, THEN (re-)post m.call.member. Element Call subscribes to a ghost's
        // track when it processes the membership WITH the track already live; posting
        // membership before the track (as a naive presence-first flow does) leaves a
        // window where EC sees the member with no track and never re-subscribes to
        // the late track — which silenced a Discord user who left and rejoined
        // mid-call. The Room handle is moved into the event-drain task, which keeps
        // the connection up and closes it on cancel; we keep only the source to feed.
        // Honor cancellation across the connect: `connect_and_publish` does network
        // I/O that does not itself observe `cancel`, so race it. This is what makes a
        // self-heal effective — if this ghost was retracted mid-connect (e.g. it was
        // minted for one of our OWN puppet bots in a startup race, see
        // `retract_self_ghost`), we must abort BEFORE publishing a track or posting
        // membership, or the puppet would briefly surface in the call (echo).
        let (source, track, media_key) = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                if let Some(h) = presence_membership.take() {
                    h.abort();
                }
                if let Err(e) = post_membership(&cfg, &ghost_mxid, &device_id, &focus, true).await {
                    tracing::warn!(error = %e, "ghost post_membership(leave) failed");
                }
                tracing::info!("ghost retired (cancelled before publish)");
                return;
            }
            res = connect_and_publish(&cfg, &focus, &ghost_mxid, &device_id, cancel.clone()) => match res {
                Ok(out) => out,
                Err(e) => {
                    tracing::error!(error = %e, "ghost LiveKit connect/publish failed — retracting");
                    if let Some(h) = presence_membership.take() {
                        h.abort();
                    }
                    if let Err(e) = post_membership(&cfg, &ghost_mxid, &device_id, &focus, true).await {
                        tracing::warn!(error = %e, "ghost post_membership(leave) failed");
                    }
                    return;
                }
            }
        };
        // Track is live → post membership now (EC subscribes off this). Drop any
        // presence-phase post first so this is the last writer at the state key.
        if let Some(h) = presence_membership.take() {
            h.abort();
        }
        let membership_handle = tokio::spawn(
            post_membership_until_joined(
                cfg.clone(),
                ghost_mxid.clone(),
                device_id.clone(),
                focus.clone(),
                cancel.clone(),
            )
            .instrument(tracing::Span::current()),
        );
        tracing::info!(display_name = %display_name, "ghost publishing Discord speaker audio");

        // 5b. E2EE only: distribute this ghost's media key to the call's humans so
        // they can decrypt its encrypted frames. The transport (plaintext Path 2 vs
        // Olm Path 3) is built inside the spawned task so audio pumping below is never
        // blocked on crypto-client bootstrap. Runs until cancel (re-sends to catch
        // joiners).
        if let Some(media_key) = media_key {
            tokio::spawn(
                distribute_key_task(
                    client,
                    cfg.clone(),
                    localpart.clone(),
                    ghost_mxid.clone(),
                    device_id.clone(),
                    media_key,
                    members_gen_rx,
                    cancel.clone(),
                )
                .instrument(tracing::Span::current()),
            );
        }

        // 6. Pump frames until cancelled, mirroring the Discord user's mute state
        // onto the track as it changes (and applying the current state up front, so
        // a user who joined muted starts muted).
        pump_audio(frame_rx, &source, &track, mute_rx, &cancel).await;

        // 7. Teardown. Stop the membership-retry task first (it observes `cancel`)
        // and await it so a late join can't race past the leave below and linger.
        let _ = membership_handle.await;
        // Retract call membership (best-effort; a no-op if we never joined). The
        // LiveKit room is closed by the event-drain task once `cancel` fires.
        if let Err(e) = post_membership(&cfg, &ghost_mxid, &device_id, &focus, true).await {
            tracing::warn!(error = %e, "ghost post_membership(leave) failed");
        }
        tracing::info!("ghost retired");
    }
    .instrument(span)
    .await;
}

/// Drain the ghost's frame channel through a jitter buffer into its LiveKit
/// source as 10 ms mono frames. Mirrors the bridge bot's former audio consumer,
/// one per ghost. Returns when cancelled or the Discord side drops the sender.
async fn pump_audio(
    mut frame_rx: mpsc::Receiver<Vec<i16>>,
    source: &NativeAudioSource,
    track: &LocalAudioTrack,
    mut mute_rx: watch::Receiver<bool>,
    cancel: &CancellationToken,
) {
    // Mirror the Discord speaker's mute state onto the LiveKit track. `mute()`/
    // `unmute()` signal the server so Element Call peers see the mic-off indicator
    // (constraint: a muted Discord user must show muted in EC). Apply the current
    // state up front — covers a user who joined already muted — then track changes.
    apply_track_mute(track, *mute_rx.borrow_and_update());
    let mut jitter = JitterBuffer::new(JITTER_TARGET_MS, JITTER_MAX_MS, SAMPLE_RATE);
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            res = mute_rx.changed() => {
                // Sender drops only when the ghost retires (which also cancels,
                // handled by the biased arm above); ignore Err and re-check.
                if res.is_ok() {
                    let muted = *mute_rx.borrow_and_update();
                    apply_track_mute(track, muted);
                    tracing::info!(muted, "ghost track mute mirrored from Discord");
                }
            }
            frame = frame_rx.recv() => {
                let Some(frame20) = frame else { break };
                jitter.push(&frame20);
                while let Some(f10) = jitter.pop_10ms() {
                    let af = AudioFrame {
                        data: Cow::Owned(f10),
                        sample_rate: SAMPLE_RATE,
                        num_channels: 1,
                        samples_per_channel: SAMPLES_10MS,
                    };
                    if let Err(e) = source.capture_frame(&af).await {
                        tracing::warn!(error = %e, "ghost capture_frame failed — yielding");
                        tokio::task::yield_now().await;
                    }
                }
            }
        }
    }
}

/// Set the ghost track's mute state to `muted`. `LocalAudioTrack::mute`/`unmute`
/// are idempotent and cheap, so re-applying the same state (e.g. the up-front
/// apply) is harmless. The caller logs on an actual toggle.
fn apply_track_mute(track: &LocalAudioTrack, muted: bool) {
    if muted {
        track.mute();
    } else {
        track.unmute();
    }
}

/// Connect the ghost to LiveKit, publish its audio track, and spawn the
/// room-event drain. Returns the source to feed and, when media E2EE is on, the
/// 16-byte media key this ghost encrypts its frames under (to be distributed to
/// the call's humans by [`distribute_key_loop`]). The `Room` is owned by the
/// drain task (kept up until `cancel`, then closed).
async fn connect_and_publish(
    cfg: &GhostConfig,
    focus: &LivekitFocusInfo,
    ghost_mxid: &str,
    device_id: &str,
    cancel: CancellationToken,
) -> Result<(NativeAudioSource, LocalAudioTrack, Option<[u8; 16]>)> {
    let openid_token = fetch_openid_token(cfg, ghost_mxid).await?;
    let jwt_resp = lk_jwt::fetch_livekit_token(
        &cfg.http,
        cfg.lk_jwt_url.as_deref().unwrap_or(&focus.service_url),
        &cfg.room_id,
        device_id,
        &openid_token,
    )
    .await?;

    let (identity, _room) = decode_jwt_claims(&jwt_resp.jwt)?;
    tracing::info!(identity = %identity, "ghost LiveKit JWT obtained");

    // A ghost only publishes; disable auto-subscribe so it doesn't pull every
    // other participant's audio (the bridge bot is the sole subscriber). Without
    // this, N ghosts would each receive the whole call — needless bandwidth.
    let mut room_opts = RoomOptions::default();
    room_opts.auto_subscribe = false;

    // E2EE: configure HKDF/GCM frame encryption with the SAME key-provider options
    // the subscriber uses (`rtc::session`), generate a per-ghost media key, and set
    // it under this ghost's own LiveKit identity so its outbound frames encrypt
    // under it. Element Call peers install the same key (under the same identity)
    // from the to-device event `distribute_key_loop` sends → they can decrypt us.
    let media_key = if cfg.media_e2ee {
        let key_provider = KeyProvider::new(KeyProviderOptions {
            ratchet_window_size: 16,
            ratchet_salt: b"LKFrameEncryptionKey".to_vec(),
            failure_tolerance: -1,
            key_ring_size: 256,
            key_derivation_algorithm: KeyDerivationAlgorithm::HKDF,
        });
        let key = keys::generate_key();
        // The to-device `member.claimed_device_id` (= `device_id`) makes peers
        // reconstruct exactly this identity (`{mxid}:{device_id}`); they must agree
        // or peers install the key under the wrong identity and hear noise.
        if !key_provider.set_key(&ParticipantIdentity::from(identity.clone()), 0, key.to_vec()) {
            return Err(anyhow!("failed to set ghost media key on key provider"));
        }
        room_opts.encryption = Some(E2eeOptions {
            encryption_type: EncryptionType::Gcm,
            key_provider,
        });
        Some(key)
    } else {
        None
    };

    let (room, events) = Room::connect(
        cfg.lk_url.as_deref().unwrap_or(&jwt_resp.url),
        &jwt_resp.jwt,
        room_opts,
    )
    .await
    .context("ghost LiveKit Room::connect")?;
    if media_key.is_some() {
        room.e2ee_manager().set_enabled(true);
        tracing::info!(identity = %identity, "ghost E2EE enabled — publishing encrypted frames");
    }
    let (source, track) = publish::publish(&room).await?;

    // Move the room + events into a drain task: it keeps the connection alive,
    // logs events so the unbounded channel never fills, and closes the room when
    // cancelled (ghost retired).
    tokio::spawn(async move {
        let mut events = events;
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                ev = events.recv() => match ev {
                    Some(livekit::RoomEvent::Disconnected { reason }) => {
                        tracing::warn!(reason = ?reason, "ghost LiveKit room disconnected");
                        break;
                    }
                    Some(other) => tracing::trace!(event = ?other, "ghost LiveKit room event"),
                    None => break,
                },
            }
        }
        let _ = room.close().await;
        tracing::debug!("ghost LiveKit room closed");
    });

    Ok((source, track, media_key))
}

// ─── E2EE media-key distribution (Path 2 plaintext / Path 3 Olm) ────────────────

/// Number of key re-sends in the burst that follows a membership change.
const KEY_BURST_COUNT: u32 = 6;
/// Spacing between burst re-sends (~`COUNT * INTERVAL` = the covered window).
const KEY_BURST_INTERVAL: Duration = Duration::from_secs(3);
/// Steady-state re-send period once a burst is done — a slow safety net for a key
/// dropped even by the burst. Mild (≈1/ghost/min) vs. the old 5s spam.
const KEY_HEARTBEAT: Duration = Duration::from_secs(60);

/// Olm crypto-client bootstrap attempts before giving up (no plaintext downgrade —
/// see [`distribute_key_task`]).
const OLM_BOOTSTRAP_ATTEMPTS: u32 = 3;
/// Backoff between Olm bootstrap attempts.
const OLM_BOOTSTRAP_RETRY: Duration = Duration::from_secs(5);

/// Build the configured key transport, then run [`distribute_key_loop`]. Spawned so
/// the (potentially slow) Olm crypto-client bootstrap never blocks the ghost's audio
/// pump.
///
/// **No silent downgrade.** When `key_transport = "olm"` and bootstrap ultimately
/// fails, we give up *without* falling back to plaintext: in an encrypted room a
/// plaintext key send would leak the frame key to the homeserver, defeating the
/// whole point of Path 3. The operator chooses plaintext explicitly via config; the
/// bridge never chooses it for them at runtime.
#[allow(clippy::too_many_arguments)]
async fn distribute_key_task(
    client: Client,
    cfg: GhostConfig,
    localpart: String,
    ghost_mxid: String,
    device_id: String,
    key: [u8; 16],
    members_gen: watch::Receiver<u64>,
    cancel: CancellationToken,
) {
    let transport: Arc<dyn KeyTransport> = match cfg.key_transport {
        KeyTransportMode::Plaintext => Arc::new(PlaintextTransport::new(
            cfg.http.clone(),
            cfg.homeserver_url.clone(),
            cfg.as_token.clone(),
            cfg.room_id.clone(),
            ghost_mxid.clone(),
            device_id.clone(),
        )),
        KeyTransportMode::Olm => {
            match bootstrap_olm_transport(&cfg, &localpart, &ghost_mxid, &device_id, &cancel).await {
                Some(t) => t,
                None => return, // bootstrap failed or cancelled; already logged.
            }
        }
    };
    let source: Arc<dyn RecipientSource> = Arc::new(CallRecipients { client, cfg });
    distribute_key_loop(source, key, transport, members_gen, cancel).await;
}

/// Source of the current key-send recipients (the call's human members). Abstracted
/// from [`distribute_key_loop`] so the loop's burst/heartbeat policy is unit-testable
/// without a live homeserver (tests supply a scripted source).
#[async_trait]
trait RecipientSource: Send + Sync {
    async fn recipients(&self) -> Result<Vec<OwnedUserId>>;
}

/// Production source: reads call memberships via the bridge bot's joined client.
struct CallRecipients {
    client: Client,
    cfg: GhostConfig,
}

#[async_trait]
impl RecipientSource for CallRecipients {
    async fn recipients(&self) -> Result<Vec<OwnedUserId>> {
        enumerate_key_recipients(&self.client, &self.cfg).await
    }
}

/// Bootstrap an [`OlmTransport`] with bounded retries. Returns `None` if cancelled
/// or if every attempt fails (caller must NOT downgrade to plaintext).
async fn bootstrap_olm_transport(
    cfg: &GhostConfig,
    localpart: &str,
    ghost_mxid: &str,
    device_id: &str,
    cancel: &CancellationToken,
) -> Option<Arc<dyn KeyTransport>> {
    for attempt in 1..=OLM_BOOTSTRAP_ATTEMPTS {
        if cancel.is_cancelled() {
            return None;
        }
        match build_ghost_crypto_client(cfg, localpart, ghost_mxid, device_id, cancel).await {
            Ok(crypto_client) => {
                tracing::info!("ghost Olm key transport ready (per-ghost crypto client)");
                return Some(Arc::new(OlmTransport::new(
                    crypto_client,
                    cfg.room_id.clone(),
                    device_id.to_owned(),
                )));
            }
            Err(e) => {
                tracing::warn!(attempt, error = %e, "ghost Olm transport bootstrap failed; retrying");
                tokio::select! {
                    _ = cancel.cancelled() => return None,
                    _ = tokio::time::sleep(OLM_BOOTSTRAP_RETRY) => {}
                }
            }
        }
    }
    tracing::error!(
        attempts = OLM_BOOTSTRAP_ATTEMPTS,
        "ghost Olm key transport bootstrap failed — media key NOT distributed (refusing to \
         downgrade to plaintext in an encrypted room); this ghost is inaudible in Element Call \
         until it reconnects"
    );
    None
}

/// Make a ghost a crypto-capable client: appservice-login it (asserting its stable
/// `device_id`), open a persistent SQLite crypto store under
/// `<store_path>/ghosts/<localpart>`, restore the session, sync once to upload its
/// device keys + one-time keys, and spawn a background sync (cancelled with the
/// ghost) so its device lists stay fresh for `encrypt_and_send_raw_to_device`.
/// Mirrors the bridge bot's `build_client`; we add no crypto of our own. The store
/// persists across rejoins so Olm sessions + uploaded keys are reused.
async fn build_ghost_crypto_client(
    cfg: &GhostConfig,
    localpart: &str,
    ghost_mxid: &str,
    device_id: &str,
    cancel: &CancellationToken,
) -> Result<Client> {
    let store_dir = format!("{}/ghosts/{}", cfg.store_path.trim_end_matches('/'), localpart);
    tokio::fs::create_dir_all(&store_dir)
        .await
        .with_context(|| format!("creating ghost crypto store dir '{store_dir}'"))?;

    let access_token =
        crate::matrix::appservice_login_as(&cfg.homeserver_url, &cfg.as_token, localpart, device_id)
            .await
            .context("ghost appservice login")?;

    let client = Client::builder()
        .homeserver_url(&cfg.homeserver_url)
        .sqlite_store(&store_dir, None)
        .build()
        .await
        .context("building ghost crypto client")?;

    let user_id: OwnedUserId = ghost_mxid
        .parse()
        .with_context(|| format!("parsing ghost mxid '{ghost_mxid}'"))?;
    let session = MatrixSession {
        meta: SessionMeta {
            user_id,
            device_id: device_id.into(),
        },
        tokens: SessionTokens {
            access_token,
            refresh_token: None,
        },
    };
    client
        .restore_session(session)
        .await
        .context("restoring ghost crypto session")?;

    // First sync uploads this ghost's device keys + one-time keys so peers can
    // Olm-encrypt to it and it can claim theirs. Bounded so a wedged homeserver
    // can't hang key distribution forever.
    match tokio::time::timeout(
        Duration::from_secs(60),
        client.sync_once(SyncSettings::default()),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => return Err(anyhow!("ghost initial sync failed: {e}")),
        Err(_) => return Err(anyhow!("ghost initial sync timed out after 60s")),
    }

    // Perpetual sync for ongoing device-list tracking; dies with the ghost.
    let sync_client = client.clone();
    let sync_cancel = cancel.clone();
    tokio::spawn(
        async move {
            tokio::select! {
                _ = sync_cancel.cancelled() => {}
                r = sync_client.sync(SyncSettings::default()) => {
                    if let Err(e) = r {
                        tracing::warn!(error = %e, "ghost crypto sync loop ended");
                    }
                }
            }
        }
        .instrument(tracing::Span::current()),
    );

    tracing::info!(device_id, store = %store_dir, "ghost crypto client ready (device keys + OTKs uploaded)");
    Ok(client)
}

/// Distribute this ghost's media key to the call's humans until cancelled, over the
/// given [`KeyTransport`].
///
/// **Burst-on-change + slow heartbeat.** A one-shot send is fragile: a recipient
/// whose Element Call client is still completing call setup drops the to-device key
/// and (with naive "send only to new recipients") never gets re-keyed — so a Matrix
/// listener can't hear ghosts that were already live when it joined. Instead, each
/// membership change (a new human, or this ghost going live with humans present)
/// triggers a short **burst** of re-sends to ALL current humans, covering that
/// setup window; between bursts a slow **heartbeat** re-send is a backstop. The
/// `members_gen` watch (bumped on each join) wakes the loop immediately, so a new
/// joiner is keyed without waiting for the heartbeat, while a stable call only
/// re-sends about once a minute. The transport is opaque here: plaintext (Path 2)
/// and Olm (Path 3) share this exact retry policy.
async fn distribute_key_loop(
    source: Arc<dyn RecipientSource>,
    key: [u8; 16],
    transport: Arc<dyn KeyTransport>,
    mut members_gen: watch::Receiver<u64>,
    cancel: CancellationToken,
) {
    // Recipients keyed so far — used only to detect a NEW human (which restarts a
    // burst); the sends themselves are not gated by it (we re-send to all current).
    let mut keyed: HashSet<OwnedUserId> = HashSet::new();
    // Burst re-sends still owed after the last membership change. >0 ⇒ wake fast.
    let mut burst: u32 = 0;
    loop {
        match source.recipients().await {
            Ok(current) => {
                // A newly-seen recipient (re)starts the burst to cover its client's
                // call-setup window, during which it may drop the to-device key.
                if current.iter().any(|u| !keyed.contains(u)) {
                    burst = KEY_BURST_COUNT;
                }
                if current.is_empty() {
                    keyed.clear();
                } else {
                    match transport.send_key(&current, &key, 0).await {
                        // Every targeted device got the key.
                        Ok(o) if o.failed.is_empty() => tracing::info!(
                            recipients = ?current.iter().map(|u| u.as_str()).collect::<Vec<_>>(),
                            transport = transport.kind(),
                            delivered = o.delivered,
                            burst,
                            "ghost media key sent"
                        ),
                        // Partial success — the key reached the live device(s); the
                        // unreachable ones are almost always stale/offline sessions
                        // with no one-time keys (can't be fixed from the bridge). This
                        // is the normal, healthy case, so it's INFO not WARN. The
                        // per-device id list is DEBUG to keep the default log quiet;
                        // burst/heartbeat keeps retrying in case any are transient.
                        Ok(o) if o.delivered > 0 => {
                            tracing::info!(
                                transport = transport.kind(),
                                delivered = o.delivered,
                                unreachable = o.failed.len(),
                                burst,
                                "ghost media key delivered (some recipient devices unreachable — likely stale/offline sessions)"
                            );
                            tracing::debug!(
                                unreachable_devices = ?o.failed.iter().map(|f| format!("{}:{}", f.user_id, f.device_id)).collect::<Vec<_>>(),
                                "ghost media key: unreachable devices"
                            );
                        }
                        // Reached NO devices — the real failure (e.g. the live device
                        // is out of one-time keys, or the ghost's sync hasn't tracked
                        // anyone yet). Burst/heartbeat retries; surfaced loudly.
                        Ok(o) => tracing::warn!(
                            transport = transport.kind(),
                            unreachable = o.failed.len(),
                            burst,
                            "ghost media key reached NO recipient devices — nobody can decrypt this ghost (will retry)"
                        ),
                        Err(e) => tracing::warn!(
                            error = %e,
                            transport = transport.kind(),
                            "ghost media-key send failed"
                        ),
                    }
                    keyed = current.into_iter().collect();
                    burst = burst.saturating_sub(1);
                }
            }
            Err(e) => tracing::warn!(error = %e, "ghost media key: recipient enumeration failed"),
        }
        let wait = if burst > 0 { KEY_BURST_INTERVAL } else { KEY_HEARTBEAT };
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            // A human joined (generation bumped) → re-evaluate and burst-key them.
            res = members_gen.changed() => {
                if res.is_err() {
                    // Sender dropped (shutdown). cancel will fire imminently; wait
                    // for it rather than busy-looping on a closed channel.
                    cancel.cancelled().await;
                    break;
                }
            }
            _ = tokio::time::sleep(wait) => {}
        }
    }
    tracing::debug!("ghost media-key distribution stopped");
}

/// Enumerate the call's human members (key-send recipients), read via the bridge
/// bot's joined client. Excludes our ghosts ([`is_ghost_identity`], which also
/// drops *this* ghost) and the bridge bot itself (the subscriber skips ghost
/// tracks, so it never needs ghost keys — constraint #5). Deduplicated by MXID.
async fn enumerate_key_recipients(client: &Client, cfg: &GhostConfig) -> Result<Vec<OwnedUserId>> {
    let room_id = RoomId::parse(&cfg.room_id)
        .with_context(|| format!("parsing room_id '{}'", cfg.room_id))?;
    let Some(room) = client.get_room(&room_id) else {
        return Ok(Vec::new());
    };
    let raws = room
        .get_state_events_static::<CallMemberEventContent>()
        .await
        .context("reading call memberships for key distribution")?;

    let mut recipients: Vec<OwnedUserId> = Vec::new();
    for raw in raws {
        let Ok(ev) = raw.deserialize() else { continue };
        let orig = match ev {
            SyncOrStrippedState::Sync(SyncStateEvent::Original(o)) => o,
            _ => continue,
        };
        // Skip retracted/expired memberships.
        if orig.content.active_memberships(Some(orig.origin_server_ts)).is_empty() {
            continue;
        }
        let sender = orig.sender;
        if is_bridge_excluded_identity(sender.as_str()) || sender.as_str() == cfg.bot_user_id {
            continue;
        }
        if !recipients.contains(&sender) {
            recipients.push(sender);
        }
    }
    Ok(recipients)
}


// ─── Raw client-server API helpers (appservice masquerade) ──────────────────────

/// Build a Matrix HTTP API URL from full path segments, percent-encoding each,
/// with an optional `?user_id=` masquerade query.
fn matrix_url(homeserver: &str, segments: &[&str], masquerade: Option<&str>) -> Result<Url> {
    let mut url = Url::parse(homeserver).with_context(|| format!("parsing homeserver '{homeserver}'"))?;
    {
        let mut path = url
            .path_segments_mut()
            .map_err(|_| anyhow!("homeserver URL cannot be a base: {homeserver}"))?;
        path.extend(segments);
    }
    if let Some(user_id) = masquerade {
        url.query_pairs_mut().append_pair("user_id", user_id);
    }
    Ok(url)
}

/// Build a client-server API URL under `/_matrix/client/v3` (the common case).
/// `pub(crate)` so the plaintext key transport (`rtc::key_transport`) can reuse it.
pub(crate) fn cs_url(homeserver: &str, segments: &[&str], masquerade: Option<&str>) -> Result<Url> {
    let mut full = vec!["_matrix", "client", "v3"];
    full.extend_from_slice(segments);
    matrix_url(homeserver, &full, masquerade)
}

/// Read an error response body for logging (bearer tokens are never in bodies).
/// `pub(crate)` so the plaintext key transport (`rtc::key_transport`) can reuse it.
pub(crate) async fn err_body(resp: reqwest::Response) -> String {
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    format!("{status}: {text}")
}

/// Register the ghost as an appservice user (idempotent). Tolerates an existing
/// user (`M_USER_IN_USE`); all later calls masquerade with the `as_token`, so we
/// don't need the registration's access token.
async fn register_ghost(cfg: &GhostConfig, localpart: &str) -> Result<()> {
    let url = cs_url(&cfg.homeserver_url, &["register"], None)?;
    let resp = cfg
        .http
        .post(url)
        .bearer_auth(&cfg.as_token)
        .json(&serde_json::json!({
            "type": "m.login.application_service",
            "username": localpart,
        }))
        .send()
        .await
        .context("POST /register")?;
    if resp.status().is_success() {
        tracing::info!(localpart, "ghost registered");
        return Ok(());
    }
    // M_USER_IN_USE (the ghost already exists) is the expected steady state.
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if text.contains("M_USER_IN_USE") {
        tracing::debug!(localpart, "ghost already registered");
        return Ok(());
    }
    Err(anyhow!("POST /register failed: {status}: {text}"))
}

/// Set the ghost's display name (masqueraded).
async fn set_displayname(cfg: &GhostConfig, ghost_mxid: &str, name: &str) -> Result<()> {
    let url = cs_url(
        &cfg.homeserver_url,
        &["profile", ghost_mxid, "displayname"],
        Some(ghost_mxid),
    )?;
    let resp = cfg
        .http
        .put(url)
        .bearer_auth(&cfg.as_token)
        .json(&serde_json::json!({ "displayname": name }))
        .send()
        .await
        .context("PUT /profile/displayname")?;
    if !resp.status().is_success() {
        return Err(anyhow!("set displayname failed: {}", err_body(resp).await));
    }
    Ok(())
}

/// Mirror a Discord avatar onto the ghost: download it from Discord's CDN, upload
/// it to the homeserver media repo (masqueraded), and set the ghost's avatar_url.
async fn set_avatar(cfg: &GhostConfig, ghost_mxid: &str, discord_avatar_url: &str) -> Result<()> {
    // 1. Download from the Discord CDN (public, no auth).
    let resp = cfg
        .http
        .get(discord_avatar_url)
        .send()
        .await
        .context("GET discord avatar")?;
    if !resp.status().is_success() {
        return Err(anyhow!("discord avatar fetch returned {}", resp.status()));
    }
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("image/png")
        .to_owned();
    let bytes = resp.bytes().await.context("reading discord avatar bytes")?;

    // 2. Upload to the Matrix media repo as the ghost → mxc:// URI.
    let mxc = upload_media(cfg, ghost_mxid, &content_type, bytes.to_vec()).await?;

    // 3. Point the ghost's profile at it.
    let url = cs_url(
        &cfg.homeserver_url,
        &["profile", ghost_mxid, "avatar_url"],
        Some(ghost_mxid),
    )?;
    let resp = cfg
        .http
        .put(url)
        .bearer_auth(&cfg.as_token)
        .json(&serde_json::json!({ "avatar_url": mxc }))
        .send()
        .await
        .context("PUT /profile/avatar_url")?;
    if !resp.status().is_success() {
        return Err(anyhow!("set avatar_url failed: {}", err_body(resp).await));
    }
    tracing::info!(mxc = %mxc, "ghost avatar set");
    Ok(())
}

/// Upload bytes to the homeserver media repo masqueraded as the ghost; returns
/// the `mxc://` content URI.
async fn upload_media(
    cfg: &GhostConfig,
    ghost_mxid: &str,
    content_type: &str,
    bytes: Vec<u8>,
) -> Result<String> {
    let url = matrix_url(
        &cfg.homeserver_url,
        &["_matrix", "media", "v3", "upload"],
        Some(ghost_mxid),
    )?;
    let resp = cfg
        .http
        .post(url)
        .bearer_auth(&cfg.as_token)
        .header(reqwest::header::CONTENT_TYPE, content_type)
        .body(bytes)
        .send()
        .await
        .context("POST /_matrix/media/v3/upload")?;
    if !resp.status().is_success() {
        return Err(anyhow!("media upload failed: {}", err_body(resp).await));
    }
    let v: serde_json::Value = resp.json().await.context("parsing media upload response")?;
    v.get("content_uri")
        .and_then(|x| x.as_str())
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("media upload response missing content_uri"))
}

/// Invite the ghost to the call room **as the bridge bot**, via its matrix-sdk
/// `Client` (NOT the `as_token` masquerade — that only works for the appservice's
/// own namespace users, so it fails `M_EXCLUSIVE` for a regular-account bot like an
/// unencrypted bridge's). Required for **restricted** rooms (MSC3083): a ghost's
/// bare `/join` is refused there unless invited. Idempotent — skips when the ghost
/// is already joined/invited.
async fn invite_ghost(client: &Client, room_id: &str, ghost_mxid: &str) -> Result<()> {
    let rid = RoomId::parse(room_id).with_context(|| format!("parsing room_id '{room_id}'"))?;
    let Some(room) = client.get_room(&rid) else {
        return Err(anyhow!("bridge bot is not in room {room_id} — cannot invite ghost"));
    };
    let ghost: OwnedUserId =
        UserId::parse(ghost_mxid).with_context(|| format!("parsing ghost mxid '{ghost_mxid}'"))?;
    // Already joined/invited → nothing to do (avoids needless invites + log noise).
    if let Ok(Some(member)) = room.get_member_no_sync(&ghost).await {
        use matrix_sdk::ruma::events::room::member::MembershipState;
        if matches!(member.membership(), MembershipState::Join | MembershipState::Invite) {
            return Ok(());
        }
    }
    room.invite_user_by_id(&ghost).await.context("inviting ghost to room")?;
    tracing::debug!(ghost = %ghost_mxid, "ghost invited to room");
    Ok(())
}

/// Join the call room as the ghost (masqueraded). Idempotent on the server.
async fn join_room(cfg: &GhostConfig, ghost_mxid: &str) -> Result<()> {
    let url = cs_url(&cfg.homeserver_url, &["join", &cfg.room_id], Some(ghost_mxid))?;
    let resp = cfg
        .http
        .post(url)
        .bearer_auth(&cfg.as_token)
        .json(&serde_json::json!({}))
        .send()
        .await
        .context("POST /join")?;
    if !resp.status().is_success() {
        return Err(anyhow!("join room failed: {}", err_body(resp).await));
    }
    Ok(())
}

/// Fetch an OpenID token for the ghost (masqueraded). This is the call lk-jwt
/// validates by federation — the key live risk (architecture open-Q #3); the
/// status body is surfaced on failure so a rejection is diagnosable.
async fn fetch_openid_token(cfg: &GhostConfig, ghost_mxid: &str) -> Result<OpenIdToken> {
    let url = cs_url(
        &cfg.homeserver_url,
        &["user", ghost_mxid, "openid", "request_token"],
        Some(ghost_mxid),
    )?;
    let resp = cfg
        .http
        .post(url)
        .bearer_auth(&cfg.as_token)
        .json(&serde_json::json!({}))
        .send()
        .await
        .context("POST /openid/request_token")?;
    if !resp.status().is_success() {
        return Err(anyhow!("ghost openid request failed: {}", err_body(resp).await));
    }
    let token: OpenIdToken = resp
        .json()
        .await
        .context("parsing ghost openid response")?;
    Ok(token)
}

/// Post the ghost's `m.call.member` join, retrying with backoff until it lands or
/// `cancel` fires. Self-heals a transient failure (e.g. a room whose
/// `org.matrix.msc3401.call.member` power level is wrong, then fixed) without the
/// Discord user having to rejoin. Bounded in practice by the ghost's lifetime.
async fn post_membership_until_joined(
    cfg: GhostConfig,
    ghost_mxid: String,
    device_id: String,
    focus: LivekitFocusInfo,
    cancel: CancellationToken,
) {
    let mut backoff = Duration::from_secs(2);
    let max = Duration::from_secs(30);
    loop {
        match post_membership(&cfg, &ghost_mxid, &device_id, &focus, false).await {
            Ok(()) => {
                tracing::info!("ghost call membership posted — visible in call");
                return;
            }
            Err(e) => tracing::warn!(
                error = %e,
                retry_in_s = backoff.as_secs(),
                "ghost post_membership failed — retrying (room may need m.call.member at PL 0)"
            ),
        }
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(max);
    }
}

/// Post (or retract, when `leave`) the ghost's `m.call.member` state event,
/// masqueraded. Reuses ruma's `CallMemberEventContent` serialization so the wire
/// shape exactly matches the bridge bot's membership (`rtc::membership`).
async fn post_membership(
    cfg: &GhostConfig,
    ghost_mxid: &str,
    device_id: &str,
    focus: &LivekitFocusInfo,
    leave: bool,
) -> Result<()> {
    let user_id: OwnedUserId = UserId::parse(ghost_mxid)
        .with_context(|| format!("parsing ghost mxid '{ghost_mxid}'"))?;
    let device: OwnedDeviceId = device_id.into();
    let member_id = format!("{device_id}_m.call");
    let state_key = CallMemberStateKey::new(user_id, Some(member_id), true);

    let content = if leave {
        CallMemberEventContent::new_empty(None)
    } else {
        build_membership(&device, focus)
    };
    put_call_member_state(cfg, ghost_mxid, state_key.as_ref(), &content).await
}

/// PUT a ghost's `m.call.member` state event (masqueraded as the ghost). The
/// low-level primitive shared by [`post_membership`] (which builds a fresh
/// state_key) and [`retract_membership_raw`] (which reuses an observed one).
async fn put_call_member_state(
    cfg: &GhostConfig,
    ghost_mxid: &str,
    state_key: &str,
    content: &CallMemberEventContent,
) -> Result<()> {
    let body = serde_json::to_value(content).context("serializing m.call.member content")?;
    let url = cs_url(
        &cfg.homeserver_url,
        &["rooms", &cfg.room_id, "state", CALL_MEMBER_EVENT_TYPE, state_key],
        Some(ghost_mxid),
    )?;
    let resp = cfg
        .http
        .put(url)
        .bearer_auth(&cfg.as_token)
        .json(&body)
        .send()
        .await
        .context("PUT /rooms/.../state/m.call.member")?;
    if !resp.status().is_success() {
        return Err(anyhow!("put m.call.member failed: {}", err_body(resp).await));
    }
    Ok(())
}

/// Retract a ghost membership at an **observed** state_key (from room state),
/// masqueraded as the ghost. Used by the startup sweep: it clears the exact
/// leftover key a crashed run wrote, even if our key-derivation has since
/// changed (it does not rebuild `D{key}`).
async fn retract_membership_raw(
    cfg: &GhostConfig,
    ghost_mxid: &str,
    state_key: &str,
) -> Result<()> {
    let content = CallMemberEventContent::new_empty(None);
    put_call_member_state(cfg, ghost_mxid, state_key, &content).await
}

/// Build the ghost's `m.call.member` join content — mirrors `rtc::membership::join`.
fn build_membership(device_id: &OwnedDeviceId, focus: &LivekitFocusInfo) -> CallMemberEventContent {
    let foci_preferred = vec![Focus::Livekit(LivekitFocus::new(
        focus.alias.clone(),
        focus.service_url.clone(),
    ))];
    CallMemberEventContent::new(
        Application::Call(CallApplicationContent::new(String::new(), CallScope::Room)),
        device_id.clone(),
        ActiveFocus::Livekit(ActiveLivekitFocus::new()),
        foci_preferred,
        None,
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ghost_identity_detection() {
        assert!(is_ghost_identity("@nether-voicebridge_12345:nether.im"));
        assert!(is_ghost_identity("@nether-voicebridge_12345:nether.im:DEVICE"));
        assert!(!is_ghost_identity("@dark:nether.im"));
        assert!(!is_ghost_identity("@dark:nether.im:DEVICE"));
        // mautrix-discord's namespace must NOT be detected as a *current* ghost.
        assert!(!is_ghost_identity("@discord_12345:nether.im"));
        assert!(!is_ghost_identity("not-an-mxid"));
    }

    #[test]
    fn legacy_ghost_identity_detection() {
        // Our pre-rename ghosts were `@discord_<numeric-discord-id>`.
        assert!(is_legacy_ghost_identity("@discord_996161727068643380:nether.im"));
        assert!(is_legacy_ghost_identity("@discord_12345:nether.im:DEVICE"));
        // A human/non-numeric localpart must NOT be caught.
        assert!(!is_legacy_ghost_identity("@discord_alice:nether.im"));
        assert!(!is_legacy_ghost_identity("@discord_:nether.im"));
        assert!(!is_legacy_ghost_identity("@dark:nether.im"));
        // Current-prefix ghosts are not "legacy".
        assert!(!is_legacy_ghost_identity("@nether-voicebridge_123:nether.im"));

        // The combined exclusion helper covers both current and legacy ghosts.
        assert!(is_bridge_excluded_identity("@nether-voicebridge_123:nether.im"));
        assert!(is_bridge_excluded_identity("@discord_996161727068643380:nether.im"));
        assert!(!is_bridge_excluded_identity("@dark:nether.im"));
    }

    #[test]
    fn cs_url_encodes_segments_and_masquerade() {
        let url = cs_url(
            "https://nether.im",
            &["profile", "@nether-voicebridge_1:nether.im", "displayname"],
            Some("@nether-voicebridge_1:nether.im"),
        )
        .unwrap();
        let s = url.as_str();
        assert!(s.starts_with("https://nether.im/_matrix/client/v3/profile/"));
        // Path segment is percent-encoded (@ → %40, : → %3A; - and _ stay literal).
        assert!(s.contains("%40nether-voicebridge_1%3Anether.im"));
        // Masquerade query is present and encoded.
        assert!(s.contains("user_id=%40nether-voicebridge_1%3Anether.im"));
    }

    #[test]
    fn sendtodevice_url_targets_encryption_keys() {
        let url = cs_url(
            "https://nether.im",
            &["sendToDevice", "io.element.call.encryption_keys", "D42-key-3"],
            Some("@nether-voicebridge_42:nether.im"),
        )
        .unwrap();
        let s = url.as_str();
        assert!(s.contains("/_matrix/client/v3/sendToDevice/io.element.call.encryption_keys/D42-key-3"));
        assert!(s.contains("user_id=%40nether-voicebridge_42%3Anether.im"));
    }

    #[test]
    fn key_content_matches_ec_wire_shape() {
        // The to-device content must carry the exact fields Element Call reads to
        // map a key to the ghost's LiveKit identity. A regression here = peers
        // install the key under the wrong identity → noise (constraint #3-adjacent).
        let content = keys::build_key_content("!room:nether.im", "D42", "AAAA".to_owned(), 0, 1234);
        let v = serde_json::to_value(&content).unwrap();
        assert_eq!(v["keys"]["index"], 0);
        assert_eq!(v["keys"]["key"], "AAAA");
        assert_eq!(v["room_id"], "!room:nether.im");
        assert_eq!(v["member"]["claimed_device_id"], "D42");
        assert_eq!(v["member"]["id"], "D42_m.call");
        assert_eq!(v["session"]["application"], "m.call");
        assert_eq!(v["session"]["scope"], "m.room");
        assert_eq!(v["sent_ts"], 1234);
    }

    // ── distribute_key_loop, driven with mock transport + recipient source ──────

    use super::super::key_transport::KeySendOutcome;

    /// Records the recipient lists passed to each `send_key`.
    #[derive(Default)]
    struct MockTransport {
        sends: StdMutex<Vec<Vec<OwnedUserId>>>,
    }

    #[async_trait]
    impl KeyTransport for MockTransport {
        async fn send_key(
            &self,
            recipients: &[OwnedUserId],
            _key: &[u8; 16],
            _key_index: u8,
        ) -> Result<KeySendOutcome> {
            self.sends.lock().unwrap().push(recipients.to_vec());
            Ok(KeySendOutcome {
                delivered: recipients.len(),
                failed: Vec::new(),
            })
        }
        fn kind(&self) -> &'static str {
            "mock"
        }
    }

    /// Returns a fixed recipient list every poll.
    struct MockSource {
        users: Vec<OwnedUserId>,
    }

    #[async_trait]
    impl RecipientSource for MockSource {
        async fn recipients(&self) -> Result<Vec<OwnedUserId>> {
            Ok(self.users.clone())
        }
    }

    #[tokio::test]
    async fn key_loop_sends_to_current_recipients_then_stops_on_cancel() {
        let alice: OwnedUserId = "@alice:nether.im".parse().unwrap();
        let source = Arc::new(MockSource {
            users: vec![alice.clone()],
        });
        let transport = Arc::new(MockTransport::default());
        let (_gen_tx, gen_rx) = watch::channel(0u64);
        let cancel = CancellationToken::new();

        let handle = tokio::spawn(distribute_key_loop(
            source,
            [7u8; 16],
            transport.clone(),
            gen_rx,
            cancel.clone(),
        ));

        // The first iteration sends immediately, then waits the 3s burst interval
        // (a new recipient sets burst > 0). Cancel inside that window → exactly one
        // send, to the current recipient set.
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();
        handle.await.unwrap();

        let sends = transport.sends.lock().unwrap();
        assert_eq!(sends.len(), 1, "expected one send before cancel");
        assert_eq!(sends[0], vec![alice]);
    }

    #[tokio::test]
    async fn key_loop_skips_send_when_no_recipients() {
        let source = Arc::new(MockSource { users: vec![] });
        let transport = Arc::new(MockTransport::default());
        let (_gen_tx, gen_rx) = watch::channel(0u64);
        let cancel = CancellationToken::new();

        let handle = tokio::spawn(distribute_key_loop(
            source,
            [0u8; 16],
            transport.clone(),
            gen_rx,
            cancel.clone(),
        ));
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();
        handle.await.unwrap();

        assert!(
            transport.sends.lock().unwrap().is_empty(),
            "no recipients ⇒ no key send"
        );
    }

    #[tokio::test]
    async fn plaintext_transport_empty_recipients_is_noop() {
        // The empty guard must short-circuit before any network call.
        let t = PlaintextTransport::new(
            reqwest::Client::new(),
            "https://nether.im".into(),
            "as_tok".into(),
            "!r:nether.im".into(),
            "@nether-voicebridge_1:nether.im".into(),
            "D1".into(),
        );
        let outcome = t.send_key(&[], &[0u8; 16], 0).await.unwrap();
        assert!(outcome.failed.is_empty());
        assert_eq!(outcome.delivered, 0);
        assert_eq!(t.kind(), "plaintext");
    }
}
