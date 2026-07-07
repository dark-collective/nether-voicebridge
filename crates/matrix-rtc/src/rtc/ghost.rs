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
use std::sync::atomic::{AtomicU64, Ordering};
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
    deserialized_responses::SyncOrStrippedState,
    ruma::{
        events::{
            SyncStateEvent,
            call::member::{
                ActiveFocus, ActiveLivekitFocus, Application, CallApplicationContent,
                CallMemberEventContent, CallMemberStateKey, CallScope, Focus, LivekitFocus,
            },
        },
        MilliSecondsSinceUnixEpoch, OwnedDeviceId, OwnedUserId, RoomId, UserId,
    },
    Client,
};
use reqwest::Url;
use std::borrow::Cow;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use nvb_audio::jitter::JitterBuffer;
use nvb_audio::routing::{CaptureRouter, CaptureSink};
use nvb_audio::wav::WavMonoWriter;
use nvb_config::{KeyTransport as KeyTransportMode, LiveKitConfig, MatrixConfig};

use super::key_transport::{
    GhostCrypto, GhostCryptoCache, KeyRecipient, KeyTransport, OlmTransport, PlaintextTransport,
    RefreshMode,
};
use super::keys;
use super::lk_jwt;
use super::openid::OpenIdToken;
use super::publish;
use super::session::decode_jwt_claims;
use super::well_known::{self, LivekitFocusInfo};

/// Build the ghost localpart prefix (`{sender_localpart}_`) from the appservice
/// sender localpart. Ghost MXIDs are `@{sender_localpart}_{discord_user_id}:server`,
/// which matches the appservice namespace regex `@{sender_localpart}_.*`. Falls back
/// to `"nether-voicebridge_"` when `sender_localpart` is absent (defensive only —
/// the config validator requires it). Deliberately NOT `discord_` — that namespace is
/// owned by mautrix-discord, and sharing it on the same homeserver would collide.
pub(crate) fn ghost_localpart_prefix(sender_localpart: Option<&str>) -> String {
    sender_localpart.unwrap_or("nether-voicebridge").to_owned() + "_"
}

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
/// identity is `@{sender_localpart}_{id}:server[:device]`; the localpart is the
/// segment between `@` and the first `:`.
///
/// Matched as `{ghost_prefix}` followed by an **all-digits** suffix — ghost
/// localparts are always `{prefix}{discord_user_id}` and Discord user ids are
/// numeric snowflakes. The digit check matters because the bridge bot's own MXID
/// is `@{sender_localpart}_bot` (see `resolve_bot_user_ids`), which shares the
/// ghost prefix; without it the bot would be misclassified as a ghost and its own
/// `m.call.member` retracted by `sweep_stale_ghosts`. Mirrors
/// [`is_legacy_ghost_identity`].
pub(crate) fn is_ghost_identity(identity: &str, ghost_prefix: &str) -> bool {
    identity
        .strip_prefix('@')
        .and_then(|rest| rest.split(':').next())
        .and_then(|localpart| localpart.strip_prefix(ghost_prefix))
        .is_some_and(|suffix| !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()))
}

/// Prefixes this bridge minted ghosts under *before* the current
/// `ghost_localpart_prefix` (derived from config `sender_localpart`). `discord_`
/// collided with mautrix-discord, so we never owned it. `nether-voicebridge_` was
/// the previous hard-coded default (issue #37); a deployment that later sets a
/// different `sender_localpart` leaves `@nether-voicebridge_<id>` ghosts behind in
/// old room state. Stale entries under either prefix may linger and must still be
/// recognized for cleanup/exclusion.
const LEGACY_GHOST_PREFIXES: [&str; 2] = ["discord_", "nether-voicebridge_"];

/// Is this identity a *legacy* ghost (`@{legacy_prefix}<digits>`) we minted before
/// the current derived prefix? Matched only as a legacy prefix followed by an
/// all-digits localpart (the Discord-user-id form we used) so a human
/// `@discord_alice` / `@nether-voicebridge_alice` is never caught.
///
/// Used for **exclusion** ([`is_bridge_excluded_identity`]) and for the startup
/// `sweep_stale_ghosts` legacy branch (which retracts via a bot-write `Empty`,
/// with masquerade fallback). mautrix-discord's own `@discord_<id>` ghosts match
/// too, which is fine: they must never be counted as our call participants, and
/// they never post `m.call.member` anyway. When the *current* prefix is
/// `nether-voicebridge_` (the default fallback), the sweep checks
/// [`is_ghost_identity`] first, so those ghosts still take the masquerade path —
/// the legacy match here only fires once the current prefix has moved on.
pub(crate) fn is_legacy_ghost_identity(identity: &str) -> bool {
    let Some(localpart) = identity.strip_prefix('@').and_then(|rest| rest.split(':').next()) else {
        return false;
    };
    LEGACY_GHOST_PREFIXES.iter().any(|prefix| {
        localpart
            .strip_prefix(prefix)
            .is_some_and(|rest| !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit()))
    })
}

/// Should this identity be excluded from call-participant counting, key recipients,
/// and capture/forward? True for our current ghosts and for stale legacy ghosts.
/// (Constraint #5 echo-prevention plus the post-rename leftover cleanup.)
pub(crate) fn is_bridge_excluded_identity(identity: &str, ghost_prefix: &str) -> bool {
    is_ghost_identity(identity, ghost_prefix) || is_legacy_ghost_identity(identity)
}

/// A [`CaptureSink`] for one ghost: pushes a Discord speaker's 20 ms mono frames
/// onto the ghost's frame channel. The per-ghost lifecycle task drains the
/// channel through a jitter buffer into the ghost's LiveKit source.
struct GhostSink {
    frame_tx: mpsc::Sender<Vec<i16>>,
    /// Speaker key (for diagnostics attribution).
    key: String,
    /// Cumulative frames dropped because the channel was **full** (pump can't
    /// keep up). A suspect for issue #32's glitching; surfaced via a rate-limited
    /// warn here and counted into the pump's health line. Closed-channel drops
    /// (normal teardown) are not counted.
    dropped_full: AtomicU64,
}

impl CaptureSink for GhostSink {
    fn push(&self, mono: &[i16]) {
        // Never block on the capture hot path; drop on a full/closed channel.
        match self.frame_tx.try_send(mono.to_vec()) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                let n = self.dropped_full.fetch_add(1, Ordering::Relaxed) + 1;
                if n.is_multiple_of(50) {
                    tracing::warn!(
                        key = %self.key,
                        dropped = n,
                        "ghost frame channel full — dropping captured frames (pump stalled?)"
                    );
                }
            }
            // The receiver is gone (ghost retiring) — benign, don't count/log.
            Err(TrySendError::Closed(_)) => {}
        }
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
    /// Base URL for `.well-known/matrix/client` discovery. Equals
    /// `https://<matrix_domain>` when that field is set (Docker/proxy deployments
    /// where `homeserver_url` is an internal address), otherwise equals
    /// `homeserver_url`. See `MatrixConfig::well_known_base_url`.
    well_known_base_url: String,
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
    /// Per-store salt (resolved once via [`crate::matrix::resolve_ghost_device_salt`])
    /// folded into every ghost device id as `D{key}-{salt}`, making the otherwise
    /// machine-deterministic `D{key}` unique to this crypto store (the #30 fix,
    /// extended to ghosts). Empty ⇒ no store ⇒ ghosts fall back to bare `D{key}`.
    ghost_device_salt: String,
    /// Bridge crypto store base (`matrix.store_path`). Each ghost persists its stable
    /// crypto identity (device id + Olm account pickle) at `<store_path>/ghosts/
    /// <localpart>.json` so it reuses ONE device across restarts instead of minting a
    /// throwaway per run (#35). Empty ⇒ no store ⇒ fall back to an in-memory device.
    store_path: String,
    /// Optional config overrides, mirroring the subscriber path. `None` ⇒ derive
    /// the lk-jwt endpoint from the focus and the SFU ws URL from the lk-jwt
    /// response. Set only to override (non-Nether deploy / no `rtc_foci`).
    lk_jwt_url: Option<String>,
    lk_url: Option<String>,
    /// When true, each ghost writes "Tap B" of the issue-#32 bisection: a
    /// `publish_<key>.wav` of the exact mono PCM it feeds LiveKit. Mirrors the
    /// discord crate's `capture_wav` (Tap A); off in production.
    capture_wav: bool,
    /// Ghost MXID localpart prefix: `{sender_localpart}_`. Must match the
    /// appservice namespace regex. Derived once in `GhostPool::new` from
    /// `matrix.sender_localpart` so all ghosts use the same prefix.
    ghost_localpart_prefix: String,
    /// Delete ids each ghost's identity blob retired (#53, history-scoped).
    /// Mirrors `matrix.prune_stale_devices` (default on).
    prune_stale_devices: bool,
    /// Opportunistically rotate ghost outbound Olm sessions (#52) on membership
    /// changes + max-age. Mirrors `matrix.rotate_ghost_sessions` (default on);
    /// only effective on the Olm transport.
    rotate_ghost_sessions: bool,
    /// Admission cap on concurrent ghosts for this bridge (`[limits]
    /// max_ghosts_per_bridge`, #47). At the cap a NEW speaker is not bridged
    /// (WARN + null sink); a slot frees when a ghost is released. `None` ⇒
    /// unlimited. Bounds the super-linear Olm key-distribution cost per room.
    max_ghosts: Option<usize>,
}

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

/// Build a ghost's Matrix device id from its key and the per-store salt.
///
/// `D{key}` alone is identical on every machine; folding in the per-store salt
/// (`D{key}-{salt}`) makes it unique to this crypto store so peers never see one
/// device id carrying two different signing keys. An empty salt (no store) yields
/// the bare `D{key}` — unchanged legacy behavior.
fn ghost_device_id(key: &str, salt: &str) -> String {
    if salt.is_empty() {
        format!("D{key}")
    } else {
        format!("D{key}-{salt}")
    }
}

/// Per-ghost media key reused across respawns, mirroring [`GhostCryptoCache`]: a Discord
/// speaker leaving and rejoining voice respawns `run_ghost`, which must reuse the SAME
/// 16-byte key under the SAME LiveKit identity at index 0. Otherwise a listener already
/// holding the old key at `(identity, 0)` has to re-receive the new one, and any missed
/// delivery (a known EC reliability gap) leaves that publisher silent until the identity
/// changes. Keyed by ghost MXID; never evicted (fresh only on full process restart, which
/// tears down all calls anyway).
type GhostKeyCache = Arc<tokio::sync::Mutex<HashMap<String, [u8; 16]>>>;

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
    crypto_cache: GhostCryptoCache,
    key_cache: GhostKeyCache,
) {
    let ghost_mxid = format!("@{}{key}:{}", cfg.ghost_localpart_prefix, cfg.server_name);
    let localpart = format!("{}{key}", cfg.ghost_localpart_prefix);
    // Stable per-ghost device id; must agree between the lk-jwt identity and the
    // m.call.member content so EC maps the ghost's media to its membership. The
    // per-store salt makes the otherwise machine-deterministic id unique to this
    // crypto store (no cross-store SigningKeyChanged — the #30 fix for ghosts).
    let device_id = ghost_device_id(&key, &cfg.ghost_device_salt);
    // Generate the media key up front (E2EE rooms only) so we can BOTH warm the
    // crypto + start key distribution during the presence window AND install the
    // same key on the LiveKit frame crypto when we publish. Decouples the ~10-20s
    // crypto bootstrap from `call_active` (see the early distribute_key_task spawn).
    // E2EE rooms only. Reuse this ghost's key across respawns (get-or-insert keyed by
    // MXID) so a Discord speaker's leave→rejoin keeps the SAME key at the SAME identity
    // — a listener already holding it stays able to decrypt, with nothing to re-deliver.
    // The key feeds BOTH the frame-encryptor install (connect_and_publish) and the
    // to-device send; the fp is logged here + at both consumers to prove send==encrypt.
    let media_key = if cfg.media_e2ee {
        use std::collections::hash_map::Entry;
        let mut cache = key_cache.lock().await;
        match cache.entry(ghost_mxid.clone()) {
            Entry::Occupied(e) => {
                let k = *e.get();
                tracing::info!(mxid = %ghost_mxid, key_fp = %keys::key_fp(&k), "ghost media key reused from cache (stable across respawn)");
                Some(k)
            }
            Entry::Vacant(e) => {
                let k = keys::generate_key();
                tracing::info!(mxid = %ghost_mxid, key_fp = %keys::key_fp(&k), "ghost media key generated");
                Some(*e.insert(k))
            }
        }
    } else {
        None
    };

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
        // 3b. E2EE only: warm the crypto client + start key distribution NOW — on
        // presence, not on `call_active`. The ~10-20s crypto bootstrap (this ghost's
        // own /sync) then runs during the idle presence window, so when a human joins
        // every present ghost is already warm instead of stampeding its sync onto the
        // homeserver simultaneously. distribute_key_loop idles (no recipients) until a
        // human appears, then sends instantly. The SAME media_key is installed on the
        // LiveKit frame crypto in connect_and_publish below.
        if let Some(media_key) = media_key {
            tokio::spawn(
                distribute_key_task(
                    cfg.clone(),
                    localpart.clone(),
                    ghost_mxid.clone(),
                    device_id.clone(),
                    media_key,
                    members_gen_rx,
                    cancel.clone(),
                    crypto_cache,
                )
                .instrument(tracing::Span::current()),
            );
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
        let (source, track) = tokio::select! {
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
            res = connect_and_publish(&cfg, &focus, &ghost_mxid, &device_id, media_key, cancel.clone()) => match res {
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
        // (Key distribution + crypto bootstrap were started on presence — step 3b —
        // so a pre-warmed ghost can deliver its key the instant a human joins.)

        // 6. Pump frames until cancelled, mirroring the Discord user's mute state
        // onto the track as it changes (and applying the current state up front, so
        // a user who joined muted starts muted).
        pump_audio(frame_rx, &source, &track, mute_rx, &cancel, cfg.capture_wav, &key).await;

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
///
/// Diagnostics (issue #32, always on): a periodic per-ghost health line (frames
/// in/out, jitter overflow drops + depth, `capture_frame` errors, RMS) and a
/// frame-shape warning. When `capture_wav` is set it also writes "Tap B" — the
/// exact mono PCM handed to LiveKit — to `publish_<key>.wav`.
async fn pump_audio(
    mut frame_rx: mpsc::Receiver<Vec<i16>>,
    source: &NativeAudioSource,
    track: &LocalAudioTrack,
    mut mute_rx: watch::Receiver<bool>,
    cancel: &CancellationToken,
    capture_wav: bool,
    key: &str,
) {
    // Mirror the Discord speaker's mute state onto the LiveKit track. `mute()`/
    // `unmute()` signal the server so Element Call peers see the mic-off indicator
    // (constraint: a muted Discord user must show muted in EC). Apply the current
    // state up front — covers a user who joined already muted — then track changes.
    apply_track_mute(track, *mute_rx.borrow_and_update());
    let mut jitter = JitterBuffer::new(JITTER_TARGET_MS, JITTER_MAX_MS, SAMPLE_RATE);

    // Tap B: stream the published PCM to a WAV for the bisection (off by default).
    let mut tap_b = if capture_wav {
        let path = format!("publish_{key}.wav");
        match WavMonoWriter::create(&path) {
            Ok(w) => {
                tracing::info!(path, "ghost publish-tap WAV (Tap B) capturing");
                Some(w)
            }
            Err(e) => {
                tracing::warn!(path, error = %e, "failed to open publish-tap WAV — skipping Tap B");
                None
            }
        }
    } else {
        None
    };

    // Health/diagnostic counters. Logged every ~250 input frames (~5 s) to match
    // the voicetick-health cadence, then again on exit so a short call still reports.
    let mut frames_in: u64 = 0;
    let mut frames_out: u64 = 0;
    let mut capture_errs: u64 = 0;
    let mut shape_warns: u64 = 0;
    let mut last_rms = f32::NEG_INFINITY;
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
                frames_in += 1;
                jitter.push(&frame20);
                while let Some(f10) = jitter.pop_10ms() {
                    // Shape invariant: every popped frame must be exactly 10 ms.
                    // A mismatch would desync the encoder — warn (rate-limited) so a
                    // reframing bug shows up instead of silently corrupting audio.
                    if f10.len() != SAMPLES_10MS as usize {
                        shape_warns += 1;
                        if shape_warns.is_multiple_of(50) {
                            tracing::warn!(
                                key,
                                len = f10.len(),
                                expected = SAMPLES_10MS,
                                count = shape_warns,
                                "ghost publish frame is not 10 ms — reframing anomaly"
                            );
                        }
                    }
                    last_rms = nvb_audio::rms_dbfs(&f10);
                    if let Some(w) = tap_b.as_mut() {
                        w.write(&f10);
                    }
                    let af = AudioFrame {
                        data: Cow::Owned(f10),
                        sample_rate: SAMPLE_RATE,
                        num_channels: 1,
                        samples_per_channel: SAMPLES_10MS,
                    };
                    if let Err(e) = source.capture_frame(&af).await {
                        capture_errs += 1;
                        if capture_errs.is_multiple_of(50) {
                            tracing::warn!(key, error = %e, count = capture_errs, "ghost capture_frame failing — yielding");
                        }
                        tokio::task::yield_now().await;
                    } else {
                        frames_out += 1;
                    }
                }
                if frames_in.is_multiple_of(250) {
                    tracing::info!(
                        key,
                        frames_in,
                        frames_out,
                        jitter_dropped = jitter.dropped_samples(),
                        jitter_depth = jitter.depth_samples(),
                        capture_errs,
                        rms_db = format_args!("{last_rms:.1}"),
                        "ghost audio health"
                    );
                }
            }
        }
    }

    // Final health snapshot + finalize Tap B so the WAV header is valid.
    tracing::info!(
        key,
        frames_in,
        frames_out,
        jitter_dropped = jitter.dropped_samples(),
        capture_errs,
        shape_warns,
        "ghost audio pump exiting"
    );
    if let Some(w) = tap_b {
        match w.finalize() {
            Ok(()) => tracing::info!(key, "publish-tap WAV (Tap B) finalised"),
            Err(e) => tracing::warn!(key, error = %e, "publish-tap WAV finalise failed"),
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
    media_key: Option<[u8; 16]>,
    cancel: CancellationToken,
) -> Result<(NativeAudioSource, LocalAudioTrack)> {
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
    // Install the (pre-generated, in run_ghost) media key on the LiveKit frame
    // crypto so this ghost's outbound frames encrypt under it. The same key was
    // already (or is being) distributed to peers via to-device by the key task
    // started on presence — so it must be the SAME bytes here, hence passing it in.
    if let Some(key) = media_key {
        let key_provider = KeyProvider::new(KeyProviderOptions {
            ratchet_window_size: 16,
            ratchet_salt: b"LKFrameEncryptionKey".to_vec(),
            failure_tolerance: -1,
            key_ring_size: 256,
            key_derivation_algorithm: KeyDerivationAlgorithm::HKDF,
        });
        // The to-device `member.claimed_device_id` (= `device_id`) makes peers
        // reconstruct exactly this identity (`{mxid}:{device_id}`); they must agree
        // or peers install the key under the wrong identity and hear noise.
        if !key_provider.set_key(&ParticipantIdentity::from(identity.clone()), 0, key.to_vec()) {
            return Err(anyhow!("failed to set ghost media key on key provider"));
        }
        // #51 follow-up: fp of the key our OWN frames encrypt under, at index 0.
        // Must match this ghost's "media key generated" + "ghost media key sent" fps;
        // and peers' "installed peer media key" fp for this identity must match too.
        tracing::info!(
            identity = %identity,
            key_index = 0,
            key_fp = %keys::key_fp(&key),
            "ghost frame-encrypt key installed"
        );
        room_opts.encryption = Some(E2eeOptions {
            encryption_type: EncryptionType::Gcm,
            key_provider,
        });
    }

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
                    // The ghost's own frame-ENCRYPT state (it's a pure publisher). Surfaced
                    // at INFO to correlate with a listener that can't decrypt this ghost.
                    Some(livekit::RoomEvent::E2eeStateChanged { participant, state }) => {
                        tracing::info!(identity = %participant.identity(), ?state, "ghost LiveKit E2EE state changed");
                    }
                    Some(other) => tracing::trace!(event = ?other, "ghost LiveKit room event"),
                    None => break,
                },
            }
        }
        let _ = room.close().await;
        tracing::debug!("ghost LiveKit room closed");
    });

    Ok((source, track))
}

// ─── E2EE media-key distribution (Path 2 plaintext / Path 3 Olm) ────────────────

/// Number of key re-sends in the burst that follows a membership change.
const KEY_BURST_COUNT: u32 = 6;
/// Spacing between burst re-sends (~`COUNT * INTERVAL` = the covered window).
const KEY_BURST_INTERVAL: Duration = Duration::from_secs(3);
/// Steady-state re-send period once a burst is done — a slow safety net for a key
/// dropped even by the burst. Mild (≈1/ghost/min) vs. the old 5s spam.
const KEY_HEARTBEAT: Duration = Duration::from_secs(60);

/// Minimum spacing between Olm session rotations (#52). Membership-driven rotations
/// are rate-limited to this so Element Call's ~90s membership flaps don't churn
/// one-time keys on every flap.
const SESSION_ROTATE_MIN_INTERVAL: Duration = Duration::from_secs(90);
/// Maximum age of a ghost's outbound Olm sessions before the heartbeat rotates them
/// anyway (#52). This is what bounds a listener-side dead-session wedge in a static
/// call (no membership changes): worst-case inaudibility ≈ this + one heartbeat,
/// instead of "until the bridge restarts".
const SESSION_ROTATE_MAX_AGE: Duration = Duration::from_secs(15 * 60);

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
    cfg: GhostConfig,
    localpart: String,
    ghost_mxid: String,
    device_id: String,
    key: [u8; 16],
    members_gen: watch::Receiver<u64>,
    cancel: CancellationToken,
    crypto_cache: GhostCryptoCache,
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
            match bootstrap_olm_transport(
                &cfg,
                &localpart,
                &ghost_mxid,
                &device_id,
                &cancel,
                &crypto_cache,
            )
            .await
            {
                Some(t) => t,
                None => return, // bootstrap failed or cancelled; already logged.
            }
        }
    };
    // Session rotation (#52) only applies to the Olm transport (plaintext has no
    // sessions); the config knob can turn it off entirely.
    let rotate_sessions =
        cfg.rotate_ghost_sessions && matches!(cfg.key_transport, KeyTransportMode::Olm);
    let source: Arc<dyn RecipientSource> = Arc::new(CallRecipients { cfg, ghost_mxid });
    distribute_key_loop(source, key, transport, rotate_sessions, members_gen, cancel).await;
}

/// Source of the current key-send recipients (the call's human members). Abstracted
/// from [`distribute_key_loop`] so the loop's burst/heartbeat policy is unit-testable
/// without a live homeserver (tests supply a scripted source).
#[async_trait]
trait RecipientSource: Send + Sync {
    async fn recipients(&self) -> Result<Vec<KeyRecipient>>;
}

/// Production source: reads call memberships over the raw CS-API, masquerading as
/// the ghost (a joined member) — the media-key path has no matrix-rust-sdk
/// `Client` dependency (#50).
struct CallRecipients {
    cfg: GhostConfig,
    ghost_mxid: String,
}

#[async_trait]
impl RecipientSource for CallRecipients {
    async fn recipients(&self) -> Result<Vec<KeyRecipient>> {
        enumerate_key_recipients(&self.cfg, &self.ghost_mxid).await
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
    crypto_cache: &GhostCryptoCache,
) -> Option<Arc<dyn KeyTransport>> {
    // Reuse this ghost's crypto across rejoins: a respawn with a NEW crypto device
    // would not be picked up by Element Call for an already-keyed participant (the
    // rejoiner goes silent). The cache keeps the device + Olm sessions stable for the
    // process lifetime — see [`GhostCryptoCache`].
    if let Some(crypto) = crypto_cache
        .lock()
        .await
        .get(ghost_mxid)
        .cloned()
    {
        tracing::info!("ghost Olm key transport ready (reused cached OlmMachine across rejoin)");
        return Some(Arc::new(OlmTransport::new(
            crypto,
            cfg.room_id.clone(),
            device_id.to_owned(),
        )));
    }

    for attempt in 1..=OLM_BOOTSTRAP_ATTEMPTS {
        if cancel.is_cancelled() {
            return None;
        }
        match GhostCrypto::bootstrap(
            cfg.http.clone(),
            &cfg.homeserver_url,
            &cfg.as_token,
            localpart,
            ghost_mxid,
            &cfg.store_path,
            cfg.prune_stale_devices,
        )
        .await
        {
            Ok(crypto) => {
                tracing::info!("ghost Olm key transport ready (send-only OlmMachine)");
                let crypto = Arc::new(crypto);
                // Cache for this ghost's future rejoins (insert-if-absent; a racing
                // insert would only have built an equivalent fresh device).
                crypto_cache
                    .lock()
                    .await
                    .entry(ghost_mxid.to_owned())
                    .or_insert_with(|| crypto.clone());
                return Some(Arc::new(OlmTransport::new(
                    crypto,
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

// The send-only Olm crypto engine (`GhostCrypto`, in `key_transport.rs`) replaced
// the old per-ghost full `matrix_sdk::Client` here (its SQLite crypto store,
// `sync_once`, and perpetual `/sync`) — issue #44. The bootstrap now lives in
// `GhostCrypto::bootstrap` and uses an in-memory `OlmMachine` with on-demand
// `/keys/query`, so there is no per-ghost crypto store or sync loop.

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
    rotate_sessions: bool,
    mut members_gen: watch::Receiver<u64>,
    cancel: CancellationToken,
) {
    // Recipients keyed so far — used only to detect a NEW human (which restarts a
    // burst); the sends themselves are not gated by it (we re-send to all current).
    let mut keyed: HashSet<KeyRecipient> = HashSet::new();
    // Burst re-sends still owed after the last membership change. >0 ⇒ wake fast.
    let mut burst: u32 = 0;
    // Whether the NEXT send forces a fresh /keys/query for the recipients (issue #51).
    // The first send forces (humans may already be present); thereafter only a
    // membership-generation bump re-arms it — one forced query per membership change,
    // not per 3s burst tick.
    let mut refresh = RefreshMode::Force;
    // When the current re-key attempt began (a new joiner or a membership bump), so we
    // can log per-ghost "membership change → key delivered" latency (the #51 symptom).
    let mut rekey_started: Option<tokio::time::Instant> = None;
    // Olm session rotation (#52). A membership bump requests a rotation (a wedged
    // listener's natural remedy is rejoining the call — rotation is what actually
    // heals them, unlike today where a rejoin keeps the dead session); the max-age
    // check bounds a wedge in a static call. Bootstrap/cache-reuse just (re)built or
    // reused a machine, so the loop starts with the clock at "now".
    let mut rotate_pending = false;
    let mut last_rotation = tokio::time::Instant::now();
    loop {
        // Did this iteration deliver the key to at least one device? A send that
        // reaches NOBODY is almost always a cold-start race: this ghost's own crypto
        // client hasn't yet tracked the recipients' devices (its background sync /
        // `/keys/query` lags the first send), so `get_user_devices` returns empty.
        // Such an iteration must keep retrying at the FAST burst interval until sync
        // catches up — NOT drain the burst, mark recipients keyed, and drop to the
        // 60s heartbeat, which would leave that speaker silent in an encrypted call
        // for up to a minute (observed on a busy VPS: 57 such failures, multi-minute
        // gaps). Defaults true so empty / enumeration-error iterations idle normally.
        let mut reached_someone = true;
        match source.recipients().await {
            Ok(current) => {
                // A newly-seen recipient (re)starts the burst to cover its client's
                // call-setup window, during which it may drop the to-device key.
                if current.iter().any(|r| !keyed.contains(r)) {
                    burst = KEY_BURST_COUNT;
                    if rekey_started.is_none() {
                        rekey_started = Some(tokio::time::Instant::now());
                    }
                }
                if current.is_empty() {
                    keyed.clear();
                } else {
                    // Rotate BEFORE the send so the fresh sessions carry this very
                    // key delivery. Membership-driven rotations wait out the
                    // min-interval (`rotate_pending` persists, so a deferred one
                    // fires on a later tick); the max-age path needs no pending flag.
                    if rotate_sessions {
                        let elapsed = last_rotation.elapsed();
                        let reason = if rotate_pending && elapsed >= SESSION_ROTATE_MIN_INTERVAL {
                            Some("membership")
                        } else if elapsed >= SESSION_ROTATE_MAX_AGE {
                            Some("max-age")
                        } else {
                            None
                        };
                        if let Some(reason) = reason {
                            if transport.rotate_sessions(reason).await {
                                last_rotation = tokio::time::Instant::now();
                                rotate_pending = false;
                            }
                        }
                    }
                    reached_someone = match transport.send_key(&current, &key, 0, refresh).await {
                        // Every targeted device got the key.
                        Ok(o) if o.failed.is_empty() => {
                            tracing::info!(
                                recipients = ?current.iter().map(|r| format!("{}:{}", r.user_id, r.device_id)).collect::<Vec<_>>(),
                                transport = transport.kind(),
                                delivered = o.delivered,
                                key_fp = %keys::key_fp(&key),
                                burst,
                                "ghost media key sent"
                            );
                            true
                        }
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
                            true
                        }
                        // Reached NO devices — the real failure (e.g. the live device
                        // is out of one-time keys, or the ghost's sync hasn't tracked
                        // anyone yet). Keep fast-retrying (see `reached_someone`).
                        Ok(o) => {
                            tracing::warn!(
                                transport = transport.kind(),
                                unreachable = o.failed.len(),
                                burst,
                                "ghost media key reached NO recipient devices — nobody can decrypt this ghost (will retry)"
                            );
                            false
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                transport = transport.kind(),
                                "ghost media-key send failed"
                            );
                            false
                        }
                    };
                    // Whether it reached anyone or not, one send has now consumed the
                    // forced refresh: a genuine cold start is re-covered by the burst,
                    // and re-forcing every 3s tick would multiply /keys/query needlessly.
                    // The members_gen bump arm below re-arms it on the next membership change.
                    refresh = RefreshMode::Steady;
                    if reached_someone {
                        // Progress: remember who we've keyed and spend one burst slot.
                        keyed = current.into_iter().collect();
                        burst = burst.saturating_sub(1);
                        if let Some(t) = rekey_started.take() {
                            tracing::info!(
                                latency_ms = t.elapsed().as_millis() as u64,
                                transport = transport.kind(),
                                "ghost re-key delivered after membership change"
                            );
                        }
                    }
                    // else: reached nobody — leave `keyed` and `burst` untouched, so the
                    // new-recipient check re-arms the burst next loop and we retry at the
                    // fast interval until this ghost's crypto sync catches up.
                }
            }
            Err(e) => tracing::warn!(error = %e, "ghost media key: recipient enumeration failed"),
        }
        // Fast cadence while bursting OR while we've reached nobody (cold-sync recovery).
        let wait = if burst > 0 || !reached_someone {
            KEY_BURST_INTERVAL
        } else {
            KEY_HEARTBEAT
        };
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
                // Force ONE fresh /keys/query on the next send so a same-user rejoin's
                // (possibly rotated) returning device is re-fetched and re-keyed at once
                // rather than after the slow burst/heartbeat convergence (issue #51).
                refresh = RefreshMode::Force;
                // And request a session rotation (#52): a rejoining listener may hold
                // a dead inbound session for this ghost that only fresh sessions fix.
                rotate_pending = true;
                if rekey_started.is_none() {
                    rekey_started = Some(tokio::time::Instant::now());
                }
            }
            _ = tokio::time::sleep(wait) => {}
        }
    }
    tracing::debug!("ghost media-key distribution stopped");
}

/// Enumerate the call's human members (key-send recipients), read via a raw
/// masqueraded `GET /rooms/{room_id}/state` — the ghost is already a joined
/// member, so the read is authorized; no matrix-rust-sdk `Client` involved (#50).
/// Excludes our ghosts ([`is_ghost_identity`], which also drops *this* ghost) and
/// the bridge bot itself (the subscriber skips ghost tracks, so it never needs
/// ghost keys — constraint #5). Deduplicated by (user, device).
async fn enumerate_key_recipients(cfg: &GhostConfig, ghost_mxid: &str) -> Result<Vec<KeyRecipient>> {
    let url = cs_url(
        &cfg.homeserver_url,
        &["rooms", &cfg.room_id, "state"],
        Some(ghost_mxid),
    )?;
    let resp = cfg
        .http
        .get(url)
        .bearer_auth(&cfg.as_token)
        .send()
        .await
        .context("GET /rooms/{roomId}/state")?;
    if !resp.status().is_success() {
        return Err(anyhow!(
            "reading call memberships for key distribution failed: {}",
            err_body(resp).await
        ));
    }
    let events: Vec<serde_json::Value> = resp
        .json()
        .await
        .context("parsing room state for key distribution")?;
    Ok(recipients_from_room_state(
        events,
        &cfg.ghost_localpart_prefix,
        &cfg.bot_user_id,
    ))
}

/// The subset of a full state event [`recipients_from_room_state`] reads. The raw
/// `/state` endpoint returns full-event JSON (not the sync-format wrapper the SDK
/// used to hand us), so the shape is hand-rolled; `content` stays raw until the
/// event-type filter has matched.
#[derive(serde::Deserialize)]
struct RawStateEvent {
    #[serde(rename = "type")]
    event_type: String,
    sender: OwnedUserId,
    origin_server_ts: MilliSecondsSinceUnixEpoch,
    content: serde_json::Value,
}

/// Filter a room's full state down to the key-send recipients: one recipient per
/// ACTIVE `m.call.member` membership = one (user, device) actually in the call.
/// m.call.member is per-device, so this targets exactly the in-call device(s) — not
/// every device the user owns (which would waste Olm OTK claims on offline sessions).
/// Tolerant per event: anything unparseable is skipped (a redacted membership's
/// pruned `{}` content deserializes to the empty variant → no active memberships).
fn recipients_from_room_state(
    events: Vec<serde_json::Value>,
    ghost_localpart_prefix: &str,
    bot_user_id: &str,
) -> Vec<KeyRecipient> {
    let mut recipients: Vec<KeyRecipient> = Vec::new();
    for raw in events {
        let Ok(ev) = serde_json::from_value::<RawStateEvent>(raw) else { continue };
        if ev.event_type != CALL_MEMBER_EVENT_TYPE {
            continue;
        }
        if is_bridge_excluded_identity(ev.sender.as_str(), ghost_localpart_prefix)
            || ev.sender.as_str() == bot_user_id
        {
            continue;
        }
        let Ok(content) = serde_json::from_value::<CallMemberEventContent>(ev.content) else {
            continue;
        };
        for m in content.active_memberships(Some(ev.origin_server_ts)) {
            let r = KeyRecipient {
                user_id: ev.sender.clone(),
                device_id: m.device_id().to_owned(),
            };
            if !recipients.contains(&r) {
                recipients.push(r);
            }
        }
    }
    recipients
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

// #38 e2e testkit: fake Matrix call participants for the live harness. Declared
// as a CHILD of this module (not a sibling of `rtc`) so it can drive the private
// ghost machinery — `GhostConfig`, register/join/membership/`connect_and_publish`,
// `distribute_key_loop` — without widening any production visibility. The file
// itself lives at `rtc/testkit.rs`; `lib.rs` re-exports it feature-gated.
#[cfg(feature = "testkit")]
#[path = "testkit.rs"]
pub mod testkit;

#[cfg(test)]
mod tests {
    use super::*;

    fn test_pool(max_ghosts: Option<usize>) -> Arc<GhostPool> {
        let matrix_cfg = MatrixConfig {
            homeserver_url: "https://nether.im".into(),
            room_id: "!r:nether.im".into(),
            bot_user_id: "@bridge:nether.im".into(),
            ..MatrixConfig::default()
        };
        GhostPool::new(&matrix_cfg, &LiveKitConfig::default(), false, max_ghosts)
            .expect("test pool")
    }

    fn sink_count(pool: &GhostPool) -> usize {
        pool.sinks.lock().unwrap_or_else(PoisonError::into_inner).len()
    }

    #[test]
    fn assign_enforces_ghost_cap() {
        let pool = test_pool(Some(2));
        // Two speakers fit.
        let _a = pool.assign("a", "Alice", None);
        let _b = pool.assign("b", "Bob", None);
        assert_eq!(sink_count(&pool), 2);
        // A third is capped: null sink, no ghost registered.
        let _c = pool.assign("c", "Carol", None);
        assert_eq!(sink_count(&pool), 2);
        // Idempotent re-assign of an EXISTING speaker is never capped.
        let _a2 = pool.assign("a", "Alice", None);
        assert_eq!(sink_count(&pool), 2);
        // A release frees the slot; the previously-capped speaker now fits.
        pool.release("a");
        assert_eq!(sink_count(&pool), 1);
        let _c2 = pool.assign("c", "Carol", None);
        assert_eq!(sink_count(&pool), 2);
    }

    #[test]
    fn assign_without_cap_is_unlimited() {
        let pool = test_pool(None);
        for i in 0..50 {
            let _ = pool.assign(&format!("u{i}"), "U", None);
        }
        assert_eq!(sink_count(&pool), 50);
    }

    #[test]
    fn ghost_identity_detection() {
        let pfx = "nether-voicebridge_";
        assert!(is_ghost_identity("@nether-voicebridge_12345:nether.im", pfx));
        assert!(is_ghost_identity("@nether-voicebridge_12345:nether.im:DEVICE", pfx));
        assert!(!is_ghost_identity("@dark:nether.im", pfx));
        assert!(!is_ghost_identity("@dark:nether.im:DEVICE", pfx));
        // mautrix-discord's namespace must NOT be detected as a *current* ghost.
        assert!(!is_ghost_identity("@discord_12345:nether.im", pfx));
        assert!(!is_ghost_identity("not-an-mxid", pfx));
        // The bridge bot (`@{sender_localpart}_bot`) shares the ghost prefix but is
        // NOT a ghost: the all-digits suffix check excludes it. Without this the
        // startup sweep would retract the bot's own `m.call.member`.
        assert!(!is_ghost_identity("@nether-voicebridge_bot:nether.im", pfx));
        assert!(!is_ghost_identity("@nether-voicebridge_bot:nether.im:DEVICE", pfx));
        // A human in the namespace with a non-numeric localpart is likewise not a ghost.
        assert!(!is_ghost_identity("@nether-voicebridge_alice:nether.im", pfx));
        assert!(!is_ghost_identity("@nether-voicebridge_:nether.im", pfx));
        // A different deployment prefix works too.
        let test_pfx = "test-nether-voicebridge_";
        assert!(is_ghost_identity("@test-nether-voicebridge_12345:shadowdrake.org", test_pfx));
        assert!(!is_ghost_identity("@nether-voicebridge_12345:shadowdrake.org", test_pfx));
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
        // The previous hard-coded default prefix is recognized as legacy too (#37
        // migration): a deployment that moved to a different sender_localpart leaves
        // `@nether-voicebridge_<id>` ghosts behind, and the startup sweep must still
        // clean them up. (When the *current* prefix is still `nether-voicebridge_`,
        // the sweep checks `is_ghost_identity` first, so those take the masquerade
        // path — this legacy match only fires once the prefix has moved on.)
        assert!(is_legacy_ghost_identity("@nether-voicebridge_123:nether.im"));
        assert!(is_legacy_ghost_identity("@nether-voicebridge_12345:nether.im:DEVICE"));
        // ...but a non-numeric localpart under that prefix (the bot, or a human) is not.
        assert!(!is_legacy_ghost_identity("@nether-voicebridge_bot:nether.im"));
        assert!(!is_legacy_ghost_identity("@nether-voicebridge_alice:nether.im"));

        // The combined exclusion helper covers both current and legacy ghosts.
        let pfx = "nether-voicebridge_";
        assert!(is_bridge_excluded_identity("@nether-voicebridge_123:nether.im", pfx));
        assert!(is_bridge_excluded_identity("@discord_996161727068643380:nether.im", pfx));
        assert!(!is_bridge_excluded_identity("@dark:nether.im", pfx));
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
    fn ghost_device_id_folds_in_salt() {
        assert_eq!(ghost_device_id("42", "abc"), "D42-abc");
        // Empty salt (no store) → unchanged legacy bare id.
        assert_eq!(ghost_device_id("42", ""), "D42");
    }

    #[test]
    fn salted_device_id_keeps_wire_shape() {
        // A salted device id must still produce the exact claimed_device_id / member.id
        // EC reads — guards the `_m.call` suffix concatenation against separator drift.
        let content =
            keys::build_key_content("!room:nether.im", "D42-abc", "AAAA".to_owned(), 0, 1234);
        let v = serde_json::to_value(&content).unwrap();
        assert_eq!(v["member"]["claimed_device_id"], "D42-abc");
        assert_eq!(v["member"]["id"], "D42-abc_m.call");
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

    // ── recipients_from_room_state, on raw /rooms/{id}/state JSON ───────────────

    /// Wrap a content object in the full-event JSON shape the raw `/state`
    /// endpoint returns (only the fields [`RawStateEvent`] reads, plus the usual
    /// bystanders).
    fn state_event(
        event_type: &str,
        sender: &str,
        ts_ms: u64,
        content: serde_json::Value,
    ) -> serde_json::Value {
        serde_json::json!({
            "type": event_type,
            "sender": sender,
            "state_key": format!("_{sender}_DEV1_m.call"),
            "origin_server_ts": ts_ms,
            "event_id": "$test",
            "content": content,
        })
    }

    /// A currently-active session-format membership for `device_id` (the shape
    /// `rtc::membership::join` posts), as raw content JSON.
    fn session_member_content(device_id: &str, expires: Option<Duration>) -> serde_json::Value {
        let content = CallMemberEventContent::new(
            Application::Call(CallApplicationContent::new(String::new(), CallScope::Room)),
            device_id.into(),
            ActiveFocus::Livekit(ActiveLivekitFocus::new()),
            Vec::new(),
            None,
            expires,
        );
        serde_json::to_value(&content).unwrap()
    }

    fn now_ms() -> u64 {
        u64::from(matrix_sdk::ruma::MilliSecondsSinceUnixEpoch::now().get())
    }

    const PFX: &str = "nether-voicebridge_";
    const BOT: &str = "@nether-voicebridge_bot:nether.im";

    #[test]
    fn room_state_recipients_include_active_humans_only() {
        let events = vec![
            // A human with an active membership → included, right device.
            state_event(
                CALL_MEMBER_EVENT_TYPE,
                "@alice:nether.im",
                now_ms(),
                session_member_content("ALICEDEV", None),
            ),
            // Our own ghost and a stale legacy ghost → excluded (constraint #5).
            state_event(
                CALL_MEMBER_EVENT_TYPE,
                "@nether-voicebridge_123:nether.im",
                now_ms(),
                session_member_content("D123", None),
            ),
            state_event(
                CALL_MEMBER_EVENT_TYPE,
                "@discord_456:nether.im",
                now_ms(),
                session_member_content("D456", None),
            ),
            // The bridge bot (subscriber) → excluded.
            state_event(
                CALL_MEMBER_EVENT_TYPE,
                BOT,
                now_ms(),
                session_member_content("BOTDEV", None),
            ),
            // Non-call.member state → ignored even with a plausible content.
            state_event(
                "m.room.member",
                "@bob:nether.im",
                now_ms(),
                serde_json::json!({"membership": "join"}),
            ),
        ];
        let recipients = recipients_from_room_state(events, PFX, BOT);
        assert_eq!(recipients.len(), 1);
        assert_eq!(recipients[0].user_id.as_str(), "@alice:nether.im");
        assert_eq!(recipients[0].device_id.as_str(), "ALICEDEV");
    }

    #[test]
    fn room_state_recipients_skip_expired_and_empty_memberships() {
        let events = vec![
            // Expired: posted at t=1s epoch with a 1 ms expiry.
            state_event(
                CALL_MEMBER_EVENT_TYPE,
                "@alice:nether.im",
                1_000,
                session_member_content("ALICEDEV", Some(Duration::from_millis(1))),
            ),
            // Left the call (Empty content, as posted by a leave/retraction).
            state_event(
                CALL_MEMBER_EVENT_TYPE,
                "@bob:nether.im",
                now_ms(),
                serde_json::to_value(CallMemberEventContent::new_empty(None)).unwrap(),
            ),
            // Redacted membership: pruned `{}` content parses to Empty → no actives.
            state_event(
                CALL_MEMBER_EVENT_TYPE,
                "@carol:nether.im",
                now_ms(),
                serde_json::json!({}),
            ),
        ];
        assert!(recipients_from_room_state(events, PFX, BOT).is_empty());
    }

    #[test]
    fn room_state_recipients_dedupe_and_tolerate_malformed_events() {
        let dup = state_event(
            CALL_MEMBER_EVENT_TYPE,
            "@alice:nether.im",
            now_ms(),
            session_member_content("ALICEDEV", None),
        );
        let events = vec![
            // Garbage / missing fields must not fail the rest of the scan.
            serde_json::json!("not-an-object"),
            serde_json::json!({"type": CALL_MEMBER_EVENT_TYPE, "content": {}}),
            state_event(
                CALL_MEMBER_EVENT_TYPE,
                "@mallory:nether.im",
                now_ms(),
                serde_json::json!({"memberships": "not-an-array"}),
            ),
            // The same (user, device) twice (e.g. two state keys) → one recipient.
            dup.clone(),
            dup,
            // Legacy-format membership (memberships array) still enumerates.
            state_event(
                CALL_MEMBER_EVENT_TYPE,
                "@bob:nether.im",
                now_ms(),
                serde_json::to_value(CallMemberEventContent::new_legacy(vec![
                    matrix_sdk::ruma::events::call::member::LegacyMembershipDataInit {
                        application: Application::Call(CallApplicationContent::new(
                            String::new(),
                            CallScope::Room,
                        )),
                        device_id: "BOBDEV".into(),
                        expires: Duration::from_secs(3600),
                        foci_active: Vec::new(),
                        membership_id: "0".to_owned(),
                    }
                    .into(),
                ]))
                .unwrap(),
            ),
        ];
        let recipients = recipients_from_room_state(events, PFX, BOT);
        assert_eq!(recipients.len(), 2);
        assert_eq!(recipients[0].user_id.as_str(), "@alice:nether.im");
        assert_eq!(recipients[0].device_id.as_str(), "ALICEDEV");
        assert_eq!(recipients[1].user_id.as_str(), "@bob:nether.im");
        assert_eq!(recipients[1].device_id.as_str(), "BOBDEV");
    }

    // ── distribute_key_loop, driven with mock transport + recipient source ──────

    use super::super::key_transport::{FailedRecipient, KeyRecipient, KeySendOutcome, RefreshMode};

    /// Test helper: a call-member recipient with a fixed device id.
    fn rcpt(user: &str) -> KeyRecipient {
        KeyRecipient {
            user_id: user.parse().unwrap(),
            device_id: "DEV1".into(),
        }
    }

    /// Records the recipient lists, `refresh` mode, and session rotations (#52)
    /// passed to the transport.
    #[derive(Default)]
    struct MockTransport {
        sends: StdMutex<Vec<Vec<KeyRecipient>>>,
        refreshes: StdMutex<Vec<RefreshMode>>,
        rotations: StdMutex<Vec<&'static str>>,
    }

    #[async_trait]
    impl KeyTransport for MockTransport {
        async fn send_key(
            &self,
            recipients: &[KeyRecipient],
            _key: &[u8; 16],
            _key_index: u8,
            refresh: RefreshMode,
        ) -> Result<KeySendOutcome> {
            self.sends.lock().unwrap().push(recipients.to_vec());
            self.refreshes.lock().unwrap().push(refresh);
            Ok(KeySendOutcome {
                delivered: recipients.len(),
                failed: Vec::new(),
            })
        }
        fn kind(&self) -> &'static str {
            "mock"
        }
        async fn rotate_sessions(&self, reason: &'static str) -> bool {
            self.rotations.lock().unwrap().push(reason);
            true
        }
    }

    /// Returns a fixed recipient list every poll.
    struct MockSource {
        recipients: Vec<KeyRecipient>,
    }

    #[async_trait]
    impl RecipientSource for MockSource {
        async fn recipients(&self) -> Result<Vec<KeyRecipient>> {
            Ok(self.recipients.clone())
        }
    }

    #[tokio::test]
    async fn key_loop_sends_to_current_recipients_then_stops_on_cancel() {
        let alice = rcpt("@alice:nether.im");
        let source = Arc::new(MockSource {
            recipients: vec![alice.clone()],
        });
        let transport = Arc::new(MockTransport::default());
        let (_gen_tx, gen_rx) = watch::channel(0u64);
        let cancel = CancellationToken::new();

        let handle = tokio::spawn(distribute_key_loop(
            source,
            [7u8; 16],
            transport.clone(),
            false,
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
        let source = Arc::new(MockSource { recipients: vec![] });
        let transport = Arc::new(MockTransport::default());
        let (_gen_tx, gen_rx) = watch::channel(0u64);
        let cancel = CancellationToken::new();

        let handle = tokio::spawn(distribute_key_loop(
            source,
            [0u8; 16],
            transport.clone(),
            false,
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

    /// Reaches NOBODY for the first `fail_first` sends (simulating a ghost whose
    /// crypto client hasn't tracked the recipients' devices yet), then succeeds.
    struct ColdThenWarmTransport {
        fail_first: usize,
        calls: StdMutex<usize>,
    }

    #[async_trait]
    impl KeyTransport for ColdThenWarmTransport {
        async fn send_key(
            &self,
            recipients: &[KeyRecipient],
            _key: &[u8; 16],
            _key_index: u8,
            _refresh: RefreshMode,
        ) -> Result<KeySendOutcome> {
            let mut n = self.calls.lock().unwrap();
            *n += 1;
            if *n <= self.fail_first {
                // delivered=0 with a non-empty `failed` set ⇒ the "reached NO
                // recipient devices" arm (cold crypto sync), which must fast-retry.
                Ok(KeySendOutcome {
                    delivered: 0,
                    failed: recipients
                        .iter()
                        .map(|r| FailedRecipient {
                            user_id: r.user_id.clone(),
                            device_id: r.device_id.clone(),
                        })
                        .collect(),
                })
            } else {
                Ok(KeySendOutcome {
                    delivered: recipients.len(),
                    failed: Vec::new(),
                })
            }
        }
        fn kind(&self) -> &'static str {
            "cold-then-warm"
        }
    }

    /// Regression: a ghost that keeps reaching nobody (its crypto sync hasn't caught
    /// up) must keep retrying at the FAST 3s burst interval — NOT drain the burst on
    /// the failures and drop to the 60s heartbeat, which left speakers silent for a
    /// minute on a busy VPS. The window (40s) deliberately runs PAST the ~18s it
    /// takes the initial burst (6 × 3s) to drain: with the fix the cadence stays fast
    /// (~14 sends in 40s); the old bug drained the burst and went quiet (~6 sends,
    /// then a 60s gap). `>= 10` cleanly separates the two.
    #[tokio::test(start_paused = true)]
    async fn key_loop_fast_retries_until_crypto_sync_catches_up() {
        let source = Arc::new(MockSource {
            recipients: vec![rcpt("@alice:nether.im")],
        });
        // Stays cold for the whole window so we measure the sustained retry cadence.
        let transport = Arc::new(ColdThenWarmTransport {
            fail_first: usize::MAX,
            calls: StdMutex::new(0),
        });
        let (_gen_tx, gen_rx) = watch::channel(0u64);
        let cancel = CancellationToken::new();

        let handle = tokio::spawn(distribute_key_loop(
            source,
            [1u8; 16],
            transport.clone(),
            false,
            gen_rx,
            cancel.clone(),
        ));

        tokio::time::sleep(Duration::from_secs(40)).await;
        cancel.cancel();
        handle.await.unwrap();

        let calls = *transport.calls.lock().unwrap();
        assert!(
            calls >= 10,
            "expected sustained fast retries while reaching nobody (≈14 sends in 40s \
             at the 3s interval); got {calls} — burst drained on failure → 60s \
             heartbeat (the bug)"
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
        let outcome = t
            .send_key(&[], &[0u8; 16], 0, RefreshMode::Steady)
            .await
            .unwrap();
        assert!(outcome.failed.is_empty());
        assert_eq!(outcome.delivered, 0);
        assert_eq!(t.kind(), "plaintext");
    }

    /// Issue #51: the first send forces a fresh device-list query, steady burst ticks
    /// do NOT, and a membership-generation bump re-arms exactly one forced query — so a
    /// same-user rejoin re-fetches the returning device instead of stalling.
    #[tokio::test(start_paused = true)]
    async fn key_loop_forces_refresh_on_first_send_and_after_gen_bump() {
        let source = Arc::new(MockSource {
            recipients: vec![rcpt("@alice:nether.im")],
        });
        let transport = Arc::new(MockTransport::default());
        let (gen_tx, gen_rx) = watch::channel(0u64);
        let cancel = CancellationToken::new();

        let handle = tokio::spawn(distribute_key_loop(
            source,
            [7u8; 16],
            transport.clone(),
            false,
            gen_rx,
            cancel.clone(),
        ));

        // Let the first send fire, then advance one burst interval for a steady tick.
        tokio::time::sleep(Duration::from_millis(10)).await;
        tokio::time::advance(KEY_BURST_INTERVAL).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        // Simulate a listener rejoin: bump the membership generation.
        gen_tx.send_modify(|g| *g = g.wrapping_add(1));
        tokio::time::sleep(Duration::from_millis(10)).await;
        cancel.cancel();
        handle.await.unwrap();

        let refreshes = transport.refreshes.lock().unwrap().clone();
        assert!(
            refreshes.len() >= 3,
            "expected first send + a steady burst tick + a post-bump send; got {refreshes:?}"
        );
        assert_eq!(
            refreshes[0],
            RefreshMode::Force,
            "first send must force a fresh /keys/query"
        );
        assert_eq!(
            refreshes[1],
            RefreshMode::Steady,
            "an ordinary burst tick must NOT force a query"
        );
        assert!(
            refreshes[2..].contains(&RefreshMode::Force),
            "a membership-generation bump must re-arm one forced query; got {refreshes:?}"
        );
    }

    /// #52: a membership bump requests a session rotation, but it must wait out the
    /// min-interval (no rotation right at the bump — EC membership flaps would churn
    /// one-time keys) and then fire on a later send tick with reason "membership".
    #[tokio::test(start_paused = true)]
    async fn key_loop_rotates_on_membership_after_min_interval() {
        let source = Arc::new(MockSource {
            recipients: vec![rcpt("@alice:nether.im")],
        });
        let transport = Arc::new(MockTransport::default());
        let (gen_tx, gen_rx) = watch::channel(0u64);
        let cancel = CancellationToken::new();

        let handle = tokio::spawn(distribute_key_loop(
            source,
            [7u8; 16],
            transport.clone(),
            true,
            gen_rx,
            cancel.clone(),
        ));

        // First send fires; a fresh loop must not rotate (bootstrap just built the
        // machine).
        tokio::time::sleep(Duration::from_millis(10)).await;
        gen_tx.send_modify(|g| *g = g.wrapping_add(1));
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            transport.rotations.lock().unwrap().is_empty(),
            "a bump inside the min-interval must NOT rotate yet"
        );

        // Once the min-interval has passed, the still-pending request fires.
        tokio::time::advance(SESSION_ROTATE_MIN_INTERVAL).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        cancel.cancel();
        handle.await.unwrap();

        let rotations = transport.rotations.lock().unwrap().clone();
        assert_eq!(
            rotations,
            vec!["membership"],
            "exactly one deferred membership rotation"
        );
    }

    /// #52: with no membership changes at all, the max-age bound still rotates —
    /// this is what caps a listener-side dead-session wedge in a static call.
    #[tokio::test(start_paused = true)]
    async fn key_loop_rotates_at_max_age_in_static_call() {
        let source = Arc::new(MockSource {
            recipients: vec![rcpt("@alice:nether.im")],
        });
        let transport = Arc::new(MockTransport::default());
        let (_gen_tx, gen_rx) = watch::channel(0u64);
        let cancel = CancellationToken::new();

        let handle = tokio::spawn(distribute_key_loop(
            source,
            [7u8; 16],
            transport.clone(),
            true,
            gen_rx,
            cancel.clone(),
        ));

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(transport.rotations.lock().unwrap().is_empty());

        tokio::time::advance(SESSION_ROTATE_MAX_AGE).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        cancel.cancel();
        handle.await.unwrap();

        let rotations = transport.rotations.lock().unwrap().clone();
        assert_eq!(rotations, vec!["max-age"], "one age-driven rotation");
    }

    /// #52: the config knob off ⇒ no rotation ever, regardless of bumps or age.
    #[tokio::test(start_paused = true)]
    async fn key_loop_never_rotates_when_disabled() {
        let source = Arc::new(MockSource {
            recipients: vec![rcpt("@alice:nether.im")],
        });
        let transport = Arc::new(MockTransport::default());
        let (gen_tx, gen_rx) = watch::channel(0u64);
        let cancel = CancellationToken::new();

        let handle = tokio::spawn(distribute_key_loop(
            source,
            [7u8; 16],
            transport.clone(),
            false,
            gen_rx,
            cancel.clone(),
        ));

        tokio::time::sleep(Duration::from_millis(10)).await;
        gen_tx.send_modify(|g| *g = g.wrapping_add(1));
        tokio::time::advance(SESSION_ROTATE_MAX_AGE + SESSION_ROTATE_MIN_INTERVAL).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        cancel.cancel();
        handle.await.unwrap();

        assert!(
            transport.rotations.lock().unwrap().is_empty(),
            "rotation disabled by config must never fire"
        );
    }
}
