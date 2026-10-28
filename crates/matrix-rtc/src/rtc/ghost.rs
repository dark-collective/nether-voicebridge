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
                Application, CallApplicationContent,
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
use tokio::sync::{mpsc, oneshot, watch, Notify};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use nvb_companion::stream::{AttachError, AttachReply, StreamGrant, StreamProvider, StreamTransport};

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

/// Per-request timeout on the ghost HTTP client (#67). Every ghost control-plane
/// request (register/join/membership/leave, key sends, `/keys/query`) is bounded
/// so a hung homeserver connection can't wedge the single-task driver, which
/// awaits ghost teardown inline.
const GHOST_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// How often the ghost driver looks for orphaned ghost call memberships — ones
/// whose ghost is gone but whose `m.call.member` is still in the room because the
/// teardown leave PUT failed (a slow/erroring homeserver). Left alone, such an
/// orphan shows the Discord user "in the call" until it expires (up to 4 h, and
/// longer in clients that ignore `expires`). Live-observed 2026-10-07: a 03:17
/// leave timed out and the ghost lingered in Light Voice all day. The sweep reads
/// the bot's synced room state (no request unless something needs retracting).
const ORPHAN_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// One ghost-shaped `m.call.member` observed in room state, for
/// [`orphan_ghost_memberships`].
#[derive(Debug, Clone)]
struct ObservedGhostMembership {
    sender: String,
    state_key: String,
    /// `false` once every membership in the event has expired.
    active: bool,
}

/// Which observed ghost memberships to retract: a current-prefix ghost with no
/// live ghost in this pool, and the membership is either **expired** (harmless
/// to clear, whoever wrote it) or **ours** — its state_key carries this store's
/// device salt (`D{key}-{salt}`), so a second bridge on the same room (allowed,
/// warn-only) never has its live ghosts swept. With no salt (no store) ownership
/// can't be told apart, so only expired ones go. Pure — unit-tested.
fn orphan_ghost_memberships(
    observed: &[ObservedGhostMembership],
    live_keys: &HashSet<String>,
    live_streams: &HashSet<String>,
    ghost_prefix: &str,
    salt: &str,
) -> Vec<ObservedGhostMembership> {
    observed
        .iter()
        .filter(|m| {
            if !is_ghost_identity(&m.sender, ghost_prefix) {
                return false;
            }
            let Some(key) = m
                .sender
                .strip_prefix('@')
                .and_then(|r| r.split(':').next())
                .and_then(|lp| lp.strip_prefix(ghost_prefix))
            else {
                return false;
            };
            // A ghost has its voice device (`D…`) and, while a companion stream is attached, a stream device
            // (`S…`, ADR 0004): each is live only while its own owner is.
            let device = m
                .state_key
                .strip_prefix(&format!("_{}_", m.sender))
                .and_then(|r| r.strip_suffix("_m.call"))
                .unwrap_or("");
            let stream = device.starts_with('S');
            if (stream && live_streams.contains(key)) || (!stream && live_keys.contains(key)) {
                return false;
            }
            if !m.active {
                return true;
            }
            let ours = if stream { stream_device_id(key, salt) } else { ghost_device_id(key, salt) };
            !salt.is_empty() && m.state_key == format!("_{}_{}_m.call", m.sender, ours)
        })
        .cloned()
        .collect()
}

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
    /// Attach a companion stream device to the ghost for `key` (ADR 0004 §5) until `cancel` fires.
    AttachStream {
        key: String,
        cancel: CancellationToken,
        reply: oneshot::Sender<Result<StreamGrant, AttachError>>,
    },
}

/// A running stream device (ADR 0004 §5), held by the driver like a ghost.
struct StreamHandle {
    cancel: CancellationToken,
    join: tokio::task::JoinHandle<()>,
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
    /// Minimum spacing between membership-driven Olm session rotations (#52),
    /// from the resolved `[timing]` profile (`TimingConfig::
    /// session_rotate_min_interval`). Historical const was 90 s; the compressed
    /// test profile shrinks it so the rotation scenario runs in seconds.
    session_rotate_min_interval: Duration,
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
        session_rotate_min_interval: Duration,
    ) -> Result<Arc<Self>> {
        let bot_user: OwnedUserId = matrix_cfg
            .bot_user_id
            .parse()
            .with_context(|| format!("parsing bot_user_id '{}'", matrix_cfg.bot_user_id))?;
        let http = reqwest::Client::builder()
            .use_rustls_tls()
            // Bound every ghost request (#67). The single-task `GhostPool::run`
            // driver awaits ghost teardown inline (`handle.join.await`), and
            // `run_ghost`'s teardown does un-raced HTTP (the final leave PUT); a
            // homeserver that accepts the connection but never answers would
            // otherwise wedge the whole bridge's ghost lifecycle until the
            // process restarts. This client is cloned into every transport, so
            // key sends and `/keys/query` are bounded too.
            .timeout(GHOST_HTTP_TIMEOUT)
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
                session_rotate_min_interval,
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

    /// Retract orphaned ghost memberships (see [`ORPHAN_SWEEP_INTERVAL`]). Called
    /// from the driver with the keys of the ghosts it currently runs. Best-effort:
    /// a failed retract is retried on the next tick.
    async fn sweep_orphaned_ghosts(
        &self,
        client: &Client,
        live_keys: &HashSet<String>,
        live_streams: &HashSet<String>,
        seen: &mut HashSet<String>,
    ) {
        let Ok(room_id) = RoomId::parse(&self.cfg.room_id) else { return };
        let Some(room) = client.get_room(&room_id) else { return };
        let raws = match room.get_state_events_static::<CallMemberEventContent>().await {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!(error = %e, "orphan ghost sweep: could not read call memberships");
                return;
            }
        };
        let observed: Vec<ObservedGhostMembership> = raws
            .into_iter()
            .filter_map(|raw| match raw.deserialize().ok()? {
                SyncOrStrippedState::Sync(SyncStateEvent::Original(o)) => {
                    if matches!(o.content, CallMemberEventContent::Empty(_)) {
                        return None;
                    }
                    Some(ObservedGhostMembership {
                        sender: o.sender.to_string(),
                        state_key: o.state_key.as_ref().to_owned(),
                        active: !o.content.active_memberships(Some(o.origin_server_ts)).is_empty(),
                    })
                }
                _ => None,
            })
            .collect();
        let orphans = orphan_ghost_memberships(
            &observed,
            live_keys,
            live_streams,
            &self.cfg.ghost_localpart_prefix,
            &self.cfg.ghost_device_salt,
        );
        let mut next: HashSet<String> = orphans.iter().map(|m| m.state_key.clone()).collect();
        for m in orphans {
            if !seen.contains(&m.state_key) {
                continue;
            }
            match retract_membership_raw(&self.cfg, &m.sender, &m.state_key).await {
                Ok(()) => {
                    // Start over if sync still shows it: don't write it twice in a row.
                    next.remove(&m.state_key);
                    tracing::info!(
                        ghost = %m.sender, expired = !m.active,
                        "retracted orphaned ghost call membership (its ghost is gone)"
                    );
                }
                Err(e) => tracing::warn!(
                    ghost = %m.sender, error = format!("{e:#}"),
                    "orphan ghost sweep: retract failed — retrying next tick"
                ),
            }
        }
        *seen = next;
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
        // The call's elected SFU (published by `rtc::run`). Each ghost publishes
        // there and migrates when it changes — see [`run_ghost`].
        call_focus_rx: watch::Receiver<Option<LivekitFocusInfo>>,
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
        // Companion stream devices (ADR 0004), keyed like ghosts; one per ghost at most.
        let mut streams: HashMap<String, StreamHandle> = HashMap::new();
        // Per-ghost crypto reused across rejoins for this bridge's process lifetime
        // (keeps the Olm device stable so a rejoiner stays audible) — see
        // [`GhostCryptoCache`].
        let crypto_cache: GhostCryptoCache = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        // Per-ghost media key reused across respawns — see [`GhostKeyCache`].
        let key_cache: GhostKeyCache = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        // Orphaned-membership sweep (see [`ORPHAN_SWEEP_INTERVAL`]). Runs in this
        // single-task driver so it is serialized with Assign/Release: it can never
        // retract a key that is being (re)minted. An orphan must be seen on two
        // consecutive ticks before it is retracted, so a leave that already landed
        // but hasn't synced back yet isn't written twice.
        let mut orphan_tick = tokio::time::interval_at(
            tokio::time::Instant::now() + ORPHAN_SWEEP_INTERVAL,
            ORPHAN_SWEEP_INTERVAL,
        );
        orphan_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut orphan_seen: HashSet<String> = HashSet::new();

        loop {
            let cmd = tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    // Drain every ghost to baseline: cancel + await teardown so all
                    // `post_membership(leave)` PUTs land before the driver returns.
                    tracing::info!(count = ghosts.len(), "ghost pool draining (shutdown)");
                    drain_streams(&mut streams).await;
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
                _ = orphan_tick.tick() => {
                    streams.retain(|_, h| !h.join.is_finished());
                    let live: HashSet<String> = ghosts.keys().cloned().collect();
                    let live_streams: HashSet<String> = streams.keys().cloned().collect();
                    self.sweep_orphaned_ghosts(&client, &live, &live_streams, &mut orphan_seen).await;
                    continue;
                }
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
                        run_ghost(cfg, focus, client.clone(), members_gen_rx.clone(), call_active_rx.clone(), call_focus_rx.clone(), key.clone(), display_name, avatar_url, frame_rx, mute_rx, cancel.clone(), crypto_cache.clone(), key_cache.clone())
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
                    if let Some(s) = streams.remove(&key) {
                        s.cancel.cancel();
                        let _ = s.join.await;
                    }
                    if let Some(handle) = ghosts.remove(&key) {
                        handle.cancel.cancel();
                        let _ = handle.join.await;
                    }
                }
                GhostCommand::ReleaseAll => {
                    drain_streams(&mut streams).await;
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
                GhostCommand::AttachStream { key, cancel, reply } => {
                    streams.retain(|_, h| !h.join.is_finished());
                    if !ghosts.contains_key(&key) {
                        let _ = reply.send(Err("not_represented"));
                        continue;
                    }
                    if streams.contains_key(&key) {
                        let _ = reply.send(Err("already_attached"));
                        continue;
                    }
                    if focus.is_none() {
                        match well_known::discover_livekit_focus(&self.cfg.http, &self.cfg.well_known_base_url, &self.cfg.room_id).await {
                            Ok(f) => focus = Some(f),
                            Err(e) => {
                                tracing::warn!(error = %e, "stream device: focus discovery failed");
                                let _ = reply.send(Err("sfu_unavailable"));
                                continue;
                            }
                        }
                    }
                    let mut cfg = self.cfg.clone();
                    cfg.media_e2ee = media_e2ee;
                    cfg.key_transport = key_transport;
                    let Some(default_focus) = focus.clone() else { continue };
                    let join = tokio::spawn(
                        run_stream_device(
                            cfg,
                            default_focus,
                            client.clone(),
                            members_gen_rx.clone(),
                            call_focus_rx.clone(),
                            key.clone(),
                            cancel.clone(),
                            crypto_cache.clone(),
                            reply,
                        )
                        .instrument(tracing::info_span!(parent: None, "bridge", name = %bridge)),
                    );
                    streams.insert(key, StreamHandle { cancel, join });
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

/// A ghost's companion stream device (ADR 0004 §5): `S{key}-{salt}`, the same store salt as its voice device.
fn stream_device_id(key: &str, salt: &str) -> String {
    if salt.is_empty() {
        format!("S{key}")
    } else {
        format!("S{key}-{salt}")
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
    // The bridge's own SFU (from `.well-known`): the fallback publish target
    // when nothing is elected.
    default_focus: LivekitFocusInfo,
    client: Client,
    members_gen_rx: watch::Receiver<u64>,
    mut call_active_rx: watch::Receiver<bool>,
    mut call_focus_rx: watch::Receiver<Option<LivekitFocusInfo>>,
    key: String,
    display_name: String,
    avatar_url: Option<String>,
    mut frame_rx: mpsc::Receiver<Vec<i16>>,
    mut mute_rx: watch::Receiver<bool>,
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

    // Where this ghost publishes: the call's ELECTED SFU (`rtc::focus::
    // elected_focus`, read authoritatively from room state), not necessarily
    // ours. A legacy `oldest_membership` client (Commet, pre-0.21 Element Call)
    // listens only on the oldest member's SFU — when that member is on another
    // homeserver, a ghost publishing on our SFU is inaudible to it (live-observed
    // 2026-09-29: Commet on matrix.org heard nothing from Discord). The membership
    // advertises exactly this SFU, so multi-SFU clients find the track there too.
    // Re-elected on every (re)connect; migrated mid-call when the election moves.
    let mut focus = elected_focus_or(&client, &cfg, &default_focus).await;
    // Fixed at the first post and repeated on every re-post (presence → active,
    // migration): a membership's age is its `created_ts`, and letting it reset
    // would reshuffle who is "oldest" — and with it the election — each time.
    let created_ts = Some(MilliSecondsSinceUnixEpoch::now());

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
        // never blocks the ghost going live). Checked on each assign so it tracks
        // avatar changes across rejoins; unchanged avatars are not re-uploaded.
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
                error = format!("{e:#}"),
                bot = %cfg.bot_user_id,
                room = %cfg.room_id,
                "ghost invite failed — trying join anyway (restricted rooms need the bot to have invite power)"
            );
        }
        // 3. Join the room (required before posting m.call.member).
        if let Err(e) = join_room(&cfg, &ghost_mxid).await {
            tracing::error!(error = format!("{e:#}"), "ghost join_room failed — skipping");
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
                keep_membership_posted(
                    cfg.clone(),
                    ghost_mxid.clone(),
                    device_id.clone(),
                    focus.clone(),
                    created_ts,
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
                        // retract and retire without ever touching LiveKit. `cancel`
                        // has fired, so the membership task will settle; await it (not
                        // abort) so an in-flight join PUT can't be serialized AFTER the
                        // leave below and leave a phantom membership behind.
                        if let Some(h) = presence_membership.take() {
                            let _ = h.await;
                        }
                        if let Err(e) = post_membership(&cfg, &ghost_mxid, &device_id, &focus, true).await {
                            tracing::warn!(error = format!("{e:#}"), "ghost post_membership(leave) failed — the orphan sweep will retry");
                        }
                        tracing::info!("ghost retired (was presence-only)");
                        return;
                    }
                    // A `call_active` transition (or, if the sender was dropped
                    // without our cancel firing, a park on cancel — never a
                    // busy-spin on the closed channel, #67). Re-check at the loop
                    // top either way; the biased cancel arm retracts + retires.
                    _ = changed_or_park(&mut call_active_rx, &cancel) => {}
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
        // Per-connection-attempt disconnect signal, fired by the drain task when
        // the room dies (#66). Replaced with a fresh token on every reconnect so
        // a stale room's death can't re-trigger recovery.
        let mut disconnected = CancellationToken::new();
        // The humans are in now — the election may have moved since the presence
        // phase (a legacy joiner older than us elects THEIR SFU). Re-read state.
        focus = elected_focus_or(&client, &cfg, &default_focus).await;
        // Per-attempt child of `cancel`: closes just this attempt's LiveKit room
        // (the drain task ends on it) so a focus migration can drop the old SFU
        // connection without retiring the ghost. A retire cascades through it.
        let mut attempt = cancel.child_token();
        let (mut source, mut track) = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                // `cancel` has fired; await the membership task (not abort) so any
                // in-flight join PUT settles before the leave — otherwise the server
                // can serialize the join after the leave and strand a phantom member.
                if let Some(h) = presence_membership.take() {
                    let _ = h.await;
                }
                if let Err(e) = post_membership(&cfg, &ghost_mxid, &device_id, &focus, true).await {
                    tracing::warn!(error = format!("{e:#}"), "ghost post_membership(leave) failed — the orphan sweep will retry");
                }
                tracing::info!("ghost retired (cancelled before publish)");
                return;
            }
            res = connect_and_publish(&cfg, &focus, &ghost_mxid, &device_id, media_key, attempt.clone(), disconnected.clone()) => match res {
                Ok(out) => out,
                Err(e) => {
                    tracing::error!(error = %e, "ghost LiveKit connect/publish failed — retracting");
                    // Connect failed, not a cancel, so `cancel` is still live: fire it
                    // first (we are retiring the ghost), THEN await the membership task
                    // so an in-flight join PUT settles before the leave — an abort here
                    // could let the join land after the leave and strand a phantom member.
                    cancel.cancel();
                    if let Some(h) = presence_membership.take() {
                        let _ = h.await;
                    }
                    if let Err(e) = post_membership(&cfg, &ghost_mxid, &device_id, &focus, true).await {
                        tracing::warn!(error = format!("{e:#}"), "ghost post_membership(leave) failed — the orphan sweep will retry");
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
        let mut membership_handle = tokio::spawn(
            keep_membership_posted(
                cfg.clone(),
                ghost_mxid.clone(),
                device_id.clone(),
                focus.clone(),
                created_ts,
                cancel.clone(),
            )
            .instrument(tracing::Span::current()),
        );
        tracing::info!(display_name = %display_name, sfu = %focus.service_url, "ghost publishing Discord speaker audio");
        // (Key distribution + crypto bootstrap were started on presence — step 3b —
        // so a pre-warmed ghost can deliver its key the instant a human joins.)

        // 6. Pump frames until retired, mirroring the Discord user's mute state
        // onto the track as it changes (and applying the current state up front, so
        // a user who joined muted starts muted). If the LiveKit room dies mid-call
        // (#66) reconnect with backoff instead of retiring: the ghost's membership
        // is posted and its media key distributed, so a silent retire here would
        // leave the speaker visibly in the call but inaudible until they left and
        // rejoined Discord. Membership + key tasks keep running across reconnects —
        // the identity, device id, and media key are all stable, so peers need
        // nothing re-delivered; the fresh room just republishes the track.
        'pump: loop {
            let exit = pump_audio(
                &mut frame_rx,
                &source,
                &track,
                &mut mute_rx,
                &mut call_focus_rx,
                &focus,
                &cancel,
                &disconnected,
                cfg.capture_wav,
                &key,
            )
            .await;
            let migrating = match exit {
                // Retire: the ghost was released (cancel) or the Discord side
                // dropped the speaker's sink (channel closed) — step 7 below.
                PumpExit::Cancelled | PumpExit::SourceClosed => break 'pump,
                PumpExit::Disconnected => {
                    tracing::warn!("ghost LiveKit room disconnected — reconnecting");
                    false
                }
                PumpExit::FocusChanged => {
                    // The watch says the election moved. Re-read room state (the
                    // authority) and only migrate on a real change of SFU.
                    let new_focus = elected_focus_or(&client, &cfg, &default_focus).await;
                    if super::focus::normalize_service_url(&new_focus.service_url)
                        == super::focus::normalize_service_url(&focus.service_url)
                    {
                        continue 'pump;
                    }
                    tracing::info!(
                        from = %focus.service_url,
                        to = %new_focus.service_url,
                        "call-focus election moved — migrating ghost to the elected SFU"
                    );
                    focus = new_focus;
                    // Close the old SFU connection (drain ends → room closed); the
                    // ghost itself stays live.
                    attempt.cancel();
                    true
                }
            };
            let mut backoff = if migrating { Duration::ZERO } else { Duration::from_secs(2) };
            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break 'pump,
                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff.max(Duration::from_secs(1)) * 2).min(Duration::from_secs(30));
                disconnected = CancellationToken::new();
                attempt = cancel.child_token();
                match connect_and_publish(
                    &cfg,
                    &focus,
                    &ghost_mxid,
                    &device_id,
                    media_key,
                    attempt.clone(),
                    disconnected.clone(),
                )
                .await
                {
                    Ok((s, t)) => {
                        source = s;
                        track = t;
                        break;
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, sfu = %focus.service_url, "ghost LiveKit reconnect failed — retrying");
                    }
                }
            }
            // Frames buffered while the room was down are stale audio —
            // drop them so the rejoined track starts live, not delayed.
            while frame_rx.try_recv().is_ok() {}
            if migrating {
                // Advertise the new SFU: re-post membership (same state key, same
                // created_ts) so every client resolves our transport to where the
                // track now lives. Drop the previous poster first.
                membership_handle.abort();
                membership_handle = tokio::spawn(
                    keep_membership_posted(
                        cfg.clone(),
                        ghost_mxid.clone(),
                        device_id.clone(),
                        focus.clone(),
                        created_ts,
                        cancel.clone(),
                    )
                    .instrument(tracing::Span::current()),
                );
                tracing::info!(sfu = %focus.service_url, "ghost migrated — publishing on the elected SFU");
            } else {
                tracing::info!("ghost LiveKit reconnected — resuming publish");
            }
        }

        // 7. Teardown. The membership keeper runs until `cancel` fires (it also
        // refreshes the expiry hourly), so make sure it has — a `SourceClosed`
        // exit reaches here with `cancel` still live — then await it so a post in
        // flight lands BEFORE the leave below and can't linger as a phantom.
        cancel.cancel();
        let _ = membership_handle.await;
        // Retract call membership (best-effort; a no-op if we never joined). The
        // LiveKit room is closed by the event-drain task once `cancel` fires.
        if let Err(e) = post_membership(&cfg, &ghost_mxid, &device_id, &focus, true).await {
            tracing::warn!(error = format!("{e:#}"), "ghost post_membership(leave) failed — the orphan sweep will retry");
        }
        tracing::info!("ghost retired");
    }
    .instrument(span)
    .await;
}

/// Cancel every stream device and await their teardown (each retracts its membership on the way out).
async fn drain_streams(streams: &mut HashMap<String, StreamHandle>) {
    let draining: Vec<_> = streams.drain().collect();
    for (_key, h) in &draining {
        h.cancel.cancel();
    }
    futures_util::future::join_all(draining.into_iter().map(|(_key, h)| h.join)).await;
}

/// How often a stream device's LiveKit token is replaced (lk-jwt tokens live one hour).
const STREAM_TOKEN_REFRESH: Duration = Duration::from_secs(50 * 60);

/// OpenID as the ghost → lk-jwt for the stream device → the transport a companion connects with.
async fn fetch_stream_transport(
    cfg: &GhostConfig,
    focus: &LivekitFocusInfo,
    ghost_mxid: &str,
    device_id: &str,
) -> Result<StreamTransport> {
    let openid_token = fetch_openid_token(cfg, ghost_mxid).await?;
    let jwt = lk_jwt::fetch_livekit_token(
        &cfg.http,
        cfg.lk_jwt_url.as_deref().unwrap_or(&focus.service_url),
        &cfg.room_id,
        device_id,
        &openid_token,
    )
    .await?;
    let (identity, _room) = decode_jwt_claims(&jwt.jwt)?;
    let fetched_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    Ok(StreamTransport { sfu_url: cfg.lk_url.clone().unwrap_or(jwt.url), jwt: jwt.jwt, identity, fetched_at })
}

/// A companion stream device on a ghost (ADR 0004 §5): the bridge does the Matrix half — the device's LiveKit token
/// (refreshed, re-fetched on the elected SFU when the election moves), its media key and key distribution (the
/// ghost's own Olm device sends it under the stream device's claimed device id), and its call membership, posted
/// once the companion reports its track live and kept until the lease ends. The companion publishes; the bridge
/// never touches the media.
#[allow(clippy::too_many_arguments)]
async fn run_stream_device(
    cfg: GhostConfig,
    default_focus: LivekitFocusInfo,
    client: Client,
    members_gen_rx: watch::Receiver<u64>,
    mut call_focus_rx: watch::Receiver<Option<LivekitFocusInfo>>,
    key: String,
    cancel: CancellationToken,
    crypto_cache: GhostCryptoCache,
    reply: oneshot::Sender<Result<StreamGrant, AttachError>>,
) {
    let ghost_mxid = format!("@{}{key}:{}", cfg.ghost_localpart_prefix, cfg.server_name);
    let localpart = format!("{}{key}", cfg.ghost_localpart_prefix);
    let device_id = stream_device_id(&key, &cfg.ghost_device_salt);
    // A fresh key per lease: the stream device is a new participant every time.
    let media_key = cfg.media_e2ee.then(keys::generate_key);
    let span = tracing::info_span!("stream", mxid = %ghost_mxid, device = %device_id);
    async {
        // The ghost is live (the driver checked); joining again is idempotent and covers a ghost still mid-join.
        if let Err(e) = join_room(&cfg, &ghost_mxid).await {
            tracing::warn!(error = %format_args!("{e:#}"), "stream device: ghost not joined");
            let _ = reply.send(Err("ghost_unavailable"));
            return;
        }
        let mut focus = elected_focus_or(&client, &cfg, &default_focus).await;
        let transport = match fetch_stream_transport(&cfg, &focus, &ghost_mxid, &device_id).await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %format_args!("{e:#}"), sfu = %focus.service_url, "stream device: no LiveKit token");
                let _ = reply.send(Err("sfu_unavailable"));
                return;
            }
        };
        let (transport_tx, transport_rx) = watch::channel(Some(transport));
        if let Some(k) = media_key {
            tracing::info!(key_fp = %keys::key_fp(&k), "stream device media key generated");
            tokio::spawn(
                distribute_key_task(
                    cfg.clone(),
                    localpart.clone(),
                    ghost_mxid.clone(),
                    device_id.clone(),
                    k,
                    members_gen_rx,
                    cancel.clone(),
                    crypto_cache,
                )
                .instrument(tracing::Span::current()),
            );
        }
        let live = Arc::new(Notify::new());
        let grant = StreamGrant {
            ghost_mxid: ghost_mxid.clone(),
            device_id: device_id.clone(),
            e2ee: media_key.is_some(),
            key: media_key,
            transport: transport_rx,
            live: live.clone(),
        };
        if reply.send(Ok(grant)).is_err() {
            return; // the requester gave up; nothing was posted
        }
        tracing::info!(sfu = %focus.service_url, "stream device attached — waiting for the companion's track");
        let created_ts = Some(MilliSecondsSinceUnixEpoch::now());
        let mut membership: Option<tokio::task::JoinHandle<()>> = None;
        let mut refresh = tokio::time::interval_at(tokio::time::Instant::now() + STREAM_TOKEN_REFRESH, STREAM_TOKEN_REFRESH);
        let mut focus_open = true;
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                _ = live.notified(), if membership.is_none() => {
                    membership = Some(tokio::spawn(
                        keep_membership_posted(cfg.clone(), ghost_mxid.clone(), device_id.clone(), focus.clone(), created_ts, cancel.clone())
                            .instrument(tracing::Span::current()),
                    ));
                    tracing::info!("stream device live — membership posting");
                }
                _ = refresh.tick() => match fetch_stream_transport(&cfg, &focus, &ghost_mxid, &device_id).await {
                    Ok(t) => { transport_tx.send_replace(Some(t)); }
                    Err(e) => tracing::warn!(error = %format_args!("{e:#}"), "stream device token refresh failed — retrying at the next tick"),
                },
                r = call_focus_rx.changed(), if focus_open => {
                    if r.is_err() { focus_open = false; continue; }
                    let new_focus = elected_focus_or(&client, &cfg, &default_focus).await;
                    if super::focus::normalize_service_url(&new_focus.service_url)
                        == super::focus::normalize_service_url(&focus.service_url)
                    {
                        continue;
                    }
                    tracing::info!(from = %focus.service_url, to = %new_focus.service_url, "call-focus election moved — moving the stream device");
                    focus = new_focus;
                    match fetch_stream_transport(&cfg, &focus, &ghost_mxid, &device_id).await {
                        Ok(t) => { transport_tx.send_replace(Some(t)); }
                        Err(e) => tracing::warn!(error = %format_args!("{e:#}"), "stream device: no token on the new SFU"),
                    }
                    if let Some(h) = membership.take() {
                        h.abort();
                        membership = Some(tokio::spawn(
                            keep_membership_posted(cfg.clone(), ghost_mxid.clone(), device_id.clone(), focus.clone(), created_ts, cancel.clone())
                                .instrument(tracing::Span::current()),
                        ));
                    }
                }
            }
        }
        // `cancel` has fired: the keeper settles; await it so a post in flight lands before the leave.
        if let Some(h) = membership.take() {
            let _ = h.await;
            if let Err(e) = post_membership(&cfg, &ghost_mxid, &device_id, &focus, true).await {
                tracing::warn!(error = format!("{e:#}"), "stream device leave failed — the orphan sweep will retry");
            }
        }
        transport_tx.send_replace(None);
        tracing::info!("stream device detached");
    }
    .instrument(span)
    .await;
}

impl StreamProvider for GhostPool {
    fn attach(&self, discord_user_id: u64, cancel: CancellationToken) -> AttachReply {
        let (reply, rx) = oneshot::channel();
        // A stopped driver drops `reply`; the hub reads that as "bridge not running".
        let _ = self.cmd_tx.send(GhostCommand::AttachStream { key: discord_user_id.to_string(), cancel, reply });
        rx
    }
}

/// Why the ghost's audio pump stopped — drives `run_ghost`'s retire-vs-reconnect
/// decision (#66).
#[derive(Debug, PartialEq, Eq)]
enum PumpExit {
    /// The ghost is retiring (`cancel` fired).
    Cancelled,
    /// The Discord side dropped the speaker's sink (frame channel closed) — a
    /// legitimate retire, not a fault.
    SourceClosed,
    /// The LiveKit room died (the drain task fired this attempt's `disconnected`
    /// token) — reconnect and pump again.
    Disconnected,
    /// The call-focus election watch changed to a different SFU than the one we
    /// publish on — re-read state and migrate if it really moved.
    FocusChanged,
}

/// Drain the ghost's frame channel through a jitter buffer into its LiveKit
/// source as 10 ms mono frames. Mirrors the bridge bot's former audio consumer,
/// one per ghost. Returns when cancelled, when the Discord side drops the
/// sender, or when the room dies ([`PumpExit`] says which). Takes the receivers
/// by `&mut` so a reconnect can re-enter with the same channels; note a re-entry
/// with `capture_wav` truncates and rewrites `publish_<key>.wav` (diagnostic tap,
/// acceptable).
///
/// Diagnostics (issue #32, always on): a periodic per-ghost health line (frames
/// in/out, jitter overflow drops + depth, `capture_frame` errors, RMS) and a
/// frame-shape warning. When `capture_wav` is set it also writes "Tap B" — the
/// exact mono PCM handed to LiveKit — to `publish_<key>.wav`.
#[allow(clippy::too_many_arguments)]
async fn pump_audio(
    frame_rx: &mut mpsc::Receiver<Vec<i16>>,
    source: &NativeAudioSource,
    track: &LocalAudioTrack,
    mute_rx: &mut watch::Receiver<bool>,
    focus_rx: &mut watch::Receiver<Option<LivekitFocusInfo>>,
    current_focus: &LivekitFocusInfo,
    cancel: &CancellationToken,
    disconnected: &CancellationToken,
    capture_wav: bool,
    key: &str,
) -> PumpExit {
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
    let exit = loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break PumpExit::Cancelled,
            _ = disconnected.cancelled() => break PumpExit::Disconnected,
            changed = changed_or_park(mute_rx, cancel) => {
                // Only a real change reaches here as `true`; a dropped sender
                // without our cancel firing parks on cancel (returns `false`)
                // rather than busy-spinning on the closed channel (#67), and the
                // biased cancel arm exits the pump on the next poll.
                if changed {
                    let muted = *mute_rx.borrow_and_update();
                    apply_track_mute(track, muted);
                    tracing::info!(muted, "ghost track mute mirrored from Discord");
                }
            }
            changed = changed_or_park(focus_rx, cancel) => {
                // The election moved. Only a different SFU is worth a migration;
                // `run_ghost` re-reads room state before acting on it.
                if changed {
                    let moved = focus_rx.borrow_and_update().as_ref().is_some_and(|f| {
                        super::focus::normalize_service_url(&f.service_url)
                            != super::focus::normalize_service_url(&current_focus.service_url)
                    });
                    if moved {
                        break PumpExit::FocusChanged;
                    }
                }
            }
            frame = frame_rx.recv() => {
                let Some(frame20) = frame else { break PumpExit::SourceClosed };
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
    };

    // Final health snapshot + finalize Tap B so the WAV header is valid.
    tracing::info!(
        key,
        frames_in,
        frames_out,
        jitter_dropped = jitter.dropped_samples(),
        capture_errs,
        shape_warns,
        exit = ?exit,
        "ghost audio pump exiting"
    );
    if let Some(w) = tap_b {
        match w.finalize() {
            Ok(()) => tracing::info!(key, "publish-tap WAV (Tap B) finalised"),
            Err(e) => tracing::warn!(key, error = %e, "publish-tap WAV finalise failed"),
        }
    }
    exit
}

/// Await the next value on a `watch` receiver, parking on `cancel` if the sender
/// has been dropped.
///
/// `watch::Receiver::changed()` returns Err *immediately and on every call* once
/// the sender is gone. The graceful teardown paths drop a ghost's `mute_tx` /
/// `call_active_tx` only AFTER cancelling it, so the biased `cancel` arm wins and
/// this is never reached — but a sender that drops WITHOUT the ghost's cancel
/// firing (the bridge-fatal error path that detaches a ghost, #67) would turn a
/// bare `res = rx.changed()` select arm into a 100% CPU busy-spin. Returning
/// `true` only on a real change and otherwise parking on `cancel` (which fires
/// imminently once the ghost pool drains) keeps the select cheap. Mirrors
/// [`distribute_key_loop`]'s handling of a closed `members_gen`.
async fn changed_or_park<T>(rx: &mut watch::Receiver<T>, cancel: &CancellationToken) -> bool {
    match rx.changed().await {
        Ok(()) => true,
        Err(_) => {
            cancel.cancelled().await;
            false
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
/// drain task (kept up until `cancel` or room death, then closed). `disconnected`
/// is this attempt's death signal: the drain fires it when the room dies so
/// `run_ghost` can reconnect (#66).
#[allow(clippy::too_many_arguments)]
async fn connect_and_publish(
    cfg: &GhostConfig,
    focus: &LivekitFocusInfo,
    ghost_mxid: &str,
    device_id: &str,
    media_key: Option<[u8; 16]>,
    cancel: CancellationToken,
    disconnected: CancellationToken,
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
    // the drain ends — ghost retired (`cancel`) or room death (which also fires
    // `disconnected` so run_ghost reconnects, #66).
    tokio::spawn(async move {
        drain_room_events(events, cancel, disconnected).await;
        let _ = room.close().await;
        tracing::debug!("ghost LiveKit room closed");
    });

    Ok((source, track))
}

/// Drain a ghost room's LiveKit events until the ghost retires (`cancel` — return
/// without signalling) or the room dies. Room death — a terminal `Disconnected`
/// (the SDK only emits it once its internal auto-reconnect is exhausted) or the
/// event stream ending — fires `disconnected` so `run_ghost`'s reconnect loop can
/// act (#66). Extracted from the spawn wrapper (which owns the `Room` and closes
/// it after this returns) so the signalling is unit-testable.
async fn drain_room_events(
    mut events: mpsc::UnboundedReceiver<livekit::RoomEvent>,
    cancel: CancellationToken,
    disconnected: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
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
                // #116: SFU-reported link quality — for the ghost's OWN uplink
                // (identity = this ghost) and any participant it observes.
                // Change-driven, so unthrottled logging is safe.
                Some(livekit::RoomEvent::ConnectionQualityChanged { quality, participant }) => {
                    use livekit::prelude::ConnectionQuality as Q;
                    let identity = participant.identity();
                    if matches!(quality, Q::Poor | Q::Lost) {
                        tracing::warn!(identity = %identity, ?quality, "ghost LiveKit connection quality degraded (#116)");
                    } else {
                        tracing::info!(identity = %identity, ?quality, "ghost LiveKit connection quality changed");
                    }
                }
                Some(other) => tracing::trace!(event = ?other, "ghost LiveKit room event"),
                None => break,
            },
        }
    }
    disconnected.cancel();
}

// ─── E2EE media-key distribution (Path 2 plaintext / Path 3 Olm) ────────────────

/// Number of key re-sends in the burst that follows a membership change.
const KEY_BURST_COUNT: u32 = 6;
/// Spacing between burst re-sends (~`COUNT * INTERVAL` = the covered window).
const KEY_BURST_INTERVAL: Duration = Duration::from_secs(3);
/// Steady-state re-send period once a burst is done — a slow safety net for a key
/// dropped even by the burst. Mild (≈1/ghost/min) vs. the old 5s spam.
const KEY_HEARTBEAT: Duration = Duration::from_secs(60);

/// Historical minimum spacing between Olm session rotations (#52) — the
/// production default now lives in `TimingConfig` (byte-identical 90 s) and is
/// threaded in as `distribute_key_loop`'s `rotate_min_interval` (so the
/// compressed test profile can shrink it). Retained purely as the value the
/// unit tests + test helpers pass, hence `#[cfg(test)]`. Membership-driven
/// rotations are rate-limited to this so Element Call's ~90 s membership flaps
/// don't churn one-time keys on every flap.
#[cfg(test)]
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
    // Capture before `cfg` moves into the recipient source below.
    let rotate_min_interval = cfg.session_rotate_min_interval;
    let source: Arc<dyn RecipientSource> = Arc::new(CallRecipients { cfg, ghost_mxid });
    distribute_key_loop(
        source,
        key,
        transport,
        rotate_sessions,
        rotate_min_interval,
        members_gen,
        cancel,
    )
    .await;
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
    rotate_min_interval: Duration,
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
                        let reason = if rotate_pending && elapsed >= rotate_min_interval {
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

/// Set the ghost's display name (masqueraded). A no-op when the profile already
/// carries `name`: every profile PUT fans out as a fresh `m.room.member` event into
/// every room the ghost has joined, so re-setting an unchanged name on each assign
/// buried bridged rooms under thousands of "updated their profile" events.
async fn set_displayname(cfg: &GhostConfig, ghost_mxid: &str, name: &str) -> Result<()> {
    let current = get_profile_field(cfg, ghost_mxid, "displayname").await;
    if current.ok().flatten().as_deref() == Some(name) {
        tracing::debug!("ghost display name unchanged — not re-set");
        return Ok(());
    }
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

/// Read one field of the ghost's profile; `None` when it is unset (404).
async fn get_profile_field(
    cfg: &GhostConfig,
    ghost_mxid: &str,
    field: &str,
) -> Result<Option<String>> {
    let url = cs_url(
        &cfg.homeserver_url,
        &["profile", ghost_mxid, field],
        Some(ghost_mxid),
    )?;
    let resp = cfg
        .http
        .get(url)
        .bearer_auth(&cfg.as_token)
        .send()
        .await
        .with_context(|| format!("GET /profile/{field}"))?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !resp.status().is_success() {
        return Err(anyhow!("get {field} failed: {}", err_body(resp).await));
    }
    let body: serde_json::Value = resp
        .json()
        .await
        .with_context(|| format!("decoding /profile/{field}"))?;
    Ok(body.get(field).and_then(|v| v.as_str()).map(str::to_owned))
}

/// Which `mxc://` URIs were uploaded for which Discord avatar URL — process-wide
/// (ghost MXIDs are shared by every bridge on one appservice, so a per-bridge view
/// would have two bridges re-uploading over each other) and persisted per store at
/// `<store_path>/ghosts/avatars.json` so a restart doesn't re-upload either.
/// A Discord avatar URL embeds the avatar hash, so it changes exactly when the
/// picture does.
#[derive(Default)]
struct AvatarUploads {
    by_url: HashMap<String, HashSet<String>>,
    loaded_stores: HashSet<String>,
}

static AVATAR_UPLOADS: std::sync::LazyLock<tokio::sync::Mutex<AvatarUploads>> =
    std::sync::LazyLock::new(Default::default);

fn avatar_uploads_path(store_path: &str) -> Option<std::path::PathBuf> {
    (!store_path.is_empty())
        .then(|| std::path::Path::new(store_path).join("ghosts").join("avatars.json"))
}

impl AvatarUploads {
    /// Merge a store's persisted uploads in, once per store. A missing or
    /// unreadable file just means "nothing known yet" (worst case: one re-upload).
    fn load_store(&mut self, store_path: &str) {
        if !self.loaded_stores.insert(store_path.to_owned()) {
            return;
        }
        let Some(path) = avatar_uploads_path(store_path) else { return };
        let Ok(bytes) = std::fs::read(&path) else { return };
        match serde_json::from_slice::<HashMap<String, HashSet<String>>>(&bytes) {
            Ok(map) => {
                for (url, mxcs) in map {
                    self.by_url.entry(url).or_default().extend(mxcs);
                }
            }
            Err(e) => tracing::warn!(
                error = %e,
                path = %path.display(),
                "ignoring unreadable ghost avatar cache"
            ),
        }
    }

    fn knows(&self, discord_url: &str, mxc: &str) -> bool {
        self.by_url.get(discord_url).is_some_and(|m| m.contains(mxc))
    }

    /// Record an upload and persist the (merged) map to `store_path`.
    fn record(&mut self, store_path: &str, discord_url: &str, mxc: &str) {
        self.by_url.entry(discord_url.to_owned()).or_default().insert(mxc.to_owned());
        let Some(path) = avatar_uploads_path(store_path) else { return };
        let write = || -> std::io::Result<()> {
            if let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir)?;
            }
            let tmp = path.with_extension("json.tmp");
            std::fs::write(&tmp, serde_json::to_vec(&self.by_url)?)?;
            std::fs::rename(&tmp, &path)
        };
        if let Err(e) = write() {
            tracing::warn!(
                error = %e,
                path = %path.display(),
                "could not persist ghost avatar cache"
            );
        }
    }
}

/// Mirror a Discord avatar onto the ghost: download it from Discord's CDN, upload
/// it to the homeserver media repo (masqueraded), and set the ghost's avatar_url.
/// Skipped when the profile already points at an upload of this same Discord
/// avatar — a re-upload mints a new `mxc://`, i.e. an "updated their avatar"
/// member event in every room the ghost is in, on every assign.
async fn set_avatar(cfg: &GhostConfig, ghost_mxid: &str, discord_avatar_url: &str) -> Result<()> {
    if let Ok(Some(current)) = get_profile_field(cfg, ghost_mxid, "avatar_url").await {
        let mut uploads = AVATAR_UPLOADS.lock().await;
        uploads.load_store(&cfg.store_path);
        if uploads.knows(discord_avatar_url, &current) {
            tracing::debug!("ghost avatar unchanged — not re-uploaded");
            return Ok(());
        }
    }
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
    AVATAR_UPLOADS.lock().await.record(&cfg.store_path, discord_avatar_url, &mxc);
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

// Expiry helpers are shared with the bridge bot's own roster membership — see
// `rtc::membership::{membership_expires, MEMBERSHIP_EXPIRES, MEMBERSHIP_REFRESH}`.
use super::membership::{MEMBERSHIP_REFRESH, membership_expires};

/// Keep the ghost's `m.call.member` join posted until `cancel` fires: post it
/// (retrying with backoff until it lands — self-heals e.g. a room whose
/// `org.matrix.msc3401.call.member` power level is wrong, then fixed), then
/// re-post every [`MEMBERSHIP_REFRESH`] with a fresh `expires` so a long-present
/// ghost never ages out of the call. Each post computes `expires` from the fixed
/// `created_ts` (see [`membership_expires`]). Exits promptly on `cancel`; a post
/// in flight completes first, so a caller that awaits this before posting its
/// leave never has a stale join land after it.
async fn keep_membership_posted(
    cfg: GhostConfig,
    ghost_mxid: String,
    device_id: String,
    focus: LivekitFocusInfo,
    created_ts: Option<MilliSecondsSinceUnixEpoch>,
    cancel: CancellationToken,
) {
    let mut first = true;
    loop {
        // Post (with retry) until it lands or we are cancelled.
        let mut backoff = Duration::from_secs(2);
        let max = Duration::from_secs(30);
        loop {
            match post_membership_at(&cfg, &ghost_mxid, &device_id, &focus, created_ts, false).await {
                Ok(()) => {
                    if first {
                        tracing::info!(sfu = %focus.service_url, "ghost call membership posted — visible in call");
                    } else {
                        tracing::info!(sfu = %focus.service_url, "ghost call membership refreshed (expiry extended)");
                    }
                    first = false;
                    break;
                }
                // `{e:#}`: the full chain, so a transport failure (connect /
                // timeout) is distinguishable from an HTTP 403 in the journal.
                Err(e) => tracing::warn!(
                    error = %format_args!("{e:#}"),
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
        // Landed — sleep until the next refresh (or retire).
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(MEMBERSHIP_REFRESH) => {}
        }
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
    post_membership_at(cfg, ghost_mxid, device_id, focus, None, leave).await
}

/// [`post_membership`] with an explicit `created_ts`, so a re-post (presence →
/// active, focus migration) keeps the membership's original age — the age is
/// what the call-focus election is decided on.
async fn post_membership_at(
    cfg: &GhostConfig,
    ghost_mxid: &str,
    device_id: &str,
    focus: &LivekitFocusInfo,
    created_ts: Option<MilliSecondsSinceUnixEpoch>,
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
        // A fixed `created_ts` needs a growing `expires`, or re-posts inherit the
        // original expiry (see `membership_expires`). Without `created_ts` the
        // server timestamp is the age and ruma's 4 h default applies from it.
        let now_ms = u64::from(MilliSecondsSinceUnixEpoch::now().get());
        let expires = created_ts.map(|c| membership_expires(u64::from(c.get()), now_ms));
        build_membership(&device, focus, created_ts, expires)
    };
    put_call_member_state(cfg, ghost_mxid, state_key.as_ref(), &content).await
}

/// The SFU this ghost should publish on: the call's elected focus read from room
/// state (`crate::matrix::snapshot_call_memberships`), else `default` (ours).
async fn elected_focus_or(client: &Client, cfg: &GhostConfig, default: &LivekitFocusInfo) -> LivekitFocusInfo {
    let (Ok(room_id), Ok(bot)) = (RoomId::parse(&cfg.room_id), UserId::parse(&cfg.bot_user_id)) else {
        return default.clone();
    };
    match crate::matrix::snapshot_call_memberships(client, &room_id, &bot, &cfg.ghost_localpart_prefix).await {
        Ok(snap) => snap.elected.unwrap_or_else(|| default.clone()),
        Err(e) => {
            tracing::warn!(error = %e, "ghost: could not read call memberships for the focus election — using our SFU");
            default.clone()
        }
    }
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
///
/// `focus_active` is `multi_sfu` (see `rtc::focus`): the ghost publishes on the
/// bridge's SFU regardless of who is the oldest member, and a multi-SFU client
/// must be told to look for it there — with `oldest_membership` a matrix.org
/// user who started the call would resolve our transport to *their* SFU and
/// never find the ghost's track (live-observed 2026-09-28, Limited-VC).
fn build_membership(
    device_id: &OwnedDeviceId,
    focus: &LivekitFocusInfo,
    created_ts: Option<MilliSecondsSinceUnixEpoch>,
    expires: Option<Duration>,
) -> CallMemberEventContent {
    let foci_preferred = vec![Focus::Livekit(LivekitFocus::new(
        focus.alias.clone(),
        focus.service_url.clone(),
    ))];
    CallMemberEventContent::new(
        Application::Call(CallApplicationContent::new(String::new(), CallScope::Room)),
        device_id.clone(),
        super::focus::bridge_active_focus(),
        foci_preferred,
        created_ts,
        expires,
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
    use super::super::membership::MEMBERSHIP_EXPIRES;
    use matrix_sdk::ruma::events::call::member::{ActiveFocus, ActiveLivekitFocus};

    fn test_pool(max_ghosts: Option<usize>) -> Arc<GhostPool> {
        let matrix_cfg = MatrixConfig {
            homeserver_url: "https://nether.im".into(),
            room_id: "!r:nether.im".into(),
            bot_user_id: "@bridge:nether.im".into(),
            ..MatrixConfig::default()
        };
        GhostPool::new(
            &matrix_cfg,
            &LiveKitConfig::default(),
            false,
            max_ghosts,
            SESSION_ROTATE_MIN_INTERVAL,
        )
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
    fn avatar_uploads_persist_across_loads() {
        let dir = std::env::temp_dir().join(format!("nvb-avatar-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let store = dir.to_string_lossy().into_owned();
        let url = "https://cdn.discordapp.com/avatars/1/abc.png?size=1024";

        let mut a = AvatarUploads::default();
        a.load_store(&store);
        assert!(!a.knows(url, "mxc://nether.im/one"));
        a.record(&store, url, "mxc://nether.im/one");
        assert!(a.knows(url, "mxc://nether.im/one"));

        // A fresh process (or another bridge's view) reloads it from the store.
        let mut b = AvatarUploads::default();
        b.load_store(&store);
        assert!(b.knows(url, "mxc://nether.im/one"));
        assert!(!b.knows(url, "mxc://nether.im/other"));
        let changed = "https://cdn.discordapp.com/avatars/1/def.png?size=1024";
        assert!(!b.knows(changed, "mxc://nether.im/one"));

        // No store ⇒ in-memory only, never a write.
        let mut c = AvatarUploads::default();
        c.record("", url, "mxc://nether.im/two");
        assert!(c.knows(url, "mxc://nether.im/two"));
        let _ = std::fs::remove_dir_all(&dir);
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

    fn obs(sender: &str, state_key: &str, active: bool) -> ObservedGhostMembership {
        ObservedGhostMembership { sender: sender.into(), state_key: state_key.into(), active }
    }

    #[test]
    fn orphan_sweep_takes_only_our_dead_ghosts() {
        let p = "nether-voicebridge_";
        let live: HashSet<String> = ["111".to_owned()].into();
        let observed = vec![
            // Live ghost in this pool — never touched, even if expired.
            obs("@nether-voicebridge_111:x", "_@nether-voicebridge_111:x_D111-salt_m.call", false),
            // Ours (salted), ghost gone, still unexpired — the Merlin case.
            obs("@nether-voicebridge_222:x", "_@nether-voicebridge_222:x_D222-salt_m.call", true),
            // Another bridge's live ghost in the same room (different salt).
            obs("@nether-voicebridge_333:x", "_@nether-voicebridge_333:x_D333-other_m.call", true),
            // Expired, whoever wrote it.
            obs("@nether-voicebridge_444:x", "_@nether-voicebridge_444:x_D444-other_m.call", false),
            // The bridge bot and a real user are never ghosts.
            obs("@nether-voicebridge_bot:x", "_@nether-voicebridge_bot:x_DEV_m.call", false),
            obs("@dark:x", "_@dark:x_ABC_m.call", false),
        ];
        let got: Vec<String> = orphan_ghost_memberships(&observed, &live, &HashSet::new(), p, "salt")
            .into_iter()
            .map(|m| m.sender)
            .collect();
        assert_eq!(got, ["@nether-voicebridge_222:x", "@nether-voicebridge_444:x"]);
    }

    #[test]
    fn orphan_sweep_without_salt_only_takes_expired() {
        let p = "nether-voicebridge_";
        let observed = vec![
            obs("@nether-voicebridge_222:x", "_@nether-voicebridge_222:x_D222_m.call", true),
            obs("@nether-voicebridge_444:x", "_@nether-voicebridge_444:x_D444_m.call", false),
        ];
        let got: Vec<String> = orphan_ghost_memberships(&observed, &HashSet::new(), &HashSet::new(), p, "")
            .into_iter()
            .map(|m| m.sender)
            .collect();
        assert_eq!(got, ["@nether-voicebridge_444:x"]);
    }

    #[tokio::test]
    async fn changed_or_park_returns_true_on_real_change() {
        let (tx, mut rx) = watch::channel(false);
        let cancel = CancellationToken::new();
        tx.send(true).unwrap();
        assert!(changed_or_park(&mut rx, &cancel).await);
    }

    #[tokio::test]
    async fn changed_or_park_parks_on_dropped_sender_until_cancel() {
        // The #67 hazard: sender dropped WITHOUT cancel → `changed()` returns Err
        // on every call. The helper must PARK (neither spin nor return) until
        // cancel fires — otherwise the ghost's mute/presence select busy-spins.
        let (tx, mut rx) = watch::channel(false);
        let cancel = CancellationToken::new();
        drop(tx);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), changed_or_park(&mut rx, &cancel))
                .await
                .is_err(),
            "must park on a dropped sender, not spin or return early"
        );
        // Once cancel fires it exits with `false` (a teardown, not a real change).
        cancel.cancel();
        assert!(!changed_or_park(&mut rx, &cancel).await);
    }

    #[test]
    fn membership_expires_grows_with_age_so_created_ts_can_stay_fixed() {
        // Fresh post: plain 4 h.
        assert_eq!(membership_expires(1_000, 1_000), MEMBERSHIP_EXPIRES);
        // Ten hours after created_ts (huxley): expiry must still reach 4 h past NOW.
        let ten_h = 10 * 3600 * 1000;
        assert_eq!(membership_expires(1_000, 1_000 + ten_h), Duration::from_millis(ten_h) + MEMBERSHIP_EXPIRES);
        // Clock skew (created in the future) never underflows.
        assert_eq!(membership_expires(5_000, 1_000), MEMBERSHIP_EXPIRES);
    }

    #[test]
    fn build_membership_carries_created_ts_and_expires_on_the_wire() {
        let focus = LivekitFocusInfo { alias: "!r:x".into(), service_url: "https://lk".into() };
        let created = MilliSecondsSinceUnixEpoch(1_700_000_000_000u64.try_into().unwrap());
        let c = build_membership(&"D1".into(), &focus, Some(created), Some(Duration::from_millis(36_000_000)));
        let v = serde_json::to_value(&c).unwrap();
        assert_eq!(v["created_ts"], 1_700_000_000_000u64);
        assert_eq!(v["expires"], 36_000_000u64);
        assert_eq!(v["focus_active"]["focus_selection"], "oldest_membership");
        // The ruma default when nothing is pinned (presence posts before 0.3.8 shape).
        let d = build_membership(&"D1".into(), &focus, None, None);
        let v = serde_json::to_value(&d).unwrap();
        assert_eq!(v["expires"], 14_400_000u64);
        assert!(v.get("created_ts").is_none());
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

    // ── drain_room_events disconnect signalling (#66) ───────────────────────────

    #[tokio::test]
    async fn ghost_drain_fires_disconnected_on_room_death() {
        let (tx, rx) = mpsc::unbounded_channel();
        let disconnected = CancellationToken::new();
        tokio::spawn(drain_room_events(
            rx,
            CancellationToken::new(),
            disconnected.clone(),
        ));
        tx.send(livekit::RoomEvent::Disconnected {
            reason: livekit::DisconnectReason::UnknownReason,
        })
        .expect("send");
        tokio::time::timeout(Duration::from_secs(1), disconnected.cancelled())
            .await
            .expect("disconnected token must fire on RoomEvent::Disconnected");
    }

    #[tokio::test]
    async fn ghost_drain_fires_disconnected_on_stream_end() {
        let (tx, rx) = mpsc::unbounded_channel::<livekit::RoomEvent>();
        let disconnected = CancellationToken::new();
        tokio::spawn(drain_room_events(
            rx,
            CancellationToken::new(),
            disconnected.clone(),
        ));
        drop(tx); // room object dropped/closed → event channel closes
        tokio::time::timeout(Duration::from_secs(1), disconnected.cancelled())
            .await
            .expect("disconnected token must fire when the event stream ends");
    }

    #[tokio::test]
    async fn ghost_drain_cancel_does_not_fire_disconnected() {
        let (tx, rx) = mpsc::unbounded_channel::<livekit::RoomEvent>();
        let cancel = CancellationToken::new();
        let disconnected = CancellationToken::new();
        let handle = tokio::spawn(drain_room_events(rx, cancel.clone(), disconnected.clone()));
        cancel.cancel(); // retire — must NOT read as a room death
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("drain must return on cancel")
            .expect("drain task must not panic");
        assert!(!disconnected.is_cancelled());
        drop(tx);
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
            SESSION_ROTATE_MIN_INTERVAL,
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
            SESSION_ROTATE_MIN_INTERVAL,
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
            SESSION_ROTATE_MIN_INTERVAL,
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
            SESSION_ROTATE_MIN_INTERVAL,
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
            SESSION_ROTATE_MIN_INTERVAL,
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
            SESSION_ROTATE_MIN_INTERVAL,
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
            SESSION_ROTATE_MIN_INTERVAL,
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
