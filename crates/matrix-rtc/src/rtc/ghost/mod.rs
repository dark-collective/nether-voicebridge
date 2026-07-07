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

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use nvb_audio::routing::CaptureSink;
use nvb_config::KeyTransport as KeyTransportMode;

mod appservice;
mod identity;
mod keydist;
mod lifecycle;
mod pool;
#[cfg(test)]
mod tests;

// Re-exported so every existing `rtc::ghost::*` path outside this module keeps
// resolving after the split (`key_transport`, `device_prune`, `matrix`,
// `session`, `rtc::mod`, and `lib`). `GhostPool` is `pub` below.
pub(crate) use appservice::{cs_url, err_body};
pub(crate) use identity::{ghost_localpart_prefix, is_bridge_excluded_identity};

// Test-only imports/re-exports so `tests.rs`'s `use super::*` resolves the exact
// names it did in the monolithic module (the split is a pure move; tests are
// unchanged). Absent from non-test builds, so nothing here is an unused import.
#[cfg(test)]
use anyhow::Result;
#[cfg(test)]
use async_trait::async_trait;
#[cfg(test)]
use std::sync::PoisonError;
#[cfg(test)]
use std::time::Duration;
#[cfg(test)]
use nvb_audio::routing::CaptureRouter;
#[cfg(test)]
use nvb_config::{LiveKitConfig, MatrixConfig};
#[cfg(test)]
use super::key_transport::{KeyTransport, PlaintextTransport};
#[cfg(test)]
use super::keys;
#[cfg(test)]
use identity::{is_ghost_identity, is_legacy_ghost_identity};
#[cfg(test)]
use keydist::{
    distribute_key_loop, RecipientSource, KEY_BURST_INTERVAL, SESSION_ROTATE_MAX_AGE,
    SESSION_ROTATE_MIN_INTERVAL,
};
#[cfg(test)]
use lifecycle::ghost_device_id;

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
