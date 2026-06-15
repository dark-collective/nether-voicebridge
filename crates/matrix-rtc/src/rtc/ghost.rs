//! Ghost pool — one appservice Matrix user per Discord speaker (Phase 8).
//!
//! The mirror of the Discord side's `PuppetPool`: where a puppet gives a Matrix
//! speaker a named Discord voice, a **ghost** gives a Discord speaker a named
//! Matrix presence. Each ghost is an appservice user `@discord_<id>:server` that
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
use std::sync::{Arc, Mutex as StdMutex, PoisonError};

use anyhow::{anyhow, Context, Result};
use livekit::webrtc::audio_frame::AudioFrame;
use livekit::webrtc::audio_source::native::NativeAudioSource;
use livekit::{Room, RoomOptions};
use matrix_sdk::ruma::{
    events::call::member::{
        ActiveFocus, ActiveLivekitFocus, Application, CallApplicationContent,
        CallMemberEventContent, CallMemberStateKey, CallScope, Focus, LivekitFocus,
    },
    OwnedDeviceId, OwnedUserId, UserId,
};
use reqwest::Url;
use std::borrow::Cow;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use nvb_audio::jitter::JitterBuffer;
use nvb_audio::routing::{CaptureRouter, CaptureSink};
use nvb_config::{LiveKitConfig, MatrixConfig};

use super::lk_jwt;
use super::openid::OpenIdToken;
use super::publish;
use super::session::decode_jwt_claims;
use super::well_known::{self, LivekitFocusInfo};

/// Localpart prefix for ghost MXIDs (`@discord_<discord_user_id>:server`). Must
/// match the appservice namespace regex (`@discord_.*`).
const GHOST_LOCALPART_PREFIX: &str = "discord_";

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
/// identity is `@discord_<id>:server[:device]`; the localpart is the segment
/// between `@` and the first `:`.
pub(crate) fn is_ghost_identity(identity: &str) -> bool {
    identity
        .strip_prefix('@')
        .and_then(|rest| rest.split(':').next())
        .is_some_and(|localpart| localpart.starts_with(GHOST_LOCALPART_PREFIX))
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
    /// Retire the ghost for `key`.
    Release { key: String },
    /// Retire every ghost (call teardown).
    ReleaseAll,
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
    media_e2ee: bool,
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
                media_e2ee: livekit_cfg.media_e2ee,
            },
        }))
    }

    /// Drive the ghost lifecycle. Spawned/awaited from `lib::run` alongside the
    /// Matrix sync and RTC loops. Returns when the command channel closes.
    #[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %bridge))]
    pub async fn run(self: Arc<Self>, bridge: Arc<str>) -> Result<()> {
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
        let mut ghosts: HashMap<String, CancellationToken> = HashMap::new();

        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                GhostCommand::Assign { key, display_name, avatar_url, frame_rx } => {
                    if ghosts.contains_key(&key) {
                        // Idempotent: a duplicate assign reuses the existing
                        // ghost (the sink was already returned by `assign`).
                        continue;
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
                    ghosts.insert(key.clone(), cancel.clone());
                    let cfg = self.cfg.clone();
                    let focus = focus.clone().expect("focus set above");
                    tokio::spawn(
                        run_ghost(cfg, focus, key, display_name, avatar_url, frame_rx, cancel)
                            .instrument(tracing::info_span!(parent: None, "bridge", name = %bridge)),
                    );
                }
                GhostCommand::Release { key } => {
                    if let Some(cancel) = ghosts.remove(&key) {
                        cancel.cancel();
                    }
                }
                GhostCommand::ReleaseAll => {
                    for (_key, cancel) in ghosts.drain() {
                        cancel.cancel();
                    }
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
    key: String,
    display_name: String,
    avatar_url: Option<String>,
    frame_rx: mpsc::Receiver<Vec<i16>>,
    cancel: CancellationToken,
) {
    if cfg.media_e2ee {
        // Ghost publish with EC media E2EE (per-ghost key generation + to-device
        // distribution) is not implemented; the deployment runs E2EE off.
        tracing::error!(key, "ghost publishing with media_e2ee=true is not supported — skipping ghost");
        return;
    }

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
        // 2. Join the room (required before posting m.call.member).
        if let Err(e) = join_room(&cfg, &ghost_mxid).await {
            tracing::error!(error = %e, "ghost join_room failed — skipping");
            return;
        }
        // 3. OpenID → lk-jwt → connect + publish. The Room handle is moved into
        // the event-drain task, which keeps the connection up and closes it on
        // cancel; we keep only the source to feed.
        let source = match connect_and_publish(&cfg, &focus, &ghost_mxid, &device_id, cancel.clone()).await {
            Ok(source) => source,
            Err(e) => {
                tracing::error!(error = %e, "ghost LiveKit connect/publish failed — skipping");
                return;
            }
        };
        // 4. Post m.call.member join so EC shows the ghost in the call.
        if let Err(e) = post_membership(&cfg, &ghost_mxid, &device_id, &focus, false).await {
            tracing::error!(error = %e, "ghost post_membership(join) failed — skipping");
            // We already connected + published; cancel so the drain task closes
            // the room now rather than leaking a published-but-unmembered ghost.
            cancel.cancel();
            return;
        }
        tracing::info!(display_name = %display_name, "ghost live — publishing Discord speaker audio");

        // 5. Pump frames until cancelled.
        pump_audio(frame_rx, &source, &cancel).await;

        // 6. Teardown: retract call membership (best-effort). The LiveKit room is
        // closed by the event-drain task once `cancel` fires.
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
    cancel: &CancellationToken,
) {
    let mut jitter = JitterBuffer::new(JITTER_TARGET_MS, JITTER_MAX_MS, SAMPLE_RATE);
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
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

/// Connect the ghost to LiveKit (plaintext frames — media E2EE off), publish its
/// audio track, and spawn the room-event drain. Returns the source to feed; the
/// `Room` is owned by the drain task (kept up until `cancel`, then closed).
async fn connect_and_publish(
    cfg: &GhostConfig,
    focus: &LivekitFocusInfo,
    ghost_mxid: &str,
    device_id: &str,
    cancel: CancellationToken,
) -> Result<NativeAudioSource> {
    let openid_token = fetch_openid_token(cfg, ghost_mxid).await?;
    let jwt_resp = lk_jwt::fetch_livekit_token(
        &cfg.http,
        &focus.service_url,
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
    let (room, events) = Room::connect(&jwt_resp.url, &jwt_resp.jwt, room_opts)
        .await
        .context("ghost LiveKit Room::connect")?;
    let source = publish::publish(&room).await?;

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

    Ok(source)
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
fn cs_url(homeserver: &str, segments: &[&str], masquerade: Option<&str>) -> Result<Url> {
    let mut full = vec!["_matrix", "client", "v3"];
    full.extend_from_slice(segments);
    matrix_url(homeserver, &full, masquerade)
}

/// Read an error response body for logging (bearer tokens are never in bodies).
async fn err_body(resp: reqwest::Response) -> String {
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
    let body = serde_json::to_value(&content).context("serializing m.call.member content")?;

    let state_key_str: &str = state_key.as_ref();
    let url = cs_url(
        &cfg.homeserver_url,
        &["rooms", &cfg.room_id, "state", CALL_MEMBER_EVENT_TYPE, state_key_str],
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
        return Err(anyhow!(
            "post m.call.member ({}) failed: {}",
            if leave { "leave" } else { "join" },
            err_body(resp).await
        ));
    }
    Ok(())
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
        assert!(is_ghost_identity("@discord_12345:nether.im"));
        assert!(is_ghost_identity("@discord_12345:nether.im:DEVICE"));
        assert!(!is_ghost_identity("@dark:nether.im"));
        assert!(!is_ghost_identity("@dark:nether.im:DEVICE"));
        assert!(!is_ghost_identity("not-an-mxid"));
    }

    #[test]
    fn cs_url_encodes_segments_and_masquerade() {
        let url = cs_url(
            "https://nether.im",
            &["profile", "@discord_1:nether.im", "displayname"],
            Some("@discord_1:nether.im"),
        )
        .unwrap();
        let s = url.as_str();
        assert!(s.starts_with("https://nether.im/_matrix/client/v3/profile/"));
        // Path segment is percent-encoded (@ → %40, : → %3A).
        assert!(s.contains("%40discord_1%3Anether.im"));
        // Masquerade query is present and encoded.
        assert!(s.contains("user_id=%40discord_1%3Anether.im"));
    }
}
