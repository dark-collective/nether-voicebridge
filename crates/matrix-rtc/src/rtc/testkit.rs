//! #38 e2e testkit — fake Matrix call participants for the live harness.
//!
//! A child module of [`super`] (`rtc::ghost`) so it can reuse the private ghost
//! machinery verbatim instead of duplicating it: the same register/join/
//! membership raw CS-API helpers, the same `connect_and_publish` LiveKit path
//! (E2EE frame crypto included), and the same `distribute_key_loop` burst/
//! heartbeat policy. Compiled only with `--features testkit`; production builds
//! contain none of this.
//!
//! Three participant shapes, all appservice users of the **separate test
//! appservice** (`@nvbtest_*` — never the bridge's own namespace):
//!
//! * [`join_and_publish`] — a **speaker**: joins the room + call, publishes a
//!   LiveKit audio track, and pumps caller-supplied PCM into it. In an
//!   encrypted room it Olm-distributes its media key exactly like Element Call
//!   does, so the bridge's encrypted receive path is exercised end-to-end.
//! * [`join_presence`] — a **phantom**: posts `m.call.member` but never touches
//!   LiveKit. The reaper-feed / stale-member injector (via
//!   [`TestCallHandle::kill`], which dies WITHOUT retracting).
//! * [`join_and_subscribe`] — a **listener**: joins the call and records every
//!   subscribed audio track to memory ([`TrackRecording`]) for the audio
//!   oracle. In an encrypted room it bootstraps a real crypto device (so ghost
//!   key delivery to it succeeds); *decrypting* the frames is deliberately
//!   deferred (D2.5/D3) — encrypted recordings are undecodable noise and must
//!   not be fed to the oracle.
//!
//! Identity notes:
//! * The **call** device id goes into `m.call.member`, the lk-jwt request, and
//!   the key content's `claimed_device_id` — they must agree or Element Call/
//!   the bridge attribute media to the wrong identity.
//! * In an UNENCRYPTED room it is a fresh random mint (`T` + alnum) per join,
//!   so a rerun never presents an old device id with new keys (#30's
//!   `SigningKeyChanged` trap).
//! * In an ENCRYPTED room every participant shape uses its **crypto** device
//!   id (the in-memory device [`GhostCrypto::bootstrap`] mints and uploads
//!   keys for) as the call device id — exactly like a real Element Call
//!   client, whose membership device IS its crypto device. Key senders (the
//!   bridge's ghosts included) target the membership device, so it must be
//!   able to answer `/keys/claim`; a split identity here would leave them
//!   burst-retrying an unclaimable device forever. (Production ghosts split
//!   the two only because their call device must stay stable across respawns
//!   while an in-memory crypto device cannot — a constraint test users don't
//!   have.)

use std::borrow::Cow;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, PoisonError};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use futures_util::StreamExt;
use livekit::track::RemoteTrack;
use livekit::webrtc::audio_frame::AudioFrame;
use livekit::webrtc::audio_stream::native::NativeAudioStream;
use livekit::{Room, RoomEvent, RoomOptions};
use matrix_sdk::ruma::events::call::member::CallMemberEventContent;
use matrix_sdk::ruma::{MilliSecondsSinceUnixEpoch, OwnedUserId, UInt};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use nvb_config::KeyTransport as KeyTransportMode;

use super::super::key_transport::{GhostCrypto, KeyRecipient, KeyTransport, OlmTransport};
use super::super::lk_jwt;
use super::super::session::decode_jwt_claims;
use super::super::well_known::{self, LivekitFocusInfo};
use super::super::keys;
use super::{
    apply_track_mute, connect_and_publish, cs_url, distribute_key_loop, err_body,
    fetch_openid_token, is_bridge_excluded_identity, join_room, post_membership, register_ghost,
    set_displayname, GhostConfig, RecipientSource, CALL_MEMBER_EVENT_TYPE, SAMPLES_10MS,
    SAMPLE_RATE,
};

/// Attempts × backoff for the `m.call.member` join PUT (a room with the wrong
/// `org.matrix.msc3401.call.member` power level fails fast with a clear error
/// instead of the production ghosts' retry-forever loop — a test wants to fail).
const MEMBERSHIP_ATTEMPTS: u32 = 5;
const MEMBERSHIP_RETRY: Duration = Duration::from_secs(2);

/// Everything the testkit needs to reach one room's call. One value per
/// (room, encryption) pair; cheap to clone.
#[derive(Clone)]
pub struct TestCallConfig {
    /// Homeserver base URL (also the `.well-known` discovery base).
    pub homeserver_url: String,
    /// The TEST appservice's `as_token` (the `nvbtest` registration — NOT the
    /// bridge's own token; test users must not live in the bridge namespace).
    pub as_token: String,
    pub room_id: String,
    /// MXID server part (e.g. `nether.im`).
    pub server_name: String,
    /// Whether this room's call runs media E2EE (must match the bridge's own
    /// auto-detection for the room, i.e. the room's `m.room.encryption` state).
    pub media_e2ee: bool,
    /// The BRIDGE's ghost localpart prefix (`{sender_localpart}_`). Bridge
    /// ghosts matching it are excluded from a speaker's key recipients — they
    /// are send-only and keying their (unclaimable) membership devices would
    /// only produce unreachable-device noise.
    pub bridge_ghost_prefix: String,
}

/// A fake Matrix participant identity.
pub struct TestUser {
    localpart: String,
    display_name: String,
}

impl TestUser {
    /// Validated constructor. The localpart's trailing `_`-segment must NOT be
    /// all digits: the bridge classifies `{prefix}<digits>` identities as its
    /// own ghosts (`is_ghost_identity`) and would echo-exclude the test user
    /// from capture/forward — the design doc mandates alphabetic suffixes
    /// (`nvbtest_alice`, never `nvbtest_123`).
    pub fn new(localpart: &str, display_name: &str) -> Result<Self> {
        if localpart.is_empty() || display_name.is_empty() {
            bail!("TestUser localpart/display_name must be non-empty");
        }
        let suffix = localpart.rsplit('_').next().unwrap_or(localpart);
        if !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()) {
            bail!(
                "TestUser localpart '{localpart}' has an all-digit suffix — the bridge would \
                 classify it as a ghost and echo-exclude it; use an alphabetic suffix"
            );
        }
        Ok(Self {
            localpart: localpart.to_owned(),
            display_name: display_name.to_owned(),
        })
    }

    fn mxid(&self, server_name: &str) -> String {
        format!("@{}:{server_name}", self.localpart)
    }
}

/// Mint a fresh call device id (`T` + 8 lowercase-alnum). Random per join so a
/// rerun/rejoin is a NEW device to peers — never an old id with new keys (#30).
fn mint_call_device_id() -> String {
    use rand::Rng as _;
    let mut rng = rand::thread_rng();
    let suffix: String = (0..8)
        .map(|_| {
            let n: u8 = rng.gen_range(0..36);
            if n < 10 {
                (b'0' + n) as char
            } else {
                (b'a' + n - 10) as char
            }
        })
        .collect();
    format!("T{suffix}")
}

/// Build the internal [`GhostConfig`] the reused ghost helpers run on.
/// `bot_user_id` is set to the test user's OWN mxid purely for self-exclusion
/// in any recipient enumeration; the ghost prefix carries the BRIDGE's prefix
/// so its ghosts are excluded wherever the helpers consult it.
fn build_ghost_config(cfg: &TestCallConfig, self_mxid: &str) -> Result<GhostConfig> {
    let http = reqwest::Client::builder()
        .use_rustls_tls()
        // The standing homeserver sits behind Cloudflare, which 403s default
        // library UAs (same workaround as the harness's RoomStatePoller).
        .user_agent("Mozilla/5.0 (compatible; nvb-testkit)")
        .build()
        .context("building testkit HTTP client")?;
    Ok(GhostConfig {
        http,
        homeserver_url: cfg.homeserver_url.trim_end_matches('/').to_owned(),
        well_known_base_url: cfg.homeserver_url.trim_end_matches('/').to_owned(),
        room_id: cfg.room_id.clone(),
        as_token: cfg.as_token.clone(),
        server_name: cfg.server_name.clone(),
        bot_user_id: self_mxid.to_owned(),
        media_e2ee: cfg.media_e2ee,
        key_transport: if cfg.media_e2ee {
            KeyTransportMode::Olm
        } else {
            KeyTransportMode::Plaintext
        },
        ghost_device_salt: String::new(),
        // No store: test crypto devices are ephemeral in-memory mints.
        store_path: String::new(),
        lk_jwt_url: None,
        lk_url: None,
        capture_wav: false,
        ghost_localpart_prefix: cfg.bridge_ghost_prefix.clone(),
        prune_stale_devices: false,
        rotate_ghost_sessions: false,
        max_ghosts: None,
    })
}

/// Register (idempotent) + set display name (best-effort) + join the room.
async fn admit(gcfg: &GhostConfig, user: &TestUser, mxid: &str) -> Result<()> {
    register_ghost(gcfg, &user.localpart)
        .await
        .context("registering test user")?;
    if let Err(e) = set_displayname(gcfg, mxid, &user.display_name).await {
        tracing::warn!(error = %e, mxid, "test user set_displayname failed (continuing)");
    }
    join_room(gcfg, mxid)
        .await
        .context("joining room as test user")?;
    Ok(())
}

/// Post the `m.call.member` join with bounded retries (see the consts above).
async fn post_membership_join(
    gcfg: &GhostConfig,
    mxid: &str,
    device_id: &str,
    focus: &LivekitFocusInfo,
) -> Result<()> {
    let mut last = None;
    for attempt in 1..=MEMBERSHIP_ATTEMPTS {
        match post_membership(gcfg, mxid, device_id, focus, false).await {
            Ok(()) => {
                tracing::info!(mxid, device_id, "test user call membership posted");
                return Ok(());
            }
            Err(e) => {
                tracing::warn!(attempt, error = %e, "test user post_membership failed — retrying");
                last = Some(e);
                tokio::time::sleep(MEMBERSHIP_RETRY).await;
            }
        }
    }
    Err(last.unwrap_or_else(|| anyhow!("post_membership failed"))).context(
        "posting m.call.member as test user (does the room allow \
         org.matrix.msc3401.call.member at PL 0?)",
    )
}

/// Best-effort membership retraction on the way out.
async fn retract_membership(gcfg: &GhostConfig, mxid: &str, device_id: &str, focus: &LivekitFocusInfo) {
    if let Err(e) = post_membership(gcfg, mxid, device_id, focus, true).await {
        tracing::warn!(error = %e, mxid, "test user post_membership(leave) failed");
    }
}

// ─── Speaker / phantom handle ────────────────────────────────────────────────────

/// A live speaker or phantom. Dropping it without [`leave_clean`](Self::leave_clean)
/// / [`kill`](Self::kill) aborts nothing — always consume the handle.
pub struct TestCallHandle {
    mxid: String,
    device_id: String,
    cancel: CancellationToken,
    /// Whether teardown retracts the membership. `kill()` clears it — the
    /// stale-member injector.
    retract: Arc<AtomicBool>,
    mute_tx: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
    /// Keeps the key-distribution loop's `members_gen` sender alive (its drop
    /// would stop the loop's heartbeat re-sends). Speakers in E2EE rooms only.
    _members_gen_tx: Option<watch::Sender<u64>>,
}

impl TestCallHandle {
    pub fn mxid(&self) -> &str {
        &self.mxid
    }

    /// The call device id this participant's membership (and LiveKit identity)
    /// carries — scenarios match room-state entries / log lines on it.
    pub fn device_id(&self) -> &str {
        &self.device_id
    }

    /// Mirror a mute state onto the published track (speakers only; a no-op
    /// for phantoms).
    pub fn set_muted(&self, muted: bool) {
        let _ = self.mute_tx.send(muted);
    }

    /// Leave properly: stop publishing, close LiveKit, retract `m.call.member`.
    pub async fn leave_clean(self) {
        self.cancel.cancel();
        let _ = self.task.await;
    }

    /// Die WITHOUT retracting membership — the stale-member injector for the
    /// reaper scenarios. The LiveKit connection still closes (a dead client's
    /// SFU session times out server-side anyway); only the room-state
    /// membership is deliberately left behind.
    pub async fn kill(self) {
        self.retract.store(false, Ordering::SeqCst);
        self.cancel.cancel();
        let _ = self.task.await;
    }
}

// ─── Speaker ─────────────────────────────────────────────────────────────────────

/// Join the room + call as a **speaker** and publish caller-supplied audio.
///
/// `frames_rx` carries mono 48 kHz i16 PCM in arbitrary-length chunks; the
/// feed reframes to 10 ms and lets LiveKit's `capture_frame` self-pace (its
/// ~1 s internal buffer), so the sender may run ahead of real time by that
/// much. In an encrypted room ([`TestCallConfig::media_e2ee`]) the speaker
/// generates a media key, encrypts its frames under it, and Olm-distributes it
/// to the call's members (the bridge bot included) exactly like Element Call.
pub async fn join_and_publish(
    cfg: &TestCallConfig,
    user: &TestUser,
    frames_rx: mpsc::Receiver<Vec<i16>>,
) -> Result<TestCallHandle> {
    let mxid = user.mxid(&cfg.server_name);
    let gcfg = build_ghost_config(cfg, &mxid)?;
    let span = tracing::info_span!("testkit.speaker", mxid = %mxid);

    async {
        admit(&gcfg, user, &mxid).await?;
        let focus =
            well_known::discover_livekit_focus(&gcfg.http, &gcfg.well_known_base_url, &gcfg.room_id)
                .await
                .context("discovering LiveKit focus")?;
        let cancel = CancellationToken::new();
        let retract = Arc::new(AtomicBool::new(true));
        let (mute_tx, mut mute_rx) = watch::channel(false);

        // E2EE: media key + send-only Olm crypto + the production distribution
        // loop, started BEFORE publish so the bridge can already hold the key
        // when the first encrypted frame arrives. The crypto device doubles as
        // the call device (see the module doc's identity notes).
        let media_key = cfg.media_e2ee.then(keys::generate_key);
        let mut members_gen_tx = None;
        let device_id = if let Some(key) = media_key {
            let crypto = GhostCrypto::bootstrap(
                gcfg.http.clone(),
                &gcfg.homeserver_url,
                &gcfg.as_token,
                &user.localpart,
                &mxid,
                "",
                false,
            )
            .await
            .context("bootstrapping test speaker Olm crypto")?;
            let device_id = crypto.device_id().to_owned();
            let transport: Arc<dyn KeyTransport> = Arc::new(OlmTransport::new(
                Arc::new(crypto),
                gcfg.room_id.clone(),
                device_id.clone(),
            ));
            let source: Arc<dyn RecipientSource> = Arc::new(StateRecipients {
                gcfg: gcfg.clone(),
                self_mxid: mxid.clone(),
            });
            let (gen_tx, gen_rx) = watch::channel(0u64);
            members_gen_tx = Some(gen_tx);
            tokio::spawn(
                distribute_key_loop(source, key, transport, false, gen_rx, cancel.clone())
                    .instrument(tracing::Span::current()),
            );
            device_id
        } else {
            mint_call_device_id()
        };

        // Publish FIRST, then post membership (the ghosts' proven ordering:
        // a subscriber that processes the membership wants the track already
        // live). `connect_and_publish` installs the media key on the frame
        // crypto and spawns the room-event drain that closes LiveKit on cancel.
        let (source, track) =
            connect_and_publish(&gcfg, &focus, &mxid, &device_id, media_key, cancel.clone())
                .await
                .context("test speaker LiveKit connect/publish")?;
        post_membership_join(&gcfg, &mxid, &device_id, &focus).await?;

        // Feed task: reframe caller chunks to 10 ms and hand them to LiveKit.
        let task = tokio::spawn(
            {
                let cancel = cancel.clone();
                let retract = retract.clone();
                let gcfg = gcfg.clone();
                let mxid = mxid.clone();
                let device_id = device_id.clone();
                let mut frames_rx = frames_rx;
                async move {
                    apply_track_mute(&track, *mute_rx.borrow_and_update());
                    let mut pending: Vec<i16> = Vec::new();
                    loop {
                        tokio::select! {
                            biased;
                            _ = cancel.cancelled() => break,
                            res = mute_rx.changed() => {
                                if res.is_ok() {
                                    apply_track_mute(&track, *mute_rx.borrow_and_update());
                                }
                            }
                            chunk = frames_rx.recv() => {
                                let Some(chunk) = chunk else { break };
                                pending.extend_from_slice(&chunk);
                                let mut offset = 0usize;
                                while pending.len() - offset >= SAMPLES_10MS as usize {
                                    let f10 =
                                        pending[offset..offset + SAMPLES_10MS as usize].to_vec();
                                    offset += SAMPLES_10MS as usize;
                                    let af = AudioFrame {
                                        data: Cow::Owned(f10),
                                        sample_rate: SAMPLE_RATE,
                                        num_channels: 1,
                                        samples_per_channel: SAMPLES_10MS,
                                    };
                                    // Self-pacing: awaits when LiveKit's buffer
                                    // is full, so the whole path runs real-time.
                                    if let Err(e) = source.capture_frame(&af).await {
                                        tracing::warn!(error = %e, "test speaker capture_frame failed");
                                    }
                                }
                                pending.drain(..offset);
                            }
                        }
                    }
                    if retract.load(Ordering::SeqCst) {
                        retract_membership(&gcfg, &mxid, &device_id, &focus).await;
                    }
                    tracing::info!("test speaker retired");
                }
            }
            .instrument(tracing::Span::current()),
        );

        tracing::info!(device_id, "test speaker live (published + membership posted)");
        Ok(TestCallHandle {
            mxid: mxid.clone(),
            device_id,
            cancel,
            retract,
            mute_tx,
            task,
            _members_gen_tx: members_gen_tx,
        })
    }
    .instrument(span)
    .await
}

// ─── Phantom ─────────────────────────────────────────────────────────────────────

/// Join the room and post `m.call.member` WITHOUT ever touching LiveKit — a
/// participant that looks in-call from room state alone. With `with_crypto`,
/// the membership carries a real (in-memory) crypto device that has uploaded
/// device keys + one-time keys, so key senders targeting it succeed; without,
/// the claimed device doesn't exist (which is itself a useful shape: a client
/// that died before its crypto came up).
pub async fn join_presence(
    cfg: &TestCallConfig,
    user: &TestUser,
    with_crypto: bool,
) -> Result<TestCallHandle> {
    let mxid = user.mxid(&cfg.server_name);
    let gcfg = build_ghost_config(cfg, &mxid)?;
    let span = tracing::info_span!("testkit.phantom", mxid = %mxid);

    async {
        admit(&gcfg, user, &mxid).await?;
        let focus =
            well_known::discover_livekit_focus(&gcfg.http, &gcfg.well_known_base_url, &gcfg.room_id)
                .await
                .context("discovering LiveKit focus")?;
        let device_id = if with_crypto {
            let crypto = GhostCrypto::bootstrap(
                gcfg.http.clone(),
                &gcfg.homeserver_url,
                &gcfg.as_token,
                &user.localpart,
                &mxid,
                "",
                false,
            )
            .await
            .context("bootstrapping test phantom crypto")?;
            crypto.device_id().to_owned()
        } else {
            mint_call_device_id()
        };
        post_membership_join(&gcfg, &mxid, &device_id, &focus).await?;

        let cancel = CancellationToken::new();
        let retract = Arc::new(AtomicBool::new(true));
        let (mute_tx, _mute_rx) = watch::channel(false);
        let task = tokio::spawn(
            {
                let cancel = cancel.clone();
                let retract = retract.clone();
                let gcfg = gcfg.clone();
                let mxid = mxid.clone();
                let device_id = device_id.clone();
                async move {
                    cancel.cancelled().await;
                    if retract.load(Ordering::SeqCst) {
                        retract_membership(&gcfg, &mxid, &device_id, &focus).await;
                    }
                    tracing::info!("test phantom retired");
                }
            }
            .instrument(tracing::Span::current()),
        );

        tracing::info!(device_id, with_crypto, "test phantom present (membership only)");
        Ok(TestCallHandle {
            mxid: mxid.clone(),
            device_id,
            cancel,
            retract,
            mute_tx,
            task,
            _members_gen_tx: None,
        })
    }
    .instrument(span)
    .await
}

// ─── Listener ────────────────────────────────────────────────────────────────────

/// One subscribed track's capture: the publisher's LiveKit identity
/// (`{mxid}:{device}` — a bridge ghost's is `@{prefix}<digits>:server:D…`) and
/// the mono 48 kHz samples received. In-memory: scenario probes are seconds
/// long (≈96 KB/s), far below any concerning size.
pub struct TrackRecording {
    pub identity: String,
    pub samples: Vec<i16>,
}

/// A live listener. [`stop`](Self::stop) tears down and returns the recordings.
pub struct TestListenerHandle {
    mxid: String,
    device_id: String,
    cancel: CancellationToken,
    retract: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<Vec<TrackRecording>>,
}

impl TestListenerHandle {
    pub fn mxid(&self) -> &str {
        &self.mxid
    }

    pub fn device_id(&self) -> &str {
        &self.device_id
    }

    /// Leave cleanly (retract membership, close LiveKit) and return everything
    /// recorded so far.
    pub async fn stop(self) -> Result<Vec<TrackRecording>> {
        self.cancel.cancel();
        self.task.await.context("listener task panicked")
    }

    /// Die WITHOUT retracting membership (stale-member injector), discarding
    /// the recordings.
    pub async fn kill(self) {
        self.retract.store(false, Ordering::SeqCst);
        self.cancel.cancel();
        let _ = self.task.await;
    }
}

/// Join the room + call as a **listener**: post `m.call.member`, connect to
/// LiveKit with auto-subscribe, and record every subscribed audio track.
///
/// In an encrypted room the listener's membership carries a real crypto device
/// (so publishers' Olm key sends to it succeed — the d2m assertion), but the
/// recorded frames are NOT decrypted (deferred to D2.5/D3): do not oracle them.
pub async fn join_and_subscribe(
    cfg: &TestCallConfig,
    user: &TestUser,
) -> Result<TestListenerHandle> {
    let mxid = user.mxid(&cfg.server_name);
    let gcfg = build_ghost_config(cfg, &mxid)?;
    let span = tracing::info_span!("testkit.listener", mxid = %mxid);

    async {
        admit(&gcfg, user, &mxid).await?;
        let focus =
            well_known::discover_livekit_focus(&gcfg.http, &gcfg.well_known_base_url, &gcfg.room_id)
                .await
                .context("discovering LiveKit focus")?;
        // Membership device = crypto device in E2EE rooms: key senders target
        // the membership device, and only the crypto device can answer claims.
        let device_id = if cfg.media_e2ee {
            let crypto = GhostCrypto::bootstrap(
                gcfg.http.clone(),
                &gcfg.homeserver_url,
                &gcfg.as_token,
                &user.localpart,
                &mxid,
                "",
                false,
            )
            .await
            .context("bootstrapping test listener crypto")?;
            crypto.device_id().to_owned()
        } else {
            mint_call_device_id()
        };
        // Membership first — this is what flips the bridge's call-active and
        // makes ghosts start publishing; the LiveKit join below then sees (or
        // auto-subscribes as they arrive) their tracks.
        post_membership_join(&gcfg, &mxid, &device_id, &focus).await?;

        let openid = fetch_openid_token(&gcfg, &mxid).await?;
        let jwt_resp = lk_jwt::fetch_livekit_token(
            &gcfg.http,
            &focus.service_url,
            &gcfg.room_id,
            &device_id,
            &openid,
        )
        .await?;
        let (identity, _room) = decode_jwt_claims(&jwt_resp.jwt)?;
        tracing::info!(identity = %identity, "test listener LiveKit JWT obtained");

        // Default RoomOptions = auto-subscribe on; no E2eeOptions (decrypting
        // listener is deferred — see the module doc).
        let (room, mut events) = Room::connect(&jwt_resp.url, &jwt_resp.jwt, RoomOptions::default())
            .await
            .context("test listener LiveKit Room::connect")?;

        let cancel = CancellationToken::new();
        let retract = Arc::new(AtomicBool::new(true));
        let recordings: Arc<StdMutex<Vec<TrackRecording>>> = Arc::new(StdMutex::new(Vec::new()));

        let task = tokio::spawn(
            {
                let cancel = cancel.clone();
                let retract = retract.clone();
                let recordings = recordings.clone();
                let gcfg = gcfg.clone();
                let mxid = mxid.clone();
                let device_id = device_id.clone();
                async move {
                    let mut collectors: Vec<tokio::task::JoinHandle<()>> = Vec::new();
                    loop {
                        tokio::select! {
                            biased;
                            _ = cancel.cancelled() => break,
                            ev = events.recv() => match ev {
                                Some(RoomEvent::TrackSubscribed { track, participant, .. }) => {
                                    let RemoteTrack::Audio(audio) = track else { continue };
                                    let identity = participant.identity().to_string();
                                    tracing::info!(identity = %identity, "test listener subscribed to track — recording");
                                    let recordings = recordings.clone();
                                    let ct = cancel.child_token();
                                    collectors.push(tokio::spawn(
                                        async move {
                                            let mut stream = NativeAudioStream::new(
                                                audio.rtc_track(),
                                                SAMPLE_RATE as i32,
                                                1,
                                            );
                                            let mut samples: Vec<i16> = Vec::new();
                                            loop {
                                                tokio::select! {
                                                    biased;
                                                    _ = ct.cancelled() => break,
                                                    frame = stream.next() => match frame {
                                                        Some(f) => samples.extend_from_slice(&f.data),
                                                        None => break,
                                                    },
                                                }
                                            }
                                            tracing::info!(
                                                identity = %identity,
                                                samples = samples.len(),
                                                "test listener track recording finished"
                                            );
                                            recordings
                                                .lock()
                                                .unwrap_or_else(PoisonError::into_inner)
                                                .push(TrackRecording { identity, samples });
                                        }
                                        .in_current_span(),
                                    ));
                                }
                                Some(RoomEvent::Disconnected { reason }) => {
                                    tracing::warn!(reason = ?reason, "test listener LiveKit room disconnected");
                                    break;
                                }
                                Some(other) => tracing::trace!(event = ?other, "test listener room event"),
                                None => break,
                            },
                        }
                    }
                    // Flush every per-track collector before reading the shared vec.
                    cancel.cancel();
                    for c in collectors {
                        let _ = c.await;
                    }
                    let _ = room.close().await;
                    if retract.load(Ordering::SeqCst) {
                        retract_membership(&gcfg, &mxid, &device_id, &focus).await;
                    }
                    tracing::info!("test listener retired");
                    let mut out = recordings.lock().unwrap_or_else(PoisonError::into_inner);
                    std::mem::take(&mut *out)
                }
            }
            .instrument(tracing::Span::current()),
        );

        tracing::info!(device_id, "test listener live (membership + auto-subscribe)");
        Ok(TestListenerHandle {
            mxid: mxid.clone(),
            device_id,
            cancel,
            retract,
            task,
        })
    }
    .instrument(span)
    .await
}

// ─── Key-send recipient enumeration over the raw CS API ─────────────────────────

/// [`RecipientSource`] for the test speaker's `distribute_key_loop`: reads the
/// room's `m.call.member` state over the raw CS API (masqueraded as the test
/// user, a joined member) instead of the production path's synced matrix-sdk
/// client. Excludes self and the bridge's ghosts (send-only; their membership
/// device ids are not claimable crypto devices) — the bridge BOT is included,
/// which is the point: it must receive the key to decrypt the speaker.
struct StateRecipients {
    gcfg: GhostConfig,
    self_mxid: String,
}

#[async_trait]
impl RecipientSource for StateRecipients {
    async fn recipients(&self) -> Result<Vec<KeyRecipient>> {
        let url = cs_url(
            &self.gcfg.homeserver_url,
            &["rooms", &self.gcfg.room_id, "state"],
            Some(&self.self_mxid),
        )?;
        let resp = self
            .gcfg
            .http
            .get(url)
            .bearer_auth(&self.gcfg.as_token)
            .send()
            .await
            .context("GET room state for test key recipients")?;
        if !resp.status().is_success() {
            return Err(anyhow!("room state GET failed: {}", err_body(resp).await));
        }
        let state: Vec<serde_json::Value> = resp.json().await.context("room state JSON")?;
        Ok(parse_state_recipients(
            &state,
            &self.self_mxid,
            &self.gcfg.ghost_localpart_prefix,
        ))
    }
}

/// Pure parsing half of [`StateRecipients`] (unit-testable without a homeserver):
/// active `m.call.member` devices, excluding `self_mxid` and bridge ghosts.
fn parse_state_recipients(
    state: &[serde_json::Value],
    self_mxid: &str,
    bridge_ghost_prefix: &str,
) -> Vec<KeyRecipient> {
    let mut recipients: Vec<KeyRecipient> = Vec::new();
    for ev in state {
        if ev["type"] != CALL_MEMBER_EVENT_TYPE {
            continue;
        }
        let Some(sender) = ev["sender"].as_str() else { continue };
        if sender == self_mxid || is_bridge_excluded_identity(sender, bridge_ghost_prefix) {
            continue;
        }
        let Ok(user_id) = sender.parse::<OwnedUserId>() else { continue };
        let Ok(content) =
            serde_json::from_value::<CallMemberEventContent>(ev["content"].clone())
        else {
            continue;
        };
        let ts = ev["origin_server_ts"]
            .as_u64()
            .and_then(|v| UInt::try_from(v).ok())
            .map(MilliSecondsSinceUnixEpoch);
        for m in content.active_memberships(ts) {
            let r = KeyRecipient {
                user_id: user_id.clone(),
                device_id: m.device_id().to_owned(),
            };
            if !recipients.contains(&r) {
                recipients.push(r);
            }
        }
    }
    recipients
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_rejects_digit_suffix() {
        assert!(TestUser::new("nvbtest_alice", "Alice").is_ok());
        assert!(TestUser::new("nvbtest_123", "Bot").is_err());
        assert!(TestUser::new("", "X").is_err());
        assert!(TestUser::new("nvbtest_alice", "").is_err());
        // No underscore at all: the whole localpart is the suffix.
        assert!(TestUser::new("alice", "Alice").is_ok());
        assert!(TestUser::new("12345", "Nope").is_err());
    }

    #[test]
    fn state_recipients_filter_and_parse() {
        // Membership expiry is judged against the wall clock (created_ts —
        // here the origin_server_ts — plus `expires`), so the fixtures must be
        // stamped "now" to parse as active.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_millis() as u64;
        // Build the content with the SAME builder the testkit posts through
        // (`build_membership`), so the fixture can never drift from the real
        // wire shape.
        let focus = LivekitFocusInfo {
            alias: "!room:nether.im".into(),
            service_url: "https://livekit.nether.im".into(),
        };
        let member = |sender: &str, device: &str| {
            let content = super::super::build_membership(&device.into(), &focus);
            serde_json::json!({
                "type": CALL_MEMBER_EVENT_TYPE,
                "sender": sender,
                "state_key": format!("_{sender}_{device}_m.call"),
                "origin_server_ts": now_ms,
                "content": serde_json::to_value(&content).expect("membership serializes"),
            })
        };
        let retracted = serde_json::json!({
            "type": CALL_MEMBER_EVENT_TYPE,
            "sender": "@gone:nether.im",
            "state_key": "_@gone:nether.im_DEV_m.call",
            "origin_server_ts": now_ms,
            "content": {}
        });
        let state = vec![
            member("@nvbtest_alice:nether.im", "Tselfdev"), // self — excluded
            member("@nether-voicebridge_123:nether.im", "D123-s"), // bridge ghost — excluded
            member("@nether-voicebridge_bot:nether.im", "BRIDGEDEV"), // the bot — INCLUDED
            member("@dark:nether.im", "HUMANDEV"),          // a human — included
            retracted,                                       // empty content — inactive
        ];
        let got = parse_state_recipients(&state, "@nvbtest_alice:nether.im", "nether-voicebridge_");
        let ids: Vec<String> = got
            .iter()
            .map(|r| format!("{}:{}", r.user_id, r.device_id))
            .collect();
        assert_eq!(
            ids,
            vec![
                "@nether-voicebridge_bot:nether.im:BRIDGEDEV",
                "@dark:nether.im:HUMANDEV",
            ]
        );
    }

    #[test]
    fn call_device_ids_are_prefixed_and_unique() {
        let a = mint_call_device_id();
        let b = mint_call_device_id();
        assert!(a.starts_with('T') && a.len() == 9);
        assert_ne!(a, b, "two mints must differ");
    }
}
