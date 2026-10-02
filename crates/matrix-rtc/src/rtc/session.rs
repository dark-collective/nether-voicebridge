//! LiveKit room connection with E2EE configured for Element Call compatibility.
//!
//! Element Call uses AES-GCM frame encryption with HKDF key derivation. The LiveKit
//! Rust SDK supports this natively via `KeyDerivationAlgorithm::HKDF`.
//!
//! Key identity insight: nether.im's lk-jwt-service mints a JWT whose `sub` claim is
//! the SHA-256-derived participant identity that LiveKit will use for our media tracks.
//! Decoding the JWT's `sub` avoids reconstructing the hash ourselves and guarantees
//! correctness regardless of which identity scheme the server uses.

use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose};
use futures_util::StreamExt;
use livekit::{Room, RoomOptions};
use livekit::e2ee::{E2eeOptions, EncryptionType, key_provider::KeyProvider};
use livekit::publication::RemoteTrackPublication;
use livekit::track::{RemoteTrack, TrackSource};
use livekit::webrtc::audio_stream::native::NativeAudioStream;
use matrix_sdk::Client;
use matrix_sdk::ruma::{OwnedRoomId, RoomId, UserId};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use livekit::RoomEvent;

use nvb_audio::routing::{ParticipantSink, PlaybackRouter};
use nvb_config::links::LinkBoard;
use super::keys::PeerKeyCache;

/// Sample rate / channel count we ask libwebrtc to resample inbound tracks to.
/// 48 kHz mono matches the playback path (songbird upmixes mono → stereo).
const PLAYBACK_SAMPLE_RATE: i32 = 48_000;
const PLAYBACK_CHANNELS: i32 = 1;

/// A live LiveKit session: room handle and our participant identity. The E2EE
/// key provider (when on) is owned by the caller, which builds and shares it
/// *before* connecting (#73) — see [`connect`].
pub struct LiveSession {
    pub room: Room,
    /// Our LiveKit participant identity (the JWT `sub` claim). Read only on the
    /// dormant E2EE publish path (subscribe-only bot no longer sets its own key).
    #[allow(dead_code)]
    pub our_identity: String,
    /// Cancels the event task and all per-track playback consumers on teardown.
    cancel: CancellationToken,
    /// Fired by the event task when the room dies (terminal `Disconnected`, or
    /// the event stream ending) — the owner loop selects on this to tear down
    /// and rejoin (#66). Dedicated token, deliberately NOT `cancel`: `cancel` is
    /// a child of the bridge shutdown and means "stop consumers"; a shutdown
    /// must never read as a room disconnect. Per-instance, so a signal from a
    /// session that was already replaced can never trigger a spurious recovery.
    pub(crate) disconnected: CancellationToken,
    _event_task: tokio::task::JoinHandle<()>,
}

impl LiveSession {
    /// Disconnect from the LiveKit room and clean up.
    ///
    /// Cancel first so every inbound playback consumer stops, drops its
    /// `NativeAudioStream` (which detaches the libwebrtc sink), and removes its
    /// mixer buffer — *before* the room closes and the event task is aborted.
    pub async fn disconnect(self) {
        self.cancel.cancel();
        let _ = self.room.close().await;
        self._event_task.abort();
    }
}

/// Decode and return selected fields from a JWT payload (middle base64url segment).
///
/// Returns `(sub, room_name)` where `sub` is the LiveKit participant identity
/// and `room_name` is extracted from the `video.room` claim (LiveKit convention).
pub fn decode_jwt_claims(jwt: &str) -> Result<(String, Option<String>)> {
    let parts: Vec<&str> = jwt.split('.').collect();
    if parts.len() < 2 {
        anyhow::bail!("JWT has fewer than 2 segments");
    }
    // JWT payload is base64url, no padding. Decode it.
    let payload_bytes = general_purpose::URL_SAFE_NO_PAD
        .decode(parts[1])
        .context("decoding JWT payload base64url")?;
    let payload: serde_json::Value =
        serde_json::from_slice(&payload_bytes).context("parsing JWT payload JSON")?;
    let sub = payload
        .get("sub")
        .and_then(|v| v.as_str())
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!("JWT payload has no 'sub' field"))?;
    let room = payload
        .get("video")
        .and_then(|v| v.get("room"))
        .and_then(|v| v.as_str())
        .map(str::to_owned);
    Ok((sub, room))
}

/// Connect to a LiveKit room.
///
/// When `key_provider` is `Some`, configures HKDF/GCM frame encryption with it
/// (the caller builds + shares the provider *before* connecting, so every peer
/// key already delivered is installed before the first inbound frame can hit
/// the decryptor — see #73). When `None` (the common case for deployments that
/// run EC without media E2EE), connects plainly — no frame encryption.
///
/// `key_installed_rx` (E2EE only) delivers the LiveKit identity of each peer
/// whose media key just got installed; the room-event loop uses it to heal
/// tracks that were subscribed before their key arrived (#73).
#[allow(clippy::too_many_arguments)]
pub async fn connect(
    url: &str,
    jwt: &str,
    key_provider: Option<KeyProvider>,
    key_installed_rx: Option<UnboundedReceiver<String>>,
    peer_keys: Arc<PeerKeyCache>,
    client: Client,
    room_id: OwnedRoomId,
    router: Arc<dyn PlaybackRouter>,
    bridge: Arc<str>,
    ghost_prefix: Arc<str>,
    parent_cancel: CancellationToken,
    // Linked identities (ADR 0001): a subscribed track from a bridge-owned member
    // (paired Discord account present / service account / excluded) must not
    // mint a puppet — membership and tracks are two assignment paths; both gate.
    links: Arc<LinkBoard>,
) -> Result<LiveSession> {
    let use_e2ee = key_provider.is_some();
    let span = tracing::info_span!("rtc.session", sfu_url = %url, use_e2ee);
    let _guard = span.enter();

    // Decode our identity and room name from the JWT before connecting.
    let (our_identity, jwt_room) = decode_jwt_claims(jwt)?;
    tracing::info!(identity = %our_identity, livekit_room = ?jwt_room, "LiveKit JWT claims decoded");

    let room_opts = match &key_provider {
        Some(kp) => {
            let e2ee = E2eeOptions {
                encryption_type: EncryptionType::Gcm,
                key_provider: kp.clone(),
            };
            let mut opts = RoomOptions::default();
            opts.encryption = Some(e2ee);
            opts
        }
        None => RoomOptions::default(),
    };

    tracing::info!(use_e2ee, "connecting to LiveKit room");
    let (room, events) = Room::connect(url, jwt, room_opts)
        .await
        .context("LiveKit Room::connect")?;
    tracing::info!("LiveKit room connected");

    if use_e2ee {
        room.e2ee_manager().set_enabled(true);
        tracing::info!("E2EE enabled on LiveKit room");
    }

    // Log existing participants at join time so we can confirm EC's side is present.
    let existing: Vec<_> = room.remote_participants()
        .values()
        .map(|p| p.identity().to_string())
        .collect();
    tracing::info!(existing_participants = ?existing, "LiveKit room initial participants");

    // Drain room events in a background task so the channel doesn't fill up.
    // The same task routes inbound audio tracks to per-participant puppet outputs.
    // Child of the bridge shutdown token, so a process-wide shutdown also closes
    // this session's event task + per-track consumers.
    let cancel = parent_cancel.child_token();
    let disconnected = CancellationToken::new();
    let event_task = tokio::spawn(
        handle_room_events(
            events,
            client,
            room_id,
            router,
            cancel.clone(),
            disconnected.clone(),
            bridge.clone(),
            ghost_prefix,
            links,
            peer_keys,
            key_installed_rx,
        )
        .instrument(tracing::info_span!(parent: None, "bridge", name = %bridge)),
    );

    Ok(LiveSession {
        room,
        our_identity,
        cancel,
        disconnected,
        _event_task: event_task,
    })
}

/// Drain a subscribed audio track to its routed output (a Discord puppet, or the
/// listener mix on overflow) until cancelled or the track ends.  libwebrtc
/// resamples to 48 kHz mono for us (see [`NativeAudioStream::new`]), so each
/// frame is already in the sink's format.  The route is acquired via
/// [`PlaybackRouter::assign`] (which checks out a puppet and sets its nickname to
/// `display_name`) and returned via [`PlaybackRouter::release`] on exit.
/// The router key for a LiveKit participant: the bare MXID, with the trailing
/// `:{device}` stripped. The MXID is itself `@local:server` (one `:`), so we
/// strip the final `:`-segment only when the remaining prefix parses as a real
/// MXID; otherwise (a legacy `{mxid}`-only identity) we keep it whole. Both
/// sides of the bridge key puppets on this so a user whose membership device
/// differs from their LiveKit-track device still maps to one puppet.
pub(crate) fn participant_mxid(identity: &str) -> &str {
    match identity.rsplit_once(':') {
        Some((prefix, _device)) if UserId::parse(prefix).is_ok() => prefix,
        _ => identity,
    }
}

/// Resolve a human-friendly display name for a puppet nickname.
///
/// Element Call populates neither `participant.name()` nor metadata, and the
/// LiveKit identity is `{mxid}:{device_id}` (e.g. `@dark:nether.im:2oQuErOThF`).
/// Preference order: the participant name if LiveKit ever carries one; else the
/// room member's display name (resolved from the MXID parsed out of the
/// identity via [`participant_mxid`]); else the bare MXID — still cleaner than
/// the device-suffixed identity.
async fn resolve_display_name(
    client: &Client,
    room_id: &RoomId,
    identity: &str,
    participant_name: &str,
) -> String {
    if !participant_name.is_empty() {
        return participant_name.to_owned();
    }
    let mxid_str = participant_mxid(identity);
    let Ok(user_id) = UserId::parse(mxid_str) else {
        return mxid_str.to_owned();
    };
    if let Some(room) = client.get_room(room_id) {
        match room.get_member(&user_id).await {
            Ok(Some(member)) => {
                if let Some(name) = member.display_name() {
                    return name.to_owned();
                }
            }
            Ok(None) => {}
            Err(e) => {
                tracing::debug!(error = %e, user = %user_id, "room member lookup failed for puppet name");
            }
        }
    }
    mxid_str.to_owned()
}

/// Screen-share publications (video or its audio) are never bridged (#142).
fn is_screenshare_source(publication: &RemoteTrackPublication) -> bool {
    matches!(
        publication.source(),
        TrackSource::Screenshare | TrackSource::ScreenshareAudio
    )
}

#[allow(clippy::too_many_arguments)]
async fn playback_consumer(
    track: RemoteTrack,
    // The full LiveKit participant identity `{mxid}:{device}`. The router key is
    // the bare MXID derived from it (see `participant_mxid`) so one puppet maps
    // to one participant regardless of which device the track carries — NOT the
    // track SID, so re-published tracks reuse the same puppet and (Phase P4)
    // membership owns assign/release.
    identity: String,
    participant_name: String,
    initial_muted: bool,
    client: Client,
    room_id: OwnedRoomId,
    router: Arc<dyn PlaybackRouter>,
    cancel: CancellationToken,
    links: Arc<LinkBoard>,
) {
    let RemoteTrack::Audio(audio) = track else {
        return; // video tracks are not bridged
    };
    let key = participant_mxid(&identity);
    // Resolve the puppet's Discord nickname here (in the per-track task) so a
    // homeserver round-trip never stalls the room-event loop. Uses the full
    // identity (the device suffix matters to the resolver's MXID parse).
    let display_name =
        resolve_display_name(&client, &room_id, &identity, &participant_name).await;
    // Linked identities (ADR 0001): while this member is bridge-owned, no puppet
    // and no playback; re-checked ~once a second so a companion client dropping (member
    // becomes ordinary again) starts feeding, and one arriving stops it and releases.
    let mut sink: Option<Arc<dyn ParticipantSink>> = None;
    if links.matrix_excluded(key) {
        tracing::info!(track = %key, "playback consumer: member is bridge-owned — no puppet, frames dropped until that changes");
    } else {
        sink = Some(router.assign(key, &display_name));
        // Seed the puppet's mute state right after assign (avoids racing a separate
        // set_muted against the assign that creates the puppet).
        router.set_muted(key, initial_muted);
    }
    let mut stream =
        NativeAudioStream::new(audio.rtc_track(), PLAYBACK_SAMPLE_RATE, PLAYBACK_CHANNELS);
    tracing::info!(track = %key, display_name = %display_name, "playback consumer started");

    // Health: log the first frame (confirms frames arrive, and their shape),
    // then the *peak* level over each ~5 s window — a single-frame snapshot would
    // almost always sample silence even mid-speech and tell us nothing.
    let mut frames: u64 = 0;
    let mut peak_db = f32::NEG_INFINITY;
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            frame = stream.next() => match frame {
                Some(f) => {
                    frames += 1;
                    peak_db = peak_db.max(nvb_audio::rms_dbfs(&f.data));
                    if frames == 1 {
                        tracing::info!(
                            track = %key,
                            channels = f.num_channels,
                            samples_per_channel = f.samples_per_channel,
                            sample_rate = f.sample_rate,
                            "playback: first frame from LiveKit"
                        );
                    } else if frames.is_multiple_of(500) {
                        tracing::debug!(
                            track = %key,
                            frames,
                            peak_db = format_args!("{:.1}", peak_db),
                            "playback: inbound LiveKit level (peak over window)"
                        );
                        peak_db = f32::NEG_INFINITY;
                    }
                    if frames.is_multiple_of(50) {
                        let excluded = links.matrix_excluded(key);
                        if excluded && sink.is_some() {
                            tracing::info!(track = %key, "playback consumer: member became bridge-owned — releasing puppet");
                            router.release(key);
                            sink = None;
                        } else if !excluded && sink.is_none() {
                            tracing::info!(track = %key, "playback consumer: member is ordinary again — assigning puppet");
                            sink = Some(router.assign(key, &display_name));
                        }
                    }
                    if let Some(sink) = &sink {
                        sink.push(&f.data);
                    }
                }
                None => break, // track closed
            },
        }
    }

    // Track end does NOT release the puppet — call *membership* (matrix::run)
    // owns presence now, so a member who stops transmitting keeps their bot
    // (present, silent). We simply stop feeding; the mixer drains to silence.
    // Explicit mute is handled by TrackMuted/TrackUnmuted, not inferred here.
    tracing::info!(track = %key, "playback consumer ended (puppet kept; membership owns release)");
}

/// #73 heal gate: per subscribed publisher identity, whether that peer's media
/// key was already delivered when its track was subscribed.
///
/// Why: libwebrtc's inbound frame decryptor wedges *permanently* when a track's
/// first frames arrive before the publisher's key is installed — E2EE state
/// reports `Ok` after the key lands, the frame counter advances, but every
/// output frame stays digital silence, and later key installs/rotations don't
/// heal it (live-proven on encrypted Dark Voice). All of that wedged state is
/// per-receiver (transformer + decode chain), so re-subscribing — which mints a
/// fresh receiver and frame cryptor against the now-keyed provider — reduces
/// the wedge case to the known-good "key before frames" case. This gate decides
/// *when* that one re-subscribe is owed: exactly once per keyless subscribe,
/// on the first key install for that identity.
#[derive(Default)]
struct HealGate {
    /// LiveKit identity (`{mxid}:{device}`) → had a key at subscribe time.
    had_key: HashMap<String, bool>,
}

impl HealGate {
    fn on_subscribed(&mut self, identity: &str, had_key: bool) {
        self.had_key.insert(identity.to_owned(), had_key);
    }

    fn on_unsubscribed(&mut self, identity: &str) {
        self.had_key.remove(identity);
    }

    /// A key was just installed for `identity`: `true` if its track was
    /// subscribed keyless and hasn't been healed yet. Flips the entry so
    /// follow-up installs (index rotations) don't re-subscribe again — the
    /// re-subscribe's own `TrackSubscribed` re-records it as keyed anyway.
    fn should_heal(&mut self, identity: &str) -> bool {
        match self.had_key.get_mut(identity) {
            Some(had_key) if !*had_key => {
                *had_key = true;
                true
            }
            _ => false,
        }
    }
}

/// Re-subscribe a track whose first frames beat its peer media key (#73).
///
/// Unsubscribe→resubscribe makes the SFU renegotiate the track: the SDK mints a
/// fresh RtpReceiver + frame cryptor (and we get fresh `TrackUnsubscribed` /
/// `TrackSubscribed` events, so the playback consumer is cleanly restarted).
/// The provider's key ring — which now holds the key — is the only state that
/// survives, which is exactly the known-good "key before frames" join order.
/// Runs as a short task so the brief settle delay between the two subscription
/// updates never stalls the room-event loop.
fn heal_keyless_subscription(identity: &str, publication: &RemoteTrackPublication) {
    tracing::info!(
        from_identity = %identity,
        track_sid = %publication.sid(),
        "peer media key installed for a track subscribed keyless — re-subscribing to rebuild its decryptor (#73)"
    );
    let publication = publication.clone();
    tokio::spawn(
        async move {
            publication.set_subscribed(false);
            // Let the unsubscribe reach the SFU before requesting the track
            // again, so the two updates can't collapse into a no-op.
            tokio::time::sleep(Duration::from_millis(250)).await;
            publication.set_subscribed(true);
        }
        .instrument(tracing::Span::current()),
    );
}

/// Drain LiveKit room events: log for observability, and route subscribed audio
/// tracks into the playback mixer (one consumer task per track, keyed by SID).
///
/// When the loop ends — a terminal `Disconnected` (the SDK only emits it after
/// its internal auto-reconnect is exhausted) or the event stream closing — the
/// `disconnected` token is fired unconditionally so the owner loop can tear the
/// dead session down and rejoin (#66).
///
/// `key_installed_rx` (E2EE only): each received LiveKit identity just had a
/// media key installed; a track subscribed *before* its key arrived is
/// re-subscribed once so its wedged decryptor is rebuilt keyed (#73, see
/// [`HealGate`]).
#[allow(clippy::too_many_arguments)]
async fn handle_room_events(
    mut events: UnboundedReceiver<RoomEvent>,
    client: Client,
    room_id: OwnedRoomId,
    router: Arc<dyn PlaybackRouter>,
    cancel: CancellationToken,
    disconnected: CancellationToken,
    bridge: Arc<str>,
    ghost_prefix: Arc<str>,
    links: Arc<LinkBoard>,
    peer_keys: Arc<PeerKeyCache>,
    mut key_installed_rx: Option<UnboundedReceiver<String>>,
) {
    // Per-track child tokens so an individual unsubscribe can stop just that
    // consumer; cancelling the parent (teardown) stops them all.
    let mut consumers: HashMap<String, CancellationToken> = HashMap::new();
    // #73 heal state: the gate decides IF a re-subscribe is owed; the
    // publication handles (per publisher identity, human audio tracks only)
    // are what we re-subscribe with.
    let mut heal_gate = HealGate::default();
    let mut audio_pubs: HashMap<String, RemoteTrackPublication> = HashMap::new();

    loop {
        let key_installed = async {
            match key_installed_rx.as_mut() {
                Some(rx) => rx.recv().await,
                None => std::future::pending().await,
            }
        };
        let ev = tokio::select! {
            ev = events.recv() => match ev {
                Some(ev) => ev,
                None => break,
            },
            installed = key_installed => {
                match installed {
                    Some(identity) => {
                        if heal_gate.should_heal(&identity) {
                            if let Some(publication) = audio_pubs.get(&identity) {
                                heal_keyless_subscription(&identity, publication);
                            }
                        }
                    }
                    // Sender gone (owner tore the session down) — stop polling.
                    None => key_installed_rx = None,
                }
                continue;
            }
        };
        match ev {
            RoomEvent::ParticipantConnected(p) => {
                tracing::info!(identity = %p.identity(), "LiveKit: remote participant connected");
            }
            RoomEvent::ParticipantDisconnected(p) => {
                let identity = p.identity().to_string();
                tracing::info!(identity = %identity, "LiveKit: remote participant disconnected");
                // #73: belt-and-suspenders — TrackUnsubscribed usually fires
                // too, but never leave a dangling publication handle behind.
                heal_gate.on_unsubscribed(&identity);
                audio_pubs.remove(&identity);
            }
            RoomEvent::LocalTrackPublished { publication, .. } => {
                tracing::info!(sid = %publication.sid(), kind = ?publication.kind(), muted = publication.is_muted(), "LiveKit: local track published to SFU");
            }
            RoomEvent::TrackSubscribed { track, publication, participant } => {
                let sid = publication.sid().to_string();
                let identity = participant.identity().to_string();
                // #142: screen-share media is never bridged. Discord has no counterpart
                // for a share (it is not a voice), so forwarding
                // share AUDIO into voice only doubles it or feeds it back. Video is
                // dropped by the consumer; audio must be dropped on SOURCE, not kind.
                if is_screenshare_source(&publication) {
                    tracing::info!(
                        from_identity = %identity,
                        track_sid = %sid,
                        source = ?publication.source(),
                        "LiveKit: screen-share track — not bridged, unsubscribing (#142)"
                    );
                    publication.set_subscribed(false);
                    continue;
                }
                // The puppet's Discord nickname is resolved in the consumer task
                // (a homeserver lookup, kept off this event loop). Element Call
                // sets neither `name` nor metadata — log both so a regression
                // back to empty is visible.
                let name = participant.name();
                tracing::info!(
                    from_identity = %identity,
                    participant_name = %name,
                    participant_metadata = %participant.metadata(),
                    track_sid = %sid,
                    kind = ?track.kind(),
                    "LiveKit: subscribed to remote participant track"
                );
                // Echo prevention (constraint #5): a ghost is a Discord speaker we
                // ourselves publish into this call. Re-bridging its audio back to
                // Discord would loop. Skip bridge-owned ghost identities.
                if super::ghost::is_bridge_excluded_identity(&identity, &ghost_prefix) {
                    tracing::debug!(from_identity = %identity, "skipping bridge-owned ghost track (echo prevention)");
                    continue;
                }
                // #73 heal gate (E2EE only): remember whether this publisher's
                // media key was already delivered. Subscribed keyless ⇒ the
                // inbound decryptor is (about to be) wedged; the key install
                // triggers one re-subscribe to rebuild it keyed.
                if key_installed_rx.is_some() && matches!(track, RemoteTrack::Audio(_)) {
                    let had_key = peer_keys.has_key_for_identity(&identity);
                    if !had_key {
                        tracing::info!(
                            from_identity = %identity,
                            track_sid = %sid,
                            "track subscribed before its peer media key — will re-subscribe when the key lands (#73)"
                        );
                    }
                    heal_gate.on_subscribed(&identity, had_key);
                    audio_pubs.insert(identity.clone(), publication.clone());
                }
                let initial_muted = publication.is_muted();
                // Per-track cancel token, keyed by SID (a participant may have
                // more than one track); the router key is the bare MXID derived
                // from the identity inside the consumer.
                let ct = cancel.child_token();
                consumers.insert(sid.clone(), ct.clone());
                tokio::spawn(
                    playback_consumer(
                        track,
                        identity,
                        name,
                        initial_muted,
                        client.clone(),
                        room_id.clone(),
                        router.clone(),
                        ct,
                        links.clone(),
                    )
                    .instrument(tracing::info_span!(parent: None, "bridge", name = %bridge)),
                );
            }
            RoomEvent::TrackMuted { participant, publication } => {
                let identity = participant.identity().to_string();
                tracing::info!(from_identity = %identity, track_sid = %publication.sid(), "LiveKit: track muted");
                router.set_muted(participant_mxid(&identity), true);
            }
            RoomEvent::TrackUnmuted { participant, publication } => {
                let identity = participant.identity().to_string();
                tracing::info!(from_identity = %identity, track_sid = %publication.sid(), "LiveKit: track unmuted");
                router.set_muted(participant_mxid(&identity), false);
            }
            RoomEvent::TrackUnsubscribed { publication, participant, .. } => {
                let sid = publication.sid().to_string();
                let identity = participant.identity().to_string();
                tracing::info!(
                    from_identity = %identity,
                    track_sid = %sid,
                    "LiveKit: unsubscribed from remote participant track"
                );
                if let Some(ct) = consumers.remove(&sid) {
                    ct.cancel();
                }
                // #73: drop the heal-gate entry — a later re-subscribe records
                // fresh state (a heal's own unsubscribe→subscribe cycle passes
                // through here by design).
                heal_gate.on_unsubscribed(&identity);
                audio_pubs.remove(&identity);
            }
            RoomEvent::TrackPublished { publication, participant } => {
                tracing::info!(
                    from_identity = %participant.identity(),
                    track_sid = %publication.sid(),
                    kind = ?publication.kind(),
                    source = ?publication.source(),
                    "LiveKit: remote participant published a track"
                );
                // #142: don't even pull screen-share media from the SFU.
                if is_screenshare_source(&publication) {
                    publication.set_subscribed(false);
                }
            }
            RoomEvent::Disconnected { reason } => {
                tracing::warn!(reason = ?reason, "LiveKit: room disconnected");
                break;
            }
            // The SDK handles transient drops itself (resume/restart); these are
            // informational. Only a terminal `Disconnected` (above) — emitted when
            // the SDK's own retries are exhausted — needs app-level recovery.
            RoomEvent::Reconnecting => {
                tracing::info!("LiveKit: connection lost — SDK auto-reconnecting");
            }
            RoomEvent::Reconnected => {
                tracing::info!("LiveKit: reconnected");
            }
            // Per-participant frame-decrypt state (MissingKey / KeyRatcheted /
            // DecryptionFailed / Ok). This subscriber decrypts HUMAN publishers (ghost
            // tracks are echo-skipped above), so it reports the human→bridge direction —
            // useful for general E2EE health; the ghost→listener direction is observed on
            // the listener's own client. Surfaced at INFO so a stuck identity is visible.
            RoomEvent::E2eeStateChanged { participant, state } => {
                tracing::info!(identity = %participant.identity(), ?state, "LiveKit: E2EE state changed");
            }
            // #116: the SFU's view of a participant's link quality. Emitted on
            // CHANGES only, so every event is worth a line — this is the
            // packet-loss signal choppy-audio reports previously lacked (the
            // bridge's own frame metrics stay clean when the loss is on
            // another participant's leg to the SFU).
            RoomEvent::ConnectionQualityChanged { quality, participant } => {
                use livekit::prelude::ConnectionQuality as Q;
                let identity = participant.identity();
                if matches!(quality, Q::Poor | Q::Lost) {
                    tracing::warn!(identity = %identity, ?quality, "LiveKit: connection quality degraded (#116)");
                } else {
                    tracing::info!(identity = %identity, ?quality, "LiveKit: connection quality changed");
                }
            }
            other => {
                tracing::debug!(event = ?other, "LiveKit room event");
            }
        }
    }

    // The room is dead (terminal Disconnected above, or the event channel closed
    // because the room object was dropped/closed). Signal the owner loop — it
    // decides whether this is teardown-echo (session already replaced/being
    // closed; token never selected) or a mid-call death needing a rejoin.
    disconnected.cancel();
}

#[cfg(test)]
mod tests {
    use super::*;
    use livekit::DisconnectReason;
    use nvb_audio::routing::ParticipantSink;
    use std::time::Duration;
    use tokio::sync::mpsc;

    #[test]
    fn strips_device_suffix() {
        // Modern EC identity `{mxid}:{device}` → bare MXID.
        assert_eq!(participant_mxid("@a:b.com:DEV"), "@a:b.com");
    }

    #[test]
    fn keeps_legacy_identity_whole() {
        // Legacy `{mxid}`-only identity: the trailing `:`-segment is the server,
        // not a device — must NOT be stripped (the remaining `@a` is not a MXID).
        assert_eq!(participant_mxid("@a:b.com"), "@a:b.com");
    }

    // ── HealGate (#73) ───────────────────────────────────────────────────────────

    #[test]
    fn heal_gate_heals_keyless_subscription_exactly_once() {
        let mut gate = HealGate::default();
        gate.on_subscribed("@a:x:DEV", false);
        // First key install after a keyless subscribe → heal.
        assert!(gate.should_heal("@a:x:DEV"));
        // Follow-up installs (key index rotations) must not re-subscribe.
        assert!(!gate.should_heal("@a:x:DEV"));
    }

    #[test]
    fn heal_gate_never_heals_a_keyed_subscription() {
        let mut gate = HealGate::default();
        gate.on_subscribed("@a:x:DEV", true);
        assert!(!gate.should_heal("@a:x:DEV"));
    }

    #[test]
    fn heal_gate_ignores_unknown_and_unsubscribed_identities() {
        let mut gate = HealGate::default();
        assert!(!gate.should_heal("@nobody:x:DEV"));
        gate.on_subscribed("@a:x:DEV", false);
        gate.on_unsubscribed("@a:x:DEV");
        assert!(!gate.should_heal("@a:x:DEV"));
    }

    #[test]
    fn heal_gate_resubscribe_records_fresh_state() {
        let mut gate = HealGate::default();
        // Keyless subscribe → key lands → heal fires…
        gate.on_subscribed("@a:x:DEV", false);
        assert!(gate.should_heal("@a:x:DEV"));
        // …the heal's own unsubscribe→subscribe cycle re-records the entry
        // (now keyed) and must stay quiet on later installs.
        gate.on_unsubscribed("@a:x:DEV");
        gate.on_subscribed("@a:x:DEV", true);
        assert!(!gate.should_heal("@a:x:DEV"));
    }

    // ── handle_room_events disconnect signalling (#66) ──────────────────────────

    struct NoopSink;
    impl ParticipantSink for NoopSink {
        fn push(&self, _mono: &[i16]) {}
    }

    struct NoopRouter;
    impl PlaybackRouter for NoopRouter {
        fn assign(&self, _key: &str, _display_name: &str) -> Arc<dyn ParticipantSink> {
            Arc::new(NoopSink)
        }
        fn set_muted(&self, _key: &str, _muted: bool) {}
        fn release(&self, _key: &str) {}
        fn release_all(&self) {}
    }

    /// Spawn `handle_room_events` with an offline Client (URL parse only, no I/O)
    /// and return the event sender + the disconnected token it should fire.
    async fn spawn_event_loop() -> (mpsc::UnboundedSender<RoomEvent>, CancellationToken) {
        let client = Client::builder()
            .homeserver_url("https://example.invalid")
            .build()
            .await
            .expect("offline client");
        let room_id: OwnedRoomId = "!r:example.invalid".try_into().expect("room id");
        let (tx, rx) = mpsc::unbounded_channel();
        let disconnected = CancellationToken::new();
        tokio::spawn(handle_room_events(
            rx,
            client,
            room_id,
            Arc::new(NoopRouter),
            CancellationToken::new(),
            disconnected.clone(),
            Arc::from("test"),
            Arc::from("nether-voicebridge_"),
            Arc::new(LinkBoard::new("!test:hs", Arc::new(nvb_config::pairings::PairingStore::in_memory()), Arc::new(nvb_config::relays::ServiceAccounts::in_memory()), [], [])),
            Arc::new(PeerKeyCache::default()),
            None,
        ));
        (tx, disconnected)
    }

    #[tokio::test]
    async fn disconnected_event_fires_signal() {
        let (tx, disconnected) = spawn_event_loop().await;
        tx.send(RoomEvent::Disconnected {
            reason: DisconnectReason::UnknownReason,
        })
        .expect("send");
        tokio::time::timeout(Duration::from_secs(1), disconnected.cancelled())
            .await
            .expect("disconnected token must fire on RoomEvent::Disconnected");
    }

    #[tokio::test]
    async fn event_stream_end_fires_signal() {
        let (tx, disconnected) = spawn_event_loop().await;
        drop(tx); // room object dropped/closed → event channel closes
        tokio::time::timeout(Duration::from_secs(1), disconnected.cancelled())
            .await
            .expect("disconnected token must fire when the event stream ends");
    }

    #[tokio::test]
    async fn sdk_reconnect_events_do_not_fire_signal() {
        let (tx, disconnected) = spawn_event_loop().await;
        tx.send(RoomEvent::Reconnecting).expect("send");
        tx.send(RoomEvent::Reconnected).expect("send");
        // Give the loop time to process; the token must stay unfired (the SDK
        // is handling the drop itself — only terminal Disconnected recovers).
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!disconnected.is_cancelled());
        drop(tx);
    }
}
