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
use livekit::e2ee::{E2eeOptions, EncryptionType, key_provider::{KeyDerivationAlgorithm, KeyProvider, KeyProviderOptions}};
use livekit::track::RemoteTrack;
use livekit::webrtc::audio_stream::native::NativeAudioStream;
use matrix_sdk::Client;
use matrix_sdk::ruma::{OwnedRoomId, RoomId, UserId};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use livekit::RoomEvent;

use nvb_audio::routing::PlaybackRouter;

/// Sample rate / channel count we ask libwebrtc to resample inbound tracks to.
/// 48 kHz mono matches the playback path (songbird upmixes mono → stereo).
const PLAYBACK_SAMPLE_RATE: i32 = 48_000;
const PLAYBACK_CHANNELS: i32 = 1;

/// A live LiveKit session: room handle, optional key provider, and our participant identity.
pub struct LiveSession {
    pub room: Room,
    /// Present only when E2EE is enabled (`media_e2ee = true` in config).
    pub key_provider: Option<KeyProvider>,
    /// Our LiveKit participant identity (the JWT `sub` claim). Read only on the
    /// dormant E2EE publish path (subscribe-only bot no longer sets its own key).
    #[allow(dead_code)]
    pub our_identity: String,
    /// Cancels the event task and all per-track playback consumers on teardown.
    cancel: CancellationToken,
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
/// When `use_e2ee` is true, configures HKDF/GCM frame encryption and returns
/// a `KeyProvider` in the session. When false (the common case for deployments
/// that run EC without media E2EE), connects plainly — no frame encryption.
#[allow(clippy::too_many_arguments)]
pub async fn connect(
    url: &str,
    jwt: &str,
    use_e2ee: bool,
    client: Client,
    room_id: OwnedRoomId,
    router: Arc<dyn PlaybackRouter>,
    bridge: Arc<str>,
    parent_cancel: CancellationToken,
) -> Result<LiveSession> {
    let span = tracing::info_span!("rtc.session", sfu_url = %url, use_e2ee);
    let _guard = span.enter();

    // Decode our identity and room name from the JWT before connecting.
    let (our_identity, jwt_room) = decode_jwt_claims(jwt)?;
    tracing::info!(identity = %our_identity, livekit_room = ?jwt_room, "LiveKit JWT claims decoded");

    let (room_opts, key_provider) = if use_e2ee {
        // ratchet_salt is used as the HKDF salt when deriving the actual AES-GCM key
        // from the raw key bytes supplied via set_key. EC's livekit-client-js uses the
        // livekit SDK default "LKFrameEncryptionKey". Empty salt produces a different
        // derived key → decryption fails → raw encrypted bytes output as audio = noise.
        let key_provider_opts = KeyProviderOptions {
            ratchet_window_size: 16, // match livekit-client-js default
            ratchet_salt: b"LKFrameEncryptionKey".to_vec(),
            failure_tolerance: -1,
            key_ring_size: 256,
            key_derivation_algorithm: KeyDerivationAlgorithm::HKDF,
        };
        let key_provider = KeyProvider::new(key_provider_opts);
        let e2ee = E2eeOptions {
            encryption_type: EncryptionType::Gcm,
            key_provider: key_provider.clone(),
        };
        let mut opts = RoomOptions::default();
        opts.encryption = Some(e2ee);
        (opts, Some(key_provider))
    } else {
        (RoomOptions::default(), None)
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
    let event_task = tokio::spawn(
        handle_room_events(events, client, room_id, router, cancel.clone(), bridge.clone())
            .instrument(tracing::info_span!(parent: None, "bridge", name = %bridge)),
    );

    Ok(LiveSession {
        room,
        key_provider,
        our_identity,
        cancel,
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
    let sink = router.assign(key, &display_name);
    // Seed the puppet's mute state right after assign (avoids racing a separate
    // set_muted against the assign that creates the puppet).
    router.set_muted(key, initial_muted);
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
                    sink.push(&f.data);
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

/// Drain LiveKit room events: log for observability, and route subscribed audio
/// tracks into the playback mixer (one consumer task per track, keyed by SID).
async fn handle_room_events(
    mut events: UnboundedReceiver<RoomEvent>,
    client: Client,
    room_id: OwnedRoomId,
    router: Arc<dyn PlaybackRouter>,
    cancel: CancellationToken,
    bridge: Arc<str>,
) {
    // Per-track child tokens so an individual unsubscribe can stop just that
    // consumer; cancelling the parent (teardown) stops them all.
    let mut consumers: HashMap<String, CancellationToken> = HashMap::new();

    while let Some(ev) = events.recv().await {
        match ev {
            RoomEvent::ParticipantConnected(p) => {
                tracing::info!(identity = %p.identity(), "LiveKit: remote participant connected");
            }
            RoomEvent::ParticipantDisconnected(p) => {
                tracing::info!(identity = %p.identity(), "LiveKit: remote participant disconnected");
            }
            RoomEvent::LocalTrackPublished { publication, .. } => {
                tracing::info!(sid = %publication.sid(), kind = ?publication.kind(), muted = publication.is_muted(), "LiveKit: local track published to SFU");
            }
            RoomEvent::TrackSubscribed { track, publication, participant } => {
                let sid = publication.sid().to_string();
                let identity = participant.identity().to_string();
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
                if super::ghost::is_bridge_excluded_identity(&identity) {
                    tracing::debug!(from_identity = %identity, "skipping bridge-owned ghost track (echo prevention)");
                    continue;
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
                tracing::info!(
                    from_identity = %participant.identity(),
                    track_sid = %sid,
                    "LiveKit: unsubscribed from remote participant track"
                );
                if let Some(ct) = consumers.remove(&sid) {
                    ct.cancel();
                }
            }
            RoomEvent::TrackPublished { publication, participant } => {
                tracing::info!(
                    from_identity = %participant.identity(),
                    track_sid = %publication.sid(),
                    kind = ?publication.kind(),
                    "LiveKit: remote participant published a track"
                );
            }
            RoomEvent::Disconnected { reason } => {
                tracing::warn!(reason = ?reason, "LiveKit: room disconnected");
                break;
            }
            other => {
                tracing::debug!(event = ?other, "LiveKit room event");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::participant_mxid;

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
}
