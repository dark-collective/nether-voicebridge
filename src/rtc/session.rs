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
use livekit::{Room, RoomOptions};
use livekit::e2ee::{E2eeOptions, EncryptionType, key_provider::{KeyDerivationAlgorithm, KeyProvider, KeyProviderOptions}};
use tokio::sync::mpsc::UnboundedReceiver;
use livekit::RoomEvent;

/// A live LiveKit session: room handle, optional key provider, and our participant identity.
pub struct LiveSession {
    pub room: Room,
    /// Present only when E2EE is enabled (`media_e2ee = true` in config).
    pub key_provider: Option<KeyProvider>,
    /// Our LiveKit participant identity (the JWT `sub` claim).
    pub our_identity: String,
    _event_task: tokio::task::JoinHandle<()>,
}

impl LiveSession {
    /// Disconnect from the LiveKit room and clean up.
    pub async fn disconnect(self) {
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
pub async fn connect(url: &str, jwt: &str, use_e2ee: bool) -> Result<LiveSession> {
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
    let event_task = tokio::spawn(log_room_events(events));

    Ok(LiveSession {
        room,
        key_provider,
        our_identity,
        _event_task: event_task,
    })
}

/// Log LiveKit room events for observability.
async fn log_room_events(mut events: UnboundedReceiver<RoomEvent>) {
    while let Some(ev) = events.recv().await {
        match &ev {
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
                tracing::info!(
                    from_identity = %participant.identity(),
                    track_sid = %publication.sid(),
                    kind = ?track.kind(),
                    "LiveKit: subscribed to remote participant track"
                );
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
            _ => {
                tracing::debug!(event = ?ev, "LiveKit room event");
            }
        }
    }
}
