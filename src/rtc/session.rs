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
use livekit::{
    Room, RoomOptions,
    e2ee::{E2eeOptions, EncryptionType, key_provider::{KeyProvider, KeyProviderOptions}},
};
use livekit::e2ee::key_provider::KeyDerivationAlgorithm;
use tokio::sync::mpsc::UnboundedReceiver;
use livekit::RoomEvent;

/// A live LiveKit session: room handle, key provider, and our participant identity.
pub struct LiveSession {
    pub room: Room,
    pub key_provider: KeyProvider,
    /// Our LiveKit participant identity (the JWT `sub` claim). Used to set our own
    /// key in the provider so LiveKit encrypts our outbound frames.
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

/// Decode the `sub` field from the JWT payload (middle base64url segment).
///
/// This gives us the LiveKit participant identity that the SFU assigned to us
/// when it minted the JWT — no need to reconstruct the SHA-256 hash ourselves.
pub fn decode_jwt_sub(jwt: &str) -> Result<String> {
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
    payload
        .get("sub")
        .and_then(|v| v.as_str())
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!("JWT payload has no 'sub' field"))
}

/// Connect to a LiveKit room with E2EE configured for Element Call (HKDF, GCM).
///
/// Returns a `LiveSession` holding the room, a clone of the key provider, and
/// our participant identity decoded from the JWT `sub` claim.
pub async fn connect(url: &str, jwt: &str) -> Result<LiveSession> {
    let span = tracing::info_span!("rtc.session", sfu_url = %url);
    let _guard = span.enter();

    // Decode our identity from the JWT before connecting.
    let our_identity = decode_jwt_sub(jwt)?;
    tracing::info!(identity = %our_identity, "LiveKit participant identity (from JWT sub)");

    // Build the key provider with HKDF so it interoperates with Element Call.
    let key_provider_opts = KeyProviderOptions {
        ratchet_window_size: 10,
        ratchet_salt: vec![],
        failure_tolerance: -1,
        key_ring_size: 256,
        key_derivation_algorithm: KeyDerivationAlgorithm::HKDF,
    };
    let key_provider = KeyProvider::new(key_provider_opts);

    let e2ee = E2eeOptions {
        encryption_type: EncryptionType::Gcm,
        key_provider: key_provider.clone(),
    };

    let mut room_opts = RoomOptions::default();
    room_opts.encryption = Some(e2ee);

    tracing::info!("connecting to LiveKit room");
    let (room, events) = Room::connect(url, jwt, room_opts)
        .await
        .context("LiveKit Room::connect")?;
    tracing::info!("LiveKit room connected");

    // Enable E2EE on the room's manager.
    room.e2ee_manager().set_enabled(true);
    tracing::info!("E2EE enabled on LiveKit room");

    // Drain room events in a background task so the channel doesn't fill up.
    let event_task = tokio::spawn(log_room_events(events));

    Ok(LiveSession {
        room,
        key_provider,
        our_identity,
        _event_task: event_task,
    })
}

/// Log LiveKit room events for observability. Participant join/leave events are
/// especially useful for debugging identity matching.
async fn log_room_events(mut events: UnboundedReceiver<RoomEvent>) {
    while let Some(ev) = events.recv().await {
        match &ev {
            RoomEvent::ParticipantConnected(p) => {
                tracing::info!(identity = %p.identity(), "LiveKit: remote participant connected");
            }
            RoomEvent::ParticipantDisconnected(p) => {
                tracing::info!(identity = %p.identity(), "LiveKit: remote participant disconnected");
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
