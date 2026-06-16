//! Element Call media-key exchange: wire types, key generation, content building,
//! and peer-key installation. The *transport* (how the content reaches recipients)
//! lives in [`super::key_transport`] — plaintext to-device (Path 2) or Olm-encrypted
//! to-device (Path 3); both carry the identical content built here.
//!
//! Wire protocol: `io.element.call.encryption_keys`. Content shape mirrors
//! matrix-js-sdk `ToDeviceKeyTransport`:
//!   { "keys": {"index": u8, "key": "<base64-std-padded 16 bytes>"},
//!     "room_id": "...", "member": {"claimed_device_id": "...", "id": "..."},
//!     "session": {"call_id": "", "application": "m.call", "scope": "m.room"},
//!     "sent_ts": <ms u64> }
//!
//! Keys are 16 random bytes. Element Call uses HKDF internally (done by LiveKit's
//! KeyProvider) so we pass the raw bytes straight to `key_provider.set_key`.

use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose};
use livekit::e2ee::key_provider::KeyProvider;
use livekit::id::ParticipantIdentity;
use matrix_sdk::ruma::events::macros::EventContent;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

// ── Wire-format types ────────────────────────────────────────────────────────

/// The `keys` field: a single key object (named "keys" in the spec but singular).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncKeyEntry {
    pub index: u8,
    /// Raw 16-byte key, base64 standard+padded.
    pub key: String,
}

/// The `member` sub-object in the to-device content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncKeyMember {
    pub claimed_device_id: String,
    pub id: String,
}

/// The `session` sub-object in the to-device content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncKeySession {
    pub call_id: String,
    pub application: String,
    pub scope: String,
}

/// Full content of `io.element.call.encryption_keys` to-device events.
#[derive(Clone, Debug, Serialize, Deserialize, EventContent)]
#[ruma_event(type = "io.element.call.encryption_keys", kind = ToDevice)]
pub struct CallEncryptionKeysEventContent {
    /// Single key object (field name is plural by spec convention).
    pub keys: EncKeyEntry,
    pub room_id: String,
    pub member: EncKeyMember,
    pub session: EncKeySession,
    pub sent_ts: u64,
}

// ── Key generation ────────────────────────────────────────────────────────────
//
// E2EE *publish-side* helpers used by the ghost publish path (`rtc::ghost` +
// `rtc::key_transport`): generate a per-ghost media key, encode it, build the
// to-device content. The transport that carries the content is chosen in
// `key_transport` (plaintext or Olm).

/// Generate a fresh 16-byte random media key.
pub fn generate_key() -> [u8; 16] {
    let mut key = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut key);
    key
}

/// Encode 16 raw key bytes as base64 standard with padding (matching JS-SDK output).
pub fn encode_key(key: &[u8]) -> String {
    general_purpose::STANDARD.encode(key)
}

/// Decode a base64-encoded key (accepts padded or unpadded standard base64).
pub fn decode_key(s: &str) -> Result<Vec<u8>> {
    // Try padded first, then no-pad — be liberal on receive.
    general_purpose::STANDARD
        .decode(s)
        .or_else(|_| general_purpose::STANDARD_NO_PAD.decode(s))
        .context("decoding media key base64")
}

/// Current Unix timestamp in milliseconds (the `sent_ts` field).
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ── Content building ──────────────────────────────────────────────────────────

/// Build the `io.element.call.encryption_keys` to-device content for a ghost key.
/// `member.id` (`{device_id}_m.call`) and `member.claimed_device_id` let Element
/// Call peers reconstruct the ghost's LiveKit identity and attribute the key. The
/// shape is transport-independent: both [`super::key_transport`] impls send exactly
/// this content (plaintext or Olm-wrapped).
pub(crate) fn build_key_content(
    room_id: &str,
    device_id: &str,
    key_b64: String,
    key_index: u8,
    sent_ts: u64,
) -> CallEncryptionKeysEventContent {
    CallEncryptionKeysEventContent {
        keys: EncKeyEntry { index: key_index, key: key_b64 },
        room_id: room_id.to_owned(),
        member: EncKeyMember {
            claimed_device_id: device_id.to_owned(),
            id: format!("{device_id}_m.call"),
        },
        session: EncKeySession {
            call_id: String::new(),
            application: "m.call".to_owned(),
            scope: "m.room".to_owned(),
        },
        sent_ts,
    }
}

// ── Receive peers' keys and install into KeyProvider ──────────────────────────

/// Compute a peer's LiveKit participant identity from their Matrix IDs.
///
/// nether.im's Element Call clients use the **legacy** lk-jwt body (no `member`),
/// so lk-jwt gives them the **plaintext** identity scheme:
///   `{matrixID}:{device_id}`
///
/// For example, @dark's identity is `@dark:nether.im:NhxQ7HRSwT` — the device_id
/// comes from their membership content `device_id` and the to-device event's
/// `content.member.claimed_device_id`.
///
/// `user_id`   — Matrix MXID of the sender (from `event.getSender()`)
/// `device_id` — from `content.member.claimed_device_id` in the to-device event
pub fn peer_livekit_identity(user_id: &str, device_id: &str) -> String {
    format!("{}:{}", user_id, device_id)
}

/// Install a received peer key into the key provider.
///
/// `sender_user_id`   — Matrix user ID of the sender (from the to-device event sender)
/// `claimed_device_id` — from `content.member.claimed_device_id`
/// `key_index`         — from `content.keys.index`
/// `key_b64`           — from `content.keys.key` (base64 encoded)
/// `key_provider`      — the LiveKit KeyProvider for the current session
///
/// Returns `true` if the key was successfully installed.
pub fn install_peer_key(
    sender_user_id: &str,
    claimed_device_id: &str,
    key_index: u8,
    key_b64: &str,
    key_provider: &KeyProvider,
) -> bool {
    let identity_str = peer_livekit_identity(sender_user_id, claimed_device_id);
    let identity = ParticipantIdentity::from(identity_str.clone());

    let key_bytes = match decode_key(key_b64) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(
                sender = %sender_user_id,
                device = %claimed_device_id,
                error = %e,
                "failed to decode peer media key — ignoring"
            );
            return false;
        }
    };

    let ok = key_provider.set_key(&identity, key_index as i32, key_bytes);
    tracing::info!(
        peer_identity = %identity_str,
        key_index,
        installed = ok,
        "installed peer media key"
    );
    ok
}
