//! Element Call media-key exchange via encrypted to-device events.
//!
//! Wire protocol: `io.element.call.encryption_keys` (encrypted to-device).
//! Content shape mirrors matrix-js-sdk `ToDeviceKeyTransport`:
//!   { "keys": {"index": u8, "key": "<base64-std-padded 16 bytes>"},
//!     "room_id": "...", "member": {"claimed_device_id": "...", "id": "..."},
//!     "session": {"call_id": "", "application": "m.call", "scope": "m.room"},
//!     "sent_ts": <ms u64> }
//!
//! Keys are 16 random bytes. Element Call uses HKDF internally (done by LiveKit's
//! KeyProvider) so we pass the raw bytes straight to `key_provider.set_key`.
//!
//! Receive direction is best-effort (well-behaved bridge); the critical direction
//! for the acceptance test is SEND — humans need our key to decrypt our tone.

use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose};
use livekit::e2ee::key_provider::KeyProvider;
use livekit::id::ParticipantIdentity;
use matrix_sdk::{
    Client, Room,
    ruma::{
        OwnedUserId,
        events::{
            AnyToDeviceEventContent,
            call::member::CallMemberEventContent,
            macros::EventContent,
        },
        serde::Raw,
    },
};
use matrix_sdk_crypto::CollectStrategy;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
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

/// Current Unix timestamp in milliseconds.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ── Send our key to all current call members ──────────────────────────────────

/// Send our media key to every other current call member via Olm-encrypted to-device.
///
/// This is the critical direction for the acceptance test: human participants need
/// our key installed in their LiveKit client before they can decrypt our tone.
///
/// `our_user_id`   — the bridge bot's MXID (OwnedUserId)
/// `our_device_id` — the bridge bot's Matrix device ID (used as `member.id` / `slot_id`)
/// `room_id`       — the Matrix room ID string
/// `key_index`     — 0 for initial key
/// `key`           — 16 raw bytes
pub async fn send_key(
    client: &Client,
    room: &Room,
    our_user_id: &OwnedUserId,
    our_device_id: &str,
    room_id: &str,
    key_index: u8,
    key: &[u8; 16],
) -> Result<()> {
    let span = tracing::info_span!("rtc.keys.send", key_index, %room_id);
    let _guard = span.enter();

    // Collect all active call members that are not us.
    let raws = room
        .get_state_events_static::<CallMemberEventContent>()
        .await
        .context("reading call member state events")?;

    // Unique user_ids to send to (send to all their devices).
    let mut recipient_users: Vec<OwnedUserId> = Vec::new();
    for raw in raws {
        let Ok(ev) = raw.deserialize() else { continue };
        use matrix_sdk::deserialized_responses::SyncOrStrippedState;
        use matrix_sdk::ruma::events::SyncStateEvent;
        let orig = match ev {
            SyncOrStrippedState::Sync(SyncStateEvent::Original(o)) => o,
            _ => continue,
        };
        // Skip our own membership.
        if &orig.sender == our_user_id {
            continue;
        }
        // Skip empty / expired memberships.
        let ts = orig.origin_server_ts;
        if orig.content.active_memberships(Some(ts)).is_empty() {
            continue;
        }
        recipient_users.push(orig.sender);
    }

    if recipient_users.is_empty() {
        tracing::info!("no other call members to send key to");
        return Ok(());
    }

    tracing::info!(
        recipient_count = recipient_users.len(),
        key_index,
        "sending encryption key to call members"
    );

    // Collect Device handles for every device of each recipient.
    // We send to ALL devices (matching Element Call behaviour).
    let mut all_devices: Vec<matrix_sdk::encryption::identities::Device> = Vec::new();
    for user_id in &recipient_users {
        let user_devices = client
            .encryption()
            .get_user_devices(user_id)
            .await
            .with_context(|| format!("get_user_devices for {user_id}"))?;
        all_devices.extend(user_devices.devices());
    }

    if all_devices.is_empty() {
        tracing::warn!("no Device handles found — keys/query may not have run yet; will retry on next membership event");
        return Ok(());
    }

    // Build the to-device content.
    let content = CallEncryptionKeysEventContent {
        keys: EncKeyEntry {
            index: key_index,
            key: encode_key(key),
        },
        room_id: room_id.to_owned(),
        member: EncKeyMember {
            claimed_device_id: our_device_id.to_owned(),
            id: our_device_id.to_owned(),
        },
        session: EncKeySession {
            call_id: String::new(),
            application: "m.call".to_owned(),
            scope: "m.room".to_owned(),
        },
        sent_ts: now_ms(),
    };

    let json = serde_json::to_string(&content).context("serializing encryption key content")?;
    let raw: Raw<AnyToDeviceEventContent> =
        Raw::from_json_string(json).context("building Raw<AnyToDeviceEventContent>")?;

    let failures = client
        .encryption()
        .encrypt_and_send_raw_to_device(
            all_devices.iter().collect(),
            "io.element.call.encryption_keys",
            raw,
            CollectStrategy::AllDevices,
        )
        .await
        .context("encrypt_and_send_raw_to_device")?;

    if failures.is_empty() {
        tracing::info!(key_index, "encryption key sent to all recipients");
    } else {
        tracing::warn!(
            failure_count = failures.len(),
            key_index,
            "encryption key send had failures (some devices may not decrypt our audio)"
        );
        for (user, device) in &failures {
            tracing::warn!(user = %user, device = %device, "key send failure");
        }
    }

    Ok(())
}

// ── Receive peers' keys and install into KeyProvider ──────────────────────────

/// Compute a peer's LiveKit participant identity from their Matrix IDs.
///
/// nether.im's newer lk-jwt-service uses:
///   base64url-no-pad( sha256( json_array([user_id, device_id, member_id]) ) )
/// where member_id = device_id (the `slot_id` we confirmed from the lk-jwt format).
pub fn peer_livekit_identity(user_id: &str, device_id: &str) -> String {
    // member_id = device_id (slot_id convention used in our lk-jwt requests)
    let json_arr = serde_json::to_string(&[user_id, device_id, device_id])
        .unwrap_or_default();
    let hash = Sha256::digest(json_arr.as_bytes());
    general_purpose::URL_SAFE_NO_PAD.encode(hash)
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
