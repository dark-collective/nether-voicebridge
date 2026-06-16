//! Element Call media-key exchange via **plaintext** to-device events.
//!
//! Wire protocol: `io.element.call.encryption_keys` (plaintext to-device).
//! The room has no `m.room.encryption`, so EC sends and expects these events
//! without Olm encryption. We use the raw Matrix `sendToDevice` endpoint.
//!
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
        OwnedTransactionId, OwnedUserId, TransactionId,
        api::client::to_device::send_event_to_device,
        events::{
            AnyToDeviceEventContent,
            ToDeviceEventType,
            call::member::CallMemberEventContent,
            macros::EventContent,
        },
        serde::Raw,
        to_device::DeviceIdOrAllDevices,
    },
};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
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
// E2EE *publish-side* helpers (generate a key, encode it, send it to peers).
// `generate_key`/`encode_key` are used by the ghost publish path (`rtc::ghost`,
// per-ghost media keys in encrypted rooms). `send_key` (the bridge-bot/matrix-sdk
// variant) stays dormant: the bridge bot is subscribe-only, and ghosts send their
// keys over the raw masqueraded CS-API, not via a matrix-sdk `Client`.

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
#[allow(dead_code)]
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ── Send our key to all current call members ──────────────────────────────────

/// Send our media key to every other current call member via **plaintext** to-device.
///
/// The room has no `m.room.encryption`, so EC uses unencrypted to-device events
/// for the `io.element.call.encryption_keys` exchange. Using Olm encryption here
/// fails because OTK claims return nothing (no Olm sessions exist).
///
/// `our_user_id`   — the bridge bot's MXID (OwnedUserId)
/// `our_device_id` — the bridge bot's Matrix device ID
/// `room_id`       — the Matrix room ID string
/// `key_index`     — 0 for initial key
/// `key`           — 16 raw bytes
#[allow(dead_code)]
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

    // Collect active call members that are not us (deduplicated by user_id).
    let raws = room
        .get_state_events_static::<CallMemberEventContent>()
        .await
        .context("reading call member state events")?;

    let mut recipient_users: Vec<OwnedUserId> = Vec::new();
    for raw in raws {
        let Ok(ev) = raw.deserialize() else { continue };
        use matrix_sdk::deserialized_responses::SyncOrStrippedState;
        use matrix_sdk::ruma::events::SyncStateEvent;
        let orig = match ev {
            SyncOrStrippedState::Sync(SyncStateEvent::Original(o)) => o,
            _ => continue,
        };
        if &orig.sender == our_user_id {
            continue;
        }
        let ts = orig.origin_server_ts;
        if orig.content.active_memberships(Some(ts)).is_empty() {
            continue;
        }
        if !recipient_users.contains(&orig.sender) {
            recipient_users.push(orig.sender);
        }
    }

    if recipient_users.is_empty() {
        tracing::info!("no other call members to send key to");
        return Ok(());
    }

    // Build the to-device content.
    let content = CallEncryptionKeysEventContent {
        keys: EncKeyEntry { index: key_index, key: encode_key(key) },
        room_id: room_id.to_owned(),
        member: EncKeyMember {
            claimed_device_id: our_device_id.to_owned(),
            // member.id is the full session member-id suffix: "{device_id}_m.call"
            // EC uses this to look up our membership and attribute the key.
            id: format!("{our_device_id}_m.call"),
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
        Raw::from_json_string(json).context("building Raw content")?;

    // Send plaintext (unencrypted) to-device to all recipients.
    // We send to "*" (all devices) matching EC behaviour.
    let mut messages: BTreeMap<OwnedUserId, BTreeMap<DeviceIdOrAllDevices, Raw<AnyToDeviceEventContent>>> =
        BTreeMap::new();
    for user_id in &recipient_users {
        let mut device_map: BTreeMap<DeviceIdOrAllDevices, Raw<AnyToDeviceEventContent>> =
            BTreeMap::new();
        device_map.insert(DeviceIdOrAllDevices::AllDevices, raw.clone());
        messages.insert(user_id.clone(), device_map);
    }

    let event_type = ToDeviceEventType::from("io.element.call.encryption_keys");
    let txn_id: OwnedTransactionId = TransactionId::new();
    let request = send_event_to_device::v3::Request::new_raw(event_type, txn_id, messages);

    client.send(request).await.context("send plaintext encryption_keys to-device")?;

    tracing::info!(
        recipients = ?recipient_users.iter().map(|u| u.as_str()).collect::<Vec<_>>(),
        key_index,
        "encryption key sent (plaintext to-device) to call members"
    );

    Ok(())
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
