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

/// A short, non-reversible fingerprint of a media key for diagnostics: it lets us
/// correlate "the key a ghost sends", "the key it encrypts frames under", and "the
/// key a subscriber installs" WITHOUT logging the key bytes. Two logs sharing a
/// `key_fp` carry the same key; a mismatch is a bug. Non-cryptographic (std hasher)
/// and only 8 hex chars, so it reveals nothing usable about the key material.
pub(crate) fn key_fp(key: &[u8]) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut h);
    format!("{:08x}", (h.finish() as u32))
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

    let ok = key_provider.set_key(&identity, key_index as i32, key_bytes.clone());
    tracing::info!(
        peer_identity = %identity_str,
        key_index,
        key_fp = %key_fp(&key_bytes),
        installed = ok,
        "installed peer media key"
    );
    ok
}

// ── Peer-key cache: survive a subscriber LiveKit rejoin (#66) ─────────────────

/// Every peer media key delivered to the subscriber, keyed
/// `(sender_mxid, claimed_device_id, key_index)` — last writer wins per triple.
///
/// Why: a disconnect recovery re-runs `join_livekit`, which mints a fresh, EMPTY
/// `KeyProvider`. Peers don't re-send keys on our LiveKit reconnect (they can't
/// see it — it's not a Matrix event), and an idempotent membership re-post is a
/// no-change to their membership diff, so nothing would re-deliver — the rejoined
/// subscriber couldn't decrypt anyone until their next natural rotation. But the
/// bot's Matrix sync never dies during an SFU outage, so every key ever sent
/// (including mid-outage rotations) still reaches the to-device handler: record
/// them all here (write-through, before the provider install) and [`replay`]
/// into each fresh provider. No roster flicker, no forced key rotation.
///
/// [`replay`]: PeerKeyCache::replay
#[derive(Default)]
pub(crate) struct PeerKeyCache {
    keys: std::sync::Mutex<std::collections::HashMap<(String, String, u8), String>>,
}

impl PeerKeyCache {
    /// Record a delivered peer key (overwrites any previous key for the triple).
    pub(crate) fn record(
        &self,
        sender_user_id: &str,
        claimed_device_id: &str,
        key_index: u8,
        key_b64: &str,
    ) {
        self.keys.lock().expect("peer-key cache mutex poisoned").insert(
            (
                sender_user_id.to_owned(),
                claimed_device_id.to_owned(),
                key_index,
            ),
            key_b64.to_owned(),
        );
    }

    /// Whether ANY key has been delivered for the peer publishing as this
    /// LiveKit identity (`{mxid}:{device}` — see [`peer_livekit_identity`]).
    /// Drives the #73 heal gate: a track subscribed while this is `false` had
    /// its first frames hit the decryptor keyless, which wedges it permanently
    /// — the session re-subscribes once the key lands.
    pub(crate) fn has_key_for_identity(&self, identity: &str) -> bool {
        self.keys
            .lock()
            .expect("peer-key cache mutex poisoned")
            .keys()
            .any(|(sender, device, _)| peer_livekit_identity(sender, device) == identity)
    }

    /// Install every cached key into `key_provider` (a fresh provider after a
    /// rejoin). A no-op on the first join of a call (nothing recorded yet).
    pub(crate) fn replay(&self, key_provider: &KeyProvider) {
        let keys = self.keys.lock().expect("peer-key cache mutex poisoned");
        if keys.is_empty() {
            return;
        }
        tracing::info!(count = keys.len(), "replaying cached peer media keys into fresh provider");
        for ((sender, device, index), key_b64) in keys.iter() {
            install_peer_key(sender, device, *index, key_b64, key_provider);
        }
    }

    /// Forget everything (call ended — the next call's keys are all fresh).
    pub(crate) fn clear(&self) {
        self.keys.lock().expect("peer-key cache mutex poisoned").clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_key_cache_records_last_writer_and_clears() {
        let cache = PeerKeyCache::default();
        cache.record("@a:x", "DEV1", 0, "old");
        cache.record("@a:x", "DEV1", 0, "new"); // same triple → overwrite
        cache.record("@a:x", "DEV1", 1, "idx1"); // distinct index coexists
        cache.record("@a:x", "DEV2", 0, "dev2"); // distinct device coexists
        cache.record("@b:x", "DEV1", 0, "b"); // distinct sender coexists
        {
            let keys = cache.keys.lock().unwrap();
            assert_eq!(keys.len(), 4);
            assert_eq!(keys[&("@a:x".into(), "DEV1".into(), 0)], "new");
        }
        cache.clear();
        assert!(cache.keys.lock().unwrap().is_empty());
    }

    #[test]
    fn peer_key_cache_knows_which_identities_are_keyed() {
        let cache = PeerKeyCache::default();
        assert!(!cache.has_key_for_identity("@a:x:DEV1"));
        cache.record("@a:x", "DEV1", 3, "key");
        assert!(cache.has_key_for_identity("@a:x:DEV1"));
        // Same user, different device (LiveKit identities are per-device).
        assert!(!cache.has_key_for_identity("@a:x:DEV2"));
        cache.clear();
        assert!(!cache.has_key_for_identity("@a:x:DEV1"));
    }

    #[test]
    fn peer_key_cache_replays_into_a_fresh_provider() {
        use livekit::e2ee::key_provider::{KeyProvider, KeyProviderOptions};
        use livekit::id::ParticipantIdentity;

        let cache = PeerKeyCache::default();
        // A real (valid-base64, 16-byte) key so install_peer_key succeeds.
        cache.record("@a:x", "DEV1", 0, &encode_key(&[7u8; 16]));

        let provider = KeyProvider::new(KeyProviderOptions::default());
        cache.replay(&provider);
        let installed = provider.get_key(&ParticipantIdentity::from("@a:x:DEV1".to_owned()), 0);
        assert_eq!(installed, Some([7u8; 16].to_vec()));
    }
}
