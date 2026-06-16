//! Media-key transport — how a ghost delivers its LiveKit frame key to the call's
//! human members. One trait, two implementations, so the distribution loop
//! ([`super::ghost::distribute_key_loop`]) keeps its burst+heartbeat retry policy
//! transport-agnostic and unit-testable with a mock.
//!
//! * [`PlaintextTransport`] — **Path 2.** An unencrypted
//!   `io.element.call.encryption_keys` to-device PUT masqueraded as the ghost
//!   (`as_token` + `?user_id=`). The homeserver can read the key; correct for an
//!   unencrypted room, where Element Call has no media-E2EE security model anyway.
//!
//! * [`OlmTransport`] — **Path 3.** The *same* content, Olm-encrypted per recipient
//!   device by the SDK's [`encrypt_and_send_raw_to_device`]. True end-to-end
//!   encryption: the homeserver cannot read the key. **We write no crypto** — Olm
//!   (X3DH, double-ratchet, AES-GCM), one-time-key claiming, device tracking, and
//!   request chunking all live in vodozemac via matrix-sdk-crypto behind that single
//!   call (the same one the SDK's own Element Call widget driver uses).
//!
//! [`encrypt_and_send_raw_to_device`]: matrix_sdk::encryption::Encryption::encrypt_and_send_raw_to_device

use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use matrix_sdk::{
    encryption::identities::Device,
    ruma::{events::AnyToDeviceEventContent, serde::Raw, OwnedDeviceId, OwnedUserId, UserId},
    Client,
};
use matrix_sdk_crypto::CollectStrategy;

use super::ghost::{cs_url, err_body};
use super::keys;

/// A recipient device the transport could not deliver to. The distribution loop
/// logs these and keeps bursting/heartbeating so a transient gap (e.g. a recipient
/// device not yet tracked, or out of one-time keys) is retried rather than dropped.
#[derive(Debug, Clone)]
pub(crate) struct FailedRecipient {
    pub user_id: OwnedUserId,
    pub device_id: OwnedDeviceId,
}

/// Outcome of one key send. `delivered` is how many recipient devices got the key;
/// `failed` is the ones that didn't. The two together let the caller tell a working
/// partial send (delivered > 0, some stale/offline devices skipped — normal) apart
/// from a real failure (delivered == 0).
#[derive(Debug, Default)]
pub(crate) struct KeySendOutcome {
    pub delivered: usize,
    pub failed: Vec<FailedRecipient>,
}

/// Delivers a ghost's 16-byte LiveKit frame key to the call's human members.
#[async_trait]
pub(crate) trait KeyTransport: Send + Sync {
    /// Deliver `key` (index `key_index`) to all `recipients`. `Ok(outcome)` reports
    /// how many devices got it and which didn't (the caller may keep bursting for the
    /// failures); `Err` is a hard send failure (nothing delivered).
    async fn send_key(
        &self,
        recipients: &[OwnedUserId],
        key: &[u8; 16],
        key_index: u8,
    ) -> Result<KeySendOutcome>;

    /// Short label for logs ("plaintext" / "olm").
    fn kind(&self) -> &'static str;
}

// ── Path 2: plaintext to-device (masqueraded raw CS-API) ────────────────────────

/// Sends the key in the clear via a masqueraded `/sendToDevice` PUT. Byte-identical
/// to the original Path-2 behavior; kept as a config-selectable fallback for
/// unencrypted rooms.
pub(crate) struct PlaintextTransport {
    http: reqwest::Client,
    homeserver_url: String,
    as_token: String,
    room_id: String,
    ghost_mxid: String,
    device_id: String,
    /// Monotonic, RNG-free transaction-id counter so a re-send can't collide with a
    /// prior txn and be de-duped by the server.
    seq: AtomicU64,
}

impl PlaintextTransport {
    pub(crate) fn new(
        http: reqwest::Client,
        homeserver_url: String,
        as_token: String,
        room_id: String,
        ghost_mxid: String,
        device_id: String,
    ) -> Self {
        Self {
            http,
            homeserver_url,
            as_token,
            room_id,
            ghost_mxid,
            device_id,
            seq: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl KeyTransport for PlaintextTransport {
    async fn send_key(
        &self,
        recipients: &[OwnedUserId],
        key: &[u8; 16],
        key_index: u8,
    ) -> Result<KeySendOutcome> {
        if recipients.is_empty() {
            return Ok(KeySendOutcome::default());
        }
        let content = keys::build_key_content(
            &self.room_id,
            &self.device_id,
            keys::encode_key(key),
            key_index,
            keys::now_ms(),
        );
        let content_json =
            serde_json::to_value(&content).context("serializing encryption key content")?;

        // { messages: { "@user:hs": { "*": <content> } } }
        let per_device = serde_json::json!({ "*": content_json });
        let mut messages = serde_json::Map::new();
        for recipient in recipients {
            messages.insert(recipient.to_string(), per_device.clone());
        }
        let body = serde_json::json!({ "messages": serde_json::Value::Object(messages) });

        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let txn = format!("{}-key-{}", self.device_id, seq);
        let url = cs_url(
            &self.homeserver_url,
            &["sendToDevice", "io.element.call.encryption_keys", &txn],
            Some(&self.ghost_mxid),
        )?;
        let resp = self
            .http
            .put(url)
            .bearer_auth(&self.as_token)
            .json(&body)
            .send()
            .await
            .context("PUT /sendToDevice (encryption_keys)")?;
        if !resp.status().is_success() {
            return Err(anyhow!("sendToDevice failed: {}", err_body(resp).await));
        }
        // A plaintext PUT is all-or-nothing per request — the server fans it out to
        // every recipient device (`*`), so on success all recipients are "delivered"
        // and there are no per-device failures.
        Ok(KeySendOutcome {
            delivered: recipients.len(),
            failed: Vec::new(),
        })
    }

    fn kind(&self) -> &'static str {
        "plaintext"
    }
}

// ── Path 3: Olm-encrypted to-device (per-ghost crypto client) ────────────────────

/// Sends the key Olm-encrypted, per recipient device, via the ghost's own
/// crypto-capable matrix-sdk [`Client`]. The homeserver cannot read the key.
pub(crate) struct OlmTransport {
    /// The per-ghost crypto client (its own device + persistent crypto store, kept
    /// in sync by its own background sync task so device lists stay fresh).
    client: Client,
    room_id: String,
    device_id: String,
}

impl OlmTransport {
    pub(crate) fn new(client: Client, room_id: String, device_id: String) -> Self {
        Self {
            client,
            room_id,
            device_id,
        }
    }
}

#[async_trait]
impl KeyTransport for OlmTransport {
    async fn send_key(
        &self,
        recipients: &[OwnedUserId],
        key: &[u8; 16],
        key_index: u8,
    ) -> Result<KeySendOutcome> {
        if recipients.is_empty() {
            return Ok(KeySendOutcome::default());
        }
        let content = keys::build_key_content(
            &self.room_id,
            &self.device_id,
            keys::encode_key(key),
            key_index,
            keys::now_ms(),
        );
        let json = serde_json::to_string(&content).context("serializing encryption key content")?;
        let raw: Raw<AnyToDeviceEventContent> =
            Raw::from_json_string(json).context("building Raw to-device content")?;

        // Resolve every recipient's devices from THIS ghost's crypto store (kept
        // current by its sync task). A device the ghost hasn't tracked yet is simply
        // absent here; the caller's burst/heartbeat retries once sync catches up.
        let mut devices: Vec<Device> = Vec::new();
        for user in recipients {
            let uid: &UserId = user;
            match self.client.encryption().get_user_devices(uid).await {
                Ok(user_devices) => devices.extend(user_devices.devices()),
                Err(e) => {
                    tracing::warn!(user = %user, error = %e, "olm: get_user_devices failed");
                }
            }
        }
        if devices.is_empty() {
            return Err(anyhow!(
                "no recipient devices known yet (ghost crypto sync not caught up)"
            ));
        }

        let device_refs: Vec<&Device> = devices.iter().collect();
        let attempted = device_refs.len();
        // CollectStrategy::AllDevices matches Path 2's "send to *" semantics: we do
        // not verify devices, and a stricter strategy would silently drop unverified
        // recipients → they would hear static. Olm session setup + OTK claiming +
        // request chunking happen inside this call.
        let failed = self
            .client
            .encryption()
            .encrypt_and_send_raw_to_device(
                device_refs,
                "io.element.call.encryption_keys",
                raw,
                CollectStrategy::AllDevices,
            )
            .await
            .context("encrypt_and_send_raw_to_device")?;

        let failed: Vec<FailedRecipient> = failed
            .into_iter()
            .map(|(user_id, device_id)| FailedRecipient { user_id, device_id })
            .collect();
        Ok(KeySendOutcome {
            delivered: attempted.saturating_sub(failed.len()),
            failed,
        })
    }

    fn kind(&self) -> &'static str {
        "olm"
    }
}
