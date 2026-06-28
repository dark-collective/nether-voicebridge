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
use matrix_sdk::ruma::{
    api::{
        client::{
            keys::{
                claim_keys::v3::Response as KeysClaimResponse,
                get_keys::v3::Response as KeysQueryResponse,
                upload_keys::v3::Response as KeysUploadResponse,
            },
            to_device::send_event_to_device::v3::Response as ToDeviceResponse,
        },
        IncomingResponse,
    },
    to_device::DeviceIdOrAllDevices,
    OwnedDeviceId, OwnedUserId, UserId,
};
use matrix_sdk_crypto::{
    types::requests::{AnyOutgoingRequest, OutgoingRequest},
    CollectStrategy, DeviceData, OlmMachine,
};
use tokio::sync::Mutex;

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

/// A single call-member device to deliver the media key to. The bridge targets the
/// EXACT device(s) actually in the call (from each `m.call.member`), not every device
/// a user owns: most users carry several other logged-in/offline sessions that aren't
/// in the call, and keying those wastes Olm one-time-key claims (frequently doomed —
/// offline devices run out of OTKs) and slows delivery to the device that matters.
/// Mirrors what Element Call itself does. `Eq`/`Hash` let the loop dedup and detect
/// new joiners.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct KeyRecipient {
    pub user_id: OwnedUserId,
    pub device_id: OwnedDeviceId,
}

/// Delivers a ghost's 16-byte LiveKit frame key to the call's human members.
#[async_trait]
pub(crate) trait KeyTransport: Send + Sync {
    /// Deliver `key` (index `key_index`) to all `recipients`. `Ok(outcome)` reports
    /// how many devices got it and which didn't (the caller may keep bursting for the
    /// failures); `Err` is a hard send failure (nothing delivered).
    async fn send_key(
        &self,
        recipients: &[KeyRecipient],
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
        recipients: &[KeyRecipient],
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

        // { messages: { "@user:hs": { "<device>": <content>, ... } } } — target the
        // exact call-member devices (not "*"), mirroring the Olm path.
        let mut messages: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
        for r in recipients {
            let user_entry = messages
                .entry(r.user_id.to_string())
                .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
            if let Some(obj) = user_entry.as_object_mut() {
                obj.insert(r.device_id.to_string(), content_json.clone());
            }
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

// ── Path 3: Olm-encrypted to-device (send-only OlmMachine, in-memory) ─────────────

/// A send-only Olm crypto engine for one ghost (issue #44). Replaces the per-ghost
/// full `matrix_sdk::Client` — its SQLite crypto store (a deadpool pool of ~32 FDs),
/// its `sync_once`, and its perpetual `/sync` task — with a standalone
/// [`OlmMachine`] backed by an in-memory store, driven by a small hand-rolled HTTP
/// loop over the ghost's own access token.
///
/// The ghost is **send-only**: it never receives or decrypts. Its only job is to
/// Olm-encrypt one media key to the call's in-call recipient devices. So instead of
/// a perpetual `/sync` to keep device-lists fresh, it refreshes them **on demand**
/// with a `/keys/query` right before a key send. This makes the per-ghost crypto FD
/// cost zero (an in-memory store opens no files) and removes the O(ghosts) `/sync`
/// fan-out on the homeserver.
///
/// We write no crypto: Olm (X3DH, double-ratchet, AES-GCM), one-time-key claiming,
/// device tracking, and request building all live in vodozemac via
/// matrix-sdk-crypto. We only carry the machine's outgoing requests to the
/// homeserver and feed the responses back — exactly what `matrix_sdk::Client` did
/// for us behind `encrypt_and_send_raw_to_device`, now lower-level.
pub(crate) struct GhostCrypto {
    machine: OlmMachine,
    /// Shared reqwest pool (cloned from the ghost config) — opens no new connection
    /// pool per ghost, so crypto FD cost stays independent of ghost count.
    http: reqwest::Client,
    homeserver_url: String,
    /// The ghost's OWN access token (from `appservice_login_as`, asserting a fresh
    /// per-lifecycle crypto device id). The four crypto endpoints are ordinary
    /// authenticated CS calls under it — no `as_token` masquerade needed.
    access_token: String,
    /// Serializes the machine's request lifecycle. The distribution loop is a single
    /// task per ghost so contention is nil, but `outgoing_requests` must not run
    /// concurrently (the SDK warns this would duplicate-send); the lock documents and
    /// enforces that.
    req_lock: Mutex<()>,
}

/// Bound on `drain_outgoing` rounds — a backstop against a request that perpetually
/// re-queues. A send-only ghost converges in 1–2 rounds (upload, then query/claim).
const DRAIN_MAX_ROUNDS: usize = 8;

impl GhostCrypto {
    /// Log the ghost in (asserting a fresh, unique **crypto** device id — distinct
    /// from the stable *call* device id in the membership/key content), build an
    /// in-memory [`OlmMachine`], and upload its device keys + one-time keys. The
    /// fresh crypto device id is mandatory: an in-memory account mints new signing
    /// keys every lifecycle, so reusing the stable id would make peers reject it as
    /// `SigningKeyChanged` (#30). A brand-new short-lived device is peer-safe.
    pub(crate) async fn bootstrap(
        http: reqwest::Client,
        homeserver_url: &str,
        as_token: &str,
        localpart: &str,
        ghost_mxid: &str,
    ) -> Result<Self> {
        let user_id: OwnedUserId = ghost_mxid
            .parse()
            .with_context(|| format!("parsing ghost mxid '{ghost_mxid}'"))?;
        // Fresh, unique crypto device id per lifecycle (e.g. `Cab12cd34`). NOT the
        // stable `D{key}-{salt}` call device id.
        let crypto_device_id = mint_crypto_device_id();

        let access_token = crate::matrix::appservice_login_as(
            homeserver_url,
            as_token,
            localpart,
            &crypto_device_id,
        )
        .await
        .context("ghost crypto appservice login")?;

        let machine = OlmMachine::new(&user_id, crypto_device_id.as_str().into()).await;

        let me = Self {
            machine,
            http,
            homeserver_url: homeserver_url.to_owned(),
            access_token,
            req_lock: Mutex::new(()),
        };
        // Upload device keys + OTKs so peers can Olm-encrypt to us / we can be
        // attributed. The OTKs are not strictly needed (the ghost is a pure sender,
        // never an Olm recipient) but `keys_for_upload` bundles them in one request,
        // so we just send what the machine yields.
        {
            let _g = me.req_lock.lock().await;
            me.drain_outgoing().await.context("ghost crypto initial key upload")?;
        }
        tracing::info!(
            crypto_device_id,
            "ghost crypto ready (in-memory OlmMachine; device keys + OTKs uploaded, no /sync)"
        );
        Ok(me)
    }

    /// Olm-encrypt `content` (the `io.element.call.encryption_keys` payload) to
    /// `recipients` and send it. Refreshes device lists + Olm sessions on demand
    /// first. `delivered`/`failed` mirror the old transport so the distribution
    /// loop's burst/heartbeat policy is unchanged.
    pub(crate) async fn encrypt_and_send_key(
        &self,
        recipients: &[KeyRecipient],
        content: &serde_json::Value,
    ) -> Result<KeySendOutcome> {
        let _g = self.req_lock.lock().await;

        let devices = self.ensure_tracked_and_sessions(recipients).await?;
        if devices.is_empty() {
            return Err(anyhow!(
                "no recipient devices known yet (ghost /keys/query not caught up)"
            ));
        }
        let attempted = devices.len();

        // CollectStrategy::AllDevices matches Path 2's "send to *" semantics: we do
        // not verify devices, and a stricter strategy would silently drop unverified
        // recipients → they would hear static. `withheld` are devices the strategy
        // blocked; treated as failed so the caller keeps retrying.
        let (requests, withheld) = self
            .machine
            .encrypt_content_for_devices(
                devices,
                "io.element.call.encryption_keys",
                content,
                CollectStrategy::AllDevices,
            )
            .await
            .context("encrypt_content_for_devices")?;

        let mut failed: Vec<FailedRecipient> = withheld
            .iter()
            .map(|(d, _code)| FailedRecipient {
                user_id: d.user_id().to_owned(),
                device_id: d.device_id().to_owned(),
            })
            .collect();

        // Each ToDeviceRequest is an Olm `m.room.encrypted` envelope for a chunk of
        // recipient devices. A failed PUT fails every device in that chunk; the loop
        // re-sends.
        for req in &requests {
            if let Err(e) = self.put_to_device(req).await {
                tracing::warn!(error = %e, "olm: /sendToDevice chunk failed");
                for (user, devmap) in &req.messages {
                    for dev in devmap.keys() {
                        if let DeviceIdOrAllDevices::DeviceId(id) = dev {
                            failed.push(FailedRecipient {
                                user_id: user.clone(),
                                device_id: id.clone(),
                            });
                        }
                    }
                }
            }
        }

        Ok(KeySendOutcome {
            delivered: attempted.saturating_sub(failed.len()),
            failed,
        })
    }

    /// Track `recipients`' users, run any pending `/keys/query` + `/keys/claim`, and
    /// resolve the in-call devices to [`DeviceData`] for encryption. A device that is
    /// still unknown (its user just joined with a new device, or a cold start) is
    /// simply absent from the result; the caller's burst/heartbeat retries.
    async fn ensure_tracked_and_sessions(
        &self,
        recipients: &[KeyRecipient],
    ) -> Result<Vec<DeviceData>> {
        let mut users: Vec<OwnedUserId> = recipients.iter().map(|r| r.user_id.clone()).collect();
        users.sort_unstable();
        users.dedup();
        // Pass `&UserId`s as an owned `IntoIter` (its item lifetime is `users`'s
        // concrete lifetime). A borrowed `.iter()` or a `|u| &**u` closure makes the
        // lifetime late-bound, which trips the async_trait Send / HRTB inference.
        let user_refs = || -> Vec<&UserId> { users.iter().map(|u| u.as_ref()).collect() };

        // Track newly-seen users (a no-op for already-tracked ones), then run the
        // resulting /keys/query.
        self.machine
            .update_tracked_users(user_refs())
            .await
            .context("update_tracked_users")?;
        self.drain_outgoing().await?;

        let mut devices = self.resolve_devices(recipients).await;
        // If a requested device is missing (e.g. a known user rejoined on a NEW
        // device, so update_tracked_users didn't re-query that user), force a fresh
        // /keys/query and retry once. Steady-state sends skip this entirely.
        if devices.len() < recipients.len() {
            self.machine
                .mark_all_tracked_users_as_dirty()
                .await
                .context("mark_all_tracked_users_as_dirty")?;
            self.drain_outgoing().await?;
            devices = self.resolve_devices(recipients).await;
        }

        // Establish Olm sessions (claim one-time keys) for these users.
        if let Some((txn_id, claim)) = self
            .machine
            .get_missing_sessions(user_refs().into_iter())
            .await
            .context("get_missing_sessions")?
        {
            let mut body = serde_json::Map::new();
            body.insert(
                "one_time_keys".to_owned(),
                serde_json::to_value(&claim.one_time_keys).context("serializing claim keys")?,
            );
            if let Some(t) = claim.timeout {
                body.insert("timeout".to_owned(), (t.as_millis() as u64).into());
            }
            let resp = self
                .post(&["keys", "claim"], &serde_json::Value::Object(body))
                .await?;
            let parsed: KeysClaimResponse = parse_response(resp, "/keys/claim").await?;
            self.machine
                .mark_request_as_sent(&txn_id, &parsed)
                .await
                .context("mark /keys/claim sent")?;
        }

        Ok(devices)
    }

    /// Look up each recipient's in-call device in the machine's store (populated by
    /// `/keys/query`), returning the [`DeviceData`] to encrypt to. Missing/errored
    /// devices are skipped (logged) — same semantics as the old `get_device` path.
    async fn resolve_devices(&self, recipients: &[KeyRecipient]) -> Vec<DeviceData> {
        let mut out = Vec::with_capacity(recipients.len());
        for r in recipients {
            match self.machine.get_device(&r.user_id, &r.device_id, None).await {
                Ok(Some(dev)) => out.push((*dev).clone()),
                Ok(None) => tracing::debug!(
                    user = %r.user_id, device = %r.device_id,
                    "olm: call-member device not tracked yet (will retry)"
                ),
                Err(e) => tracing::warn!(
                    user = %r.user_id, device = %r.device_id, error = %e,
                    "olm: get_device failed"
                ),
            }
        }
        out
    }

    /// Carry the machine's pending outgoing requests to the homeserver and feed the
    /// responses back, until none remain. The caller must hold `req_lock`.
    async fn drain_outgoing(&self) -> Result<()> {
        for _ in 0..DRAIN_MAX_ROUNDS {
            let reqs = self
                .machine
                .outgoing_requests()
                .await
                .context("outgoing_requests")?;
            if reqs.is_empty() {
                return Ok(());
            }
            for req in &reqs {
                self.dispatch(req).await?;
            }
        }
        tracing::warn!(
            rounds = DRAIN_MAX_ROUNDS,
            "ghost crypto drain hit round cap (outgoing requests still pending)"
        );
        Ok(())
    }

    /// Dispatch one outgoing request over raw HTTP and mark it sent. Only the
    /// send-only subset is expected (upload / query / claim / to-device); anything
    /// else is logged and dropped (it will not be marked, but a send-only ghost
    /// never produces signature/room-message requests).
    async fn dispatch(&self, req: &OutgoingRequest) -> Result<()> {
        match req.request() {
            AnyOutgoingRequest::KeysUpload(r) => {
                let mut body = serde_json::Map::new();
                if let Some(dk) = &r.device_keys {
                    body.insert(
                        "device_keys".to_owned(),
                        serde_json::to_value(dk).context("serializing device_keys")?,
                    );
                }
                body.insert(
                    "one_time_keys".to_owned(),
                    serde_json::to_value(&r.one_time_keys).context("serializing one_time_keys")?,
                );
                body.insert(
                    "fallback_keys".to_owned(),
                    serde_json::to_value(&r.fallback_keys).context("serializing fallback_keys")?,
                );
                let resp = self
                    .post(&["keys", "upload"], &serde_json::Value::Object(body))
                    .await?;
                let parsed: KeysUploadResponse = parse_response(resp, "/keys/upload").await?;
                self.machine
                    .mark_request_as_sent(req.request_id(), &parsed)
                    .await
                    .context("mark /keys/upload sent")?;
            }
            AnyOutgoingRequest::KeysQuery(r) => {
                let mut body = serde_json::Map::new();
                body.insert(
                    "device_keys".to_owned(),
                    serde_json::to_value(&r.device_keys).context("serializing query device_keys")?,
                );
                if let Some(t) = r.timeout {
                    body.insert("timeout".to_owned(), (t.as_millis() as u64).into());
                }
                let resp = self
                    .post(&["keys", "query"], &serde_json::Value::Object(body))
                    .await?;
                let parsed: KeysQueryResponse = parse_response(resp, "/keys/query").await?;
                self.machine
                    .mark_request_as_sent(req.request_id(), &parsed)
                    .await
                    .context("mark /keys/query sent")?;
            }
            AnyOutgoingRequest::KeysClaim(r) => {
                let mut body = serde_json::Map::new();
                body.insert(
                    "one_time_keys".to_owned(),
                    serde_json::to_value(&r.one_time_keys).context("serializing claim one_time_keys")?,
                );
                if let Some(t) = r.timeout {
                    body.insert("timeout".to_owned(), (t.as_millis() as u64).into());
                }
                let resp = self
                    .post(&["keys", "claim"], &serde_json::Value::Object(body))
                    .await?;
                let parsed: KeysClaimResponse = parse_response(resp, "/keys/claim").await?;
                self.machine
                    .mark_request_as_sent(req.request_id(), &parsed)
                    .await
                    .context("mark /keys/claim sent")?;
            }
            AnyOutgoingRequest::ToDeviceRequest(r) => {
                self.put_to_device(r).await?;
                // ToDevice has an empty response; the machine needs it marked so the
                // request isn't re-emitted.
                let parsed = ToDeviceResponse::new();
                self.machine
                    .mark_request_as_sent(req.request_id(), &parsed)
                    .await
                    .context("mark /sendToDevice sent")?;
            }
            other => {
                tracing::debug!(?other, "ghost crypto: skipping non-send outgoing request");
            }
        }
        Ok(())
    }

    /// PUT one crypto `ToDeviceRequest` (an Olm `m.room.encrypted` envelope).
    async fn put_to_device(
        &self,
        req: &matrix_sdk_crypto::types::requests::ToDeviceRequest,
    ) -> Result<()> {
        let event_type = req.event_type.to_string();
        let txn = req.txn_id.to_string();
        let url = cs_url(
            &self.homeserver_url,
            &["sendToDevice", &event_type, &txn],
            None,
        )?;
        let body = serde_json::json!({ "messages": req.messages });
        let resp = self
            .http
            .put(url)
            .bearer_auth(&self.access_token)
            .json(&body)
            .send()
            .await
            .context("PUT /sendToDevice (olm)")?;
        if !resp.status().is_success() {
            return Err(anyhow!("/sendToDevice (olm) failed: {}", err_body(resp).await));
        }
        Ok(())
    }

    /// POST a JSON body to a `/_matrix/client/v3` endpoint with the ghost's token.
    async fn post(&self, segments: &[&str], body: &serde_json::Value) -> Result<reqwest::Response> {
        let url = cs_url(&self.homeserver_url, segments, None)?;
        self.http
            .post(url)
            .bearer_auth(&self.access_token)
            .json(body)
            .send()
            .await
            .with_context(|| format!("POST /_matrix/client/v3/{}", segments.join("/")))
    }
}

/// Mint a fresh, unique crypto device id (`C` + 8 lowercase-alnum). Unique per ghost
/// lifecycle so an in-memory account's fresh signing keys never collide with a prior
/// device id (which peers would reject as `SigningKeyChanged`). Distinct from the
/// stable `D{key}-{salt}` *call* device id.
fn mint_crypto_device_id() -> String {
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
    format!("C{suffix}")
}

/// Parse a raw reqwest response into a typed ruma response via
/// [`IncomingResponse::try_from_http_response`] (which also turns a Matrix error
/// body into a typed error). Centralizes the `http::Response` rebuild.
async fn parse_response<R>(resp: reqwest::Response, ctx: &str) -> Result<R>
where
    R: IncomingResponse,
    R::EndpointError: std::error::Error + Send + Sync + 'static,
{
    let status = resp.status();
    let bytes = resp
        .bytes()
        .await
        .with_context(|| format!("reading {ctx} response body"))?;
    let http_resp = http::Response::builder()
        .status(status)
        .body(bytes.to_vec())
        .with_context(|| format!("rebuilding {ctx} http::Response"))?;
    R::try_from_http_response(http_resp).map_err(|e| anyhow!("{ctx} parse/error: {e}"))
}

/// Sends the key Olm-encrypted, per recipient device, via the ghost's send-only
/// [`GhostCrypto`] engine. The homeserver cannot read the key.
pub(crate) struct OlmTransport {
    crypto: std::sync::Arc<GhostCrypto>,
    room_id: String,
    /// The stable *call* device id (`D{key}-{salt}`) — goes in the key content's
    /// `member` so Element Call attributes the key to the ghost's LiveKit identity.
    /// Distinct from the crypto device id inside `GhostCrypto`.
    device_id: String,
}

impl OlmTransport {
    pub(crate) fn new(
        crypto: std::sync::Arc<GhostCrypto>,
        room_id: String,
        device_id: String,
    ) -> Self {
        Self {
            crypto,
            room_id,
            device_id,
        }
    }
}

#[async_trait]
impl KeyTransport for OlmTransport {
    async fn send_key(
        &self,
        recipients: &[KeyRecipient],
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
        let content_value =
            serde_json::to_value(&content).context("serializing encryption key content")?;
        self.crypto.encrypt_and_send_key(recipients, &content_value).await
    }

    fn kind(&self) -> &'static str {
        "olm"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The minted crypto device id must be a fresh, unique, path-safe token. A new
    /// one per lifecycle is what keeps the in-memory account's fresh signing keys
    /// from colliding with a prior device id (which peers reject as
    /// `SigningKeyChanged`, #30).
    #[test]
    fn crypto_device_id_is_fresh_and_well_formed() {
        let a = mint_crypto_device_id();
        let b = mint_crypto_device_id();
        assert_ne!(a, b, "two mints must differ (fresh per lifecycle)");
        assert_eq!(a.len(), 9, "C + 8 chars");
        assert!(a.starts_with('C'));
        assert!(
            a[1..].chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()),
            "suffix is lowercase alnum: {a}"
        );
    }

    /// `parse_response` rebuilds an `http::Response` from raw bytes and hands it to
    /// ruma's `IncomingResponse::try_from_http_response`. This pins the contract the
    /// whole hand-rolled crypto loop depends on: the `http` crate version we build
    /// with must match the one ruma's trait expects, and a real Matrix success body
    /// must deserialize into the typed response. We exercise the trait directly with
    /// the same `http::Response::builder().status(..).body(..)` shape `parse_response`
    /// uses (a live `reqwest::Response` would need a server).
    #[test]
    fn keys_upload_response_parses_from_raw_http() {
        let body = br#"{"one_time_key_counts":{"signed_curve25519":50}}"#.to_vec();
        let http_resp = http::Response::builder()
            .status(200u16)
            .body(body)
            .expect("build http::Response");
        let parsed = KeysUploadResponse::try_from_http_response(http_resp)
            .expect("ruma must parse a /keys/upload success body");
        let count = parsed
            .one_time_key_counts
            .values()
            .next()
            .copied()
            .unwrap_or_default();
        assert_eq!(u64::from(count), 50);
    }

    /// Same contract for `/keys/query` — the response that populates the machine's
    /// device store so `get_device` can resolve in-call recipient devices.
    #[test]
    fn keys_query_response_parses_from_raw_http() {
        let body = br#"{"device_keys":{},"failures":{}}"#.to_vec();
        let http_resp = http::Response::builder()
            .status(200u16)
            .body(body)
            .expect("build http::Response");
        let parsed = KeysQueryResponse::try_from_http_response(http_resp)
            .expect("ruma must parse a /keys/query success body");
        assert!(parsed.device_keys.is_empty());
        assert!(parsed.failures.is_empty());
    }
}
