//! [`MatrixCryptoDmClient`] — a fake self-serve user that DMs the bridge's
//! management bot over an **encrypted** room (#99).
//!
//! The E2EE sibling of [`super::mgmt_client::MatrixManagementClient`]: real
//! Element users open encrypted DMs by default, so the plaintext raw-CS client
//! structurally cannot cover the path the first real self-serve user hit
//! (#96/#97). This actor runs a full matrix-sdk crypto [`Client`] — the same
//! shape as the testkit's decrypting listener (`join_and_subscribe_decrypting`)
//! — as an `@nvbtest_*` user with an **ephemeral, server-minted device** and
//! in-memory stores:
//!
//! * register/login ride the test appservice's `as_token`
//!   (`m.login.application_service`), mirroring
//!   `nvb-matrix-rtc::matrix::appservice_login_as` (which is `pub(crate)`
//!   there — reimplemented here rather than widening that seam);
//! * the initial `sync_once` uploads the device's Olm keys + one-time keys so
//!   the bot can encrypt to us from its very first reply;
//! * a perpetual `sync()` task drives the OlmMachine (room-key to-device
//!   delivery);
//! * reads go through [`Room::messages`], which attempts decryption per call —
//!   polling it retries decryption until the room key lands, and its
//!   [`TimelineEventKind`] doubles as the wire-encryption proof (`Decrypted`
//!   ⇔ the event crossed the server as `m.room.encrypted`; a `PlainText`
//!   `m.room.message` is a transport leak);
//! * teardown leaves the room and calls `logout()`, deleting the ephemeral
//!   device server-side so reruns don't accumulate devices on the standing
//!   `nvbtest_*` users (stale keyless devices show up as `unreachable` noise
//!   in every peer's key sends).

use crate::config::E2eConfig;
use anyhow::{Context, Result, bail};
use matrix_sdk::{
    Client, RoomState, SessionMeta,
    authentication::{SessionTokens, matrix::MatrixSession},
    config::SyncSettings,
    deserialized_responses::TimelineEventKind,
    room::MessagesOptions,
    ruma::{OwnedRoomId, events::room::message::RoomMessageEventContent},
};
use serde_json::Value;
use std::time::Duration;

/// A message observed in the DM timeline, tagged with how it crossed the wire.
#[derive(Debug)]
struct SeenMessage {
    sender: String,
    body: String,
    /// `true` ⇔ the wire event was `m.room.encrypted` (successfully decrypted).
    encrypted: bool,
}

/// A self-serve Matrix user driving the management bot over an ENCRYPTED DM.
pub struct MatrixCryptoDmClient {
    http: reqwest::Client,
    base: String,
    as_token: String,
    mxid: String,
    bot_mxid: String,
    client: Client,
    sync_task: tokio::task::JoinHandle<()>,
}

impl MatrixCryptoDmClient {
    /// Register `@{localpart}:<domain>` (idempotent) via the test appservice,
    /// login a fresh crypto device, and start syncing. Same localpart rule as
    /// the plaintext client: a digit suffix is echo-classified as a ghost.
    pub async fn register(cfg: &E2eConfig, localpart: &str, display_name: &str) -> Result<Self> {
        let suffix = localpart.rsplit('_').next().unwrap_or(localpart);
        if suffix.is_empty() || suffix.bytes().all(|b| b.is_ascii_digit()) {
            bail!("crypto-dm localpart '{localpart}' needs a non-empty, non-all-digit suffix");
        }
        let http = reqwest::Client::builder()
            .use_rustls_tls()
            .user_agent("Mozilla/5.0 (compatible; nvb-e2e)")
            .build()
            .context("building crypto-dm http")?;
        let base = cfg.homeserver_url.trim_end_matches('/').to_owned();
        let as_token = cfg.test_as_token()?.to_owned();
        let mxid = format!("@{localpart}:{}", cfg.server_domain());

        // Idempotent appservice register (M_USER_IN_USE is success).
        let reg = http
            .post(format!("{base}/_matrix/client/v3/register"))
            .bearer_auth(&as_token)
            .json(&serde_json::json!({
                "type": "m.login.application_service",
                "username": localpart,
            }))
            .send()
            .await
            .context("crypto-dm register request")?;
        if !reg.status().is_success() {
            let status = reg.status();
            let body = reg.text().await.unwrap_or_default();
            if !body.contains("M_USER_IN_USE") {
                bail!("crypto-dm register failed: {status}: {body}");
            }
        }

        // Login WITHOUT asserting a device id → the server mints a fresh one,
        // unique per run (no #30 SigningKeyChanged trap, nothing to persist).
        let resp = http
            .post(format!("{base}/_matrix/client/v3/login"))
            .bearer_auth(&as_token)
            .json(&serde_json::json!({
                "type": "m.login.application_service",
                "identifier": { "type": "m.id.user", "user": localpart },
                "initial_device_display_name": display_name,
            }))
            .send()
            .await
            .context("crypto-dm login request")?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("crypto-dm login failed: {status}: {body}");
        }
        let login: Value = serde_json::from_str(&body).context("crypto-dm login JSON")?;
        let access_token = login["access_token"]
            .as_str()
            .context("login response missing access_token")?
            .to_owned();
        let device_id = login["device_id"]
            .as_str()
            .context("login response missing device_id")?
            .to_owned();

        // Full crypto Client with in-memory stores — an ephemeral device needs
        // no sqlite (and leaves nothing to clean up).
        let client = Client::builder()
            .homeserver_url(&base)
            .build()
            .await
            .context("building crypto-dm Client")?;
        client
            .restore_session(MatrixSession {
                meta: SessionMeta {
                    user_id: mxid.parse().context("parsing crypto-dm mxid")?,
                    device_id: device_id.as_str().into(),
                },
                tokens: SessionTokens { access_token, refresh_token: None },
            })
            .await
            .context("restoring crypto-dm session")?;
        // First sync uploads device keys + one-time keys — the bot must be able
        // to claim an OTK for this device BEFORE it encrypts its first reply.
        client
            .sync_once(SyncSettings::default())
            .await
            .context("crypto-dm initial sync")?;
        // Perpetual sync drives the OlmMachine (room-key to-device delivery).
        let sync_task = tokio::spawn({
            let sync_client = client.clone();
            async move {
                if let Err(e) = sync_client.sync(SyncSettings::default()).await {
                    tracing::warn!(error = %e, "crypto-dm sync loop ended");
                }
            }
        });

        Ok(Self {
            http,
            base,
            as_token,
            mxid,
            bot_mxid: cfg.bot_user_id(),
            client,
            sync_task,
        })
    }

    /// This client's own MXID (for assertions).
    pub fn mxid(&self) -> &str {
        &self.mxid
    }

    /// Create an **encrypted** direct room inviting the bridge bot — the shape
    /// Element mints for a new DM by default (`m.room.encryption` in
    /// `initial_state`, `is_direct` riding the invite). Raw CS call over the
    /// `as_token` masquerade (same creator MXID; the crypto client picks the
    /// room up on its next sync).
    pub async fn create_encrypted_dm(&self) -> Result<String> {
        let resp = self
            .http
            .post(format!("{}/_matrix/client/v3/createRoom", self.base))
            .query(&[("user_id", &self.mxid)])
            .bearer_auth(&self.as_token)
            .json(&serde_json::json!({
                "preset": "trusted_private_chat",
                "is_direct": true,
                "invite": [self.bot_mxid],
                "initial_state": [{
                    "type": "m.room.encryption",
                    "state_key": "",
                    "content": { "algorithm": "m.megolm.v1.aes-sha2" },
                }],
            }))
            .send()
            .await
            .context("crypto-dm createRoom request")?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("crypto-dm createRoom failed: {status}: {body}");
        }
        let json: Value = serde_json::from_str(&body).context("createRoom response JSON")?;
        json["room_id"]
            .as_str()
            .map(str::to_owned)
            .context("createRoom response missing room_id")
    }

    /// The SDK room, once the sync loop has caught up to our (server-side)
    /// membership. The room was created over the raw CS API, so the client
    /// only learns of it from sync — poll briefly.
    async fn sdk_room(&self, room: &str, timeout: Duration) -> Result<matrix_sdk::Room> {
        let room_id: OwnedRoomId = room.parse().context("parsing room id")?;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(r) = self.client.get_room(&room_id) {
                if r.state() == RoomState::Joined {
                    return Ok(r);
                }
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("crypto-dm client never saw {room} as joined (sync lagging?)");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Send a message through the SDK — in this encrypted room it goes out as
    /// `m.room.encrypted` (the SDK queries the bot's devices and shares the
    /// megolm session automatically). Returns the event id.
    pub async fn send(&self, room: &str, body: &str) -> Result<String> {
        let sdk_room = self.sdk_room(room, Duration::from_secs(30)).await?;
        let resp = sdk_room
            .send(RoomMessageEventContent::text_plain(body))
            .await
            .context("crypto-dm encrypted send")?;
        Ok(resp.event_id.to_string())
    }

    /// One decryption pass over the recent timeline (newest-first `/messages`
    /// page; [`Room::messages`] attempts decryption per call, so polling this
    /// retries until the room key arrives). Returns oldest-first.
    async fn recent_messages(&self, room: &str) -> Result<Vec<SeenMessage>> {
        let sdk_room = self.sdk_room(room, Duration::from_secs(30)).await?;
        let mut opts = MessagesOptions::backward();
        opts.limit = 50u32.into();
        let page = sdk_room.messages(opts).await.context("crypto-dm messages page")?;
        let mut seen = Vec::new();
        for ev in &page.chunk {
            let encrypted = match &ev.kind {
                TimelineEventKind::Decrypted(_) => true,
                TimelineEventKind::PlainText { .. } => false,
                // Key not delivered yet — the next poll retries.
                TimelineEventKind::UnableToDecrypt { .. } => continue,
            };
            // `raw()` yields the DECRYPTED payload for `Decrypted` events.
            let Ok(json) = serde_json::to_value(ev.raw()) else { continue };
            if json["type"] != "m.room.message" {
                continue;
            }
            let (Some(sender), Some(body)) =
                (json["sender"].as_str(), json["content"]["body"].as_str())
            else {
                continue;
            };
            seen.push(SeenMessage {
                sender: sender.to_owned(),
                body: body.to_owned(),
                encrypted,
            });
        }
        seen.reverse();
        Ok(seen)
    }

    /// Poll until the bot posts a **decryptable** message containing `needle`.
    /// Needle-scan, never reply-count: the bot's greeting / "Unrecognized
    /// command" chatter in a 2-member room is expected.
    pub async fn wait_for_bot_message_containing(
        &self,
        room: &str,
        needle: &str,
        timeout: Duration,
    ) -> Result<String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            for m in self.recent_messages(room).await? {
                if m.sender == self.bot_mxid && m.encrypted && m.body.contains(needle) {
                    return Ok(m.body);
                }
            }
            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "no decryptable bot message containing {needle:?} in {room} within {timeout:?}"
                );
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// The transport proof: every `m.room.message` in the DM — ours AND the
    /// bot's — must have crossed the wire as `m.room.encrypted` (i.e. decoded
    /// via the `Decrypted` timeline kind, never `PlainText`). Also requires at
    /// least one message from each side so "no leaks" can't pass vacuously.
    pub async fn assert_transport_encrypted(&self, room: &str) -> Result<()> {
        let seen = self.recent_messages(room).await?;
        let mut ours = 0usize;
        let mut bots = 0usize;
        for m in &seen {
            if !m.encrypted {
                bail!(
                    "PLAINTEXT m.room.message from {} leaked into the encrypted DM: {:?}",
                    m.sender,
                    m.body
                );
            }
            if m.sender == self.mxid {
                ours += 1;
            } else if m.sender == self.bot_mxid {
                bots += 1;
            }
        }
        if ours == 0 || bots == 0 {
            bail!(
                "transport proof needs traffic both ways (saw {ours} from us, {bots} from the bot)"
            );
        }
        Ok(())
    }

    /// Teardown: leave the DM (so drill rooms don't accumulate), stop the sync
    /// loop, and `logout()` — deleting this run's ephemeral device server-side.
    pub async fn teardown(self, room: &str) -> Result<()> {
        if let Ok(sdk_room) = self.sdk_room(room, Duration::from_secs(5)).await {
            if let Err(e) = sdk_room.leave().await {
                tracing::warn!(error = %e, "crypto-dm leave failed (continuing to logout)");
            }
        }
        self.sync_task.abort();
        self.client.logout().await.context("crypto-dm logout")?;
        Ok(())
    }
}
