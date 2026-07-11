//! [`MatrixManagementClient`] — a fake self-serve user that DMs the bridge's
//! Matrix management bot and scrapes its replies (#76).
//!
//! Unlike the call actors ([`super::matrix`]) this drives no LiveKit / call
//! membership: it is a plain Matrix user that opens a DM with the management
//! bot and exercises the self-serve `link`/`unlink`/`list`/`help` command tier
//! and the two-sided auth gates (#43/#47/#71). It authenticates the same way
//! every harness Matrix actor does — the SEPARATE `nvbtest` appservice's
//! `as_token`, masquerading as `@nvbtest_*` via the `?user_id=` query — but
//! over raw CS-API calls (create room, invite, send message, read timeline),
//! because neither the bridge crates nor the testkit expose a text-message /
//! room-creation helper.
//!
//! The browser UA + rustls mirror [`crate::observers::RoomStatePoller`]: the
//! standing homeserver sits behind Cloudflare, which 403s default library UAs.

use crate::config::E2eConfig;
use anyhow::{Context, Result, bail};
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Process-wide transaction-id counter for `PUT .../send/...` — a send only
/// needs a value unique per (access token, txn), and a monotonic counter is
/// that within a run (`Date`/`rand` are avoided so the actor stays trivially
/// deterministic).
static TXN: AtomicU64 = AtomicU64::new(1);

/// A self-serve Matrix user driving the management bot over DM.
pub struct MatrixManagementClient {
    http: reqwest::Client,
    base: String,
    as_token: String,
    mxid: String,
    bot_mxid: String,
}

impl MatrixManagementClient {
    /// Register `@{localpart}:<domain>` (idempotent) via the test appservice
    /// and set its display name (best-effort). `localpart` must have a
    /// non-digit suffix (a digit suffix is echo-classified as a bridge ghost —
    /// same rule the testkit's `TestUser` enforces).
    pub async fn register(cfg: &E2eConfig, localpart: &str, display_name: &str) -> Result<Self> {
        let suffix = localpart.rsplit('_').next().unwrap_or(localpart);
        if suffix.is_empty() || suffix.bytes().all(|b| b.is_ascii_digit()) {
            bail!("management-client localpart '{localpart}' needs a non-empty, non-all-digit suffix");
        }
        let http = reqwest::Client::builder()
            .use_rustls_tls()
            .user_agent("Mozilla/5.0 (compatible; nvb-e2e)")
            .build()
            .context("building management-client http")?;
        let me = Self {
            http,
            base: cfg.homeserver_url.trim_end_matches('/').to_owned(),
            as_token: cfg.test_as_token()?.to_owned(),
            mxid: format!("@{localpart}:{}", cfg.server_domain()),
            bot_mxid: cfg.bot_user_id(),
        };
        me.register_user(localpart).await?;
        me.set_display_name(display_name).await;
        Ok(me)
    }

    pub fn mxid(&self) -> &str {
        &self.mxid
    }

    /// Idempotent appservice register (`M_USER_IN_USE` is success — the
    /// standing `nvbtest_*` users persist across runs).
    async fn register_user(&self, localpart: &str) -> Result<()> {
        let resp = self
            .http
            .post(format!("{}/_matrix/client/v3/register", self.base))
            .bearer_auth(&self.as_token)
            .json(&serde_json::json!({
                "type": "m.login.application_service",
                "username": localpart,
            }))
            .send()
            .await
            .context("register request")?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            if !body.contains("M_USER_IN_USE") {
                bail!("register failed: {status}: {body}");
            }
        }
        Ok(())
    }

    async fn set_display_name(&self, name: &str) {
        let url = format!(
            "{}/_matrix/client/v3/profile/{}/displayname",
            self.base,
            enc(&self.mxid)
        );
        let r = self
            .http
            .put(url)
            .query(&[("user_id", &self.mxid)])
            .bearer_auth(&self.as_token)
            .json(&serde_json::json!({ "displayname": name }))
            .send()
            .await;
        if let Err(e) = r {
            tracing::warn!(error = %e, "management-client set displayname failed (continuing)");
        }
    }

    /// Create a **direct** (DM) room inviting the bridge bot. The `is_direct`
    /// flag rides the bot's invite membership, which is what the bot's
    /// open-registration invite gate (`handle_invite`) requires. Returns the
    /// room id.
    pub async fn create_dm(&self) -> Result<String> {
        self.create_room(true).await
    }

    /// Create a room inviting the bot with a **non-direct** invite — the
    /// invite-gate negative case: a self-serve user must not be able to pull
    /// the bot into a non-DM room (`handle_invite` rejects + leaves).
    pub async fn create_room_nondirect(&self) -> Result<String> {
        self.create_room(false).await
    }

    async fn create_room(&self, is_direct: bool) -> Result<String> {
        let resp = self
            .http
            .post(format!("{}/_matrix/client/v3/createRoom", self.base))
            .query(&[("user_id", &self.mxid)])
            .bearer_auth(&self.as_token)
            .json(&serde_json::json!({
                "preset": "trusted_private_chat",
                "is_direct": is_direct,
                "invite": [self.bot_mxid],
            }))
            .send()
            .await
            .context("createRoom request")?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("createRoom failed: {status}: {body}");
        }
        let json: Value = serde_json::from_str(&body).context("createRoom response JSON")?;
        json["room_id"]
            .as_str()
            .map(str::to_owned)
            .context("createRoom response missing room_id")
    }

    /// Invite `mxid` into `room` (as this user). Used to grow a DM past the
    /// two-member `is_command_room` threshold for the #71 gate test.
    pub async fn invite(&self, room: &str, mxid: &str) -> Result<()> {
        let url = format!("{}/_matrix/client/v3/rooms/{}/invite", self.base, enc(room));
        let resp = self
            .http
            .post(url)
            .query(&[("user_id", &self.mxid)])
            .bearer_auth(&self.as_token)
            .json(&serde_json::json!({ "user_id": mxid }))
            .send()
            .await
            .context("invite request")?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("invite failed: {status}: {body}");
        }
        Ok(())
    }

    /// Poll `joined_members` until the bot is a joined member (its DM auto-join
    /// is a raw appservice `POST /join`, so it lands server-side within a beat
    /// of the invite). Errors at `timeout`.
    pub async fn await_bot_joined(&self, room: &str, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.joined_members(room).await?.iter().any(|m| m == &self.bot_mxid) {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("bridge bot never joined {room} (invite not accepted?)");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// Wait until the bot has posted at least one message in `room` — its join
    /// greeting ("👋 Voice bridge management …"). Consume it BEFORE issuing any
    /// command so later reply/silence markers are unambiguous: the greeting is
    /// a bot message not tied to any command, and `greet_when_joined` delays it
    /// up to ~10 s (it could otherwise land after a command marker and be
    /// mistaken for the reply, or trip `expect_silence`).
    pub async fn await_greeting(&self, room: &str, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            // A sentinel that never equals a real event id → collect every bot
            // message in the page.
            if !self.bot_replies_after(room, "\u{0}").await?.is_empty() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("bot never greeted {room} (SDK never reached Joined?)");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// The MXIDs currently joined to `room`.
    pub async fn joined_members(&self, room: &str) -> Result<Vec<String>> {
        let url =
            format!("{}/_matrix/client/v3/rooms/{}/joined_members", self.base, enc(room));
        let resp = self
            .http
            .get(url)
            .query(&[("user_id", &self.mxid)])
            .bearer_auth(&self.as_token)
            .send()
            .await
            .context("joined_members request")?;
        if !resp.status().is_success() {
            bail!("joined_members failed: {}", resp.status());
        }
        let json: Value = resp.json().await.context("joined_members JSON")?;
        Ok(json["joined"]
            .as_object()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default())
    }

    /// Send a plaintext message; returns the sent event id (the marker
    /// [`wait_for_reply`](Self::wait_for_reply) / [`expect_silence`](Self::expect_silence)
    /// read replies relative to).
    pub async fn send(&self, room: &str, body: &str) -> Result<String> {
        let txn = TXN.fetch_add(1, Ordering::Relaxed);
        let url = format!(
            "{}/_matrix/client/v3/rooms/{}/send/m.room.message/nvb{txn}",
            self.base,
            enc(room)
        );
        let resp = self
            .http
            .put(url)
            .query(&[("user_id", &self.mxid)])
            .bearer_auth(&self.as_token)
            .json(&serde_json::json!({ "msgtype": "m.text", "body": body }))
            .send()
            .await
            .context("send request")?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("send failed: {status}: {text}");
        }
        let json: Value = serde_json::from_str(&text).context("send response JSON")?;
        json["event_id"]
            .as_str()
            .map(str::to_owned)
            .context("send response missing event_id")
    }

    /// Fetch the bot's `m.room.message` bodies that appear AFTER `after_event`
    /// in the timeline (newest first). Walks the reverse-chronological
    /// `/messages` page until it reaches our marker event, collecting bot
    /// messages seen along the way. The marker guarantees we never mistake an
    /// earlier reply (or the join greeting) for the answer to this command.
    async fn bot_replies_after(&self, room: &str, after_event: &str) -> Result<Vec<String>> {
        let url = format!("{}/_matrix/client/v3/rooms/{}/messages", self.base, enc(room));
        let resp = self
            .http
            .get(url)
            .query(&[("dir", "b"), ("limit", "50"), ("user_id", self.mxid.as_str())])
            .bearer_auth(&self.as_token)
            .send()
            .await
            .context("messages request")?;
        if !resp.status().is_success() {
            bail!("messages failed: {}", resp.status());
        }
        let json: Value = resp.json().await.context("messages JSON")?;
        let empty = Vec::new();
        let chunk = json["chunk"].as_array().unwrap_or(&empty);
        let mut replies = Vec::new();
        for ev in chunk {
            if ev["event_id"].as_str() == Some(after_event) {
                break; // reached our command — everything after is the reply set
            }
            if ev["type"] == "m.room.message" && ev["sender"].as_str() == Some(&self.bot_mxid) {
                if let Some(body) = ev["content"]["body"].as_str() {
                    replies.push(body.to_owned());
                }
            }
        }
        // Collected newest-first; the reply immediately following our command
        // is the oldest of these — reverse so index 0 is that first reply.
        replies.reverse();
        Ok(replies)
    }

    /// Poll until the bot replies to the command marked by `after_event`,
    /// returning the reply body. Errors at `timeout` (a missing reply is a real
    /// failure — the bot ignored a command it should have served).
    pub async fn wait_for_reply(
        &self,
        room: &str,
        after_event: &str,
        timeout: Duration,
    ) -> Result<String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let replies = self.bot_replies_after(room, after_event).await?;
            if let Some(first) = replies.into_iter().next() {
                return Ok(first);
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("no bot reply to the command in {room} within {timeout:?}");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// Assert the bot sends NO reply to the command marked by `after_event`
    /// within `window` — the #71 DM-gate expectation (a command from a
    /// non-DM/shared room must be silently ignored). Fails loudly if a reply
    /// leaks (the exact regression this scenario guards).
    pub async fn expect_silence(
        &self,
        room: &str,
        after_event: &str,
        window: Duration,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + window;
        loop {
            let replies = self.bot_replies_after(room, after_event).await?;
            if let Some(leaked) = replies.into_iter().next() {
                bail!("bot replied in a non-DM room (#71 gate leak): {leaked:?}");
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

/// Register `@{localpart}:<domain>` and join it to `room` via an
/// appservice-asserted `POST /join` (the user must already be invited). A
/// throwaway extra member used to push a DM past the two-member DM-gate
/// threshold — no full client, just presence.
pub async fn register_and_join(cfg: &E2eConfig, localpart: &str, room: &str) -> Result<String> {
    let http = reqwest::Client::builder()
        .use_rustls_tls()
        .user_agent("Mozilla/5.0 (compatible; nvb-e2e)")
        .build()
        .context("building extra-member http")?;
    let as_token = cfg.test_as_token()?;
    let mxid = format!("@{localpart}:{}", cfg.server_domain());
    // Idempotent register.
    let reg = http
        .post(format!("{}/_matrix/client/v3/register", cfg.homeserver_url.trim_end_matches('/')))
        .bearer_auth(as_token)
        .json(&serde_json::json!({ "type": "m.login.application_service", "username": localpart }))
        .send()
        .await
        .context("extra-member register")?;
    if !reg.status().is_success() {
        let body = reg.text().await.unwrap_or_default();
        if !body.contains("M_USER_IN_USE") {
            bail!("extra-member register failed: {body}");
        }
    }
    // Appservice-asserted join (full member, no guest_access needed).
    let url = format!(
        "{}/_matrix/client/v3/join/{}",
        cfg.homeserver_url.trim_end_matches('/'),
        enc(room)
    );
    let resp = http
        .post(url)
        .query(&[("user_id", &mxid)])
        .bearer_auth(as_token)
        .json(&serde_json::json!({}))
        .send()
        .await
        .context("extra-member join")?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        bail!("extra-member join failed: {status}: {body}");
    }
    Ok(mxid)
}

/// Percent-encode a path segment (room ids carry `!`/`:`; be conservative and
/// escape everything outside the RFC 3986 unreserved set). Mirrors
/// `observers::urlencode`.
fn enc(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enc_escapes_room_id() {
        assert_eq!(enc("!abc:nether.im"), "%21abc%3Anether.im");
        assert_eq!(enc("nvbtest_mgr"), "nvbtest_mgr");
    }
}
