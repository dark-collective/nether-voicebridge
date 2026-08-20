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

use crate::config::{AsHomeserver, E2eConfig};
use anyhow::{Context, Result, bail};
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Process-wide transaction-id counter for `PUT .../send/...`. The homeserver
/// deduplicates a txn per (access token, txn id) — and every masqueraded send
/// here rides the SAME shared appservice `as_token`, so a bare monotonic counter
/// (which resets to 1 each process) collides ACROSS runs: a later run replaying
/// `nvb1` gets the first run's event id back and NO new event, so the command
/// never lands and `wait_for_reply` can't find its marker.
///
/// The pid alone (`nvb{pid}-{n}`) is NOT a sufficient per-run nonce: the nightly
/// CI job boots an identical container each night, so with a warm compile cache
/// the harness lands on the SAME pid night after night — and Tuwunel persists
/// its txn→event_id map in RocksDB with no expiry. Every send whose (pid, n)
/// was used on ANY earlier night is then silently swallowed (the server returns
/// the OLD night's event id, creates nothing, and the command never lands),
/// which is exactly how nightlies #327–#329 lost `mgmt_self_serve_link` &
/// friends: `wait_for_reply` never found its marker and matched the join
/// greeting instead. Verified live 2026-08-19: replaying run #309's `nvb3300-1`
/// six days later returned the 08-13 event id. Hence the additional per-run
/// wall-clock nonce below — epoch millis captured once at first send.
static TXN: AtomicU64 = AtomicU64::new(1);

/// Epoch-millis captured at the first send of this process — the per-run nonce
/// that makes txn ids unique across (identically-pid'd) nightly containers.
static RUN_NONCE: std::sync::OnceLock<u64> = std::sync::OnceLock::new();

fn run_nonce() -> u64 {
    *RUN_NONCE.get_or_init(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    })
}

/// One `/messages` page distilled for the reply helpers: all event ids in
/// reverse-chronological page order, and the bot's `(event_id, body)` messages
/// among them (same order).
#[derive(Default)]
struct BotPage {
    event_ids: Vec<String>,
    messages: Vec<(String, String)>,
}

/// A self-serve Matrix user driving the management bot over DM.
pub struct MatrixManagementClient {
    http: reqwest::Client,
    base: String,
    as_token: String,
    mxid: String,
    bot_mxid: String,
}

impl MatrixManagementClient {
    /// Register `@{localpart}:<domain>` (idempotent) via the PRIMARY test
    /// appservice and set its display name (best-effort). `localpart` must
    /// have a non-digit suffix (a digit suffix is echo-classified as a bridge
    /// ghost — same rule the testkit's `TestUser` enforces).
    pub async fn register(cfg: &E2eConfig, localpart: &str, display_name: &str) -> Result<Self> {
        Self::register_on(&cfg.primary_as()?, &cfg.bot_user_id(), localpart, display_name).await
    }

    /// [`Self::register`] against an explicit appservice endpoint — the #109
    /// federation fixture mints REMOTE users this way (`hs` = the fixture,
    /// `bot_mxid` = the bridge's mgmt bot on ITS homeserver: a fixture-side
    /// user talking to `@…_bot:nether.im` is a genuinely federated exchange).
    pub async fn register_on(
        hs: &AsHomeserver,
        bot_mxid: &str,
        localpart: &str,
        display_name: &str,
    ) -> Result<Self> {
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
            base: hs.base_url.clone(),
            as_token: hs.as_token.clone(),
            mxid: format!("@{localpart}:{}", hs.domain),
            bot_mxid: bot_mxid.to_owned(),
        };
        me.register_user(localpart).await?;
        me.set_display_name(display_name).await;
        Ok(me)
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

    /// Create a **PUBLIC v12 (domainless-id) room** — the #103 shape: the
    /// room id carries no `:server` tail, and creator authority is implicit
    /// (v12 forbids a `users[]` entry for creators, so an authority check
    /// must recognize creatorship rather than look up a power level). Public
    /// join rule on purpose: inviting the bot instead would trip its own
    /// non-direct invite gate (a self-serve user's invite is auto-rejected →
    /// membership `leave` → the probe join is then refused; live-confirmed on
    /// the #109 fixture), and a public room still exercises the whole #103
    /// surface — id parsing, the via-hinted first-contact probe join, and
    /// v12 creator authority. The `m.call.member` PL-0 override is the
    /// standard Element Call setup the link probes expect of any
    /// to-be-bridged room.
    pub async fn create_room_v12(&self) -> Result<String> {
        let resp = self
            .http
            .post(format!("{}/_matrix/client/v3/createRoom", self.base))
            .query(&[("user_id", &self.mxid)])
            .bearer_auth(&self.as_token)
            .json(&serde_json::json!({
                "room_version": "12",
                "preset": "public_chat",
                "power_level_content_override": {
                    "events": { "org.matrix.msc3401.call.member": 0 },
                },
            }))
            .send()
            .await
            .context("createRoom(v12) request")?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("createRoom(v12) failed: {status}: {body}");
        }
        let json: Value = serde_json::from_str(&body).context("createRoom(v12) response JSON")?;
        json["room_id"]
            .as_str()
            .map(str::to_owned)
            .context("createRoom(v12) response missing room_id")
    }

    /// Create an **invite-only** room WITHOUT inviting the bot (#111): the
    /// link-an-invite-only-room drill runs `link` first (probe join 403s →
    /// awaiting-invite intent), then sends the bot the invite via
    /// [`invite`](Self::invite). Ships the `m.call.member` PL-0 override so
    /// the resumed probe's prereq checks pass; the creator's explicit PL 100
    /// satisfies `require_room_authority`.
    pub async fn create_private_room_prepped(&self) -> Result<String> {
        let resp = self
            .http
            .post(format!("{}/_matrix/client/v3/createRoom", self.base))
            .query(&[("user_id", &self.mxid)])
            .bearer_auth(&self.as_token)
            .json(&serde_json::json!({
                "preset": "private_chat",
                "power_level_content_override": {
                    "events": { "org.matrix.msc3401.call.member": 0 },
                },
            }))
            .send()
            .await
            .context("createRoom(private) request")?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("createRoom(private) failed: {status}: {body}");
        }
        let json: Value =
            serde_json::from_str(&body).context("createRoom(private) response JSON")?;
        json["room_id"]
            .as_str()
            .map(str::to_owned)
            .context("createRoom(private) response missing room_id")
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

    /// This client's own MXID (for teardown leaves and assertions).
    pub fn mxid(&self) -> &str {
        &self.mxid
    }

    /// Leave `room` as `mxid` (appservice masquerade) — scenario teardown so
    /// drill rooms don't accumulate across runs. Best-effort semantics belong
    /// to the caller.
    pub async fn leave_as(&self, room: &str, mxid: &str) -> Result<()> {
        let url = format!("{}/_matrix/client/v3/rooms/{}/leave", self.base, enc(room));
        let resp = self
            .http
            .post(url)
            .query(&[("user_id", &mxid.to_string())])
            .bearer_auth(&self.as_token)
            .json(&serde_json::json!({}))
            .send()
            .await
            .context("leave request")?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("leave failed: {status}: {body}");
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
            if !self.bot_page(room).await?.messages.is_empty() {
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
            "{}/_matrix/client/v3/rooms/{}/send/m.room.message/nvb{}-{}-{txn}",
            self.base,
            enc(room),
            std::process::id(),
            run_nonce(),
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

    /// One reverse-chronological `/messages` page of `room`: every bot
    /// `m.room.message` body (newest first) plus the set of event ids seen —
    /// the raw material for the marker-relative helpers below.
    async fn bot_page(&self, room: &str) -> Result<BotPage> {
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
        let mut page = BotPage::default();
        for ev in chunk {
            if let Some(id) = ev["event_id"].as_str() {
                page.event_ids.push(id.to_owned());
            }
            if ev["type"] == "m.room.message" && ev["sender"].as_str() == Some(self.bot_mxid.as_str()) {
                if let Some(body) = ev["content"]["body"].as_str() {
                    page.messages.push((ev["event_id"].as_str().unwrap_or_default().to_owned(), body.to_owned()));
                }
            }
        }
        Ok(page)
    }

    /// Fetch the bot's `m.room.message` bodies that appear AFTER `after_event`
    /// in the timeline (oldest-after-marker first), or `None` when the marker
    /// event is not (yet) visible in the page.
    ///
    /// The marker guarantees we never mistake an earlier reply (or the join
    /// greeting) for the answer to this command — and the `None` arm is
    /// load-bearing: a just-sent marker can lag the `/messages` view (and a
    /// txn-deduplicated send never materializes at all, see `TXN`), and the
    /// old break-at-marker walk silently degraded to "every bot message ever
    /// is a reply" in that case, matching the greeting as the command's
    /// answer (nightlies #327–#329). Not-visible must read as "not ready",
    /// never as a reply set.
    async fn bot_replies_after(&self, room: &str, after_event: &str) -> Result<Option<Vec<String>>> {
        let page = self.bot_page(room).await?;
        if !page.event_ids.iter().any(|id| id == after_event) {
            return Ok(None);
        }
        let mut replies = Vec::new();
        for id in &page.event_ids {
            if id == after_event {
                break; // reached our command — everything after is the reply set
            }
            if let Some((_, body)) = page.messages.iter().find(|(mid, _)| mid == id) {
                replies.push(body.clone());
            }
        }
        // Collected newest-first; the reply immediately following our command
        // is the oldest of these — reverse so index 0 is that first reply.
        replies.reverse();
        Ok(Some(replies))
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
        let mut marker_seen = false;
        loop {
            if let Some(replies) = self.bot_replies_after(room, after_event).await? {
                marker_seen = true;
                if let Some(first) = replies.into_iter().next() {
                    return Ok(first);
                }
            }
            if tokio::time::Instant::now() >= deadline {
                if !marker_seen {
                    bail!(
                        "command marker {after_event} never became visible in {room} within {timeout:?} — the send did not materialize (txn-dedup swallow?)"
                    );
                }
                bail!("no bot reply to the command in {room} within {timeout:?}");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// Poll until the bot posts a message containing `needle` after
    /// `after_event` — for UNPROMPTED bot messages (#95: the maintenance
    /// sweep's room-misconfigured notice), where earlier bot chatter (the
    /// greeting) must be skipped rather than returned.
    pub async fn wait_for_bot_message_containing(
        &self,
        room: &str,
        after_event: &str,
        needle: &str,
        timeout: Duration,
    ) -> Result<String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            for m in self.bot_replies_after(room, after_event).await?.unwrap_or_default() {
                if m.contains(needle) {
                    return Ok(m);
                }
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("no bot message containing {needle:?} in {room} within {timeout:?}");
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
        let mut marker_seen = false;
        loop {
            if let Some(replies) = self.bot_replies_after(room, after_event).await? {
                marker_seen = true;
                if let Some(leaked) = replies.into_iter().next() {
                    bail!("bot replied in a non-DM room (#71 gate leak): {leaked:?}");
                }
            }
            if tokio::time::Instant::now() >= deadline {
                if !marker_seen {
                    // Silence is only meaningful if the command actually
                    // landed — an invisible marker means the send was
                    // swallowed, not that the bot stayed quiet.
                    bail!(
                        "command marker {after_event} never became visible in {room} within {window:?} — cannot attest silence"
                    );
                }
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
