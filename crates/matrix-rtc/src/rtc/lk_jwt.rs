//! Fetch a LiveKit JWT from lk-jwt-service using the **legacy** `/sfu/get` body.
//!
//! ## Why legacy body, not the new `member`-bearing body
//!
//! lk-jwt-service has two identity code paths selected by request body shape:
//!
//! * **Legacy** body `{room, device_id, openid_token}` (no `member` field):
//!   - Room: `sha256(json([room, "m.call#ROOM"]))` — the shared room (hardcoded on server)
//!   - Identity: **`{matrixID}:{device_id}`** — plaintext
//!
//! * **New** body `{room_id, slot_id, member{…}}`:
//!   - Room: `sha256(json([room_id, slot_id]))` — shared when `slot_id = "m.call#ROOM"`
//!   - Identity: **`sha256(json([matrixID, claimed_device_id, member.id]))`** — hash
//!
//! nether.im's Element Call clients (plaintext-era) use the legacy body → plaintext
//! identity. They map incoming media keys to `{senderMXID}:{device_id}` (plaintext)
//! derived from the sender's `m.call.member` membership. If we use the new body,
//! our real LiveKit identity is a sha256 hash that EC can never match → "waiting
//! for media". Using the legacy body makes our identity `@voicebridge:nether.im:BRIDGE01`
//! which EC correctly derives from our membership content `device_id = "BRIDGE01"`.
//!
//! Do NOT add a `member` field back without rethinking the key-attribution scheme.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::rtc::openid::OpenIdToken;

/// Legacy request body for `POST {livekit_service_url}/sfu/get`.
///
/// The absence of a `member` field selects lk-jwt's legacy identity path:
/// `identity = matrixID + ":" + device_id` (plaintext, not sha256-hashed).
#[derive(Debug, Serialize)]
struct LegacyLkJwtRequest<'a> {
    /// The Matrix room ID. lk-jwt derives the LiveKit room as
    /// `sha256(json([room, "m.call#ROOM"]))` — the shared room, internally hardcoded.
    room: &'a str,
    /// The bot's Matrix device ID. Becomes the colon-suffix in our plaintext identity.
    /// Must match the membership content `device_id` so EC can map our key to our
    /// LiveKit participant.
    device_id: &'a str,
    /// Fresh Matrix OpenID token for authentication.
    openid_token: &'a OpenIdToken,
}

/// Response from lk-jwt-service.
#[derive(Debug, Deserialize)]
pub struct LkJwtResponse {
    /// The LiveKit JWT. Some lk-jwt versions call this field "token".
    #[serde(alias = "token")]
    pub jwt: String,
    /// WebSocket URL of the LiveKit SFU to connect to.
    pub url: String,
}

/// Fetch a LiveKit JWT from lk-jwt-service using the legacy body format.
///
/// Yields a **plaintext** participant identity (`{userID}:{deviceID}`) so Element
/// Call can correctly map our media key to our LiveKit participant.
///
/// `service_url`  — `livekit_service_url` from the `.well-known` focus entry
/// `room_id`      — the Matrix room ID (maps to the `room` body field)
/// `device_id`    — the bot's Matrix device ID (maps to the `device_id` body field)
/// `openid_token` — a freshly-minted Matrix OpenID token for the bot's account
pub async fn fetch_livekit_token(
    client: &reqwest::Client,
    service_url: &str,
    room_id: &str,
    device_id: &str,
    openid_token: &OpenIdToken,
) -> Result<LkJwtResponse> {
    // NOTE: do not hold an `.entered()` span guard across the awaits below — it
    // would make this future `!Send`, and Phase 8 calls this from a spawned
    // (Send-required) ghost task. The `url`/`room_id` context is logged inline.
    let url = format!("{service_url}/sfu/get");

    let body = LegacyLkJwtRequest { room: room_id, device_id, openid_token };

    let raw = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;

    let status = raw.status();
    let bytes = raw.bytes().await.with_context(|| format!("reading body from {url}"))?;

    if !status.is_success() {
        let body_text = String::from_utf8_lossy(&bytes);
        anyhow::bail!("POST {url} returned {status}: {body_text}");
    }

    let resp: LkJwtResponse = serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing lk-jwt JSON response from {url}"))?;

    // Log the SFU URL only; never log the JWT itself (it is a bearer credential).
    // Expected sub (our LiveKit identity): "@voicebridge:nether.im:{device_id}"
    tracing::info!(
        sfu_url = %resp.url,
        "LiveKit JWT obtained (legacy body → expected plaintext identity)"
    );
    Ok(resp)
}
