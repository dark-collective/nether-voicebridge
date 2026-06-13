//! Fetch a LiveKit JWT from lk-jwt-service.
//!
//! lk-jwt-service accepts a Matrix OpenID token and returns a LiveKit JWT
//! that can be used to connect to the SFU's LiveKit room.
//!
//! Endpoint: `POST {livekit_service_url}/sfu/get`
//! Body:  `{ "room_id": "...", "slot_id": "...", "openid_token": { ... } }`
//! Response: `{ "jwt": "<token>", "url": "wss://..." }`  (jwt also aliased as "token")
//!
//! The server verifies the openid_token with the Matrix homeserver to determine
//! the caller's identity — no separate `member` field is needed in the request.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::rtc::openid::OpenIdToken;

/// The `member` sub-object identifying the calling participant.
#[derive(Debug, Serialize)]
struct Member<'a> {
    id: &'a str,
    claimed_user_id: &'a str,
    claimed_device_id: &'a str,
}

/// Request body for `POST {livekit_service_url}/sfu/get`.
#[derive(Debug, Serialize)]
struct LkJwtRequest<'a> {
    room_id: &'a str,
    slot_id: &'a str,
    member: Member<'a>,
    openid_token: &'a OpenIdToken,
}

/// Response from lk-jwt-service.
#[derive(Debug, Deserialize)]
pub struct LkJwtResponse {
    /// The LiveKit JWT. The service calls it "jwt" but some versions use "token".
    /// Used in Stage 2 to connect to the LiveKit room.
    #[serde(alias = "token")]
    #[allow(dead_code)]
    pub jwt: String,
    /// WebSocket URL of the LiveKit SFU to connect to.
    pub url: String,
}

/// Fetch a LiveKit JWT for the given room from the SFU's lk-jwt-service.
///
/// `service_url` — `livekit_service_url` from the `.well-known` focus entry
/// `room_id`     — the Matrix room ID (used as the LiveKit room name)
/// `slot_id`     — the participant slot identifier (Matrix device ID)
/// `user_id`     — the bot's Matrix user ID
/// `openid_token` — a freshly-minted Matrix OpenID token
pub async fn fetch_livekit_token(
    client: &reqwest::Client,
    service_url: &str,
    room_id: &str,
    slot_id: &str,
    user_id: &str,
    openid_token: &OpenIdToken,
) -> Result<LkJwtResponse> {
    let url = format!("{service_url}/sfu/get");
    let _span = tracing::info_span!("rtc.lk_jwt", %url, %room_id).entered();

    let member = Member { id: slot_id, claimed_user_id: user_id, claimed_device_id: slot_id };
    let body = LkJwtRequest { room_id, slot_id, member, openid_token };

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

    // Log the SFU URL; never log the JWT itself (it is a bearer credential).
    tracing::info!(
        sfu_url = %resp.url,
        "LiveKit JWT obtained from lk-jwt-service"
    );
    Ok(resp)
}
