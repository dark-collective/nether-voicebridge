//! Fetch a LiveKit JWT from lk-jwt-service.
//!
//! lk-jwt-service accepts a Matrix OpenID token and returns a LiveKit JWT
//! that can be used to connect to the SFU's LiveKit room.
//!
//! Endpoint: `POST {lk_jwt_url}/sfu/get`
//! Body:  `{ "device_id": "...", "room": "...", "openid_token": { ... } }`
//! Response: `{ "jwt": "<token>", "url": "wss://..." }`  (jwt also aliased as "token")

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::rtc::openid::OpenIdToken;

/// Request body for `POST {lk_jwt_url}/sfu/get`.
#[derive(Debug, Serialize)]
struct LkJwtRequest<'a> {
    device_id: &'a str,
    room: &'a str,
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

/// Fetch a LiveKit JWT for the given `room` from lk-jwt-service.
///
/// `lk_jwt_url` — base URL of the lk-jwt-service (e.g. `https://lk-jwt.nether.im`)
/// `room`       — the LiveKit room name / alias
/// `device_id`  — the Matrix device id of the joining participant
/// `openid_token` — a freshly-minted Matrix OpenID token
pub async fn fetch_livekit_token(
    client: &reqwest::Client,
    lk_jwt_url: &str,
    room: &str,
    device_id: &str,
    openid_token: &OpenIdToken,
) -> Result<LkJwtResponse> {
    let url = format!("{lk_jwt_url}/sfu/get");
    let _span = tracing::info_span!("rtc.lk_jwt", %url, %room).entered();

    let body = LkJwtRequest { device_id, room, openid_token };

    let resp: LkJwtResponse = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?
        .error_for_status()
        .with_context(|| format!("POST {url} returned error status"))?
        .json()
        .await
        .with_context(|| format!("parsing lk-jwt JSON response from {url}"))?;

    // Log the SFU URL; never log the JWT itself (it is a bearer credential).
    tracing::info!(
        sfu_url = %resp.url,
        "LiveKit JWT obtained from lk-jwt-service"
    );
    Ok(resp)
}
