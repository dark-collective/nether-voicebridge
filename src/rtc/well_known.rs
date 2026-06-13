//! Discover the MatrixRTC SFU focus from the homeserver's `.well-known` endpoint.
//!
//! Element Call publishes preferred foci at:
//!   `GET {homeserver}/.well-known/matrix/client`
//! under the key `org.matrix.msc4143.rtc_foci`, per MSC4143.

use anyhow::{Context, Result, anyhow};
use serde::Deserialize;

/// A LiveKit focus entry from the `.well-known/matrix/client` endpoint.
#[derive(Debug, Clone)]
pub struct LivekitFocusInfo {
    /// The alias used to identify the LiveKit room within the SFU.
    pub alias: String,
    /// The URL of the lk-jwt-service (token minting endpoint for this SFU).
    pub service_url: String,
}

/// Parsed `.well-known/matrix/client` response — only the RTC-foci key.
#[derive(Debug, Deserialize)]
struct WellKnownClient {
    #[serde(rename = "org.matrix.msc4143.rtc_foci", default)]
    rtc_foci: Vec<RtcFocus>,
}

/// One focus entry in the foci list. Only livekit type is handled.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RtcFocus {
    Livekit {
        /// Alias of the LiveKit room on the SFU.
        livekit_alias: String,
        /// URL of the lk-jwt-service for token exchange.
        livekit_service_url: String,
    },
    #[serde(other)]
    Unknown,
}

/// Fetch the first LiveKit focus from the homeserver's `.well-known/matrix/client`.
///
/// Returns the alias and lk-jwt-service URL.
pub async fn discover_livekit_focus(
    client: &reqwest::Client,
    homeserver_url: &str,
) -> Result<LivekitFocusInfo> {
    let url = format!("{homeserver_url}/.well-known/matrix/client");
    let _span = tracing::info_span!("rtc.well_known", %url).entered();

    let resp: WellKnownClient = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?
        .error_for_status()
        .with_context(|| format!("GET {url} returned error status"))?
        .json()
        .await
        .with_context(|| format!("parsing well-known JSON from {url}"))?;

    let focus = resp
        .rtc_foci
        .into_iter()
        .find_map(|f| match f {
            RtcFocus::Livekit { livekit_alias, livekit_service_url } => {
                Some(LivekitFocusInfo { alias: livekit_alias, service_url: livekit_service_url })
            }
            RtcFocus::Unknown => None,
        })
        .ok_or_else(|| anyhow!("no livekit focus found in {url}"))?;

    tracing::info!(
        alias = %focus.alias,
        service_url = %focus.service_url,
        "LiveKit focus discovered"
    );
    Ok(focus)
}
