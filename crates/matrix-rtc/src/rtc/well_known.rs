//! Discover the MatrixRTC SFU focus from the homeserver's `.well-known` endpoint.
//!
//! Element Call publishes preferred foci at:
//!   `GET {homeserver}/.well-known/matrix/client`
//! under the key `org.matrix.msc4143.rtc_foci`, per MSC4143.

use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use tracing::Instrument;

/// A LiveKit focus entry from the `.well-known/matrix/client` endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
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
        /// Alias of the LiveKit room on the SFU. Optional — some deployments
        /// (including nether.im) omit it from `.well-known`; callers should
        /// fall back to the Matrix room ID as the LiveKit room name.
        #[serde(default)]
        livekit_alias: Option<String>,
        /// URL of the lk-jwt-service for token exchange.
        livekit_service_url: String,
    },
    #[serde(other)]
    Unknown,
}

/// Fetch the first LiveKit focus from the homeserver's `.well-known/matrix/client`.
///
/// `room_id` is used as the LiveKit room alias when the well-known entry
/// doesn't include `livekit_alias` (which is optional per observed deployments).
pub async fn discover_livekit_focus(
    client: &reqwest::Client,
    homeserver_url: &str,
    room_id: &str,
) -> Result<LivekitFocusInfo> {
    let url = format!("{homeserver_url}/.well-known/matrix/client");
    // `.instrument`, not `.entered()`: an EnteredSpan held across an await
    // makes the future !Send, and the management bot calls this from a
    // Send event handler.
    let span = tracing::info_span!("rtc.well_known", %url);
    async move {
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
                    Some(LivekitFocusInfo {
                        alias: livekit_alias.unwrap_or_else(|| room_id.to_owned()),
                        service_url: livekit_service_url,
                    })
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
    .instrument(span)
    .await
}
