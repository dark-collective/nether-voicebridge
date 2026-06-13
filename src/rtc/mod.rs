//! MatrixRTC session management — Stage 1: join the call as a participant.
//!
//! This module handles the SFU focus discovery, OpenID token exchange,
//! lk-jwt-service token fetch, and m.call.member state event lifecycle.
//! Media (LiveKit connection + audio) comes in Stage 2.
//!
//! # Lifecycle
//! On startup: discover foci → get OpenID token → get LiveKit JWT (proves infra).
//! On call-active: post m.call.member join.
//! On call-ended / shutdown: post m.call.member leave (Empty).

mod lk_jwt;
mod membership;
mod openid;
mod well_known;

use anyhow::Result;
use matrix_sdk::{Client, ruma::RoomId};
use tokio::sync::watch;

use crate::config::{LiveKitConfig, MatrixConfig};

/// Run the RTC participation loop.
///
/// * `client` — a ready matrix-sdk client (produced by `matrix::build_client`)
/// * `lk_cfg` — LiveKit / lk-jwt config from TOML
/// * `mx_cfg` — Matrix config (bot user_id + device_id + room_id)
/// * `call_active_rx` — watch receiver signalled by `matrix::run` when human
///   participants join (`true`) or all leave (`false`)
pub async fn run(
    client: Client,
    _lk_cfg: &LiveKitConfig,
    mx_cfg: &MatrixConfig,
    mut call_active_rx: watch::Receiver<bool>,
) -> Result<()> {
    let span = tracing::info_span!("rtc");
    let _guard = span.enter();

    // Build a dedicated HTTP client on rustls (consistent with the global ring provider).
    let http = reqwest::Client::builder()
        .use_rustls_tls()
        .build()?;

    // ── Infra probe ──────────────────────────────────────────────────────────
    // These run at startup regardless of whether a call is active. They prove
    // that the homeserver, lk-jwt-service, and config are all reachable before
    // we ever need to join a call.

    let focus = well_known::discover_livekit_focus(&http, &mx_cfg.homeserver_url, &mx_cfg.room_id).await?;
    let openid_token = openid::get_openid_token(&client).await?;

    // The "room" name for the LiveKit room. Element Call uses the Matrix room ID.
    let room_name = mx_cfg.room_id.as_str();

    let user_id = client
        .user_id()
        .ok_or_else(|| anyhow::anyhow!("Matrix client not authenticated"))?
        .to_string();

    let jwt_resp = lk_jwt::fetch_livekit_token(
        &http,
        &focus.service_url,
        room_name,           // room_id
        &mx_cfg.device_id,  // slot_id
        &user_id,
        &openid_token,
    )
    .await?;

    tracing::info!(
        sfu_url = %jwt_resp.url,
        "RTC infra probe complete — focus, OpenID, and lk-jwt all working"
    );

    // ── Call participation loop ───────────────────────────────────────────────
    let user_id = client
        .user_id()
        .ok_or_else(|| anyhow::anyhow!("Matrix client not authenticated"))?
        .to_owned();
    let device_id = client
        .device_id()
        .ok_or_else(|| anyhow::anyhow!("Matrix client has no device id"))?
        .to_owned();
    let room_id = RoomId::parse(&mx_cfg.room_id)?;

    let mut joined = false;

    loop {
        // Wait for the value to change OR process the current value on first entry.
        // `changed()` blocks until the next send; for the first iteration we use
        // `borrow_and_update` to consume the initial value without blocking.
        let active = *call_active_rx.borrow_and_update();

        match (joined, active) {
            (false, true) => {
                // A human appeared; post our membership.
                if let Some(room) = client.get_room(&room_id) {
                    match membership::join(&room, &user_id, &device_id, &focus).await {
                        Ok(()) => joined = true,
                        Err(e) => tracing::error!(error = %e, "failed to post call membership"),
                    }
                } else {
                    tracing::warn!(room_id = %room_id, "room not in local state; cannot join call");
                }
            }
            (true, false) => {
                // All humans left; retract our membership.
                if let Some(room) = client.get_room(&room_id) {
                    match membership::leave(&room, &user_id, &device_id).await {
                        Ok(()) => joined = false,
                        Err(e) => tracing::error!(error = %e, "failed to post call leave"),
                    }
                }
            }
            _ => {} // no-op: (false, false) wait | (true, true) already joined
        }

        // Block until the next call-active state change.
        if call_active_rx.changed().await.is_err() {
            // Sender dropped — main is shutting down.
            break;
        }
    }

    // Clean up: leave if still joined when the sender drops.
    if joined {
        if let Some(room) = client.get_room(&room_id) {
            let _ = membership::leave(&room, &user_id, &device_id).await;
        }
    }

    tracing::info!("RTC task exiting");
    Ok(())
}
