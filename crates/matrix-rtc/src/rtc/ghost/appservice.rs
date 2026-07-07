//! Raw client-server API primitives, masqueraded as the ghost via `?user_id=`
//! with the appservice `as_token`. matrix-sdk 0.18 has no appservice support, so
//! ghost register / profile / join / openid / `m.call.member` all go over the raw
//! CS-API here.

use anyhow::{anyhow, Context, Result};
use matrix_sdk::{
    ruma::{
        events::call::member::{
            ActiveFocus, ActiveLivekitFocus, Application, CallApplicationContent,
            CallMemberEventContent, CallMemberStateKey, CallScope, Focus, LivekitFocus,
        },
        OwnedDeviceId, OwnedUserId, RoomId, UserId,
    },
    Client,
};
use reqwest::Url;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use super::super::openid::OpenIdToken;
use super::super::well_known::LivekitFocusInfo;
use super::GhostConfig;

/// MatrixRTC call-membership state event type (MSC3401). Matches what
/// `rtc::membership` posts via ruma's typed `send_state_event_for_key`; here we
/// post over the raw CS-API (masquerade) so the type is spelled out.
const CALL_MEMBER_EVENT_TYPE: &str = "org.matrix.msc3401.call.member";

/// Build a Matrix HTTP API URL from full path segments, percent-encoding each,
/// with an optional `?user_id=` masquerade query.
fn matrix_url(homeserver: &str, segments: &[&str], masquerade: Option<&str>) -> Result<Url> {
    let mut url = Url::parse(homeserver).with_context(|| format!("parsing homeserver '{homeserver}'"))?;
    {
        let mut path = url
            .path_segments_mut()
            .map_err(|_| anyhow!("homeserver URL cannot be a base: {homeserver}"))?;
        path.extend(segments);
    }
    if let Some(user_id) = masquerade {
        url.query_pairs_mut().append_pair("user_id", user_id);
    }
    Ok(url)
}

/// Build a client-server API URL under `/_matrix/client/v3` (the common case).
/// `pub(crate)` so the plaintext key transport (`rtc::key_transport`) can reuse it.
pub(crate) fn cs_url(homeserver: &str, segments: &[&str], masquerade: Option<&str>) -> Result<Url> {
    let mut full = vec!["_matrix", "client", "v3"];
    full.extend_from_slice(segments);
    matrix_url(homeserver, &full, masquerade)
}

/// Read an error response body for logging (bearer tokens are never in bodies).
/// `pub(crate)` so the plaintext key transport (`rtc::key_transport`) can reuse it.
pub(crate) async fn err_body(resp: reqwest::Response) -> String {
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    format!("{status}: {text}")
}

/// Register the ghost as an appservice user (idempotent). Tolerates an existing
/// user (`M_USER_IN_USE`); all later calls masquerade with the `as_token`, so we
/// don't need the registration's access token.
pub(super) async fn register_ghost(cfg: &GhostConfig, localpart: &str) -> Result<()> {
    let url = cs_url(&cfg.homeserver_url, &["register"], None)?;
    let resp = cfg
        .http
        .post(url)
        .bearer_auth(&cfg.as_token)
        .json(&serde_json::json!({
            "type": "m.login.application_service",
            "username": localpart,
        }))
        .send()
        .await
        .context("POST /register")?;
    if resp.status().is_success() {
        tracing::info!(localpart, "ghost registered");
        return Ok(());
    }
    // M_USER_IN_USE (the ghost already exists) is the expected steady state.
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if text.contains("M_USER_IN_USE") {
        tracing::debug!(localpart, "ghost already registered");
        return Ok(());
    }
    Err(anyhow!("POST /register failed: {status}: {text}"))
}

/// Set the ghost's display name (masqueraded).
pub(super) async fn set_displayname(cfg: &GhostConfig, ghost_mxid: &str, name: &str) -> Result<()> {
    let url = cs_url(
        &cfg.homeserver_url,
        &["profile", ghost_mxid, "displayname"],
        Some(ghost_mxid),
    )?;
    let resp = cfg
        .http
        .put(url)
        .bearer_auth(&cfg.as_token)
        .json(&serde_json::json!({ "displayname": name }))
        .send()
        .await
        .context("PUT /profile/displayname")?;
    if !resp.status().is_success() {
        return Err(anyhow!("set displayname failed: {}", err_body(resp).await));
    }
    Ok(())
}

/// Mirror a Discord avatar onto the ghost: download it from Discord's CDN, upload
/// it to the homeserver media repo (masqueraded), and set the ghost's avatar_url.
pub(super) async fn set_avatar(cfg: &GhostConfig, ghost_mxid: &str, discord_avatar_url: &str) -> Result<()> {
    // 1. Download from the Discord CDN (public, no auth).
    let resp = cfg
        .http
        .get(discord_avatar_url)
        .send()
        .await
        .context("GET discord avatar")?;
    if !resp.status().is_success() {
        return Err(anyhow!("discord avatar fetch returned {}", resp.status()));
    }
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("image/png")
        .to_owned();
    let bytes = resp.bytes().await.context("reading discord avatar bytes")?;

    // 2. Upload to the Matrix media repo as the ghost → mxc:// URI.
    let mxc = upload_media(cfg, ghost_mxid, &content_type, bytes.to_vec()).await?;

    // 3. Point the ghost's profile at it.
    let url = cs_url(
        &cfg.homeserver_url,
        &["profile", ghost_mxid, "avatar_url"],
        Some(ghost_mxid),
    )?;
    let resp = cfg
        .http
        .put(url)
        .bearer_auth(&cfg.as_token)
        .json(&serde_json::json!({ "avatar_url": mxc }))
        .send()
        .await
        .context("PUT /profile/avatar_url")?;
    if !resp.status().is_success() {
        return Err(anyhow!("set avatar_url failed: {}", err_body(resp).await));
    }
    tracing::info!(mxc = %mxc, "ghost avatar set");
    Ok(())
}

/// Upload bytes to the homeserver media repo masqueraded as the ghost; returns
/// the `mxc://` content URI.
async fn upload_media(
    cfg: &GhostConfig,
    ghost_mxid: &str,
    content_type: &str,
    bytes: Vec<u8>,
) -> Result<String> {
    let url = matrix_url(
        &cfg.homeserver_url,
        &["_matrix", "media", "v3", "upload"],
        Some(ghost_mxid),
    )?;
    let resp = cfg
        .http
        .post(url)
        .bearer_auth(&cfg.as_token)
        .header(reqwest::header::CONTENT_TYPE, content_type)
        .body(bytes)
        .send()
        .await
        .context("POST /_matrix/media/v3/upload")?;
    if !resp.status().is_success() {
        return Err(anyhow!("media upload failed: {}", err_body(resp).await));
    }
    let v: serde_json::Value = resp.json().await.context("parsing media upload response")?;
    v.get("content_uri")
        .and_then(|x| x.as_str())
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("media upload response missing content_uri"))
}

/// Invite the ghost to the call room **as the bridge bot**, via its matrix-sdk
/// `Client` (NOT the `as_token` masquerade — that only works for the appservice's
/// own namespace users, so it fails `M_EXCLUSIVE` for a regular-account bot like an
/// unencrypted bridge's). Required for **restricted** rooms (MSC3083): a ghost's
/// bare `/join` is refused there unless invited. Idempotent — skips when the ghost
/// is already joined/invited.
pub(super) async fn invite_ghost(client: &Client, room_id: &str, ghost_mxid: &str) -> Result<()> {
    let rid = RoomId::parse(room_id).with_context(|| format!("parsing room_id '{room_id}'"))?;
    let Some(room) = client.get_room(&rid) else {
        return Err(anyhow!("bridge bot is not in room {room_id} — cannot invite ghost"));
    };
    let ghost: OwnedUserId =
        UserId::parse(ghost_mxid).with_context(|| format!("parsing ghost mxid '{ghost_mxid}'"))?;
    // Already joined/invited → nothing to do (avoids needless invites + log noise).
    if let Ok(Some(member)) = room.get_member_no_sync(&ghost).await {
        use matrix_sdk::ruma::events::room::member::MembershipState;
        if matches!(member.membership(), MembershipState::Join | MembershipState::Invite) {
            return Ok(());
        }
    }
    room.invite_user_by_id(&ghost).await.context("inviting ghost to room")?;
    tracing::debug!(ghost = %ghost_mxid, "ghost invited to room");
    Ok(())
}

/// Join the call room as the ghost (masqueraded). Idempotent on the server.
pub(super) async fn join_room(cfg: &GhostConfig, ghost_mxid: &str) -> Result<()> {
    let url = cs_url(&cfg.homeserver_url, &["join", &cfg.room_id], Some(ghost_mxid))?;
    let resp = cfg
        .http
        .post(url)
        .bearer_auth(&cfg.as_token)
        .json(&serde_json::json!({}))
        .send()
        .await
        .context("POST /join")?;
    if !resp.status().is_success() {
        return Err(anyhow!("join room failed: {}", err_body(resp).await));
    }
    Ok(())
}

/// Fetch an OpenID token for the ghost (masqueraded). This is the call lk-jwt
/// validates by federation — the key live risk (architecture open-Q #3); the
/// status body is surfaced on failure so a rejection is diagnosable.
pub(super) async fn fetch_openid_token(cfg: &GhostConfig, ghost_mxid: &str) -> Result<OpenIdToken> {
    let url = cs_url(
        &cfg.homeserver_url,
        &["user", ghost_mxid, "openid", "request_token"],
        Some(ghost_mxid),
    )?;
    let resp = cfg
        .http
        .post(url)
        .bearer_auth(&cfg.as_token)
        .json(&serde_json::json!({}))
        .send()
        .await
        .context("POST /openid/request_token")?;
    if !resp.status().is_success() {
        return Err(anyhow!("ghost openid request failed: {}", err_body(resp).await));
    }
    let token: OpenIdToken = resp
        .json()
        .await
        .context("parsing ghost openid response")?;
    Ok(token)
}

/// Post the ghost's `m.call.member` join, retrying with backoff until it lands or
/// `cancel` fires. Self-heals a transient failure (e.g. a room whose
/// `org.matrix.msc3401.call.member` power level is wrong, then fixed) without the
/// Discord user having to rejoin. Bounded in practice by the ghost's lifetime.
pub(super) async fn post_membership_until_joined(
    cfg: GhostConfig,
    ghost_mxid: String,
    device_id: String,
    focus: LivekitFocusInfo,
    cancel: CancellationToken,
) {
    let mut backoff = Duration::from_secs(2);
    let max = Duration::from_secs(30);
    loop {
        match post_membership(&cfg, &ghost_mxid, &device_id, &focus, false).await {
            Ok(()) => {
                tracing::info!("ghost call membership posted — visible in call");
                return;
            }
            Err(e) => tracing::warn!(
                error = %e,
                retry_in_s = backoff.as_secs(),
                "ghost post_membership failed — retrying (room may need m.call.member at PL 0)"
            ),
        }
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(max);
    }
}

/// Post (or retract, when `leave`) the ghost's `m.call.member` state event,
/// masqueraded. Reuses ruma's `CallMemberEventContent` serialization so the wire
/// shape exactly matches the bridge bot's membership (`rtc::membership`).
pub(super) async fn post_membership(
    cfg: &GhostConfig,
    ghost_mxid: &str,
    device_id: &str,
    focus: &LivekitFocusInfo,
    leave: bool,
) -> Result<()> {
    let user_id: OwnedUserId = UserId::parse(ghost_mxid)
        .with_context(|| format!("parsing ghost mxid '{ghost_mxid}'"))?;
    let device: OwnedDeviceId = device_id.into();
    let member_id = format!("{device_id}_m.call");
    let state_key = CallMemberStateKey::new(user_id, Some(member_id), true);

    let content = if leave {
        CallMemberEventContent::new_empty(None)
    } else {
        build_membership(&device, focus)
    };
    put_call_member_state(cfg, ghost_mxid, state_key.as_ref(), &content).await
}

/// PUT a ghost's `m.call.member` state event (masqueraded as the ghost). The
/// low-level primitive shared by [`post_membership`] (which builds a fresh
/// state_key) and [`retract_membership_raw`] (which reuses an observed one).
async fn put_call_member_state(
    cfg: &GhostConfig,
    ghost_mxid: &str,
    state_key: &str,
    content: &CallMemberEventContent,
) -> Result<()> {
    let body = serde_json::to_value(content).context("serializing m.call.member content")?;
    let url = cs_url(
        &cfg.homeserver_url,
        &["rooms", &cfg.room_id, "state", CALL_MEMBER_EVENT_TYPE, state_key],
        Some(ghost_mxid),
    )?;
    let resp = cfg
        .http
        .put(url)
        .bearer_auth(&cfg.as_token)
        .json(&body)
        .send()
        .await
        .context("PUT /rooms/.../state/m.call.member")?;
    if !resp.status().is_success() {
        return Err(anyhow!("put m.call.member failed: {}", err_body(resp).await));
    }
    Ok(())
}

/// Retract a ghost membership at an **observed** state_key (from room state),
/// masqueraded as the ghost. Used by the startup sweep: it clears the exact
/// leftover key a crashed run wrote, even if our key-derivation has since
/// changed (it does not rebuild `D{key}`).
pub(super) async fn retract_membership_raw(
    cfg: &GhostConfig,
    ghost_mxid: &str,
    state_key: &str,
) -> Result<()> {
    let content = CallMemberEventContent::new_empty(None);
    put_call_member_state(cfg, ghost_mxid, state_key, &content).await
}

/// Build the ghost's `m.call.member` join content — mirrors `rtc::membership::join`.
fn build_membership(device_id: &OwnedDeviceId, focus: &LivekitFocusInfo) -> CallMemberEventContent {
    let foci_preferred = vec![Focus::Livekit(LivekitFocus::new(
        focus.alias.clone(),
        focus.service_url.clone(),
    ))];
    CallMemberEventContent::new(
        Application::Call(CallApplicationContent::new(String::new(), CallScope::Room)),
        device_id.clone(),
        ActiveFocus::Livekit(ActiveLivekitFocus::new()),
        foci_preferred,
        None,
        None,
    )
}
