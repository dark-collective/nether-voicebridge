//! Link-preflight power/authority evaluation (#47): the pure power-level and
//! sender-authority checks plus the room/guild prerequisite probe that drives
//! them before anything is written to tenants.toml.

use std::time::Duration;

use super::BotCtx;
use super::handlers::query_guild_coverage;

/// Verdict of a room's `m.room.power_levels` for the bridge's ghost users:
/// can a freshly-joined ghost post `org.matrix.msc3401.call.member`?
#[derive(Debug, PartialEq, Eq)]
pub(super) struct CallPowerCheck {
    /// Effective power level required to post the call-member state event.
    pub(super) required: i64,
    /// Power level a freshly-joined appservice ghost gets. Ghosts are minted
    /// dynamically, so they are never in the `users` map — `users_default`
    /// is what they will have.
    pub(super) ghost_pl: i64,
}

impl CallPowerCheck {
    pub(super) fn ok(&self) -> bool {
        self.ghost_pl >= self.required
    }
}

/// Pure evaluation of a room's power levels (unit-tested below). `content` is
/// the raw `m.room.power_levels` event content; `None` means the event is
/// absent (GET 404). Per spec an absent event implies `state_default = 0`
/// (would pass), but such rooms are practically nonexistent and an absent
/// event here more likely means something unexpected — fail safe by treating
/// it as the spec default-with-event value of 50.
pub(super) fn evaluate_call_member_power(content: Option<&serde_json::Value>) -> CallPowerCheck {
    let Some(c) = content else {
        return CallPowerCheck { required: 50, ghost_pl: 0 };
    };
    let required = c
        .get("events")
        .and_then(|e| e.get(CALL_MEMBER_EVENT))
        .and_then(pl_int)
        .or_else(|| c.get("state_default").and_then(pl_int))
        .unwrap_or(50);
    let ghost_pl = c.get("users_default").and_then(pl_int).unwrap_or(0);
    CallPowerCheck { required, ghost_pl }
}

const CALL_MEMBER_EVENT: &str = "org.matrix.msc3401.call.member";

/// Verdict of the Matrix-side link-authority check (#47): may `sender` bridge
/// this room? The bar is "could configure the room themselves": joined, with
/// power level ≥ the room's `state_default`.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct SenderAuthority {
    pub(super) joined: bool,
    pub(super) sender_pl: i64,
    pub(super) required: i64,
}

impl SenderAuthority {
    pub(super) fn ok(&self) -> bool {
        self.joined && self.sender_pl >= self.required
    }
}

/// Pure evaluation of a sender's authority over a room (unit-tested below).
/// `pl_content` is the raw `m.room.power_levels` content (`None` = absent →
/// fail-safe required=50, same posture as the call-member check);
/// `member_content` is the sender's `m.room.member` content (`None` = no
/// membership event → not joined).
pub(super) fn evaluate_sender_authority(
    pl_content: Option<&serde_json::Value>,
    member_content: Option<&serde_json::Value>,
    sender: &str,
) -> SenderAuthority {
    let joined = member_content
        .and_then(|m| m.get("membership"))
        .and_then(|v| v.as_str())
        == Some("join");
    let (sender_pl, required) = match pl_content {
        Some(c) => {
            let users_default = c.get("users_default").and_then(pl_int).unwrap_or(0);
            let sender_pl = c
                .get("users")
                .and_then(|u| u.get(sender))
                .and_then(pl_int)
                .unwrap_or(users_default);
            let required = c.get("state_default").and_then(pl_int).unwrap_or(50);
            (sender_pl, required)
        }
        None => (0, 50),
    };
    SenderAuthority { joined, sender_pl, required }
}

/// Power-level values are canonically integers, but legacy rooms (and some
/// servers) carry them as numeric strings — accept both.
fn pl_int(v: &serde_json::Value) -> Option<i64> {
    match v {
        serde_json::Value::Number(n) => n.as_i64(),
        serde_json::Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// Warnings from probes that can have benign false negatives (currently only
/// guild coverage — bots learn their guilds at gateway `ready`, so early after
/// startup a legitimately-invited guild may not be known yet).
pub(super) struct ProbeReport {
    pub(super) warnings: Vec<String>,
}

/// Run every room/guild prerequisite probe BEFORE tenants.toml is touched.
/// `Err` carries the complete user-facing reply text — probe failures are
/// operator errors, not bugs, so they go to the DM rather than the log.
///
/// Side effect: the join probe (deliberately idempotent with the later bridge
/// spawn) leaves the bot joined to the room even when a later probe fails.
/// That is harmless — a retried `link` reuses the membership, and the bridge
/// needs it anyway. (`unlink` never leaves rooms either; out of scope here.)
/// `authority_sender`: when set (two-sided link auth, #47), the probe also
/// verifies that this MXID is a joined member of the room with power level ≥
/// the room's `state_default` — an operator allowlist entry alone no longer
/// authorizes bridging an arbitrary room.
pub(super) async fn probe_room_prereqs(
    ctx: &BotCtx,
    room_id: &str,
    guild_id: &str,
    authority_sender: Option<&str>,
) -> Result<ProbeReport, String> {
    // 1. Join (hard): covers restricted/invite-only rooms and typo'd room ids.
    match tokio::time::timeout(
        Duration::from_secs(15),
        crate::matrix::appservice_join_room(
            &ctx.homeserver_url,
            &ctx.appservice_token,
            room_id,
            ctx.own_user.as_str(),
        ),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            return Err(format!(
                "❌ The bridge bot could not join {room_id}: {e}\n\
                 If the room is invite-only or restricted, invite {} to it first, then run `link` again.\n\
                 Nothing was written.",
                ctx.own_user
            ));
        }
        Err(_) => {
            return Err(format!(
                "❌ Joining {room_id} timed out after 15 s — is the homeserver reachable?\n\
                 Nothing was written."
            ));
        }
    }

    // 2. Power levels (hard): a room that blocks ghosts from posting
    // `org.matrix.msc3401.call.member` silently breaks the Discord→Matrix
    // direction, so refuse to link it. Must run after the join (a non-member
    // GET 403s).
    let pl_content = tokio::time::timeout(
        Duration::from_secs(10),
        crate::matrix::get_room_state(
            &ctx.http,
            &ctx.homeserver_url,
            &ctx.appservice_token,
            room_id,
            "m.room.power_levels",
            "",
            ctx.own_user.as_str(),
        ),
    )
    .await
    .map_err(|_| {
        "❌ Reading the room's power levels timed out after 10 s.\nNothing was written."
            .to_string()
    })?
    .map_err(|e| {
        format!(
            "❌ Could not read the room's power levels: {e}\nNothing was written."
        )
    })?;
    let check = evaluate_call_member_power(pl_content.as_ref());
    if !check.ok() {
        return Err(format!(
            "❌ The room's power levels block the bridge: posting `{CALL_MEMBER_EVENT}` needs power level {}, but the bridge's ghost users join at level {} — Matrix would never see the Discord side of the call.\n\
             Fix: start an Element Call in the room once (that sets the level), or via /devtools set `events[\"{CALL_MEMBER_EVENT}\"] = 0` in `m.room.power_levels`. Element's \"Roles & Permissions\" UI shows \"Default\" either way, so check the raw event.\n\
             Nothing was written.",
            check.required, check.ghost_pl
        ));
    }

    // 2b. Matrix-side link authority (#47, only with `require_room_authority`):
    // the sender must hold moderator power in the room they are bridging.
    // Reuses the power-levels content just fetched; only the sender's own
    // membership event is an extra GET.
    if let Some(sender) = authority_sender {
        let member_content = tokio::time::timeout(
            Duration::from_secs(10),
            crate::matrix::get_room_state(
                &ctx.http,
                &ctx.homeserver_url,
                &ctx.appservice_token,
                room_id,
                "m.room.member",
                sender,
                ctx.own_user.as_str(),
            ),
        )
        .await
        .map_err(|_| {
            "❌ Reading your room membership timed out after 10 s.\nNothing was written."
                .to_string()
        })?
        .map_err(|e| {
            format!("❌ Could not read your room membership: {e}\nNothing was written.")
        })?;
        let authority =
            evaluate_sender_authority(pl_content.as_ref(), member_content.as_ref(), sender);
        if !authority.ok() {
            let reason = if !authority.joined {
                "you are not a joined member of it".to_string()
            } else {
                format!(
                    "your power level there is {} but linking needs ≥ {} (the room's \
                     `state_default`)",
                    authority.sender_pl, authority.required
                )
            };
            return Err(format!(
                "❌ Two-sided link auth is on: only someone with moderator power in {room_id} \
                 may bridge it, and {reason}.\n\
                 Join the room and have an admin raise your power level, or ask a room \
                 moderator to run `link`.\n\
                 Nothing was written."
            ));
        }
    }

    // 3. MatrixRTC focus (hard): a `link`-created tenant always uses the
    // default LiveKit config, so this discovery is exactly what the spawned
    // bridge will do.
    match tokio::time::timeout(
        Duration::from_secs(10),
        crate::rtc::well_known::discover_livekit_focus(&ctx.http, &ctx.well_known_base, room_id),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            return Err(format!(
                "❌ MatrixRTC does not appear to be set up on this homeserver: {e}\n\
                 `{}/.well-known/matrix/client` must publish an `org.matrix.msc4143.rtc_foci` LiveKit entry (lk-jwt-service + LiveKit). Without it no call can be bridged.\n\
                 Nothing was written.",
                ctx.well_known_base
            ));
        }
        Err(_) => {
            return Err(format!(
                "❌ MatrixRTC discovery timed out after 10 s (`{}/.well-known/matrix/client`).\n\
                 Nothing was written.",
                ctx.well_known_base
            ));
        }
    }

    // 4. Discord guild coverage (WARN only): guilds are learned per-bot at
    // gateway `ready`, so a false negative is expected shortly after startup.
    let mut warnings = Vec::new();
    match guild_id.parse::<u64>() {
        Ok(gid) => match query_guild_coverage(ctx, gid).await {
            Some(true) => {}
            Some(false) => warnings.push(format!(
                "⚠️ No puppet bot reports membership of guild {guild_id} yet — Matrix speakers will not be voiced on Discord until the puppet bots are invited to that guild. (If the process just started, the bots may still be connecting.)"
            )),
            None => warnings.push(
                "⚠️ Could not verify puppet-bot guild membership (no answer from the supervisor)."
                    .to_string(),
            ),
        },
        // Past is_numeric_id this only means a >u64 digit string.
        Err(_) => warnings.push(format!(
            "⚠️ Could not verify puppet-bot guild membership ('{guild_id}' is not a valid guild id)."
        )),
    }

    Ok(ProbeReport { warnings })
}
