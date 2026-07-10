//! Ghost (Discord speaker → Matrix) presence bookkeeping: mint/retire the
//! per-speaker capture sinks with echo-exclusion and opt-out gating.

use super::*;
use super::events::{member_avatar_url, member_display_name};

/// Mint a ghost for a Discord user in the channel if they don't have one yet.
/// Skips bridge-owned puppets and any other bot. Resolves the display name from
/// the supplied `Member` if present, else via a lookup.
pub(super) async fn ensure_ghost(ctx: &Context, channel: &ChannelCtx, user_id: u64, member: Option<&Member>) {
    // Exclude only OUR OWN puppet/anchor bots (echo prevention, constraint #5).
    // Third-party bots (e.g. a TTS bot) ARE bridged — their audio is a legitimate
    // Discord speaker to mirror into Matrix. Our bots are doubly protected: their
    // SSRCs are also in the capture `excluded` set, registered on DriverConnect.
    if channel
        .bot_ids
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .contains(&user_id)
    {
        return;
    }
    // Opted-out users (#63) are never ghosted — covers live voice events AND
    // the anchor's member-seeding path, which both route through here. The
    // capture demux drops their audio independently (defense in depth).
    if channel
        .optout
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .contains(&user_id)
    {
        return;
    }
    if channel
        .user_sinks
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .contains_key(&user_id)
    {
        return; // already has a ghost
    }

    let (name, avatar_url) = match member {
        Some(m) => (member_display_name(m), member_avatar_url(m)),
        None => match channel.guild_id.member(&ctx.http, UserId::new(user_id)).await {
            Ok(m) => (member_display_name(&m), member_avatar_url(&m)),
            Err(e) => {
                tracing::debug!(user_id, error = %e, "member lookup failed; using id as name");
                (user_id.to_string(), None)
            }
        },
    };

    // Commit under the user_sinks write lock, and RE-CHECK bot_ids here: the user's
    // gateway `ready` may have landed during the (awaited) name resolution above,
    // revealing it as one of our OWN puppet bots that was in voice before its id was
    // known (a startup race — see `retract_self_ghost`). Holding the write lock
    // across the re-check and the insert makes this race-free against
    // `retract_self_ghost`, which removes the id under the same lock.
    {
        let mut sinks = channel
            .user_sinks
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        if channel
            .bot_ids
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&user_id)
        {
            return; // our own puppet bot — never a ghost (echo prevention)
        }
        if sinks.contains_key(&user_id) {
            return; // raced with another ensure_ghost for the same user
        }
        let sink = channel
            .capture_router
            .assign(&user_id.to_string(), &name, avatar_url.as_deref());
        sinks.insert(user_id, sink);
    }
    tracing::info!(bridge = %channel.bridge, user_id, display_name = %name, "Discord user present — ghost assigned");
}

/// Retire a user's ghost if they had one (left or moved out of the channel).
/// Remove a Discord speaker's ghost. Returns `true` iff this removed the **last**
/// speaker (so the caller can release a now-idle capture-only anchor).
pub(super) fn drop_ghost(channel: &ChannelCtx, user_id: u64) -> bool {
    let now_empty = {
        let mut sinks = channel.user_sinks.write().unwrap_or_else(PoisonError::into_inner);
        if sinks.remove(&user_id).is_none() {
            return false; // wasn't a tracked speaker
        }
        sinks.is_empty()
    };
    channel.capture_router.release(&user_id.to_string());
    tracing::info!(bridge = %channel.bridge, user_id, "Discord user left — ghost released");
    now_empty
}
