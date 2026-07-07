//! Ghost identity / exclusion helpers — pure string predicates over MXIDs and
//! LiveKit participant identities (echo-prevention, constraint #5, plus the
//! post-rename legacy-ghost cleanup).

/// Build the ghost localpart prefix (`{sender_localpart}_`) from the appservice
/// sender localpart. Ghost MXIDs are `@{sender_localpart}_{discord_user_id}:server`,
/// which matches the appservice namespace regex `@{sender_localpart}_.*`. Falls back
/// to `"nether-voicebridge_"` when `sender_localpart` is absent (defensive only —
/// the config validator requires it). Deliberately NOT `discord_` — that namespace is
/// owned by mautrix-discord, and sharing it on the same homeserver would collide.
pub(crate) fn ghost_localpart_prefix(sender_localpart: Option<&str>) -> String {
    sender_localpart.unwrap_or("nether-voicebridge").to_owned() + "_"
}

/// Is this LiveKit participant identity one of our ghosts? Used by the bridge
/// bot's subscriber to skip ghost tracks (echo prevention, constraint #5). The
/// identity is `@{sender_localpart}_{id}:server[:device]`; the localpart is the
/// segment between `@` and the first `:`.
///
/// Matched as `{ghost_prefix}` followed by an **all-digits** suffix — ghost
/// localparts are always `{prefix}{discord_user_id}` and Discord user ids are
/// numeric snowflakes. The digit check matters because the bridge bot's own MXID
/// is `@{sender_localpart}_bot` (see `resolve_bot_user_ids`), which shares the
/// ghost prefix; without it the bot would be misclassified as a ghost and its own
/// `m.call.member` retracted by `sweep_stale_ghosts`. Mirrors
/// [`is_legacy_ghost_identity`].
pub(crate) fn is_ghost_identity(identity: &str, ghost_prefix: &str) -> bool {
    identity
        .strip_prefix('@')
        .and_then(|rest| rest.split(':').next())
        .and_then(|localpart| localpart.strip_prefix(ghost_prefix))
        .is_some_and(|suffix| !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()))
}

/// Prefixes this bridge minted ghosts under *before* the current
/// `ghost_localpart_prefix` (derived from config `sender_localpart`). `discord_`
/// collided with mautrix-discord, so we never owned it. `nether-voicebridge_` was
/// the previous hard-coded default (issue #37); a deployment that later sets a
/// different `sender_localpart` leaves `@nether-voicebridge_<id>` ghosts behind in
/// old room state. Stale entries under either prefix may linger and must still be
/// recognized for cleanup/exclusion.
const LEGACY_GHOST_PREFIXES: [&str; 2] = ["discord_", "nether-voicebridge_"];

/// Is this identity a *legacy* ghost (`@{legacy_prefix}<digits>`) we minted before
/// the current derived prefix? Matched only as a legacy prefix followed by an
/// all-digits localpart (the Discord-user-id form we used) so a human
/// `@discord_alice` / `@nether-voicebridge_alice` is never caught.
///
/// Used for **exclusion** ([`is_bridge_excluded_identity`]) and for the startup
/// `sweep_stale_ghosts` legacy branch (which retracts via a bot-write `Empty`,
/// with masquerade fallback). mautrix-discord's own `@discord_<id>` ghosts match
/// too, which is fine: they must never be counted as our call participants, and
/// they never post `m.call.member` anyway. When the *current* prefix is
/// `nether-voicebridge_` (the default fallback), the sweep checks
/// [`is_ghost_identity`] first, so those ghosts still take the masquerade path —
/// the legacy match here only fires once the current prefix has moved on.
pub(crate) fn is_legacy_ghost_identity(identity: &str) -> bool {
    let Some(localpart) = identity.strip_prefix('@').and_then(|rest| rest.split(':').next()) else {
        return false;
    };
    LEGACY_GHOST_PREFIXES.iter().any(|prefix| {
        localpart
            .strip_prefix(prefix)
            .is_some_and(|rest| !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit()))
    })
}

/// Should this identity be excluded from call-participant counting, key recipients,
/// and capture/forward? True for our current ghosts and for stale legacy ghosts.
/// (Constraint #5 echo-prevention plus the post-rename leftover cleanup.)
pub(crate) fn is_bridge_excluded_identity(identity: &str, ghost_prefix: &str) -> bool {
    is_ghost_identity(identity, ghost_prefix) || is_legacy_ghost_identity(identity)
}
