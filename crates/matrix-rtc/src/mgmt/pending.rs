//! Approval-code + pending-link machinery for two-sided link auth (#47):
//! minting/normalizing codes and the capped, TTL-pruned pending-link table.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use nvb_config::NewTenant;

/// A `link` that passed every probe and now awaits its Discord-side approval.
pub(super) struct PendingLink {
    pub(super) tenant: NewTenant,
    /// The MXID that requested the link (also `tenant.owner`).
    pub(super) requester: String,
    /// Room the `link` command was issued in — outcome notification target.
    pub(super) origin_room: matrix_sdk::ruma::OwnedRoomId,
    /// Probe warnings, carried into the eventual success reply.
    pub(super) warnings: Vec<String>,
    pub(super) created: Instant,
}

/// Cap on simultaneously pending links: bounds memory against a `link` spammer
/// (each entry is tiny, so this is generous) while leaving room for honest
/// concurrent onboarding.
pub(super) const PENDING_LINK_CAP: usize = 16;

/// Code alphabet: uppercase alphanumerics minus the ambiguous 0/O/1/I — codes
/// are relayed human-to-human across two chat systems.
pub(super) const CODE_ALPHABET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
pub(super) const CODE_LEN: usize = 6;

/// Mint an approval code, `NVB-XXXXXX`. 32^6 ≈ 10^9 states against a cap of
/// 16 live entries and a Discord-side actor limited to guessing through slash
/// commands — collision and brute force are both out of reach.
pub(super) fn mint_approval_code() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let body: String = (0..CODE_LEN)
        .map(|_| CODE_ALPHABET[rng.gen_range(0..CODE_ALPHABET.len())] as char)
        .collect();
    format!("NVB-{body}")
}

/// Normalize a user-typed approval code: trim, uppercase, allow the `NVB-`
/// prefix to be omitted.
pub(super) fn normalize_code(input: &str) -> String {
    let up = input.trim().to_ascii_uppercase();
    if up.starts_with("NVB-") { up } else { format!("NVB-{up}") }
}

pub(super) fn prune_expired_pending(map: &mut HashMap<String, PendingLink>, now: Instant, ttl: Duration) {
    map.retain(|_, p| now.saturating_duration_since(p.created) < ttl);
}

/// Insert a pending link under a fresh code, enforcing the cap. Returns the
/// code, or a user-facing refusal when the table is full.
pub(super) fn insert_pending(
    map: &mut HashMap<String, PendingLink>,
    pending: PendingLink,
    now: Instant,
    ttl: Duration,
) -> Result<String, String> {
    prune_expired_pending(map, now, ttl);
    if map.len() >= PENDING_LINK_CAP {
        return Err(format!(
            "There are already {PENDING_LINK_CAP} links awaiting Discord approval — try again \
             once one is approved or expires."
        ));
    }
    loop {
        let code = mint_approval_code();
        if !map.contains_key(&code) {
            map.insert(code.clone(), pending);
            return Ok(code);
        }
    }
}

/// Claim a pending link by (normalized) code + guild. A guild mismatch leaves
/// the entry in place (a typo from the wrong server must not burn the code).
pub(super) fn take_pending(
    map: &mut HashMap<String, PendingLink>,
    code: &str,
    guild_id: u64,
    now: Instant,
    ttl: Duration,
) -> Result<PendingLink, String> {
    prune_expired_pending(map, now, ttl);
    let Some(p) = map.remove(code) else {
        return Err(format!(
            "Unknown or expired code `{code}` — codes expire after {} minutes; run `link` on \
             the Matrix side again for a fresh one.",
            ttl.as_secs().div_ceil(60)
        ));
    };
    if p.tenant.guild_id != guild_id.to_string() {
        // Put it back: a typo from the wrong server must not burn the code.
        map.insert(code.to_owned(), p);
        return Err(format!(
            "Code `{code}` belongs to a link for a different Discord server — run the command \
             in the server the link targets."
        ));
    }
    Ok(p)
}
