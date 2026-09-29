//! MatrixRTC focus (SFU) selection — which LiveKit SFU each call member
//! publishes its media on.
//!
//! MSC4195 memberships carry `focus_active.focus_selection`, and there are two
//! selection modes in the wild:
//!
//! * **`oldest_membership`** (legacy, pre-EC-0.21): every participant publishes
//!   on the SFU preferred by the *oldest* active membership in the call (by
//!   `created_ts`), so one SFU carries the whole call.
//! * **`multi_sfu`** (the Element Call ≥ 0.21 default, July 2026): every
//!   participant publishes on its **own** `foci_preferred[0]` — normally its
//!   homeserver's SFU — and subscribers connect to each publisher's SFU to pull
//!   their tracks. Nothing is elected; a call spans as many SFUs as there are
//!   homeservers among the participants.
//!
//! This mirrors matrix-js-sdk's `CallMembership.getTransport()`: `multi_sfu` ⇒
//! own `foci_preferred[0]`; `oldest_membership` ⇒ the oldest membership's
//! transport (recursively, so if the oldest member is itself `multi_sfu`, its
//! own `foci_preferred[0]`). An unknown selection resolves to *no* transport.
//!
//! Why the bridge needs this: it used to assume the whole call lived on the SFU
//! from its own homeserver's `.well-known`. A matrix.org user on a current
//! Element Call publishes to matrix.org's SFU instead, so the bridge never saw
//! their audio — and, worse, the stale-membership reaper (#25) read their
//! absence from *our* SFU as "phantom" and kept emptying their live membership.
//! The RTC loop now resolves every human member's effective SFU with
//! [`resolve_effective_foci`], runs one subscriber per foreign SFU, and the
//! reaper only judges members on SFUs it is actually connected to.

use std::collections::HashMap;

use matrix_sdk::ruma::events::call::member::{
    ActiveFocus, ActiveLivekitFocus, Focus, FocusSelection,
};

use super::well_known::LivekitFocusInfo;

/// Wire value of the multi-SFU focus selection (not yet a named ruma variant).
pub const MULTI_SFU: &str = "multi_sfu";

/// A membership's focus-selection mode, reduced to what resolution needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Selection {
    /// Publishes on the oldest membership's preferred SFU.
    OldestMembership,
    /// Publishes on its own `foci_preferred[0]`.
    MultiSfu,
    /// A `focus_selection` we don't understand — no transport can be derived.
    Unknown,
}

impl Selection {
    /// Classify a membership's `focus_active`.
    pub fn from_active_focus(focus: &ActiveFocus) -> Self {
        #[allow(unreachable_patterns)] // `ActiveFocus` is non_exhaustive
        match focus {
            ActiveFocus::Livekit(ActiveLivekitFocus { focus_selection, .. }) => {
                match focus_selection {
                    FocusSelection::OldestMembership => Selection::OldestMembership,
                    other if other.as_str() == MULTI_SFU => Selection::MultiSfu,
                    _ => Selection::Unknown,
                }
            }
            _ => Selection::Unknown,
        }
    }
}

/// The `focus_active` the bridge advertises on its own memberships (ghosts and
/// the E2EE subscriber): `multi_sfu`, so a multi-SFU client resolves our
/// transport from OUR `foci_preferred` — i.e. always finds ghost tracks on the
/// bridge's SFU — even when we are not the oldest membership. Legacy clients only
/// ever consult their own `focus_active` and the oldest membership's
/// `foci_preferred`, so this is invisible to them.
pub fn bridge_active_focus() -> ActiveFocus {
    let mut f = ActiveLivekitFocus::new();
    f.focus_selection = FocusSelection::from(MULTI_SFU);
    ActiveFocus::Livekit(f)
}

/// The first LiveKit focus of a `foci_preferred` list, as our focus info.
pub fn first_livekit_focus(foci: &[Focus]) -> Option<LivekitFocusInfo> {
    foci.iter().find_map(|f| {
        #[allow(unreachable_patterns)] // `Focus` is non_exhaustive
        match f {
            Focus::Livekit(lk) => Some(LivekitFocusInfo {
                alias: lk.alias.clone(),
                service_url: lk.service_url.clone(),
            }),
            _ => None,
        }
    })
}

/// Canonical form of a `livekit_service_url` for equality: two memberships
/// naming the same lk-jwt service must compare equal even if one has a trailing
/// slash or different case in the scheme/host.
pub fn normalize_service_url(url: &str) -> String {
    url.trim().trim_end_matches('/').to_ascii_lowercase()
}

/// One active membership's focus-relevant fields.
#[derive(Debug, Clone)]
pub struct FocusEntry {
    /// The membership's unique handle (its state_key).
    pub key: String,
    /// `created_ts` (falling back to `origin_server_ts`), in ms — decides "oldest".
    pub created_ts_ms: u64,
    /// Not expired. Only active memberships can be "the oldest" (matrix-js-sdk
    /// only considers live memberships); an expired, un-retracted entry still
    /// gets resolved so the reaper can judge it, but never steers anyone else.
    pub active: bool,
    pub selection: Selection,
    /// `foci_preferred[0]` (first LiveKit entry), if any.
    pub preferred: Option<LivekitFocusInfo>,
}

/// The call's **elected** SFU: the transport the oldest active membership
/// publishes on (`multi_sfu` or `oldest_membership` ⇒ its own
/// `foci_preferred[0]`; unknown ⇒ none). This is the one SFU a *legacy*
/// (`oldest_membership`) client connects to and listens on, so it is also where
/// the bridge's ghosts must publish: a legacy client never looks anywhere else.
/// Ties on `created_ts` break on key so every observer agrees.
pub fn elected_focus(entries: &[FocusEntry]) -> Option<LivekitFocusInfo> {
    entries
        .iter()
        .filter(|e| e.active)
        .min_by(|a, b| a.created_ts_ms.cmp(&b.created_ts_ms).then_with(|| a.key.cmp(&b.key)))
        .and_then(|o| match o.selection {
            Selection::Unknown => None,
            _ => o.preferred.clone(),
        })
}

/// Resolve the SFU each membership publishes on, keyed by membership key.
///
/// `None` means no transport could be derived (unknown selection, empty
/// `foci_preferred`, or an `oldest_membership` member whose oldest peer has none).
/// The oldest membership is the minimum `created_ts_ms`, ties broken by key so
/// the result is deterministic.
pub fn resolve_effective_foci(entries: &[FocusEntry]) -> HashMap<String, Option<LivekitFocusInfo>> {
    let oldest_transport = elected_focus(entries);
    entries
        .iter()
        .map(|e| {
            let focus = match e.selection {
                Selection::MultiSfu => e.preferred.clone(),
                Selection::OldestMembership => oldest_transport.clone(),
                Selection::Unknown => None,
            };
            (e.key.clone(), focus)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use matrix_sdk::ruma::events::call::member::LivekitFocus;

    fn lk(url: &str) -> LivekitFocusInfo {
        LivekitFocusInfo { alias: "!r:x".into(), service_url: url.into() }
    }

    fn entry(key: &str, ts: u64, sel: Selection, url: Option<&str>) -> FocusEntry {
        FocusEntry { key: key.into(), created_ts_ms: ts, active: true, selection: sel, preferred: url.map(lk) }
    }

    #[test]
    fn elected_focus_is_the_oldest_active_members_transport() {
        // Commet (legacy) on matrix.org starts the call; our ghost joins later.
        // The election is matrix.org's SFU — where a legacy client listens, so
        // where the ghost must publish. With the ghost oldest, it's ours.
        let e = [
            entry("commet", 100, Selection::OldestMembership, Some("https://livekit-jwt.call.matrix.org")),
            entry("ghost", 200, Selection::MultiSfu, Some("https://livekit.nether.im")),
        ];
        assert_eq!(elected_focus(&e).unwrap().service_url, "https://livekit-jwt.call.matrix.org");
        let e = [
            entry("ghost", 50, Selection::MultiSfu, Some("https://livekit.nether.im")),
            entry("commet", 100, Selection::OldestMembership, Some("https://livekit-jwt.call.matrix.org")),
        ];
        assert_eq!(elected_focus(&e).unwrap().service_url, "https://livekit.nether.im");
        assert!(elected_focus(&[]).is_none());
        assert!(elected_focus(&[entry("x", 1, Selection::Unknown, Some("https://a"))]).is_none());
    }

    #[test]
    fn expired_memberships_never_count_as_oldest_but_still_resolve() {
        // A 4 h-old expired phantom (legacy, on SFU a) must not steer the live
        // legacy member; the oldest ACTIVE membership (multi_sfu on b) does.
        // The phantom itself still resolves (to the live oldest's transport) so
        // the reaper can observe and clear it.
        let phantom = FocusEntry { active: false, ..entry("phantom", 1, Selection::OldestMembership, Some("https://a")) };
        let e = [
            phantom,
            entry("fame", 100, Selection::MultiSfu, Some("https://b")),
            entry("legacy", 200, Selection::OldestMembership, Some("https://c")),
        ];
        let r = resolve_effective_foci(&e);
        assert_eq!(r["legacy"].as_ref().unwrap().service_url, "https://b");
        assert_eq!(r["phantom"].as_ref().unwrap().service_url, "https://b");
        // With NO active membership at all, legacy followers resolve to nothing.
        let only = [FocusEntry { active: false, ..entry("p", 1, Selection::OldestMembership, Some("https://a")) }];
        assert!(resolve_effective_foci(&only)["p"].is_none());
    }

    #[test]
    fn multi_sfu_members_publish_on_their_own_preferred_sfu() {
        // The Limited-VC case: a matrix.org human (oldest) in multi_sfu, our
        // ghost younger in multi_sfu. Each resolves to its own SFU.
        let e = [
            entry("fame", 100, Selection::MultiSfu, Some("https://livekit-jwt.call.matrix.org")),
            entry("ghost", 200, Selection::MultiSfu, Some("https://livekit.nether.im")),
        ];
        let r = resolve_effective_foci(&e);
        assert_eq!(r["fame"].as_ref().unwrap().service_url, "https://livekit-jwt.call.matrix.org");
        assert_eq!(r["ghost"].as_ref().unwrap().service_url, "https://livekit.nether.im");
    }

    #[test]
    fn legacy_members_follow_the_oldest_membership() {
        // Legacy client joins after a multi_sfu matrix.org user: it publishes
        // where the oldest membership does (js-sdk getTransport recursion).
        let e = [
            entry("fame", 100, Selection::MultiSfu, Some("https://livekit-jwt.call.matrix.org")),
            entry("legacy", 300, Selection::OldestMembership, Some("https://livekit.nether.im")),
        ];
        let r = resolve_effective_foci(&e);
        assert_eq!(r["legacy"].as_ref().unwrap().service_url, "https://livekit-jwt.call.matrix.org");
        // …and when the legacy member IS the oldest, everyone legacy uses its SFU.
        let e = [
            entry("legacy", 50, Selection::OldestMembership, Some("https://livekit.nether.im")),
            entry("legacy2", 300, Selection::OldestMembership, Some("https://livekit-jwt.call.matrix.org")),
            entry("fame", 100, Selection::MultiSfu, Some("https://livekit-jwt.call.matrix.org")),
        ];
        let r = resolve_effective_foci(&e);
        assert_eq!(r["legacy"].as_ref().unwrap().service_url, "https://livekit.nether.im");
        assert_eq!(r["legacy2"].as_ref().unwrap().service_url, "https://livekit.nether.im");
        assert_eq!(r["fame"].as_ref().unwrap().service_url, "https://livekit-jwt.call.matrix.org");
    }

    #[test]
    fn unknown_selection_resolves_to_no_transport() {
        let e = [
            entry("weird", 100, Selection::Unknown, Some("https://a")),
            entry("legacy", 200, Selection::OldestMembership, Some("https://b")),
        ];
        let r = resolve_effective_foci(&e);
        assert!(r["weird"].is_none());
        // The oldest is unknown ⇒ legacy followers get nothing either (not a guess).
        assert!(r["legacy"].is_none());
    }

    #[test]
    fn oldest_tie_breaks_on_key_deterministically() {
        let e = [
            entry("b", 100, Selection::OldestMembership, Some("https://b")),
            entry("a", 100, Selection::OldestMembership, Some("https://a")),
        ];
        let r = resolve_effective_foci(&e);
        assert_eq!(r["b"].as_ref().unwrap().service_url, "https://a");
    }

    #[test]
    fn empty_input_is_empty() {
        assert!(resolve_effective_foci(&[]).is_empty());
    }

    #[test]
    fn classifies_focus_selection_wire_values() {
        let oldest = ActiveFocus::Livekit(ActiveLivekitFocus::new());
        assert_eq!(Selection::from_active_focus(&oldest), Selection::OldestMembership);
        assert_eq!(Selection::from_active_focus(&bridge_active_focus()), Selection::MultiSfu);
        let mut f = ActiveLivekitFocus::new();
        f.focus_selection = FocusSelection::from("something_else");
        assert_eq!(Selection::from_active_focus(&ActiveFocus::Livekit(f)), Selection::Unknown);
    }

    #[test]
    fn bridge_active_focus_serializes_as_multi_sfu() {
        let v = serde_json::to_value(bridge_active_focus()).unwrap();
        assert_eq!(v["type"], "livekit");
        assert_eq!(v["focus_selection"], "multi_sfu");
    }

    #[test]
    fn first_livekit_focus_and_url_normalization() {
        let foci = vec![Focus::Livekit(LivekitFocus::new("!r:x".into(), "https://LK.example/".into()))];
        let f = first_livekit_focus(&foci).unwrap();
        assert_eq!(f.alias, "!r:x");
        assert_eq!(normalize_service_url(&f.service_url), "https://lk.example");
        assert_eq!(normalize_service_url(" https://lk.example//"), "https://lk.example");
        assert!(first_livekit_focus(&[]).is_none());
    }
}
