//! The presence rule (ADR 0001 §2) and its transitions.
//!
//! One [`LinkBoard`] per bridged (channel, room) pair, shared by the Discord side
//! (which reports who is in the voice channel) and the Matrix side (which reports
//! who is in the room call). From those two live facts plus the instance-wide
//! [`PairingStore`], per-room [`ServiceAccounts`] and the bridge's config
//! overrides, it answers two questions:
//!
//! * `discord_excluded(uid)` — is this Discord member bridge-owned *right now*
//!   (paired with a Matrix user who is in the call, or config-excluded)?
//! * `matrix_excluded(mxid)` — is this call member bridge-owned *right now*
//!   (paired with a Discord account that is in the channel, a service account
//!   for this room, or config-excluded)?
//!
//! and, because a presence change on one side flips the answer for identities on
//! the *other* side, every `set_*_present` returns the [`LinkEvent`]s describing
//! exactly which identities changed status, so each side can retire or (re)create
//! ghosts/puppets. Pure, synchronous, no I/O: the lifecycle code owns the effects.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use crate::pairings::PairingStore;
use crate::relays::ServiceAccounts;

/// An identity's bridge-owned status changed because of a presence update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinkEvent {
    /// A Discord member (present in the channel) became bridge-owned: retire its ghost.
    DiscordExcluded(u64),
    /// A Discord member (present in the channel) stopped being bridge-owned: it needs a ghost again.
    DiscordIncluded(u64),
    /// A call member (present in the call) became bridge-owned: release its puppet.
    MatrixExcluded(String),
    /// A call member (present in the call) stopped being bridge-owned: it needs a puppet again.
    /// Carries whether the member's Discord half *vanished* (true) — the "your Discord session dropped"
    /// notice case — as opposed to the member itself just arriving.
    MatrixIncluded { user: String, discord_half_vanished: bool },
}

#[derive(Debug, Default)]
struct Presence {
    discord: HashSet<u64>,
    matrix: HashSet<String>,
}

/// Where a board's events go: installed by whoever wires the two sides (the
/// supervisor), routing `Discord*` events to the Discord side and `Matrix*`
/// events to the Matrix side. A board without one just drops events.
pub type LinkDispatch = Box<dyn Fn(LinkEvent) + Send + Sync>;

/// A Discord member the bridge represents on Matrix as a ghost right now — what
/// an `observe` companion may learn about them (ADR 0004 §6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepresentedMember {
    pub discord_user_id: u64,
    /// The ghost's MXID, once the Matrix side has published its naming.
    pub ghost_mxid: Option<String>,
    pub display_name: String,
    /// Discord reports the member is streaming to the channel (`self_stream`).
    pub streaming: bool,
}

/// What the board tracks purely for companions: which members have a ghost (the
/// Discord side reports every create/retire, so opt-outs, bots and paired
/// members are never in it) and who is streaming.
#[derive(Debug, Default)]
struct Observation {
    ghosts: BTreeMap<u64, String>,
    streaming: HashSet<u64>,
    /// (ghost localpart prefix, server name), set by the Matrix side.
    ghost_naming: Option<(String, String)>,
}

pub struct LinkBoard {
    room_id: String,
    pairings: Arc<PairingStore>,
    services: Arc<ServiceAccounts>,
    config_discord: HashSet<u64>,
    config_matrix: HashSet<String>,
    presence: Mutex<Presence>,
    dispatch: Mutex<Option<LinkDispatch>>,
    observation: Mutex<Observation>,
    /// Bumped on every change a companion could observe; readers poll it.
    revision: AtomicU64,
}

impl std::fmt::Debug for LinkBoard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LinkBoard").field("room_id", &self.room_id).finish()
    }
}

impl LinkBoard {
    pub fn new(
        room_id: impl Into<String>,
        pairings: Arc<PairingStore>,
        services: Arc<ServiceAccounts>,
        config_discord: impl IntoIterator<Item = u64>,
        config_matrix: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            room_id: room_id.into(),
            pairings,
            services,
            config_discord: config_discord.into_iter().collect(),
            config_matrix: config_matrix.into_iter().collect(),
            presence: Mutex::new(Presence::default()),
            dispatch: Mutex::new(None),
            observation: Mutex::new(Observation::default()),
            revision: AtomicU64::new(0),
        }
    }

    pub fn room_id(&self) -> &str {
        &self.room_id
    }

    /// Delegates named by this person's pairings (for notices).
    pub fn delegates_for(&self, matrix_user: &str) -> Vec<String> {
        self.pairings.delegates_for_matrix(matrix_user)
    }

    pub fn set_dispatch(&self, f: LinkDispatch) {
        *self.dispatch.lock().unwrap_or_else(PoisonError::into_inner) = Some(f);
    }

    /// Deliver events to the installed dispatcher (no-op without one).
    pub fn dispatch_all(&self, events: Vec<LinkEvent>) {
        if events.is_empty() {
            return;
        }
        let guard = self.dispatch.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(f) = guard.as_ref() {
            for ev in events {
                f(ev);
            }
        }
    }

    // ── the rule ───────────────────────────────────────────────────────────────

    // Pairing-based exclusion needs BOTH halves present (ADR 0001 §2); the
    // overrides (config lists, service accounts) are unconditional.
    fn discord_excluded_in(&self, p: &Presence, uid: u64) -> bool {
        if self.config_discord.contains(&uid) {
            return true;
        }
        if !p.discord.contains(&uid) {
            return false;
        }
        match self.pairings.matrix_for_discord(uid) {
            Some(m) => p.matrix.contains(&m),
            None => false,
        }
    }

    fn matrix_excluded_in(&self, p: &Presence, mxid: &str) -> bool {
        if self.config_matrix.contains(mxid) || self.services.is_service(&self.room_id, mxid) {
            return true;
        }
        if !p.matrix.contains(mxid) {
            return false;
        }
        self.pairings.discords_for_matrix(mxid).iter().any(|d| p.discord.contains(d))
    }

    /// Is this Discord member bridge-owned right now?
    pub fn discord_excluded(&self, uid: u64) -> bool {
        let p = self.presence.lock().unwrap_or_else(PoisonError::into_inner);
        self.discord_excluded_in(&p, uid)
    }

    /// Is this call member bridge-owned right now?
    pub fn matrix_excluded(&self, mxid: &str) -> bool {
        let p = self.presence.lock().unwrap_or_else(PoisonError::into_inner);
        self.matrix_excluded_in(&p, mxid)
    }

    /// Call members present and NOT bridge-owned — the Matrix "humans".
    pub fn matrix_humans(&self) -> usize {
        let p = self.presence.lock().unwrap_or_else(PoisonError::into_inner);
        p.matrix.iter().filter(|m| !self.matrix_excluded_in(&p, m)).count()
    }

    /// Is this call member a listener WITHOUT a puppet — present, bridge-owned only
    /// because its paired Discord account is in the channel (not a service account,
    /// not config-excluded)? ADR 0003: such a person still listens from Matrix.
    pub fn matrix_listener_only(&self, mxid: &str) -> bool {
        let p = self.presence.lock().unwrap_or_else(PoisonError::into_inner);
        p.matrix.contains(mxid) && !self.never_listens(mxid) && self.matrix_excluded_in(&p, mxid)
    }

    /// Call members present who LISTEN from Matrix (ADR 0003): every member except
    /// service accounts and config-excluded ones — paired members included, unlike
    /// [`matrix_humans`](Self::matrix_humans). Drives "call active" and the Discord
    /// capture anchor; `matrix_humans` keeps deciding who gets a puppet.
    pub fn matrix_listeners(&self) -> usize {
        let p = self.presence.lock().unwrap_or_else(PoisonError::into_inner);
        p.matrix.iter().filter(|m| !self.never_listens(m)).count()
    }

    /// Service accounts and config-excluded members are helpers, not people: they
    /// neither speak through a puppet nor count as listeners.
    fn never_listens(&self, mxid: &str) -> bool {
        self.config_matrix.contains(mxid) || self.services.is_service(&self.room_id, mxid)
    }

    /// Is Discord member `uid` present AND the half of a pairing whose Matrix
    /// account is in the call right now — i.e. would its leaving hand that person
    /// back to a puppet (the failover-notice case)? The Discord side defers such a
    /// leave by `paired_leave_grace` so a reconnect is not a failover.
    pub fn paired_half_present(&self, uid: u64) -> bool {
        if self.config_discord.contains(&uid) {
            return false;
        }
        let p = self.presence.lock().unwrap_or_else(PoisonError::into_inner);
        p.discord.contains(&uid) && self.pairings.matrix_for_discord(uid).is_some_and(|m| p.matrix.contains(&m))
    }

    /// Voice members present and NOT bridge-owned — the Discord "humans".
    pub fn discord_humans(&self) -> usize {
        let p = self.presence.lock().unwrap_or_else(PoisonError::into_inner);
        p.discord.iter().filter(|d| !self.discord_excluded_in(&p, **d)).count()
    }

    // ── presence updates → transitions ─────────────────────────────────────────

    /// Record a Discord member entering/leaving the channel. Returns the status
    /// changes this causes for the member's paired Matrix user (if in the call).
    /// The member's OWN status is not an event: the caller checks
    /// `discord_excluded` right after, as the ghost gate.
    pub fn set_discord_present(&self, uid: u64, present: bool) -> Vec<LinkEvent> {
        let mut p = self.presence.lock().unwrap_or_else(PoisonError::into_inner);
        let changed = if present { p.discord.insert(uid) } else { p.discord.remove(&uid) };
        if !changed {
            return Vec::new();
        }
        if !present {
            self.observation.lock().unwrap_or_else(PoisonError::into_inner).streaming.remove(&uid);
        }
        self.bump();
        let mut events = Vec::new();
        if let Some(m) = self.pairings.matrix_for_discord(uid) {
            if p.matrix.contains(&m) {
                // Did this member's arrival/departure flip the Matrix user's status?
                // (Other paired Discord accounts of the same person may still be present.)
                let now = self.matrix_excluded_in(&p, &m);
                let before = {
                    // Evaluate as if this update had not happened.
                    if present {
                        p.discord.remove(&uid);
                    } else {
                        p.discord.insert(uid);
                    }
                    let b = self.matrix_excluded_in(&p, &m);
                    if present {
                        p.discord.insert(uid);
                    } else {
                        p.discord.remove(&uid);
                    }
                    b
                };
                if now && !before {
                    events.push(LinkEvent::MatrixExcluded(m));
                } else if !now && before {
                    events.push(LinkEvent::MatrixIncluded { user: m, discord_half_vanished: true });
                }
            }
        }
        events
    }

    /// Record a Matrix user entering/leaving the call. Returns the status changes
    /// this causes for each of the user's paired Discord accounts that are in the
    /// channel. The user's OWN status is not an event: the caller checks
    /// `matrix_excluded` right after, as the puppet gate.
    pub fn set_matrix_present(&self, mxid: &str, present: bool) -> Vec<LinkEvent> {
        let mut p = self.presence.lock().unwrap_or_else(PoisonError::into_inner);
        let changed = if present { p.matrix.insert(mxid.to_string()) } else { p.matrix.remove(mxid) };
        if !changed {
            return Vec::new();
        }
        self.bump();
        let mut events = Vec::new();
        for d in self.pairings.discords_for_matrix(mxid) {
            if !p.discord.contains(&d) || self.config_discord.contains(&d) {
                continue; // not in the channel, or excluded regardless of pairing
            }
            events.push(if present { LinkEvent::DiscordExcluded(d) } else { LinkEvent::DiscordIncluded(d) });
        }
        events
    }

    /// Re-evaluate every present identity (after a pairing/approval was added or
    /// removed at runtime): returns the events needed to make effects match.
    /// `was` must be the statuses captured *before* the store changed, via
    /// [`LinkBoard::snapshot_statuses`].
    pub fn diff_statuses(&self, was: &StatusSnapshot) -> Vec<LinkEvent> {
        // A store change can move the listener count without any event.
        self.bump();
        let p = self.presence.lock().unwrap_or_else(PoisonError::into_inner);
        let mut events = Vec::new();
        for d in &p.discord {
            let now = self.discord_excluded_in(&p, *d);
            let before = was.discord_excluded.contains(d);
            if now && !before {
                events.push(LinkEvent::DiscordExcluded(*d));
            } else if !now && before {
                events.push(LinkEvent::DiscordIncluded(*d));
            }
        }
        for m in &p.matrix {
            let now = self.matrix_excluded_in(&p, m);
            let before = was.matrix_excluded.contains(m);
            if now && !before {
                events.push(LinkEvent::MatrixExcluded(m.clone()));
            } else if !now && before {
                events.push(LinkEvent::MatrixIncluded { user: m.clone(), discord_half_vanished: false });
            }
        }
        events
    }

    // ── companion observation (ADR 0004 §6) ────────────────────────────────────

    fn bump(&self) {
        self.revision.fetch_add(1, Ordering::Relaxed);
    }

    /// Changes whenever anything [`represented`](Self::represented),
    /// [`matrix_listeners`](Self::matrix_listeners) or
    /// [`paired_present`](Self::paired_present) report may have changed.
    pub fn revision(&self) -> u64 {
        self.revision.load(Ordering::Relaxed)
    }

    /// The Discord side minted (`Some(display name)`) or retired (`None`) this
    /// member's ghost.
    pub fn note_ghost(&self, uid: u64, display_name: Option<&str>) {
        let mut o = self.observation.lock().unwrap_or_else(PoisonError::into_inner);
        let changed = match display_name {
            Some(n) => o.ghosts.insert(uid, n.to_string()).as_deref() != Some(n),
            None => o.ghosts.remove(&uid).is_some(),
        };
        drop(o);
        if changed {
            self.bump();
        }
    }

    /// Discord's streaming flag for a member in the channel.
    pub fn set_discord_streaming(&self, uid: u64, streaming: bool) {
        let mut o = self.observation.lock().unwrap_or_else(PoisonError::into_inner);
        let changed = if streaming { o.streaming.insert(uid) } else { o.streaming.remove(&uid) };
        drop(o);
        if changed {
            self.bump();
        }
    }

    /// The Matrix side publishes how ghost MXIDs are formed:
    /// `@<prefix><discord id>:<server>`.
    pub fn set_ghost_naming(&self, prefix: &str, server: &str) {
        self.observation.lock().unwrap_or_else(PoisonError::into_inner).ghost_naming =
            Some((prefix.to_string(), server.to_string()));
        self.bump();
    }

    /// Members represented by a ghost right now, by Discord id.
    pub fn represented(&self) -> Vec<RepresentedMember> {
        let o = self.observation.lock().unwrap_or_else(PoisonError::into_inner);
        o.ghosts
            .iter()
            .map(|(uid, name)| RepresentedMember {
                discord_user_id: *uid,
                ghost_mxid: o.ghost_naming.as_ref().map(|(p, s)| format!("@{p}{uid}:{s}")),
                display_name: name.clone(),
                streaming: o.streaming.contains(uid),
            })
            .collect()
    }

    /// Is `mxid` in the call while one of its paired Discord accounts is in the
    /// channel (the person is present on both sides as themselves)?
    pub fn paired_present(&self, mxid: &str) -> bool {
        let p = self.presence.lock().unwrap_or_else(PoisonError::into_inner);
        p.matrix.contains(mxid) && self.pairings.discords_for_matrix(mxid).iter().any(|d| p.discord.contains(d))
    }

    pub fn snapshot_statuses(&self) -> StatusSnapshot {
        let p = self.presence.lock().unwrap_or_else(PoisonError::into_inner);
        StatusSnapshot {
            discord_excluded: p.discord.iter().copied().filter(|d| self.discord_excluded_in(&p, *d)).collect(),
            matrix_excluded: p.matrix.iter().filter(|m| self.matrix_excluded_in(&p, m)).cloned().collect(),
        }
    }
}

/// Every live board in the process, so a runtime change to the pairing store or
/// the service approvals can be reconciled against who is present right now:
/// snapshot all boards, mutate, then dispatch each board's status diff.
#[derive(Default)]
pub struct LinkRegistry {
    boards: RwLock<Vec<(String, Arc<LinkBoard>)>>,
}

impl LinkRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, label: &str, board: Arc<LinkBoard>) {
        let mut g = self.boards.write().unwrap_or_else(PoisonError::into_inner);
        g.retain(|(l, _)| l != label);
        g.push((label.to_string(), board));
    }

    pub fn unregister(&self, label: &str) {
        self.boards.write().unwrap_or_else(PoisonError::into_inner).retain(|(l, _)| l != label);
    }

    /// The live board bridging `room_id`, if a bridge for it is running.
    pub fn board_for_room(&self, room_id: &str) -> Option<Arc<LinkBoard>> {
        self.boards
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .find(|(_, b)| b.room_id() == room_id)
            .map(|(_, b)| b.clone())
    }

    pub fn boards(&self) -> Vec<Arc<LinkBoard>> {
        self.boards.read().unwrap_or_else(PoisonError::into_inner).iter().map(|(_, b)| b.clone()).collect()
    }

    /// Run `mutate` (a pairing/approval change) and make every board's effects
    /// match afterwards. Returns `mutate`'s result.
    pub fn reconcile_after<R>(&self, mutate: impl FnOnce() -> R) -> R {
        let boards = self.boards();
        let snaps: Vec<StatusSnapshot> = boards.iter().map(|b| b.snapshot_statuses()).collect();
        let r = mutate();
        for (b, snap) in boards.iter().zip(snaps.iter()) {
            b.dispatch_all(b.diff_statuses(snap));
        }
        r
    }
}

/// The instance-wide identity state every bridge and the management bot share.
#[derive(Clone)]
pub struct LinkShared {
    pub pairings: Arc<PairingStore>,
    pub services: Arc<ServiceAccounts>,
    pub registry: Arc<LinkRegistry>,
}

impl LinkShared {
    /// Open (or create) the stores under `data_dir`.
    pub fn open(data_dir: &str) -> Result<Self, crate::ConfigError> {
        Ok(Self {
            pairings: Arc::new(PairingStore::open(crate::pairings::pairings_path(data_dir))?),
            services: Arc::new(ServiceAccounts::open(crate::relays::relays_path(data_dir))?),
            registry: Arc::new(LinkRegistry::new()),
        })
    }

    /// Everything in memory, nothing on disk (tests).
    pub fn in_memory() -> Self {
        Self {
            pairings: Arc::new(PairingStore::in_memory()),
            services: Arc::new(ServiceAccounts::in_memory()),
            registry: Arc::new(LinkRegistry::new()),
        }
    }

    /// A board for one bridge, using this instance's stores plus the bridge's
    /// config overrides (already validated as well-formed ids).
    pub fn board_for(&self, room_id: &str, excluded_discord: &[String], excluded_matrix: &[String]) -> Arc<LinkBoard> {
        Arc::new(LinkBoard::new(
            room_id,
            self.pairings.clone(),
            self.services.clone(),
            excluded_discord.iter().filter_map(|s| s.parse().ok()),
            excluded_matrix.iter().cloned(),
        ))
    }
}

/// Which present identities were bridge-owned at a point in time.
#[derive(Debug, Default, Clone)]
pub struct StatusSnapshot {
    pub discord_excluded: HashSet<u64>,
    pub matrix_excluded: HashSet<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALICE: &str = "@alice:hs";
    const ROOM: &str = "!room:hs";

    fn board(pairs: &[(&str, u64)], services: &[&str], cfg_d: &[u64], cfg_m: &[&str]) -> LinkBoard {
        let ps = Arc::new(PairingStore::in_memory());
        for (m, d) in pairs {
            ps.add(m, *d, 0).unwrap();
        }
        let sa = Arc::new(ServiceAccounts::in_memory());
        for s in services {
            sa.approve(ROOM, s, "test", 0).unwrap();
        }
        LinkBoard::new(ROOM, ps, sa, cfg_d.iter().copied(), cfg_m.iter().map(|s| s.to_string()))
    }

    /// ADR 0003: a present pair is bridge-owned (no puppet) but its Matrix half still
    /// LISTENS; service accounts and config-excluded members never do.
    #[test]
    fn paired_matrix_half_is_a_listener_but_not_a_human() {
        let b = board(&[(ALICE, 1)], &["@helper:hs"], &[], &["@cfg:hs"]);
        b.set_matrix_present(ALICE, true);
        assert_eq!((b.matrix_humans(), b.matrix_listeners()), (1, 1));
        assert!(!b.matrix_listener_only(ALICE), "puppeted, an ordinary member");
        b.set_discord_present(1, true); // the pair is complete
        assert!(b.matrix_excluded(ALICE));
        assert_eq!((b.matrix_humans(), b.matrix_listeners()), (0, 1), "no puppet, still listening");
        assert!(b.matrix_listener_only(ALICE));
        assert_eq!(b.discord_humans(), 0, "the Discord half is not a Discord listener");
        b.set_matrix_present("@helper:hs", true);
        b.set_matrix_present("@cfg:hs", true);
        assert_eq!(b.matrix_listeners(), 1, "service + config-excluded accounts never listen");
        assert!(!b.matrix_listener_only("@helper:hs"));
        assert!(!b.matrix_listener_only("@cfg:hs"));
        b.set_discord_present(1, false); // Discord half leaves → ordinary again
        assert!(!b.matrix_listener_only(ALICE));
        assert_eq!((b.matrix_humans(), b.matrix_listeners()), (1, 1));
        b.set_matrix_present(ALICE, false);
        assert!(!b.matrix_listener_only(ALICE), "absent members are nothing");
        assert_eq!(b.matrix_listeners(), 0);
    }

    #[test]
    fn unpaired_identities_are_never_excluded() {
        let b = board(&[], &[], &[], &[]);
        assert!(b.set_discord_present(1, true).is_empty());
        assert!(b.set_matrix_present(ALICE, true).is_empty());
        assert!(!b.discord_excluded(1));
        assert!(!b.matrix_excluded(ALICE));
        assert_eq!((b.discord_humans(), b.matrix_humans()), (1, 1));
    }

    #[test]
    fn paired_person_present_on_one_side_only_is_ordinary() {
        let b = board(&[(ALICE, 1)], &[], &[], &[]);
        assert!(b.set_discord_present(1, true).is_empty());
        assert!(!b.discord_excluded(1));
        assert!(!b.matrix_excluded(ALICE)); // not in the call → irrelevant
        assert!(b.set_discord_present(1, false).is_empty());
        assert!(b.set_matrix_present(ALICE, true).is_empty());
        assert!(!b.matrix_excluded(ALICE));
    }

    #[test]
    fn matrix_half_arriving_second_excludes_the_present_discord_half() {
        let b = board(&[(ALICE, 1)], &[], &[], &[]);
        b.set_discord_present(1, true);
        // Alice joins the call: her Discord account (ghosted so far) must lose its ghost.
        assert_eq!(b.set_matrix_present(ALICE, true), vec![LinkEvent::DiscordExcluded(1)]);
        assert!(b.discord_excluded(1));
        assert!(b.matrix_excluded(ALICE)); // and she gets no puppet (caller checks this)
        assert_eq!((b.discord_humans(), b.matrix_humans()), (0, 0));
        // Alice leaves the call: her Discord account needs its ghost back.
        assert_eq!(b.set_matrix_present(ALICE, false), vec![LinkEvent::DiscordIncluded(1)]);
        assert!(!b.discord_excluded(1));
    }

    #[test]
    fn paired_half_present_only_while_both_halves_are_here() {
        let b = board(&[(ALICE, 1)], &[], &[], &[]);
        assert!(!b.paired_half_present(1), "not in the channel");
        b.set_discord_present(1, true);
        assert!(!b.paired_half_present(1), "Matrix half not in the call");
        b.set_matrix_present(ALICE, true);
        assert!(b.paired_half_present(1));
        assert!(!b.paired_half_present(2), "unpaired member");
        let c = board(&[(ALICE, 1)], &[], &[1], &[]);
        c.set_discord_present(1, true);
        c.set_matrix_present(ALICE, true);
        assert!(!c.paired_half_present(1), "config-excluded regardless of pairing");
    }

    #[test]
    fn discord_half_arriving_second_excludes_the_present_matrix_half() {
        let b = board(&[(ALICE, 1)], &[], &[], &[]);
        b.set_matrix_present(ALICE, true); // puppeted so far
        // The companion client comes up: Alice's puppet must be released.
        assert_eq!(b.set_discord_present(1, true), vec![LinkEvent::MatrixExcluded(ALICE.into())]);
        assert!(b.matrix_excluded(ALICE));
        assert!(b.discord_excluded(1));
        // The companion client drops: Alice needs a puppet again, and this is the notice case.
        assert_eq!(
            b.set_discord_present(1, false),
            vec![LinkEvent::MatrixIncluded { user: ALICE.into(), discord_half_vanished: true }]
        );
        assert!(!b.matrix_excluded(ALICE));
    }

    #[test]
    fn two_paired_discord_accounts_of_one_person() {
        let b = board(&[(ALICE, 1), (ALICE, 2)], &[], &[], &[]);
        b.set_matrix_present(ALICE, true);
        assert_eq!(b.set_discord_present(1, true), vec![LinkEvent::MatrixExcluded(ALICE.into())]);
        // Second account arriving changes nothing for Alice (already excluded); it is itself excluded.
        assert!(b.set_discord_present(2, true).is_empty());
        assert!(b.discord_excluded(2));
        // First account leaving: Alice stays excluded because #2 is still there.
        assert!(b.set_discord_present(1, false).is_empty());
        assert!(b.matrix_excluded(ALICE));
        // Last one leaving flips her back.
        assert_eq!(
            b.set_discord_present(2, false),
            vec![LinkEvent::MatrixIncluded { user: ALICE.into(), discord_half_vanished: true }]
        );
    }

    #[test]
    fn both_halves_leaving_in_either_order_is_clean() {
        for matrix_first in [true, false] {
            let b = board(&[(ALICE, 1)], &[], &[], &[]);
            b.set_discord_present(1, true);
            b.set_matrix_present(ALICE, true);
            if matrix_first {
                assert_eq!(b.set_matrix_present(ALICE, false), vec![LinkEvent::DiscordIncluded(1)]);
                assert!(b.set_discord_present(1, false).is_empty());
            } else {
                assert_eq!(
                    b.set_discord_present(1, false),
                    vec![LinkEvent::MatrixIncluded { user: ALICE.into(), discord_half_vanished: true }]
                );
                assert!(b.set_matrix_present(ALICE, false).is_empty());
            }
            assert_eq!((b.discord_humans(), b.matrix_humans()), (0, 0));
        }
    }

    #[test]
    fn service_account_and_config_overrides_are_unconditional() {
        let b = board(&[(ALICE, 1)], &["@side:hs"], &[7], &["@cfg:hs"]);
        assert!(b.set_matrix_present("@side:hs", true).is_empty());
        assert!(b.matrix_excluded("@side:hs"));
        assert!(b.matrix_excluded("@cfg:hs"));
        assert!(b.discord_excluded(7));
        // A config-excluded Discord id never produces include/exclude events for itself.
        b.set_discord_present(7, true);
        assert!(b.set_matrix_present(ALICE, true).is_empty());
        assert_eq!(b.matrix_humans(), 1); // only Alice (paired to 1, which is absent)
    }

    #[test]
    fn duplicate_presence_updates_are_idempotent() {
        let b = board(&[(ALICE, 1)], &[], &[], &[]);
        b.set_matrix_present(ALICE, true);
        assert_eq!(b.set_discord_present(1, true).len(), 1);
        assert!(b.set_discord_present(1, true).is_empty());
        assert!(b.set_matrix_present(ALICE, true).is_empty());
    }

    #[test]
    fn registry_reconciles_a_runtime_pairing_change_through_dispatch() {
        let ps = Arc::new(PairingStore::in_memory());
        let b = Arc::new(LinkBoard::new(ROOM, ps.clone(), Arc::new(ServiceAccounts::in_memory()), [], []));
        let got = Arc::new(Mutex::new(Vec::new()));
        let sink = got.clone();
        b.set_dispatch(Box::new(move |ev| sink.lock().unwrap().push(ev)));
        let reg = LinkRegistry::new();
        reg.register("bridge-a", b.clone());
        b.set_discord_present(1, true);
        b.set_matrix_present(ALICE, true);
        reg.reconcile_after(|| ps.add(ALICE, 1, 0).unwrap());
        let evs = got.lock().unwrap().clone();
        assert!(evs.contains(&LinkEvent::DiscordExcluded(1)));
        assert!(evs.contains(&LinkEvent::MatrixExcluded(ALICE.into())));
        reg.unregister("bridge-a");
        assert!(reg.boards().is_empty());
    }

    #[test]
    fn runtime_pairing_change_is_reconciled_by_diff() {
        let ps = Arc::new(PairingStore::in_memory());
        let b = LinkBoard::new(ROOM, ps.clone(), Arc::new(ServiceAccounts::in_memory()), [], []);
        b.set_discord_present(1, true);
        b.set_matrix_present(ALICE, true);
        let before = b.snapshot_statuses();
        assert!(before.discord_excluded.is_empty());
        ps.add(ALICE, 1, 0).unwrap(); // paired while both are present
        let ev = b.diff_statuses(&before);
        assert!(ev.contains(&LinkEvent::DiscordExcluded(1)));
        assert!(ev.contains(&LinkEvent::MatrixExcluded(ALICE.into())));
        let before = b.snapshot_statuses();
        ps.remove(ALICE, 1).unwrap(); // unpaired
        let ev = b.diff_statuses(&before);
        assert!(ev.contains(&LinkEvent::DiscordIncluded(1)));
        assert!(ev.contains(&LinkEvent::MatrixIncluded { user: ALICE.into(), discord_half_vanished: false }));
    }

    #[test]
    fn observation_tracks_ghosts_streaming_and_revision() {
        let b = board(&[(ALICE, 1)], &[], &[], &[]);
        let r0 = b.revision();
        b.set_discord_present(7, true);
        b.note_ghost(7, Some("Seven"));
        b.set_discord_streaming(7, true);
        assert!(b.revision() > r0);
        assert_eq!(
            b.represented(),
            vec![RepresentedMember { discord_user_id: 7, ghost_mxid: None, display_name: "Seven".into(), streaming: true }]
        );
        b.set_ghost_naming("nvb_", "hs");
        assert_eq!(b.represented()[0].ghost_mxid.as_deref(), Some("@nvb_7:hs"));
        // An unchanged update does not bump.
        let r1 = b.revision();
        b.note_ghost(7, Some("Seven"));
        b.set_discord_streaming(7, true);
        assert_eq!(b.revision(), r1);
        // Leaving clears the streaming flag; retiring the ghost removes the member.
        b.set_discord_present(7, false);
        b.note_ghost(7, None);
        assert!(b.represented().is_empty());
        b.set_discord_present(7, true);
        b.note_ghost(7, Some("Seven"));
        assert!(!b.represented()[0].streaming);
        // paired_present needs both halves.
        b.set_matrix_present(ALICE, true);
        assert!(!b.paired_present(ALICE));
        b.set_discord_present(1, true);
        assert!(b.paired_present(ALICE));
    }

    #[test]
    fn registry_finds_a_board_by_room() {
        let reg = LinkRegistry::new();
        let b = Arc::new(board(&[], &[], &[], &[]));
        reg.register("x", b.clone());
        assert!(reg.board_for_room(ROOM).is_some_and(|f| Arc::ptr_eq(&f, &b)));
        assert!(reg.board_for_room("!nope:hs").is_none());
    }
}
