//! The companion hub: key issuance/revocation, authentication, rate limits and
//! the in-memory lease registry (ADR 0004 §3, §7). Leases are deliberately not
//! persisted: a bridge restart drops every lease and companions re-request.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use nvb_config::companions::{CompanionKey, CompanionKeys, KeyScope};
use nvb_config::links::LinkShared;
use nvb_config::{CompanionsConfig, ConfigError};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::stream::{self, StreamGrant};
use crate::{audit, token, unix_now};

/// Lease kinds this build serves. Keys may carry others (they are issued ahead
/// of the phases that serve them); requesting one answers `kind_unavailable`.
pub const SERVED_KINDS: &[&str] = &["observe", "stream:attach"];

/// Attached streams per room (ADR 0004 §4.6).
pub const MAX_STREAMS_PER_ROOM: usize = 4;
/// How long Discord's streaming flag may stay off before a flag-consented stream ends (reconnects, ADR 0004 §8).
pub const FLAG_GRACE: Duration = Duration::from_secs(60);
/// How long the call may stay without a Matrix listener before an attached stream ends.
pub const INACTIVE_GRACE: Duration = Duration::from_secs(60);
/// How long the bridge has to set up a stream device.
const ATTACH_TIMEOUT: Duration = Duration::from_secs(45);

/// Hard ceiling on concurrent leases across all keys.
const MAX_LEASES_TOTAL: usize = 512;

/// Why a request was refused: HTTP status, a stable machine reason, and a
/// sentence for humans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub status: u16,
    pub reason: &'static str,
    pub message: String,
}

impl Refusal {
    fn new(status: u16, reason: &'static str, message: impl Into<String>) -> Self {
        Self { status, reason, message: message.into() }
    }
}

/// What the lease's event stream reads: the lease's end (if any), its expiry,
/// and a wake-up for both.
#[derive(Debug)]
pub struct LeaseShared {
    pub id: String,
    pub kind: String,
    pub room_id: String,
    /// Issuer of the key (for `pairing_present`).
    pub issuer: String,
    expires_at: AtomicU64,
    ended: Mutex<Option<String>>,
    pub notify: Notify,
    /// For `stream:attach`: the attached stream device.
    pub stream: Option<StreamAttachment>,
}

/// The stream device behind a `stream:attach` lease.
#[derive(Debug, Clone)]
pub struct StreamAttachment {
    pub discord_user_id: u64,
    /// Why the represented person counts as agreeing (ADR 0004 §4): "streaming_flag", "personal_grant" or
    /// "own_pairing".
    pub basis: &'static str,
    pub grant: StreamGrant,
    /// Fires the stream device's teardown.
    pub cancel: CancellationToken,
}

impl LeaseShared {
    pub fn expires_at(&self) -> u64 {
        self.expires_at.load(Ordering::Relaxed)
    }

    /// The reason the lease ended, once it has.
    pub fn ended(&self) -> Option<String> {
        self.ended.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }
}

struct Lease {
    key_id: String,
    shared: Arc<LeaseShared>,
    deadline: Instant,
    /// Whether an event stream is attached right now.
    streaming: bool,
    /// Since when the lease has had no event stream.
    detached_since: Instant,
    /// Stream leases: since when Discord's streaming flag has been off (flag consent only).
    flag_off_since: Option<Instant>,
    /// Stream leases: since when the call has had no Matrix listener.
    inactive_since: Option<Instant>,
}

#[derive(Default)]
struct HubState {
    leases: HashMap<String, Lease>,
    lease_requests: HashMap<String, VecDeque<Instant>>,
    auth_failures: HashMap<String, VecDeque<Instant>>,
}

pub struct CompanionHub {
    cfg: CompanionsConfig,
    keys: Arc<CompanionKeys>,
    links: LinkShared,
    audit_path: String,
    state: Mutex<HubState>,
}

/// A granted lease, as returned to the companion.
#[derive(Debug, Clone)]
pub struct Grant {
    pub shared: Arc<LeaseShared>,
}

/// Count of events in the last minute, after dropping older ones.
fn window_count(q: &mut VecDeque<Instant>, now: Instant) -> usize {
    while q.front().is_some_and(|t| now.saturating_duration_since(*t) >= Duration::from_secs(60)) {
        q.pop_front();
    }
    q.len()
}

impl CompanionHub {
    pub fn open(cfg: CompanionsConfig, data_dir: &str, links: LinkShared) -> Result<Arc<Self>, ConfigError> {
        let keys = CompanionKeys::open(nvb_config::companions::companions_path(data_dir))?;
        Ok(Self::with_store(cfg, Arc::new(keys), links, audit::audit_path(data_dir)))
    }

    pub fn with_store(
        cfg: CompanionsConfig,
        keys: Arc<CompanionKeys>,
        links: LinkShared,
        audit_path: String,
    ) -> Arc<Self> {
        Arc::new(Self { cfg, keys, links, audit_path, state: Mutex::new(HubState::default()) })
    }

    /// The key and grant store (shared with the Discord side's `/bridge companion` commands).
    pub fn keys(&self) -> &Arc<CompanionKeys> {
        &self.keys
    }

    pub fn config(&self) -> &CompanionsConfig {
        &self.cfg
    }

    pub fn links(&self) -> &LinkShared {
        &self.links
    }

    fn state(&self) -> std::sync::MutexGuard<'_, HubState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn audit(&self, actor: &str, event: &str, detail: serde_json::Value) {
        audit::append(&self.audit_path, actor, event, detail);
    }

    // ── keys ──────────────────────────────────────────────────────────────────

    /// Mint and store a key. The caller has already checked the issuer's
    /// authority for `scope`. Returns the stored key and the token (shown once).
    pub fn create_key(
        &self,
        issuer: &str,
        scope: KeyScope,
        kinds: &[String],
        label: &str,
    ) -> Result<(CompanionKey, String), String> {
        let kinds = nvb_config::companions::validate_kinds(&scope, kinds)?;
        let minted = token::mint();
        let key = CompanionKey {
            id: minted.id,
            secret_sha256: minted.secret_sha256,
            issuer: issuer.to_string(),
            scope,
            kinds,
            label: label.chars().take(80).collect(),
            created: unix_now() as i64,
        };
        self.keys.add(key.clone()).map_err(|e| format!("cannot write the companions file: {e}"))?;
        self.audit(issuer, "companion_key_create", serde_json::json!({
            "key_id": key.id, "scope": key.scope.describe(), "kinds": key.kinds, "label": key.label,
        }));
        Ok((key, minted.token))
    }

    pub fn list_keys(&self) -> Vec<CompanionKey> {
        self.keys.list()
    }

    pub fn get_key(&self, id: &str) -> Option<CompanionKey> {
        self.keys.get(id)
    }

    /// Live leases held under a key.
    pub fn lease_count(&self, key_id: &str) -> usize {
        self.state().leases.values().filter(|l| l.key_id == key_id).count()
    }

    /// Delete a key and end its leases at once.
    pub fn revoke_key(&self, id: &str, actor: &str) -> Result<Option<CompanionKey>, ConfigError> {
        let removed = self.keys.remove(id)?;
        if let Some(k) = &removed {
            self.end_where(|l| l.key_id == k.id, "key_revoked");
            self.audit(actor, "companion_key_revoke", serde_json::json!({ "key_id": k.id, "scope": k.scope.describe() }));
        }
        Ok(removed)
    }

    /// The room's bridge was unlinked: its keys go, and their leases end.
    pub fn revoke_room(&self, room_id: &str, actor: &str) -> Result<usize, ConfigError> {
        let gone = self.keys.remove_room(room_id)?;
        for k in &gone {
            self.end_where(|l| l.key_id == k.id, "room_unlinked");
            self.audit(actor, "companion_key_revoke", serde_json::json!({ "key_id": k.id, "scope": room_id, "reason": "room_unlinked" }));
        }
        Ok(gone.len())
    }

    // ── authentication ────────────────────────────────────────────────────────

    /// Resolve a bearer token to its key. `client` keys the failure limit.
    pub fn authenticate(&self, bearer: Option<&str>, client: &str) -> Result<CompanionKey, Refusal> {
        let now = Instant::now();
        {
            let mut st = self.state();
            let q = st.auth_failures.entry(client.to_string()).or_default();
            if window_count(q, now) >= self.cfg.auth_failures_per_minute as usize {
                return Err(Refusal::new(429, "rate_limited", "too many failed authentications; retry later"));
            }
            if q.is_empty() {
                st.auth_failures.remove(client);
            }
        }
        let key = bearer.and_then(token::parse).and_then(|(id, secret)| {
            let k = self.keys.get(id)?;
            token::secret_matches(secret, &k.secret_sha256).then_some(k)
        });
        let key = key.filter(|k| match &k.scope {
            // A self-scoped key lives only as long as its issuer has a pairing.
            KeyScope::SelfScope => !self.links.pairings.discords_for_matrix(&k.issuer).is_empty(),
            KeyScope::Room(_) => true,
        });
        match key {
            Some(k) => Ok(k),
            None => {
                let mut st = self.state();
                st.auth_failures.entry(client.to_string()).or_default().push_back(now);
                // Keep the failure map bounded against address churn.
                if st.auth_failures.len() > 10_000 {
                    st.auth_failures.retain(|_, q| window_count(q, now) > 0);
                }
                Err(Refusal::new(401, "unauthorized", "missing or invalid companion key"))
            }
        }
    }

    // ── leases ────────────────────────────────────────────────────────────────

    pub fn open_lease(&self, key: &CompanionKey, kind: &str, room_id: Option<&str>) -> Result<Grant, Refusal> {
        if !nvb_config::companions::KINDS.contains(&kind) {
            return Err(Refusal::new(400, "unknown_kind", format!("unknown lease kind '{kind}'")));
        }
        if !key.allows(kind) {
            return Err(Refusal::new(403, "kind_not_allowed", format!("this key may not request '{kind}'")));
        }
        if !SERVED_KINDS.contains(&kind) {
            return Err(Refusal::new(501, "kind_unavailable", format!("'{kind}' leases are not available on this instance yet")));
        }
        if kind == "stream:attach" {
            return Err(Refusal::new(400, "bad_request", "stream:attach needs a participant (open_stream_lease)"));
        }
        let room = match (&key.scope, room_id) {
            (KeyScope::Room(r), None) => r.clone(),
            (KeyScope::Room(r), Some(req)) if req == r => r.clone(),
            (KeyScope::Room(_), Some(_)) => {
                return Err(Refusal::new(403, "wrong_room", "this key is scoped to a different room"));
            }
            (KeyScope::SelfScope, _) => {
                return Err(Refusal::new(403, "kind_not_allowed", format!("a self-scoped key cannot request '{kind}'")));
            }
        };
        let now = Instant::now();
        self.admit(key, now)?;
        let mut st = self.state();
        let ttl = Duration::from_secs(self.cfg.max_lease_hours * 3600);
        let shared = Arc::new(LeaseShared {
            id: token::lease_id(),
            kind: kind.to_string(),
            room_id: room.clone(),
            issuer: key.issuer.clone(),
            expires_at: AtomicU64::new(unix_now() + ttl.as_secs()),
            ended: Mutex::new(None),
            notify: Notify::new(),
            stream: None,
        });
        st.leases.insert(shared.id.clone(), Lease {
            key_id: key.id.clone(),
            shared: shared.clone(),
            deadline: now + ttl,
            streaming: false,
            detached_since: now,
            flag_off_since: None,
            inactive_since: None,
        });
        drop(st);
        self.audit(&key.id, "lease_open", serde_json::json!({ "lease_id": shared.id, "kind": kind, "room_id": room }));
        tracing::info!(key_id = %key.id, lease_id = %shared.id, kind, room_id = %room, "companion lease opened");
        Ok(Grant { shared })
    }

    /// Rate limit and lease caps for a new lease of `key`.
    fn admit(&self, key: &CompanionKey, now: Instant) -> Result<(), Refusal> {
        let mut st = self.state();
        let q = st.lease_requests.entry(key.id.clone()).or_default();
        if window_count(q, now) >= self.cfg.lease_requests_per_minute as usize {
            return Err(Refusal::new(429, "rate_limited", "too many lease requests for this key; retry later"));
        }
        q.push_back(now);
        let held = st.leases.values().filter(|l| l.key_id == key.id).count();
        if held >= self.cfg.max_leases_per_key {
            return Err(Refusal::new(409, "cap", format!("this key already holds {held} leases (limit {})", self.cfg.max_leases_per_key)));
        }
        if st.leases.len() >= MAX_LEASES_TOTAL {
            return Err(Refusal::new(503, "cap", "the bridge holds too many leases right now"));
        }
        Ok(())
    }

    /// A lease held by `key`, if it exists.
    pub fn lease(&self, key: &CompanionKey, lease_id: &str) -> Option<Arc<LeaseShared>> {
        self.state().leases.get(lease_id).filter(|l| l.key_id == key.id).map(|l| l.shared.clone())
    }

    /// Attach the lease's event stream. Only one at a time; the returned guard
    /// detaches it when dropped.
    pub fn attach_stream(self: &Arc<Self>, key: &CompanionKey, lease_id: &str) -> Result<StreamGuard, Refusal> {
        let mut st = self.state();
        let Some(l) = st.leases.get_mut(lease_id).filter(|l| l.key_id == key.id) else {
            return Err(Refusal::new(404, "no_lease", "no such lease"));
        };
        if l.streaming {
            return Err(Refusal::new(409, "already_connected", "this lease's event stream is already open"));
        }
        l.streaming = true;
        Ok(StreamGuard { hub: self.clone(), lease_id: lease_id.to_string(), shared: l.shared.clone() })
    }

    fn detach_stream(&self, lease_id: &str) {
        if let Some(l) = self.state().leases.get_mut(lease_id) {
            l.streaming = false;
            l.detached_since = Instant::now();
        }
    }

    pub fn renew(&self, key: &CompanionKey, lease_id: &str) -> Result<u64, Refusal> {
        let ttl = Duration::from_secs(self.cfg.max_lease_hours * 3600);
        let mut st = self.state();
        let Some(l) = st.leases.get_mut(lease_id).filter(|l| l.key_id == key.id) else {
            return Err(Refusal::new(404, "no_lease", "no such lease"));
        };
        l.deadline = Instant::now() + ttl;
        let at = unix_now() + ttl.as_secs();
        l.shared.expires_at.store(at, Ordering::Relaxed);
        l.shared.notify.notify_one();
        drop(st);
        self.audit(&key.id, "lease_renew", serde_json::json!({ "lease_id": lease_id, "expires_at": at }));
        Ok(at)
    }

    /// End a lease on the companion's request.
    pub fn release(&self, key: &CompanionKey, lease_id: &str) -> Result<(), Refusal> {
        let owned = self.state().leases.get(lease_id).is_some_and(|l| l.key_id == key.id);
        if !owned {
            return Err(Refusal::new(404, "no_lease", "no such lease"));
        }
        self.end_where(|l| l.shared.id == lease_id, "released");
        Ok(())
    }

    /// End every lease matching `pred` with `reason`.
    fn end_where(&self, pred: impl Fn(&Lease) -> bool, reason: &str) {
        let ended: Vec<(String, Lease)> = {
            let mut st = self.state();
            let ids: Vec<String> = st.leases.iter().filter(|(_, l)| pred(l)).map(|(id, _)| id.clone()).collect();
            ids.into_iter().filter_map(|id| st.leases.remove(&id).map(|l| (id, l))).collect()
        };
        for (id, l) in ended {
            if let Some(s) = &l.shared.stream {
                s.cancel.cancel();
            }
            *l.shared.ended.lock().unwrap_or_else(PoisonError::into_inner) = Some(reason.to_string());
            l.shared.notify.notify_one();
            self.audit(&l.key_id, "lease_end", serde_json::json!({ "lease_id": id, "kind": l.shared.kind, "room_id": l.shared.room_id, "reason": reason }));
            tracing::info!(key_id = %l.key_id, lease_id = %id, reason, "companion lease ended");
        }
    }

    /// One pass of lease housekeeping: expiry and event-stream grace.
    pub fn sweep(&self, now: Instant) {
        let grace = Duration::from_secs(self.cfg.sse_grace_secs);
        self.end_where(|l| now >= l.deadline, "expired");
        self.end_where(|l| !l.streaming && now.saturating_duration_since(l.detached_since) >= grace, "disconnected");
        self.sweep_streams(now);
    }

    /// Re-check every attached stream against what the bridge sees (ADR 0004 §4, §8).
    fn sweep_streams(&self, now: Instant) {
        let mut ends: Vec<(String, &'static str)> = Vec::new();
        {
            let mut st = self.state();
            for (id, l) in st.leases.iter_mut() {
                let Some(s) = l.shared.stream.as_ref() else { continue };
                if s.grant.transport.borrow().is_none() || s.cancel.is_cancelled() {
                    ends.push((id.clone(), "detached"));
                    continue;
                }
                let Some(board) = self.links.registry.board_for_room(&l.shared.room_id) else {
                    ends.push((id.clone(), "bridge_stopped"));
                    continue;
                };
                let Some(member) = board.represented().into_iter().find(|m| m.discord_user_id == s.discord_user_id) else {
                    ends.push((id.clone(), "not_represented"));
                    continue;
                };
                if s.basis == "personal_grant" && !self.keys.has_grant(s.discord_user_id, &l.key_id) {
                    ends.push((id.clone(), "consent_withdrawn"));
                    continue;
                }
                if s.basis == "streaming_flag" {
                    if member.streaming {
                        l.flag_off_since = None;
                    } else if now.saturating_duration_since(*l.flag_off_since.get_or_insert(now)) >= FLAG_GRACE {
                        ends.push((id.clone(), "consent_withdrawn"));
                        continue;
                    }
                }
                if board.matrix_listeners() > 0 {
                    l.inactive_since = None;
                } else if now.saturating_duration_since(*l.inactive_since.get_or_insert(now)) >= INACTIVE_GRACE {
                    ends.push((id.clone(), "call_inactive"));
                }
            }
        }
        for (id, reason) in ends {
            self.end_where(|l| l.shared.id == id, reason);
        }
    }

    /// A stream device published a disallowed source: end the lease that holds it.
    pub fn revoke_identity(&self, identity: &str) {
        self.end_where(
            |l| l.shared.stream.as_ref().is_some_and(|s| s.grant.transport.borrow().as_ref().is_some_and(|t| t.identity == identity)),
            "disallowed_source",
        );
    }

    /// Discord ids with an attached stream in `room_id` (for `observe`).
    pub fn attached_in(&self, room_id: &str) -> std::collections::HashSet<u64> {
        self.state()
            .leases
            .values()
            .filter(|l| l.shared.room_id == room_id)
            .filter_map(|l| l.shared.stream.as_ref().map(|s| s.discord_user_id))
            .collect()
    }

    /// `stream:attach` (ADR 0004 §4–5): checks, then asks the room's bridge for a stream device on the ghost.
    pub async fn open_stream_lease(
        &self,
        key: &CompanionKey,
        room_id: Option<&str>,
        discord_user_id: u64,
    ) -> Result<Grant, Refusal> {
        let kind = "stream:attach";
        if !key.allows(kind) {
            return Err(Refusal::new(403, "kind_not_allowed", format!("this key may not request '{kind}'")));
        }
        let room = match (&key.scope, room_id) {
            (KeyScope::Room(r), None) => r.clone(),
            (KeyScope::Room(r), Some(req)) if req == r => r.clone(),
            (KeyScope::Room(_), Some(_)) => return Err(Refusal::new(403, "wrong_room", "this key is scoped to a different room")),
            (KeyScope::SelfScope, Some(req)) => req.to_string(),
            (KeyScope::SelfScope, None) => return Err(Refusal::new(400, "bad_request", "a self-scoped key must name the room_id")),
        };
        self.admit(key, Instant::now())?;
        let Some(board) = self.links.registry.board_for_room(&room) else {
            return Err(Refusal::new(503, "bridge_not_running", "this room's bridge is not running"));
        };
        if board.paired_half_present(discord_user_id) {
            return Err(Refusal::new(409, "represented_by_self", "this person is in the call as themselves; their stream belongs on their own Matrix account"));
        }
        let Some(member) = board.represented().into_iter().find(|m| m.discord_user_id == discord_user_id) else {
            return Err(Refusal::new(409, "not_represented", "this Discord member has no ghost in the call right now"));
        };
        if board.matrix_listeners() == 0 {
            return Err(Refusal::new(409, "call_inactive", "nobody is in the Matrix call"));
        }
        let own = matches!(key.scope, KeyScope::SelfScope)
            && self.links.pairings.matrix_for_discord(discord_user_id).as_deref() == Some(key.issuer.as_str());
        let basis = if own {
            "own_pairing"
        } else if matches!(key.scope, KeyScope::Room(_)) && self.keys.has_grant(discord_user_id, &key.id) {
            "personal_grant"
        } else if matches!(key.scope, KeyScope::Room(_)) && member.streaming {
            "streaming_flag"
        } else {
            return Err(Refusal::new(409, "not_consented", "the person is not streaming on Discord, has not allowed this key (/bridge companion allow), and this is not their own key"));
        };
        {
            let st = self.state();
            let in_room: Vec<&Lease> = st.leases.values().filter(|l| l.shared.room_id == room && l.shared.stream.is_some()).collect();
            if in_room.iter().any(|l| l.shared.stream.as_ref().is_some_and(|s| s.discord_user_id == discord_user_id)) {
                return Err(Refusal::new(409, "already_attached", "a stream is already attached to this person"));
            }
            if in_room.len() >= MAX_STREAMS_PER_ROOM {
                return Err(Refusal::new(409, "cap", format!("this room already shows {MAX_STREAMS_PER_ROOM} attached streams")));
            }
        }
        let Some(provider) = stream::provider_for(&room) else {
            return Err(Refusal::new(503, "bridge_not_running", "this room's bridge is not running"));
        };
        let cancel = CancellationToken::new();
        let grant = match tokio::time::timeout(ATTACH_TIMEOUT, provider.attach(discord_user_id, cancel.clone())).await {
            Ok(Ok(Ok(g))) => g,
            Ok(Ok(Err(reason))) => {
                cancel.cancel();
                return Err(Refusal::new(if reason == "not_represented" || reason == "already_attached" { 409 } else { 503 }, reason, "the bridge could not attach a stream device"));
            }
            Ok(Err(_)) => {
                cancel.cancel();
                return Err(Refusal::new(503, "bridge_not_running", "this room's bridge stopped"));
            }
            Err(_) => {
                cancel.cancel();
                return Err(Refusal::new(503, "timeout", "the bridge did not attach a stream device in time"));
            }
        };
        let now = Instant::now();
        let ttl = Duration::from_secs(self.cfg.max_lease_hours * 3600);
        let shared = Arc::new(LeaseShared {
            id: token::lease_id(),
            kind: kind.to_string(),
            room_id: room.clone(),
            issuer: key.issuer.clone(),
            expires_at: AtomicU64::new(unix_now() + ttl.as_secs()),
            ended: Mutex::new(None),
            notify: Notify::new(),
            stream: Some(StreamAttachment { discord_user_id, basis, grant, cancel }),
        });
        self.state().leases.insert(shared.id.clone(), Lease {
            key_id: key.id.clone(),
            shared: shared.clone(),
            deadline: now + ttl,
            streaming: false,
            detached_since: now,
            flag_off_since: None,
            inactive_since: None,
        });
        self.audit(&key.id, "lease_open", serde_json::json!({
            "lease_id": shared.id, "kind": kind, "room_id": room, "discord_user_id": discord_user_id.to_string(),
            "ghost": shared.stream.as_ref().map(|s| s.grant.ghost_mxid.clone()), "consent": basis,
        }));
        tracing::info!(key_id = %key.id, lease_id = %shared.id, discord_user_id, consent = basis, "companion stream attached");
        Ok(Grant { shared })
    }

    /// The companion reports its track is published: post the stream device's membership.
    pub fn stream_live(&self, key: &CompanionKey, lease_id: &str) -> Result<(), Refusal> {
        let Some(l) = self.lease(key, lease_id) else { return Err(Refusal::new(404, "no_lease", "no such lease")) };
        let Some(s) = l.stream.as_ref() else { return Err(Refusal::new(400, "not_a_stream", "this lease has no stream")) };
        s.grant.live.notify_one();
        self.audit(&key.id, "lease_live", serde_json::json!({ "lease_id": lease_id }));
        Ok(())
    }

    /// Housekeeping loop; on shutdown ends every lease so open streams say why.
    pub async fn run_sweeper(self: Arc<Self>, shutdown: CancellationToken) {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        let mut disallowed = stream::disallowed_publications();
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tick.tick() => self.sweep(Instant::now()),
                r = disallowed.recv() => match r {
                    Ok(identity) => self.revoke_identity(&identity),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                },
            }
        }
        self.end_where(|_| true, "shutdown");
    }
}

/// Held by an open event stream; detaching starts the lease's grace.
pub struct StreamGuard {
    hub: Arc<CompanionHub>,
    lease_id: String,
    pub shared: Arc<LeaseShared>,
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        self.hub.detach_stream(&self.lease_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hub(cfg: CompanionsConfig) -> (Arc<CompanionHub>, LinkShared) {
        let links = LinkShared::in_memory();
        let dir = std::env::temp_dir().join(format!("nvb-hub-{}-{}", std::process::id(), token::lease_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let h = CompanionHub::with_store(
            cfg,
            Arc::new(CompanionKeys::in_memory()),
            links.clone(),
            dir.join("audit.jsonl").to_string_lossy().into_owned(),
        );
        (h, links)
    }

    fn room_key(h: &CompanionHub) -> (CompanionKey, String) {
        h.create_key("@mod:hs", KeyScope::Room("!r:hs".into()), &["observe".into()], "test").unwrap()
    }

    #[test]
    fn authentication_accepts_only_the_minted_token() {
        let (h, _) = hub(CompanionsConfig::default());
        let (k, tok) = room_key(&h);
        assert_eq!(h.authenticate(Some(&tok), "ip").unwrap().id, k.id);
        let wrong = format!("nvbck_{}.nope", k.id);
        assert_eq!(h.authenticate(Some(&wrong), "ip").unwrap_err().status, 401);
        assert_eq!(h.authenticate(None, "ip").unwrap_err().status, 401);
    }

    #[test]
    fn failed_authentications_are_rate_limited_per_client() {
        let (h, _) = hub(CompanionsConfig { auth_failures_per_minute: 3, ..Default::default() });
        let (_, tok) = room_key(&h);
        for _ in 0..3 {
            assert_eq!(h.authenticate(Some("nvbck_x.y"), "bad").unwrap_err().status, 401);
        }
        assert_eq!(h.authenticate(Some(&tok), "bad").unwrap_err().status, 429);
        // Another client is unaffected.
        assert!(h.authenticate(Some(&tok), "good").is_ok());
    }

    #[test]
    fn self_keys_die_with_the_last_pairing() {
        let (h, links) = hub(CompanionsConfig::default());
        links.pairings.add("@me:hs", 9, 0).unwrap();
        let (_, tok) = h.create_key("@me:hs", KeyScope::SelfScope, &["listen:self".into()], "").unwrap();
        assert!(h.authenticate(Some(&tok), "ip").is_ok());
        links.pairings.remove("@me:hs", 9).unwrap();
        assert_eq!(h.authenticate(Some(&tok), "ip").unwrap_err().status, 401);
    }

    #[test]
    fn lease_requests_are_checked() {
        let (h, _) = hub(CompanionsConfig { max_leases_per_key: 2, ..Default::default() });
        let (k, _) = room_key(&h);
        assert_eq!(h.open_lease(&k, "bogus", None).unwrap_err().reason, "unknown_kind");
        assert_eq!(h.open_lease(&k, "stream:attach", None).unwrap_err().reason, "kind_not_allowed");
        assert_eq!(h.open_lease(&k, "observe", Some("!other:hs")).unwrap_err().reason, "wrong_room");
        let g = h.open_lease(&k, "observe", None).unwrap();
        assert_eq!(g.shared.room_id, "!r:hs");
        h.open_lease(&k, "observe", Some("!r:hs")).unwrap();
        assert_eq!(h.open_lease(&k, "observe", None).unwrap_err().reason, "cap");
        assert_eq!(h.lease_count(&k.id), 2);

        let (k2, _) = h.create_key("@mod:hs", KeyScope::Room("!r:hs".into()), &["stream:standalone".into()], "").unwrap();
        assert_eq!(h.open_lease(&k2, "stream:standalone", None).unwrap_err().reason, "kind_unavailable");
    }

    #[test]
    fn lease_request_rate_is_limited_per_key() {
        let (h, _) = hub(CompanionsConfig { lease_requests_per_minute: 2, ..Default::default() });
        let (k, _) = room_key(&h);
        let a = h.open_lease(&k, "observe", None).unwrap();
        h.release(&k, &a.shared.id).unwrap();
        let b = h.open_lease(&k, "observe", None).unwrap();
        h.release(&k, &b.shared.id).unwrap();
        assert_eq!(h.open_lease(&k, "observe", None).unwrap_err().status, 429);
    }

    #[test]
    fn leases_end_on_grace_expiry_revocation_and_release() {
        let (h, _) = hub(CompanionsConfig { sse_grace_secs: 30, ..Default::default() });
        let (k, _) = room_key(&h);
        let t0 = Instant::now();

        // Never connected: ends after the grace.
        let idle = h.open_lease(&k, "observe", None).unwrap().shared;
        h.sweep(t0 + Duration::from_secs(5));
        assert!(idle.ended().is_none());
        h.sweep(t0 + Duration::from_secs(31));
        assert_eq!(idle.ended().as_deref(), Some("disconnected"));

        // Connected: survives past the grace; a second stream is refused;
        // dropping the stream starts the grace again.
        let live = h.open_lease(&k, "observe", None).unwrap().shared;
        let guard = h.attach_stream(&k, &live.id).unwrap();
        assert_eq!(h.attach_stream(&k, &live.id).err().unwrap().reason, "already_connected");
        h.sweep(Instant::now() + Duration::from_secs(60));
        assert!(live.ended().is_none());
        drop(guard);
        h.sweep(Instant::now() + Duration::from_secs(31));
        assert_eq!(live.ended().as_deref(), Some("disconnected"));

        // Past max_lease_hours unless renewed.
        let old = h.open_lease(&k, "observe", None).unwrap().shared;
        let _g = h.attach_stream(&k, &old.id).unwrap();
        h.sweep(Instant::now() + Duration::from_secs(6 * 3600 + 1));
        assert_eq!(old.ended().as_deref(), Some("expired"));

        // Released by its key only; revoking the key ends the rest.
        let mine = h.open_lease(&k, "observe", None).unwrap().shared;
        let (other, _) = room_key(&h);
        assert_eq!(h.release(&other, &mine.id).unwrap_err().status, 404);
        let _g2 = h.attach_stream(&k, &mine.id).unwrap();
        h.revoke_key(&k.id, "@mod:hs").unwrap();
        assert_eq!(mine.ended().as_deref(), Some("key_revoked"));
        assert!(h.get_key(&k.id).is_none());
    }

    #[test]
    fn renewing_moves_the_expiry() {
        let (h, _) = hub(CompanionsConfig::default());
        let (k, _) = room_key(&h);
        let l = h.open_lease(&k, "observe", None).unwrap().shared;
        let before = l.expires_at();
        let after = h.renew(&k, &l.id).unwrap();
        assert!(after >= before);
        assert_eq!(l.expires_at(), after);
    }

    #[test]
    fn unlinking_a_room_revokes_its_keys() {
        let (h, _) = hub(CompanionsConfig::default());
        let (k, _) = room_key(&h);
        let l = h.open_lease(&k, "observe", None).unwrap().shared;
        assert_eq!(h.revoke_room("!r:hs", "@op:hs").unwrap(), 1);
        assert_eq!(l.ended().as_deref(), Some("room_unlinked"));
        assert!(h.list_keys().is_empty());
    }

    // ── stream:attach ───────────────────────────────────────────────────────────

    struct FakeProvider {
        cancels: Mutex<Vec<CancellationToken>>,
    }
    impl crate::stream::StreamProvider for FakeProvider {
        fn attach(&self, uid: u64, cancel: CancellationToken) -> crate::stream::AttachReply {
            self.cancels.lock().unwrap().push(cancel);
            let (tx, rx) = tokio::sync::oneshot::channel();
            let (_ttx, trx) = tokio::sync::watch::channel(Some(crate::stream::StreamTransport {
                sfu_url: "wss://sfu".into(), jwt: "j".into(), identity: format!("@g_{uid}:hs:S{uid}"), fetched_at: 1,
            }));
            // Keep the sender alive inside the grant's lifetime by leaking it into the test's provider.
            std::mem::forget(_ttx);
            let _ = tx.send(Ok(crate::stream::StreamGrant {
                ghost_mxid: format!("@g_{uid}:hs"), device_id: format!("S{uid}"), e2ee: true, key: Some([7; 16]),
                transport: trx, live: Arc::new(Notify::new()),
            }));
            rx
        }
    }

    #[tokio::test]
    async fn stream_leases_check_consent_and_end_with_it() {
        let room = "!stream-hub-test:hs";
        let (h, links) = hub(CompanionsConfig::default());
        let board = links.board_for(room, &[], &[]);
        links.registry.register("b", board.clone());
        let fake = Arc::new(FakeProvider { cancels: Mutex::new(Vec::new()) });
        let _reg = crate::stream::register_provider(room, fake.clone());
        let (k, _) = h.create_key("@mod:hs", KeyScope::Room(room.into()), &["stream:attach".into()], "").unwrap();

        // Not represented, then no listener, then not streaming.
        assert_eq!(h.open_stream_lease(&k, None, 7).await.unwrap_err().reason, "not_represented");
        board.set_discord_present(7, true);
        board.note_ghost(7, Some("Seven"));
        assert_eq!(h.open_stream_lease(&k, None, 7).await.unwrap_err().reason, "call_inactive");
        board.set_matrix_present("@viewer:hs", true);
        assert_eq!(h.open_stream_lease(&k, None, 7).await.unwrap_err().reason, "not_consented");
        board.set_discord_streaming(7, true);
        let g = h.open_stream_lease(&k, None, 7).await.unwrap();
        assert_eq!(g.shared.stream.as_ref().unwrap().basis, "streaming_flag");
        assert_eq!(h.attached_in(room).into_iter().collect::<Vec<_>>(), vec![7]);
        assert_eq!(h.open_stream_lease(&k, None, 7).await.unwrap_err().reason, "already_attached");
        h.stream_live(&k, &g.shared.id).unwrap();

        // The flag clears: the stream survives the grace, then ends and tears the device down.
        let _sse = h.attach_stream(&k, &g.shared.id).unwrap();
        board.set_discord_streaming(7, false);
        let t0 = Instant::now();
        h.sweep(t0);
        h.sweep(t0 + FLAG_GRACE - Duration::from_secs(1));
        assert!(g.shared.ended().is_none());
        h.sweep(t0 + FLAG_GRACE + Duration::from_secs(1));
        assert_eq!(g.shared.ended().as_deref(), Some("consent_withdrawn"));
        assert!(fake.cancels.lock().unwrap()[0].is_cancelled());
    }

    #[tokio::test]
    async fn a_personal_grant_is_consent_until_withdrawn() {
        let room = "!stream-hub-test3:hs";
        let (h, links) = hub(CompanionsConfig::default());
        let board = links.board_for(room, &[], &[]);
        links.registry.register("b", board.clone());
        let _reg = crate::stream::register_provider(room, Arc::new(FakeProvider { cancels: Mutex::new(Vec::new()) }));
        let (k, _) = h.create_key("@mod:hs", KeyScope::Room(room.into()), &["stream:attach".into()], "").unwrap();
        board.set_discord_present(5, true);
        board.note_ghost(5, Some("Five"));
        board.set_matrix_present("@viewer:hs", true);
        assert_eq!(h.open_stream_lease(&k, None, 5).await.unwrap_err().reason, "not_consented");
        h.keys.allow(5, &k.id, 0).unwrap();
        let g = h.open_stream_lease(&k, None, 5).await.unwrap();
        assert_eq!(g.shared.stream.as_ref().unwrap().basis, "personal_grant");
        let _sse = h.attach_stream(&k, &g.shared.id).unwrap();
        h.sweep(Instant::now());
        assert!(g.shared.ended().is_none());
        h.keys.deny(5, &k.id).unwrap();
        h.sweep(Instant::now());
        assert_eq!(g.shared.ended().as_deref(), Some("consent_withdrawn"));
    }

    #[tokio::test]
    async fn own_pairing_is_consent_and_paired_presence_is_refused() {
        let room = "!stream-hub-test2:hs";
        let (h, links) = hub(CompanionsConfig::default());
        links.pairings.add("@me:hs", 9, 0).unwrap();
        let board = links.board_for(room, &[], &[]);
        links.registry.register("b", board.clone());
        let _reg = crate::stream::register_provider(room, Arc::new(FakeProvider { cancels: Mutex::new(Vec::new()) }));
        let (k, _) = h.create_key("@me:hs", KeyScope::SelfScope, &["stream:attach".into()], "").unwrap();
        board.set_discord_present(9, true);
        board.note_ghost(9, Some("Me"));
        board.set_matrix_present("@viewer:hs", true);
        assert_eq!(h.open_stream_lease(&k, None, 9).await.unwrap_err().reason, "bad_request");
        let g = h.open_stream_lease(&k, Some(room), 9).await.unwrap(); // not streaming: own pairing suffices
        assert_eq!(g.shared.stream.as_ref().unwrap().basis, "own_pairing");
        // Publishing a microphone under the stream device revokes the lease.
        h.revoke_identity("@g_9:hs:S9");
        assert_eq!(g.shared.ended().as_deref(), Some("disallowed_source"));
        // Once the person is in the call themselves, their stream belongs on their own account.
        board.set_matrix_present("@me:hs", true);
        assert_eq!(h.open_stream_lease(&k, Some(room), 9).await.unwrap_err().reason, "represented_by_self");
    }
}
