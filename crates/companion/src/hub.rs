//! The companion hub: key issuance/revocation, authentication, rate limits and
//! the in-memory lease registry (ADR 0004 §3, §7). Leases are deliberately not
//! persisted: a bridge restart drops every lease and companions re-request.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use nvb_config::companions::{CompanionKey, CompanionKeys, KeyScope, PendingApproval};
use nvb_config::links::LinkShared;
use nvb_config::{CompanionsConfig, ConfigError};
use tokio::sync::{mpsc, Notify, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::federation::{AddressPolicy, FederationVerifier, OpenIdVerifier, VerifyError};
use crate::selfcall::{self, OwnState, RoomFacts};
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

/// How long a room lookup (joined member, joined server) may take.
const FACTS_TIMEOUT: Duration = Duration::from_secs(5);
/// How long one enrollment's OpenID verification (discovery + userinfo) may take.
const VERIFY_TIMEOUT: Duration = Duration::from_secs(20);
/// Hard ceiling on concurrent `/self/events` streams.
const MAX_SELF_STREAMS: usize = 512;
/// How long an enrollment waits for its owner's confirmation in the management DM (ADR 0005 §2).
pub const APPROVAL_TTL: Duration = Duration::from_secs(600);
/// Wrong confirmation codes a member may send before their pending enrollments are dropped.
const MAX_APPROVE_FAILURES: u32 = 5;
/// Enrollment verifications (homeserver lookups) in flight at once, across all clients.
const MAX_VERIFICATIONS_IN_FLIGHT: usize = 16;
/// Distinct client addresses a rate-limit map tracks between sweeps; a new address beyond it is refused.
const MAX_TRACKED_CLIENTS: usize = 50_000;
/// How often the sweeper prunes rate-limit windows and expired enrollments.
const HOUSEKEEPING_EVERY: Duration = Duration::from_secs(30);

/// Asks the management bot to DM a member the confirmation code of a new enrollment (ADR 0005 §2). The code is
/// the proof that the enrollment came from the account itself, so it only ever goes to that DM.
pub struct ApprovalRequest {
    pub mxid: String,
    pub device_id: String,
    pub key_id: String,
    pub room_id: String,
    pub code: String,
    pub expires_in: Duration,
}

/// Why a `companion key approve <code>` did not activate anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApproveError {
    /// The sender has no enrollment waiting (or it expired).
    NothingPending,
    /// The reply came from a room other than the DM the code was sent to.
    WrongRoom,
    /// The code matches none of the sender's pending enrollments; `dropped` once too many wrong codes ended them.
    WrongCode { dropped: bool },
    Storage(String),
}

/// The rate-limit key of a client address: an IPv6 client is keyed by its /64 (one subscriber's whole prefix);
/// anything else as given.
pub fn rate_key(client: &str) -> String {
    match client.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V6(v6)) if v6.to_ipv4_mapped().is_none() => {
            let s = v6.segments();
            format!("{:x}:{:x}:{:x}:{:x}::/64", s[0], s[1], s[2], s[3])
        }
        Ok(std::net::IpAddr::V6(v6)) => v6.to_ipv4_mapped().map(|v4| v4.to_string()).unwrap_or_default(),
        _ => client.to_string(),
    }
}

/// A `/self/enroll` request (ADR 0005). The token is used once and dropped with the request.
pub struct EnrollRequest {
    pub room_id: String,
    pub device_id: String,
    pub openid_token: String,
    pub matrix_server_name: String,
}

impl std::fmt::Debug for EnrollRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnrollRequest")
            .field("room_id", &self.room_id)
            .field("device_id", &self.device_id)
            .field("openid_token", &"<redacted>")
            .field("matrix_server_name", &self.matrix_server_name)
            .finish()
    }
}

/// A device id the bridge will bind a key to: what Matrix servers mint, nothing that could split a LiveKit identity
/// (`@user:server:DEVICE`) or a log line.
pub fn valid_device_id(d: &str) -> bool {
    (1..=64).contains(&d.len()) && d.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b'='))
}

fn valid_room_id(r: &str) -> bool {
    r.starts_with('!') && r.len() <= 255 && r.contains(':') && !r.chars().any(char::is_whitespace)
}

/// The server part of an MXID (`@local:server` → `server`).
fn server_of(mxid: &str) -> Option<&str> {
    mxid.strip_prefix('@')?.split_once(':').map(|(_, s)| s).filter(|s| !s.is_empty())
}

async fn ask(rx: tokio::sync::oneshot::Receiver<bool>) -> Option<bool> {
    tokio::time::timeout(FACTS_TIMEOUT, rx).await.ok()?.ok()
}

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
    /// `/self/enroll` attempts per client address (IPv6 per /64).
    enroll_attempts: HashMap<String, VecDeque<Instant>>,
    /// Open `/self/events` streams by device key: (generation, cancel). A new connection for the same key cancels
    /// the old one.
    self_streams: HashMap<String, (u64, CancellationToken)>,
    self_stream_gen: u64,
    /// `/self/events` connects per device key.
    self_connects: HashMap<String, VecDeque<Instant>>,
    /// Wrong confirmation codes per member since their last success.
    approve_failures: HashMap<String, u32>,
    last_housekeeping: Option<Instant>,
}

pub struct CompanionHub {
    cfg: CompanionsConfig,
    keys: Arc<CompanionKeys>,
    links: LinkShared,
    audit_path: String,
    state: Mutex<HubState>,
    verifier: Arc<dyn OpenIdVerifier>,
    /// Where enrollment confirmations go (the management bot); `None` until it is up.
    approvals: Mutex<Option<mpsc::UnboundedSender<ApprovalRequest>>>,
    /// Bounds concurrent enrollment verifications.
    verify_slots: Arc<Semaphore>,
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
        // Bridges advertise the API in their rooms only while self enrollment is served.
        selfcall::set_public_api_base(cfg.public_url.as_deref().filter(|_| cfg.enabled && cfg.self_enroll));
        Ok(Self::with_store(cfg, Arc::new(keys), links, audit::audit_path(data_dir)))
    }

    pub fn with_store(
        cfg: CompanionsConfig,
        keys: Arc<CompanionKeys>,
        links: LinkShared,
        audit_path: String,
    ) -> Arc<Self> {
        let verifier = Arc::new(FederationVerifier::new(AddressPolicy { trusted: cfg.trusted_private_servers.clone() }));
        Self::with_verifier(cfg, keys, links, audit_path, verifier)
    }

    /// As [`with_store`](Self::with_store), with the OpenID verifier supplied (tests).
    pub fn with_verifier(
        cfg: CompanionsConfig,
        keys: Arc<CompanionKeys>,
        links: LinkShared,
        audit_path: String,
        verifier: Arc<dyn OpenIdVerifier>,
    ) -> Arc<Self> {
        let hub = Arc::new(Self {
            cfg,
            keys,
            links,
            audit_path,
            state: Mutex::new(HubState::default()),
            verifier,
            approvals: Mutex::new(None),
            verify_slots: Arc::new(Semaphore::new(MAX_VERIFICATIONS_IN_FLIGHT)),
        });
        hub.sync_listen_only();
        hub
    }

    /// Publish the enrolled (confirmed) listen-only devices for the reaper's exclusion (ADR 0005). A pending
    /// enrollment excludes nothing: an unconfirmed device must not hide anyone from the reaper.
    fn sync_listen_only(&self) {
        selfcall::set_listen_only_devices(
            self.keys
                .list()
                .into_iter()
                .filter(|k| k.is_device_key() && k.is_active())
                .filter_map(|k| Some((k.issuer, k.device?))),
        );
    }

    /// Where enrollment confirmations are delivered: the management bot installs its DM sender at startup.
    /// Without it enrollment answers 503 (nothing could confirm it).
    pub fn install_approval_sink(&self, tx: mpsc::UnboundedSender<ApprovalRequest>) {
        *self.approvals.lock().unwrap_or_else(PoisonError::into_inner) = Some(tx);
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
            device: None,
            pending: None,
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

    /// Everything live under keys that are gone: their leases, their `/self/events` streams, their rate-limit
    /// windows; and the reaper's exclusion when a device key was among them.
    fn forget_keys(&self, gone: &[CompanionKey]) {
        if gone.is_empty() {
            return;
        }
        for k in gone {
            self.end_where(|l| l.key_id == k.id, "key_revoked");
        }
        {
            let mut st = self.state();
            for k in gone {
                if let Some((_, cancel)) = st.self_streams.remove(&k.id) {
                    cancel.cancel();
                }
                st.self_connects.remove(&k.id);
                st.lease_requests.remove(&k.id);
            }
        }
        if gone.iter().any(CompanionKey::is_device_key) {
            self.sync_listen_only();
        }
    }

    /// Live leases held under a key.
    pub fn lease_count(&self, key_id: &str) -> usize {
        self.state().leases.values().filter(|l| l.key_id == key_id).count()
    }

    /// Delete a key and end its leases at once.
    pub fn revoke_key(&self, id: &str, actor: &str) -> Result<Option<CompanionKey>, ConfigError> {
        let removed = self.keys.remove(id)?;
        if let Some(k) = &removed {
            self.forget_keys(std::slice::from_ref(k));
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
            let client = rate_key(client);
            let q = st.auth_failures.entry(client.clone()).or_default();
            if window_count(q, now) >= self.cfg.auth_failures_per_minute as usize {
                return Err(Refusal::new(429, "rate_limited", "too many failed authentications; retry later"));
            }
            if q.is_empty() {
                st.auth_failures.remove(&client);
            }
        }
        let key = bearer.and_then(token::parse).and_then(|(id, secret)| {
            let k = self.keys.get(id)?;
            token::secret_matches(secret, &k.secret_sha256).then_some(k)
        });
        let key = key.filter(|k| match &k.scope {
            // A device-bound self key (ADR 0005) needs no pairing; a pairing-scoped one lives only as long as its
            // issuer has a pairing.
            KeyScope::SelfScope => k.is_device_key() || !self.links.pairings.discords_for_matrix(&k.issuer).is_empty(),
            KeyScope::Room(_) => true,
        });
        match key {
            Some(k) => Ok(k),
            None => {
                let mut st = self.state();
                let client = rate_key(client);
                // Bounded against address churn; the sweeper prunes idle windows.
                if st.auth_failures.contains_key(&client) || st.auth_failures.len() < MAX_TRACKED_CLIENTS {
                    st.auth_failures.entry(client).or_default().push_back(now);
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
        let due = {
            let mut st = self.state();
            let due = st.last_housekeeping.is_none_or(|t| now.saturating_duration_since(t) >= HOUSEKEEPING_EVERY);
            if due {
                st.last_housekeeping = Some(now);
            }
            due
        };
        if due {
            self.housekeeping(now);
        }
    }

    /// Periodic pruning (instead of on the request path): expired enrollments, and rate-limit windows that are
    /// empty or belong to keys that no longer exist.
    fn housekeeping(&self, now: Instant) {
        match self.keys.remove_expired_pending(unix_now() as i64) {
            Ok(gone) if !gone.is_empty() => {
                self.forget_keys(&gone);
                for k in &gone {
                    self.audit(&k.issuer, "self_enroll_expired", serde_json::json!({ "key_id": k.id, "device_id": k.device }));
                }
            }
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "cannot drop expired enrollments"),
        }
        let pending: std::collections::HashSet<String> =
            self.keys.list().into_iter().filter(|k| !k.is_active()).map(|k| k.issuer).collect();
        let mut st = self.state();
        st.auth_failures.retain(|_, q| window_count(q, now) > 0);
        st.enroll_attempts.retain(|_, q| window_count(q, now) > 0);
        st.lease_requests.retain(|id, q| window_count(q, now) > 0 && self.keys.get(id).is_some());
        st.self_connects.retain(|id, q| window_count(q, now) > 0 && self.keys.get(id).is_some());
        st.approve_failures.retain(|mxid, _| pending.contains(mxid));
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

    // ── your own call data (ADR 0005) ───────────────────────────────────────────

    /// `POST /self/enroll`: verify an OpenID token once and return a device-bound self key (token shown once) in
    /// the **pending** state: it opens nothing until its owner confirms it from their own account, by replying the
    /// confirmation code the management bot sends to their DM ([`approve_device_key`](Self::approve_device_key)).
    /// OpenID tokens are not audience-bound — a call's SFU token service, widgets and integration managers all
    /// receive them — so the token alone proves nothing more than "someone held a token of this account".
    ///
    /// Checks, in order: per-address rate limit (IPv6 per /64, before any work), input shape, a management bot to
    /// confirm with, a free verification slot, the room is bridged here and has a joined member on the named server
    /// (before any network call), the token on that server's own federation API (address policy inside), `sub` on
    /// that server and joined. The token is never logged or kept.
    pub async fn enroll(&self, req: &EnrollRequest, client: &str) -> Result<(CompanionKey, String), Refusal> {
        if !self.cfg.self_enroll {
            return Err(Refusal::new(404, "not_found", "self enrollment is not offered on this instance"));
        }
        let now = Instant::now();
        {
            let mut st = self.state();
            let client = rate_key(client);
            if !st.enroll_attempts.contains_key(&client) && st.enroll_attempts.len() >= MAX_TRACKED_CLIENTS {
                return Err(Refusal::new(429, "rate_limited", "too many enrollment attempts; retry later"));
            }
            let q = st.enroll_attempts.entry(client).or_default();
            if window_count(q, now) >= self.cfg.enrollments_per_minute as usize {
                return Err(Refusal::new(429, "rate_limited", "too many enrollment attempts; retry later"));
            }
            q.push_back(now);
        }
        if !valid_room_id(&req.room_id) || !valid_device_id(&req.device_id) {
            return Err(Refusal::new(400, "bad_request", "room_id or device_id is malformed"));
        }
        if req.openid_token.is_empty() || req.openid_token.len() > 1024 {
            return Err(Refusal::new(400, "bad_request", "openid_token is missing or too long"));
        }
        let server = req.matrix_server_name.to_ascii_lowercase();
        if crate::federation::parse_server_name(&server).is_none() {
            return Err(Refusal::new(400, "bad_request", "matrix_server_name is not a server name"));
        }
        let Some(approvals) = self.approvals.lock().unwrap_or_else(PoisonError::into_inner).clone() else {
            return Err(Refusal::new(503, "approval_unavailable", "this instance has no management bot to confirm enrollments with"));
        };
        let Ok(_slot) = self.verify_slots.clone().try_acquire_owned() else {
            return Err(Refusal::new(503, "busy", "too many enrollments are being verified right now; retry shortly"));
        };
        // Never contact a server that has no joined member in a room this bridge serves.
        let not_member = || Refusal::new(401, "unauthorized", "not a member of a room this bridge serves");
        let facts = selfcall::room_facts(&req.room_id).ok_or_else(not_member)?;
        if ask(facts.has_joined_server(&server)).await != Some(true) {
            return Err(not_member());
        }
        let sub = match tokio::time::timeout(VERIFY_TIMEOUT, self.verifier.userinfo(&server, &req.openid_token)).await {
            Ok(Ok(sub)) => sub,
            Ok(Err(VerifyError::TokenRejected)) => return Err(Refusal::new(401, "invalid_token", "the homeserver did not accept the OpenID token")),
            Ok(Err(VerifyError::AddressRefused)) => return Err(Refusal::new(403, "address_refused", "the homeserver resolves to an address this bridge will not contact")),
            Ok(Err(VerifyError::BadServerName)) => return Err(Refusal::new(400, "bad_request", "matrix_server_name is not a server name")),
            Ok(Err(e @ VerifyError::Unreachable(_))) => {
                tracing::info!(server_name = %server, error = %e, "self enrollment: homeserver unreachable");
                return Err(Refusal::new(502, "homeserver_unreachable", "the homeserver could not be reached"));
            }
            Err(_) => return Err(Refusal::new(504, "homeserver_unreachable", "the homeserver did not answer in time")),
        };
        // The answering server vouches only for its own users.
        if server_of(&sub).map(str::to_ascii_lowercase).as_deref() != Some(server.as_str()) {
            tracing::warn!(server_name = %server, "self enrollment: userinfo named a user of another server — refused");
            return Err(Refusal::new(401, "unauthorized", "the token's user is not on the named server"));
        }
        if ask(facts.is_joined(&sub)).await != Some(true) {
            return Err(not_member());
        }
        let minted = token::mint();
        let code = token::confirmation_code();
        let key = CompanionKey {
            id: minted.id,
            secret_sha256: minted.secret_sha256,
            issuer: sub.clone(),
            scope: KeyScope::SelfScope,
            kinds: vec![nvb_config::companions::SELF_CALL_KIND.to_string()],
            label: format!("listen-only device {}", req.device_id),
            created: unix_now() as i64,
            device: Some(req.device_id.clone()),
            pending: Some(PendingApproval {
                code_sha256: token::sha256_hex(&code),
                expires: (unix_now() + APPROVAL_TTL.as_secs()) as i64,
                dm_room: None,
            }),
        };
        let replaced = self
            .keys
            .add_pending_device_key(key.clone())
            .map_err(|e| Refusal::new(500, "storage", format!("cannot write the companions file: {e}")))?;
        self.forget_keys(&replaced);
        let ask_owner = ApprovalRequest {
            mxid: sub.clone(),
            device_id: req.device_id.clone(),
            key_id: key.id.clone(),
            room_id: req.room_id.clone(),
            code,
            expires_in: APPROVAL_TTL,
        };
        if approvals.send(ask_owner).is_err() {
            let _ = self.keys.remove(&key.id);
            return Err(Refusal::new(503, "approval_unavailable", "the management bot is not running"));
        }
        self.audit(&sub, "self_enroll", serde_json::json!({
            "key_id": key.id, "device_id": req.device_id, "room_id": req.room_id, "status": "pending_approval",
            "replaced": replaced.iter().map(|k| k.id.clone()).collect::<Vec<_>>(),
        }));
        tracing::info!(mxid = %sub, device_id = %req.device_id, key_id = %key.id, "listen-only device enrolled — waiting for its owner's confirmation");
        Ok((key, minted.token))
    }

    /// Record the DM a pending enrollment's code was sent to (the management bot, before it sends). False if the
    /// enrollment is gone.
    pub fn set_pending_dm(&self, key_id: &str, dm_room: &str) -> bool {
        self.keys.set_pending_dm(key_id, dm_room).unwrap_or_else(|e| {
            tracing::warn!(key_id, error = %e, "cannot record the confirmation DM of an enrollment");
            false
        })
    }

    /// Does `mxid` have an enrollment waiting for confirmation? (Lets the management bot hear `companion key
    /// approve` from any member, self-serve tier or not.)
    pub fn has_pending_approval(&self, mxid: &str) -> bool {
        let now = unix_now() as i64;
        self.keys.list().iter().any(|k| k.issuer == mxid && k.pending.as_ref().is_some_and(|p| p.expires > now))
    }

    /// `companion key approve <code>` from `sender` in room `dm_room`: activate the sender's pending enrollment
    /// whose code was sent to that very DM. Only the account itself can read and answer there — the proof a
    /// replayed OpenID token cannot give. Too many wrong codes drop the sender's pending enrollments.
    pub fn approve_device_key(&self, sender: &str, dm_room: &str, code: &str) -> Result<CompanionKey, ApproveError> {
        let now = unix_now() as i64;
        let pending: Vec<CompanionKey> = self
            .keys
            .list()
            .into_iter()
            .filter(|k| k.issuer == sender && k.is_device_key() && k.pending.as_ref().is_some_and(|p| p.expires > now))
            .collect();
        if pending.is_empty() {
            return Err(ApproveError::NothingPending);
        }
        let here: Vec<&CompanionKey> =
            pending.iter().filter(|k| k.pending.as_ref().and_then(|p| p.dm_room.as_deref()) == Some(dm_room)).collect();
        if here.is_empty() {
            return Err(ApproveError::WrongRoom);
        }
        let code = token::normalize_code(code);
        let hit = here.iter().find(|k| k.pending.as_ref().is_some_and(|p| token::secret_matches(&code, &p.code_sha256)));
        let Some(hit) = hit else {
            let failures = {
                let mut st = self.state();
                let n = st.approve_failures.entry(sender.to_string()).or_default();
                *n += 1;
                *n
            };
            if failures < MAX_APPROVE_FAILURES {
                return Err(ApproveError::WrongCode { dropped: false });
            }
            self.state().approve_failures.remove(sender);
            for k in &pending {
                let _ = self.keys.remove(&k.id);
            }
            self.forget_keys(&pending);
            self.audit(sender, "self_enroll_dropped", serde_json::json!({
                "key_ids": pending.iter().map(|k| k.id.clone()).collect::<Vec<_>>(), "reason": "wrong_codes",
            }));
            return Err(ApproveError::WrongCode { dropped: true });
        };
        let (key, replaced) = match self.keys.activate_device_key(&hit.id) {
            Ok(Some(v)) => v,
            Ok(None) => return Err(ApproveError::NothingPending),
            Err(e) => return Err(ApproveError::Storage(e.to_string())),
        };
        self.state().approve_failures.remove(sender);
        self.forget_keys(&replaced);
        self.sync_listen_only();
        self.audit(sender, "self_enroll_approve", serde_json::json!({
            "key_id": key.id, "device_id": key.device, "dm_room": dm_room,
            "replaced": replaced.iter().map(|k| k.id.clone()).collect::<Vec<_>>(),
        }));
        tracing::info!(mxid = %sender, device_id = ?key.device, key_id = %key.id, replaced = replaced.len(), "listen-only device confirmed by its owner");
        Ok(key)
    }

    /// `DELETE /self/enroll`: the device unlinks itself (pending or active).
    pub fn unenroll(&self, key: &CompanionKey) -> Result<(), Refusal> {
        if !key.is_device_key() {
            return Err(Refusal::new(403, "kind_not_allowed", "only a device key can be unlinked here"));
        }
        let issuer = key.issuer.clone();
        self.revoke_key(&key.id, &issuer)
            .map_err(|e| Refusal::new(500, "storage", format!("cannot write the companions file: {e}")))?;
        Ok(())
    }

    /// Is this device key still enrolled and confirmed? (The events stream ends with `revoked` once it is not.)
    pub fn key_valid(&self, key_id: &str) -> bool {
        self.keys.get(key_id).is_some_and(|k| k.is_active())
    }

    /// Open `GET /self/events` for `room_id`: only for a confirmed device key, in a room this bridge serves, while
    /// the key's owner is a joined member **and has a call membership there**; connects rate-limited. One stream per
    /// device: a new connection ends the previous one (a client that lost its connection silently can reconnect at
    /// once).
    pub async fn open_self_events(self: &Arc<Self>, key: &CompanionKey, room_id: &str) -> Result<SelfStream, Refusal> {
        if !key.is_device_key() || !key.allows(nvb_config::companions::SELF_CALL_KIND) {
            return Err(Refusal::new(403, "kind_not_allowed", "this key is not a device key"));
        }
        if !key.is_active() {
            return Err(Refusal::new(403, "pending_approval", "confirm this device first: reply to the bridge bot's DM with the code it sent you"));
        }
        let now = Instant::now();
        {
            let mut st = self.state();
            let q = st.self_connects.entry(key.id.clone()).or_default();
            if window_count(q, now) >= self.cfg.lease_requests_per_minute as usize {
                return Err(Refusal::new(429, "rate_limited", "too many connects for this key; retry later"));
            }
            q.push_back(now);
        }
        let Some(facts) = selfcall::room_facts(room_id) else {
            return Err(Refusal::new(404, "not_bridged", "this room's bridge is not running here"));
        };
        if ask(facts.is_joined(&key.issuer)).await != Some(true) {
            return Err(Refusal::new(403, "not_a_member", "the key's owner is not a member of this room"));
        }
        let device = key.device.clone().unwrap_or_default();
        let state = self.own_state(&facts, room_id, &key.issuer, &device);
        if !state.in_call {
            return Err(Refusal::new(409, "not_in_call", "the key's owner has no call membership in this room"));
        }
        let mut st = self.state();
        // Revoked or replaced while we were looking.
        if !self.key_valid(&key.id) {
            return Err(Refusal::new(401, "unauthorized", "missing or invalid companion key"));
        }
        if !st.self_streams.contains_key(&key.id) && st.self_streams.len() >= MAX_SELF_STREAMS {
            return Err(Refusal::new(503, "cap", "the bridge holds too many event streams right now"));
        }
        st.self_stream_gen += 1;
        let generation = st.self_stream_gen;
        let cancel = CancellationToken::new();
        let replaced = st.self_streams.insert(key.id.clone(), (generation, cancel.clone()));
        drop(st);
        if let Some((_, old)) = replaced {
            old.cancel();
        }
        self.audit(&key.issuer, "self_events_open", serde_json::json!({ "key_id": key.id, "room_id": room_id, "device_id": device }));
        Ok(SelfStream {
            hub: self.clone(),
            key_id: key.id.clone(),
            issuer: key.issuer.clone(),
            device,
            room_id: room_id.to_string(),
            facts,
            generation,
            cancel,
        })
    }

    /// The requester's own slice of the room's call (the one filter, [`OwnState::for_owner`]).
    pub fn own_state(&self, facts: &Arc<dyn RoomFacts>, room_id: &str, owner: &str, device: &str) -> OwnState {
        let view = facts.view().borrow().clone();
        let paired = self.links.registry.board_for_room(room_id).is_some_and(|b| b.paired_present(owner));
        OwnState::for_owner(&view, room_id, owner, device, paired)
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

/// An open `/self/events` stream; dropping it frees the device's stream slot (unless a newer connection took it).
pub struct SelfStream {
    hub: Arc<CompanionHub>,
    pub key_id: String,
    pub issuer: String,
    pub device: String,
    pub room_id: String,
    pub facts: Arc<dyn RoomFacts>,
    generation: u64,
    /// Cancelled when a newer connection for the same key takes over, or the key is revoked.
    pub cancel: CancellationToken,
}

impl std::fmt::Debug for SelfStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SelfStream").field("key_id", &self.key_id).field("room_id", &self.room_id).finish_non_exhaustive()
    }
}

impl SelfStream {
    pub fn hub(&self) -> &Arc<CompanionHub> {
        &self.hub
    }

    pub fn state(&self) -> OwnState {
        self.hub.own_state(&self.facts, &self.room_id, &self.issuer, &self.device)
    }
}

impl Drop for SelfStream {
    fn drop(&mut self) {
        let mut st = self.hub.state();
        if st.self_streams.get(&self.key_id).is_some_and(|(g, _)| *g == self.generation) {
            st.self_streams.remove(&self.key_id);
        }
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

    // ── your own call data (ADR 0005) ───────────────────────────────────────────

    use crate::federation::VerifyFuture;
    use crate::selfcall::{CallMember, PeerKey, RoomView, Transport};

    /// A homeserver stand-in: token → the `sub` its userinfo answers (or an error).
    struct FakeVerifier(HashMap<String, Result<String, VerifyError>>, Mutex<Vec<String>>);

    impl OpenIdVerifier for FakeVerifier {
        fn userinfo<'a>(&'a self, server_name: &'a str, token: &'a str) -> VerifyFuture<'a> {
            self.1.lock().unwrap().push(server_name.to_string());
            let r = self.0.get(token).cloned().unwrap_or(Err(VerifyError::TokenRejected));
            Box::pin(async move { r })
        }
    }

    struct FakeFacts {
        joined: Vec<String>,
        view: tokio::sync::watch::Sender<Arc<RoomView>>,
    }

    impl RoomFacts for FakeFacts {
        fn is_joined(&self, mxid: &str) -> tokio::sync::oneshot::Receiver<bool> {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let _ = tx.send(self.joined.iter().any(|j| j == mxid));
            rx
        }
        fn has_joined_server(&self, server_name: &str) -> tokio::sync::oneshot::Receiver<bool> {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let _ = tx.send(self.joined.iter().any(|j| server_of(j) == Some(server_name)));
            rx
        }
        fn view(&self) -> tokio::sync::watch::Receiver<Arc<RoomView>> {
            self.view.subscribe()
        }
    }

    type Approvals = tokio::sync::mpsc::UnboundedReceiver<ApprovalRequest>;

    fn self_cfg() -> CompanionsConfig {
        CompanionsConfig { self_enroll: true, ..Default::default() }
    }

    /// A hub serving self enrollment, with a management-bot stand-in collecting the confirmation requests.
    fn self_hub(cfg: CompanionsConfig, tokens: &[(&str, Result<&str, VerifyError>)]) -> (Arc<CompanionHub>, Arc<FakeVerifier>, Approvals) {
        let dir = std::env::temp_dir().join(format!("nvb-hub-{}-{}", std::process::id(), token::lease_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let v = Arc::new(FakeVerifier(
            tokens.iter().map(|(t, r)| (t.to_string(), r.clone().map(str::to_string))).collect(),
            Mutex::new(Vec::new()),
        ));
        let h = CompanionHub::with_verifier(
            cfg,
            Arc::new(CompanionKeys::in_memory()),
            LinkShared::in_memory(),
            dir.join("audit.jsonl").to_string_lossy().into_owned(),
            v.clone(),
        );
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        h.install_approval_sink(tx);
        (h, v, rx)
    }

    /// Play the management bot: take the confirmation request for `key_id`, note its DM, return its code.
    fn dm_code(h: &CompanionHub, rx: &mut Approvals, key_id: &str, dm: &str) -> String {
        let a = rx.try_recv().expect("a confirmation request for the enrollment");
        assert_eq!(a.key_id, key_id);
        assert!(h.set_pending_dm(&a.key_id, dm));
        a.code
    }

    /// Enroll and confirm in one go.
    async fn enroll_active(h: &CompanionHub, rx: &mut Approvals, r: &EnrollRequest, client: &str) -> (CompanionKey, String) {
        let (k, tok) = h.enroll(r, client).await.unwrap();
        let code = dm_code(h, rx, &k.id, "!dm:hs");
        (h.approve_device_key(&k.issuer, "!dm:hs", &code).unwrap(), tok)
    }

    fn member(mxid: &str, dev: &str) -> CallMember {
        let t = Transport { livekit_service_url: "https://lk.hs".into(), livekit_alias: "!r".into() };
        CallMember { mxid: mxid.into(), device_id: dev.into(), active: true, bridge_owned: false, transport: Some(t.clone()), preferred: Some(t) }
    }

    fn pk(sender: &str, dev: &str, key: &str) -> PeerKey {
        PeerKey { sender: sender.into(), device_id: dev.into(), member_id: None, index: 0, key_b64: key.into() }
    }

    fn facts(room: &str, joined: &[&str], view: RoomView) -> (Arc<FakeFacts>, crate::selfcall::RoomRegistration) {
        let f = Arc::new(FakeFacts { joined: joined.iter().map(|s| s.to_string()).collect(), view: tokio::sync::watch::channel(Arc::new(view)).0 });
        let reg = crate::selfcall::register_room(room, f.clone());
        (f, reg)
    }

    fn req(room: &str, dev: &str, token: &str, server: &str) -> EnrollRequest {
        EnrollRequest { room_id: room.into(), device_id: dev.into(), openid_token: token.into(), matrix_server_name: server.into() }
    }

    #[tokio::test]
    async fn enrollment_verifies_the_token_on_the_members_own_server() {
        let room = "!self-enroll-1:hs";
        let (h, v, mut rx) = self_hub(self_cfg(), &[
            ("good", Ok("@me:hs")),
            ("forged", Ok("@victim:other.example")),
            ("outsider", Ok("@stranger:hs")),
            ("private", Err(VerifyError::AddressRefused)),
        ]);
        let (_f, _reg) = facts(room, &["@me:hs", "@victim:other.example"], RoomView::default());

        // A room this bridge doesn't serve, or a server with no member: refused before any network call.
        assert_eq!(h.enroll(&req("!nope:hs", "DEV", "good", "hs"), "ip1").await.unwrap_err().status, 401);
        assert_eq!(h.enroll(&req(room, "DEV", "good", "evil.example"), "ip1").await.unwrap_err().status, 401);
        assert!(v.1.lock().unwrap().is_empty());
        // A server vouching for another server's user: refused.
        assert_eq!(h.enroll(&req(room, "DEV", "forged", "hs"), "ip2").await.unwrap_err().reason, "unauthorized");
        // A valid token for someone who is not joined: refused.
        assert_eq!(h.enroll(&req(room, "DEV", "outsider", "hs"), "ip2").await.unwrap_err().status, 401);
        // A bad token, a private address.
        assert_eq!(h.enroll(&req(room, "DEV", "bad", "hs"), "ip3").await.unwrap_err().reason, "invalid_token");
        assert_eq!(h.enroll(&req(room, "DEV", "private", "hs"), "ip3").await.unwrap_err().reason, "address_refused");
        // Malformed input.
        assert_eq!(h.enroll(&req(room, "bad device!", "good", "hs"), "ip4").await.unwrap_err().status, 400);
        assert!(rx.try_recv().is_err(), "no refused enrollment asks the owner anything");

        let (k, tok) = h.enroll(&req(room, "DEV", "good", "hs"), "ip5").await.unwrap();
        assert_eq!((k.issuer.as_str(), k.device.as_deref(), &k.scope), ("@me:hs", Some("DEV"), &KeyScope::SelfScope));
        assert!(!k.is_active(), "a fresh enrollment waits for its owner");
        assert_eq!(h.authenticate(Some(&tok), "ip5").unwrap().id, k.id);
        let code = dm_code(&h, &mut rx, &k.id, "!dm:hs");
        let k = h.approve_device_key("@me:hs", "!dm:hs", &code).unwrap();
        assert!(k.is_active() && h.key_valid(&k.id));

        // Re-enrolling the same device: pending, the working key keeps working until the new one is confirmed.
        let (k2, _) = h.enroll(&req(room, "DEV", "good", "hs"), "ip5").await.unwrap();
        assert!(h.key_valid(&k.id) && !h.key_valid(&k2.id));
        let code = dm_code(&h, &mut rx, &k2.id, "!dm:hs");
        h.approve_device_key("@me:hs", "!dm:hs", &code.to_lowercase()).unwrap();
        assert!(!h.key_valid(&k.id) && h.key_valid(&k2.id));
        assert_eq!(h.authenticate(Some(&tok), "ip6").unwrap_err().status, 401);
        // The device unlinks itself.
        h.unenroll(&h.get_key(&k2.id).unwrap()).unwrap();
        assert!(!h.key_valid(&k2.id));
    }

    /// A replayed OpenID token (lk-jwt-service, a widget, an integration manager all see one) enrolls nothing on
    /// its own: only a reply from the account itself, in the DM the code went to, with the right code, activates.
    #[tokio::test]
    async fn a_pending_enrollment_opens_nothing_until_its_owner_confirms_it_in_their_dm() {
        let room = "!self-approve-1:hs";
        let (h, _, mut rx) = self_hub(self_cfg(), &[("me", Ok("@me:hs"))]);
        let view = RoomView { encrypted: true, members: vec![member("@me:hs", "PHONE")], keys: vec![pk("@me:hs", "PHONE", "MINE")], key_params: None };
        let (_f, _reg) = facts(room, &["@me:hs", "@you:hs"], view);
        let (k, _) = h.enroll(&req(room, "APP", "me", "hs"), "a").await.unwrap();
        assert_eq!(h.open_self_events(&k, room).await.unwrap_err().reason, "pending_approval");
        assert!(h.has_pending_approval("@me:hs") && !h.has_pending_approval("@you:hs"));

        let code = dm_code(&h, &mut rx, &k.id, "!dm-me:hs");
        // Someone else, even with the right code: nothing of theirs is pending.
        assert_eq!(h.approve_device_key("@you:hs", "!dm-me:hs", &code).unwrap_err(), ApproveError::NothingPending);
        // The owner, but in another room than the DM the code went to.
        assert_eq!(h.approve_device_key("@me:hs", "!elsewhere:hs", &code).unwrap_err(), ApproveError::WrongRoom);
        // A wrong code.
        assert_eq!(h.approve_device_key("@me:hs", "!dm-me:hs", "AAAAAAAA").unwrap_err(), ApproveError::WrongCode { dropped: false });
        assert_eq!(h.open_self_events(&k, room).await.unwrap_err().reason, "pending_approval");

        let k = h.approve_device_key("@me:hs", "!dm-me:hs", &code).unwrap();
        assert!(h.open_self_events(&k, room).await.is_ok());
        assert!(!h.has_pending_approval("@me:hs"));
        // A code works once.
        assert_eq!(h.approve_device_key("@me:hs", "!dm-me:hs", &code).unwrap_err(), ApproveError::NothingPending);
    }

    #[tokio::test]
    async fn wrong_codes_and_expiry_drop_a_pending_enrollment() {
        let room = "!self-approve-2:hs";
        let (h, _, mut rx) = self_hub(self_cfg(), &[("me", Ok("@me:hs"))]);
        let (_f, _reg) = facts(room, &["@me:hs"], RoomView::default());

        let (k, _) = h.enroll(&req(room, "APP", "me", "hs"), "a").await.unwrap();
        dm_code(&h, &mut rx, &k.id, "!dm:hs");
        for _ in 1..MAX_APPROVE_FAILURES {
            assert_eq!(h.approve_device_key("@me:hs", "!dm:hs", "AAAAAAAA").unwrap_err(), ApproveError::WrongCode { dropped: false });
        }
        assert_eq!(h.approve_device_key("@me:hs", "!dm:hs", "AAAAAAAA").unwrap_err(), ApproveError::WrongCode { dropped: true });
        assert!(h.get_key(&k.id).is_none());

        // An enrollment whose window closed: refused, then swept.
        let (k, _) = h.enroll(&req(room, "APP", "me", "hs"), "b").await.unwrap();
        let code = dm_code(&h, &mut rx, &k.id, "!dm:hs");
        let mut stale = h.get_key(&k.id).unwrap();
        stale.pending.as_mut().unwrap().expires = unix_now() as i64 - 1;
        h.keys().add(stale).unwrap();
        assert!(!h.has_pending_approval("@me:hs"));
        assert_eq!(h.approve_device_key("@me:hs", "!dm:hs", &code).unwrap_err(), ApproveError::NothingPending);
        h.sweep(Instant::now());
        assert!(h.get_key(&k.id).is_none());
    }

    #[tokio::test]
    async fn enrollment_needs_the_management_bot_and_a_free_verification_slot() {
        let room = "!self-approve-3:hs";
        let (_f, _reg) = facts(room, &["@me:hs"], RoomView::default());
        let h = CompanionHub::with_verifier(
            self_cfg(),
            Arc::new(CompanionKeys::in_memory()),
            LinkShared::in_memory(),
            std::env::temp_dir().join("nvb-hub-noapprove.jsonl").to_string_lossy().into_owned(),
            Arc::new(FakeVerifier(HashMap::from([("me".to_string(), Ok("@me:hs".to_string()))]), Mutex::new(Vec::new()))),
        );
        assert_eq!(h.enroll(&req(room, "APP", "me", "hs"), "a").await.unwrap_err().reason, "approval_unavailable");
        assert!(h.list_keys().is_empty());

        let (h, _, _rx) = self_hub(self_cfg(), &[("me", Ok("@me:hs"))]);
        let _held: Vec<_> = (0..MAX_VERIFICATIONS_IN_FLIGHT).map(|_| h.verify_slots.clone().try_acquire_owned().unwrap()).collect();
        assert_eq!(h.enroll(&req(room, "APP", "me", "hs"), "a").await.unwrap_err().status, 503);
    }

    #[tokio::test]
    async fn enrollment_is_rate_limited_per_address_before_any_work() {
        let (h, v, _rx) = self_hub(CompanionsConfig { enrollments_per_minute: 2, ..self_cfg() }, &[]);
        for _ in 0..2 {
            assert_ne!(h.enroll(&req("!x:hs", "D", "t", "hs"), "ip").await.unwrap_err().status, 429);
        }
        assert_eq!(h.enroll(&req("!x:hs", "D", "t", "hs"), "ip").await.unwrap_err().status, 429);
        assert_ne!(h.enroll(&req("!x:hs", "D", "t", "hs"), "other").await.unwrap_err().status, 429);
        // IPv6 clients count per /64.
        for a in ["2001:db8:1:2::1", "2001:db8:1:2:ffff::9"] {
            assert_ne!(h.enroll(&req("!x:hs", "D", "t", "hs"), a).await.unwrap_err().status, 429);
        }
        assert_eq!(h.enroll(&req("!x:hs", "D", "t", "hs"), "2001:db8:1:2:aaaa::1").await.unwrap_err().status, 429);
        assert_ne!(h.enroll(&req("!x:hs", "D", "t", "hs"), "2001:db8:1:3::1").await.unwrap_err().status, 429);
        assert!(v.1.lock().unwrap().is_empty());
        let (h, _, _rx) = self_hub(CompanionsConfig { self_enroll: false, ..Default::default() }, &[]);
        assert_eq!(h.enroll(&req("!x:hs", "D", "t", "hs"), "ip").await.unwrap_err().status, 404);
    }

    #[test]
    fn rate_keys_group_ipv6_by_64() {
        assert_eq!(rate_key("2001:db8:1:2:3:4:5:6"), "2001:db8:1:2::/64");
        assert_eq!(rate_key("::ffff:192.0.2.1"), "192.0.2.1");
        assert_eq!(rate_key("192.0.2.1"), "192.0.2.1");
        assert_eq!(rate_key("not-an-ip"), "not-an-ip");
    }

    #[tokio::test]
    async fn self_events_need_a_call_membership_and_carry_only_own_keys() {
        let room = "!self-events-1:hs";
        let (h, _, mut rx) = self_hub(self_cfg(), &[("me", Ok("@me:hs")), ("you", Ok("@you:hs"))]);
        let (f, _reg) = facts(room, &["@me:hs", "@you:hs"], RoomView { encrypted: true, ..Default::default() });
        let (mine, _) = enroll_active(&h, &mut rx, &req(room, "APP", "me", "hs"), "a").await;
        let (yours, _) = enroll_active(&h, &mut rx, &req(room, "APP2", "you", "hs"), "b").await;
        // Not in the call yet.
        assert_eq!(h.open_self_events(&mine, room).await.unwrap_err().reason, "not_in_call");
        // Another key kind can't open it.
        let (room_key, _) = h.create_key("@me:hs", KeyScope::Room(room.into()), &["observe".into()], "").unwrap();
        assert_eq!(h.open_self_events(&room_key, room).await.unwrap_err().status, 403);

        f.view.send_replace(Arc::new(RoomView {
            encrypted: true,
            members: vec![member("@me:hs", "PHONE"), member("@you:hs", "LAPTOP")],
            // Including a key that claims to be mine but was sent by someone else.
            keys: vec![pk("@me:hs", "PHONE", "MINE"), pk("@you:hs", "LAPTOP", "YOURS"), pk("@you:hs", "PHONE", "FORGED")],
            key_params: None,
        }));
        let s = h.open_self_events(&mine, room).await.unwrap();
        let all = serde_json::to_string(&s.state().events_since(None).iter().map(|(_, v)| v.clone()).collect::<Vec<_>>()).unwrap();
        assert!(all.contains("MINE") && !all.contains("YOURS") && !all.contains("FORGED") && !all.contains("LAPTOP"));

        // A second connection for the same device takes over; the first is told to stop and its drop frees nothing.
        let s2 = h.open_self_events(&mine, room).await.unwrap();
        assert!(s.cancel.is_cancelled() && !s2.cancel.is_cancelled());
        drop(s);
        assert!(h.state().self_streams.contains_key(&mine.id));
        // Revoking the key ends the open stream at once and forgets its connect window.
        h.revoke_key(&mine.id, "@me:hs").unwrap();
        assert!(s2.cancel.is_cancelled());
        assert!(!h.state().self_connects.contains_key(&mine.id));
        drop(s2);
        assert!(h.state().self_streams.is_empty());

        // The other member's device sees only theirs.
        let s = h.open_self_events(&yours, room).await.unwrap();
        let all = serde_json::to_string(&s.state().events_since(None).iter().map(|(_, v)| v.clone()).collect::<Vec<_>>()).unwrap();
        assert!(all.contains("YOURS") && !all.contains("MINE"));
        // A member who left the room: refused.
        let (_f2, _reg2) = facts(room, &["@me:hs"], RoomView::default());
        assert_eq!(h.open_self_events(&yours, room).await.unwrap_err().reason, "not_a_member");
    }
}
