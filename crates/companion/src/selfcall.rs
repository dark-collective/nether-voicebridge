//! Your own call data for your own tools (ADR 0005): the seam between the companion hub and a running bridge for the
//! `/_nvb/v1/self/*` endpoints.
//!
//! Each running bridge registers a [`RoomFacts`] for its room: joined-member checks for enrollment, and a
//! [`RoomView`] watch (the call's memberships with their resolved SFUs, and the media keys the bridge holds as a call
//! member). [`OwnState::for_owner`] is the one filter between that view and a requester: it keeps **only the
//! requester's own** memberships and keys. Everything that leaves the bridge on `/self/events` passes through it.
//!
//! The registry of enrolled listen-only identities lives here too: the RTC loop asks [`is_listen_only_identity`]
//! before counting a LiveKit participant as evidence that a membership is alive (the stale-membership reaper).
//!
//! This crate has no Matrix or LiveKit dependency; the bridge side lives in `nvb-matrix-rtc`.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, OnceLock, PoisonError, RwLock};

use base64::Engine as _;
use sha2::{Digest, Sha256};
use tokio::sync::{oneshot, watch};

// ── LiveKit identities of a member device ─────────────────────────────────────

/// The legacy LiveKit identity lk-jwt issues for a member device: `{mxid}:{device_id}`.
pub fn legacy_identity(mxid: &str, device_id: &str) -> String {
    format!("{mxid}:{device_id}")
}

fn sha256_json(parts: &[&str]) -> String {
    let json = serde_json::to_vec(parts).unwrap_or_default();
    base64::engine::general_purpose::STANDARD_NO_PAD.encode(Sha256::digest(&json))
}

/// The hashed identity (MSC4195) lk-jwt issues when the token request names the member:
/// `unpadded_base64(sha256(json([mxid, device_id, member_id])))`.
pub fn hashed_identity_with_device(mxid: &str, device_id: &str, member_id: &str) -> String {
    sha256_json(&[mxid, device_id, member_id])
}

/// The hashed identity (MSC4195) of the newer token endpoint: `unpadded_base64(sha256(json([mxid, member_id])))`.
pub fn hashed_identity(mxid: &str, member_id: &str) -> String {
    sha256_json(&[mxid, member_id])
}

/// Every identity form a device can appear under on an SFU, given the member ids seen for it.
pub fn device_identities<'a>(mxid: &str, device_id: &str, member_ids: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    let mut out = vec![legacy_identity(mxid, device_id)];
    for m in member_ids {
        for id in [hashed_identity_with_device(mxid, device_id, m), hashed_identity(mxid, m)] {
            if !out.contains(&id) {
                out.push(id);
            }
        }
    }
    out
}

// ── enrolled listen-only identities (the reaper's exclusion) ──────────────────

#[derive(Default)]
struct ListenOnly {
    devices: HashSet<(String, String)>,
    hashed: HashSet<String>,
}

fn listen_only() -> &'static RwLock<ListenOnly> {
    static L: OnceLock<RwLock<ListenOnly>> = OnceLock::new();
    L.get_or_init(|| RwLock::new(ListenOnly::default()))
}

/// Replace the set of enrolled listen-only devices `(mxid, device_id)`. A listen-only device has no membership; when
/// it asks lk-jwt for a token under MSC4195 it names itself as the member (`member.id = device_id`), so its hashed
/// identities are known in advance.
pub fn set_listen_only_devices(devices: impl IntoIterator<Item = (String, String)>) {
    *listen_only().write().unwrap_or_else(PoisonError::into_inner) = ListenOnly::build(devices);
}

/// Is this LiveKit identity an enrolled listen-only device? Such a participant connects and publishes nothing, so it
/// is no evidence that its owner's (possibly crashed) client is still in the call.
pub fn is_listen_only_identity(identity: &str) -> bool {
    listen_only().read().unwrap_or_else(PoisonError::into_inner).contains(identity)
}

impl ListenOnly {
    fn build(devices: impl IntoIterator<Item = (String, String)>) -> Self {
        let mut next = ListenOnly::default();
        for (mxid, dev) in devices {
            next.hashed.insert(hashed_identity_with_device(&mxid, &dev, &dev));
            next.hashed.insert(hashed_identity(&mxid, &dev));
            next.devices.insert((mxid, dev));
        }
        next
    }

    fn contains(&self, identity: &str) -> bool {
        if self.devices.is_empty() {
            return false;
        }
        if self.hashed.contains(identity) {
            return true;
        }
        // Legacy `@user:server:DEVICE`: the device is after the last colon (server names may carry a port).
        identity
            .rsplit_once(':')
            .is_some_and(|(mxid, dev)| self.devices.contains(&(mxid.to_string(), dev.to_string())))
    }
}

// ── discovery (what each bridge advertises in its room) ───────────────────────

/// Room state event a bridge publishes (state key = the bridge bot) naming its
/// companion API, so a member's own tools find it from the room alone.
pub const API_STATE_EVENT: &str = "im.nether.voicebridge.api";
/// The same content under this key of the bot's own `m.room.member`, used when
/// the bot may not send [`API_STATE_EVENT`]. A homeserver may drop unknown
/// member-content keys, so clients read the state event first.
pub const API_MEMBER_FIELD: &str = "im.nether.voicebridge.api";
/// Discovery content version.
pub const API_VERSION: u32 = 1;

fn public_api() -> &'static RwLock<Option<String>> {
    static P: OnceLock<RwLock<Option<String>>> = OnceLock::new();
    P.get_or_init(|| RwLock::new(None))
}

/// Set (once the hub is up) the public `/_nvb/v1` base to advertise; `None` stops advertising.
pub fn set_public_api_base(public_url: Option<&str>) {
    let base = public_url
        .map(|u| u.trim().trim_end_matches('/'))
        .filter(|u| u.starts_with("https://") || u.starts_with("http://"))
        .map(|u| format!("{u}/_nvb/v1"));
    *public_api().write().unwrap_or_else(PoisonError::into_inner) = base;
}

/// The discovery content to publish, if anything is to be advertised.
pub fn discovery_content() -> Option<serde_json::Value> {
    let base = public_api().read().unwrap_or_else(PoisonError::into_inner).clone()?;
    Some(serde_json::json!({ "api_base": base, "version": API_VERSION }))
}

// ── what a running bridge tells the hub about its room ────────────────────────

/// An SFU as MatrixRTC names it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Transport {
    pub livekit_service_url: String,
    pub livekit_alias: String,
}

/// One call membership in the room, focus-resolved by the bridge (`rtc::focus`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallMember {
    pub mxid: String,
    pub device_id: String,
    /// Not expired.
    pub active: bool,
    /// One of the bridge's own (a ghost or the bridge bot).
    pub bridge_owned: bool,
    /// The SFU this membership publishes on per its focus selection.
    pub transport: Option<Transport>,
    /// The member's own `foci_preferred[0]` (some clients publish there regardless of the election).
    pub preferred: Option<Transport>,
}

/// A media key the bridge received as a call member, as it was labelled by its sender.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerKey {
    /// The sending account (authenticated over Olm, not over plaintext key transport).
    pub sender: String,
    /// `member.claimed_device_id` of the key event.
    pub device_id: String,
    /// `member.id` of the key event, when the client sends one.
    pub member_id: Option<String>,
    pub index: u8,
    pub key_b64: String,
}

/// The frame-crypto parameters the bridge itself decrypts peers' media with (CLAUDE.md constraint 3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyParams {
    pub key_derivation: &'static str,
    pub ratchet_salt: String,
    pub ratchet_window_size: i32,
    pub key_ring_size: i32,
    pub failure_tolerance: i32,
    pub encryption: &'static str,
}

impl KeyParams {
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "key_derivation": self.key_derivation,
            "ratchet_salt": self.ratchet_salt,
            "ratchet_window_size": self.ratchet_window_size,
            "key_ring_size": self.key_ring_size,
            "failure_tolerance": self.failure_tolerance,
            "encryption": self.encryption,
        })
    }
}

/// A bridge's current view of its room's call.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RoomView {
    /// Media E2EE resolved on for this room. Off ⇒ no key is ever handed out.
    pub encrypted: bool,
    pub members: Vec<CallMember>,
    /// Keys the bridge holds right now (cleared at call end).
    pub keys: Vec<PeerKey>,
    pub key_params: Option<KeyParams>,
}

/// A running bridge's answers about its room. Lookups go through the bridge's own synced room state.
pub trait RoomFacts: Send + Sync {
    /// Is `mxid` a joined member of the room?
    fn is_joined(&self, mxid: &str) -> oneshot::Receiver<bool>;
    /// Is any joined member of the room on `server_name`?
    fn has_joined_server(&self, server_name: &str) -> oneshot::Receiver<bool>;
    /// The call as the bridge sees it.
    fn view(&self) -> watch::Receiver<Arc<RoomView>>;
}

type Rooms = RwLock<HashMap<String, Arc<dyn RoomFacts>>>;

fn rooms() -> &'static Rooms {
    static R: OnceLock<Rooms> = OnceLock::new();
    R.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Keeps a room registered; dropping it unregisters (if it is still the registered one).
pub struct RoomRegistration {
    room_id: String,
    facts: Arc<dyn RoomFacts>,
}

impl Drop for RoomRegistration {
    fn drop(&mut self) {
        let mut g = rooms().write().unwrap_or_else(PoisonError::into_inner);
        if g.get(&self.room_id).is_some_and(|f| Arc::ptr_eq(f, &self.facts)) {
            g.remove(&self.room_id);
        }
    }
}

/// Register the running bridge for `room_id` (a restart of that bridge replaces it).
pub fn register_room(room_id: &str, facts: Arc<dyn RoomFacts>) -> RoomRegistration {
    rooms().write().unwrap_or_else(PoisonError::into_inner).insert(room_id.to_string(), facts.clone());
    RoomRegistration { room_id: room_id.to_string(), facts }
}

pub fn room_facts(room_id: &str) -> Option<Arc<dyn RoomFacts>> {
    rooms().read().unwrap_or_else(PoisonError::into_inner).get(room_id).cloned()
}

// ── the requester's own slice of the view ─────────────────────────────────────

/// One own device in the call: its identities and its current key (if any).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnDevice {
    pub device_id: String,
    /// Legacy form first, then the hashed forms known so far.
    pub identities: Vec<String>,
    /// `(index, key_b64)` of every key held for this device, by index.
    pub keys: BTreeMap<u8, String>,
}

/// What `/self/events` may say about one requester in one room. Built only by [`OwnState::for_owner`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnState {
    pub room_id: String,
    pub in_call: bool,
    pub encrypted: bool,
    /// Per SFU, the own devices publishing there.
    pub transports: BTreeMap<Transport, BTreeSet<String>>,
    pub devices: BTreeMap<String, OwnDevice>,
    pub paired_present: bool,
    pub key_params: Option<KeyParams>,
}

impl OwnState {
    /// The requester's own call data: their active, non-bridge memberships (minus the listen-only device itself), the
    /// SFUs those publish on, and **only keys whose sender is the requester**. In an unencrypted room no key is kept.
    pub fn for_owner(view: &RoomView, room_id: &str, owner: &str, listen_only_device: &str, paired_present: bool) -> Self {
        let mut transports: BTreeMap<Transport, BTreeSet<String>> = BTreeMap::new();
        let mut devices: BTreeMap<String, OwnDevice> = BTreeMap::new();
        for m in &view.members {
            if m.mxid != owner || m.bridge_owned || !m.active || m.device_id == listen_only_device {
                continue;
            }
            for t in [&m.transport, &m.preferred].into_iter().flatten() {
                transports.entry(t.clone()).or_default().insert(m.device_id.clone());
            }
            devices.entry(m.device_id.clone()).or_insert_with(|| OwnDevice {
                device_id: m.device_id.clone(),
                identities: Vec::new(),
                keys: BTreeMap::new(),
            });
        }
        let mut member_ids: HashMap<&str, BTreeSet<&str>> = HashMap::new();
        for k in view.keys.iter().filter(|k| k.sender == owner) {
            let Some(d) = devices.get_mut(&k.device_id) else { continue };
            if let Some(mid) = k.member_id.as_deref() {
                member_ids.entry(k.device_id.as_str()).or_default().insert(mid);
            }
            if view.encrypted {
                d.keys.insert(k.index, k.key_b64.clone());
            }
        }
        for d in devices.values_mut() {
            let mids = member_ids.get(d.device_id.as_str()).cloned().unwrap_or_default();
            d.identities = device_identities(owner, &d.device_id, mids);
        }
        Self {
            room_id: room_id.to_string(),
            in_call: !devices.is_empty(),
            encrypted: view.encrypted,
            transports,
            devices,
            paired_present,
            key_params: view.key_params.clone(),
        }
    }

    fn self_call_json(&self) -> serde_json::Value {
        let transports: Vec<serde_json::Value> = self
            .transports
            .iter()
            .map(|(t, devs)| {
                let identities: Vec<&String> =
                    devs.iter().filter_map(|d| self.devices.get(d)).flat_map(|d| d.identities.iter()).collect();
                serde_json::json!({
                    "livekit_service_url": t.livekit_service_url,
                    "livekit_alias": t.livekit_alias,
                    "identities": identities,
                })
            })
            .collect();
        serde_json::json!({
            "room_id": self.room_id,
            "in_call": self.in_call,
            "encrypted": self.encrypted,
            "transports": transports,
        })
    }

    fn key_events(&self) -> Vec<((String, Option<u8>), serde_json::Value)> {
        if !self.encrypted {
            return Vec::new();
        }
        let params = self.key_params.as_ref().map(KeyParams::to_json);
        let mut out = Vec::new();
        for d in self.devices.values() {
            let identity = d.identities.first().cloned().unwrap_or_default();
            if d.keys.is_empty() {
                out.push((
                    (d.device_id.clone(), None),
                    serde_json::json!({
                        "room_id": self.room_id, "device_id": d.device_id, "identity": identity,
                        "identities": d.identities, "index": null, "key": null, "params": params,
                    }),
                ));
            }
            for (index, key) in &d.keys {
                out.push((
                    (d.device_id.clone(), Some(*index)),
                    serde_json::json!({
                        "room_id": self.room_id, "device_id": d.device_id, "identity": identity,
                        "identities": d.identities, "index": index, "key": key, "params": params,
                    }),
                ));
            }
        }
        out
    }

    /// The events that bring a client holding `prev` (or nothing, on (re)connect) up to `self`. On (re)connect the
    /// whole current state is sent: `self_call`, every `key` (or a null key per device), `paired`.
    pub fn events_since(&self, prev: Option<&OwnState>) -> Vec<(&'static str, serde_json::Value)> {
        let mut out = Vec::new();
        let call = self.self_call_json();
        if prev.is_none_or(|p| p.self_call_json() != call) {
            out.push(("self_call", call));
        }
        let before: HashMap<(String, Option<u8>), serde_json::Value> =
            prev.map(|p| p.key_events().into_iter().collect()).unwrap_or_default();
        for (k, v) in self.key_events() {
            if before.get(&k) != Some(&v) {
                out.push(("key", v));
            }
        }
        if prev.is_none_or(|p| p.paired_present != self.paired_present) {
            out.push(("paired", serde_json::json!({ "room_id": self.room_id, "present": self.paired_present })));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROOM: &str = "!room:hs";
    const OWNER: &str = "@owner:hs";

    fn t(url: &str) -> Transport {
        Transport { livekit_service_url: url.into(), livekit_alias: ROOM.into() }
    }

    fn member(mxid: &str, dev: &str, sfu: &str) -> CallMember {
        CallMember { mxid: mxid.into(), device_id: dev.into(), active: true, bridge_owned: false, transport: Some(t(sfu)), preferred: Some(t(sfu)) }
    }

    fn key(sender: &str, dev: &str, mid: Option<&str>, index: u8, k: &str) -> PeerKey {
        PeerKey { sender: sender.into(), device_id: dev.into(), member_id: mid.map(Into::into), index, key_b64: k.into() }
    }

    fn params() -> KeyParams {
        KeyParams { key_derivation: "HKDF", ratchet_salt: "LKFrameEncryptionKey".into(), ratchet_window_size: 16, key_ring_size: 256, failure_tolerance: -1, encryption: "AES-GCM" }
    }

    #[test]
    fn discovery_content_names_the_api_base() {
        set_public_api_base(Some(" https://bridge.example.org/ "));
        assert_eq!(
            discovery_content(),
            Some(serde_json::json!({ "api_base": "https://bridge.example.org/_nvb/v1", "version": 1 }))
        );
        set_public_api_base(Some("bridge.example.org"));
        assert_eq!(discovery_content(), None);
        set_public_api_base(None);
    }

    #[test]
    fn hashed_identities_match_the_lk_jwt_test_vector() {
        // The vector lk-jwt-service checks its own derivation against.
        assert_eq!(hashed_identity_with_device("@alice:example.com", "DEVICE123", "memberABC"), "J+T45tGruxc+HrUOqJJlyQSV33m728Cme4+vt8/SWrU");
        assert_eq!(legacy_identity("@a:hs", "DEV"), "@a:hs:DEV");
        assert_eq!(device_identities("@a:hs", "DEV", []), vec!["@a:hs:DEV".to_string()]);
        assert_eq!(device_identities("@a:hs", "DEV", ["m1", "m1"]).len(), 3);
    }

    #[test]
    fn listen_only_identities_are_recognised_in_every_form() {
        // The registry is process-global (hub tests replace it concurrently), so test its logic directly.
        let l = ListenOnly::build([("@owner:hs:8448".to_string(), "APP".to_string())]);
        assert!(l.contains("@owner:hs:8448:APP"));
        assert!(l.contains(&hashed_identity_with_device("@owner:hs:8448", "APP", "APP")));
        assert!(l.contains(&hashed_identity("@owner:hs:8448", "APP")));
        assert!(!l.contains("@owner:hs:8448:PHONE"), "the owner's real devices are never excluded");
        assert!(!l.contains("@other:hs:APP"));
        assert!(!ListenOnly::build([]).contains("@owner:hs:8448:APP"));
    }

    #[test]
    fn only_the_requesters_own_keys_and_memberships_are_kept() {
        let view = RoomView {
            encrypted: true,
            members: vec![
                member(OWNER, "PHONE", "https://sfu.a"),
                member("@other:hs", "LAPTOP", "https://sfu.a"),
                CallMember { bridge_owned: true, ..member(OWNER, "GHOSTLIKE", "https://sfu.a") },
                member(OWNER, "APP", "https://sfu.a"),
            ],
            keys: vec![
                key(OWNER, "PHONE", Some("m1"), 0, "own0"),
                key("@other:hs", "LAPTOP", None, 0, "theirs"),
                // A key someone else labelled with the owner's device id is still theirs, never the owner's.
                key("@other:hs", "PHONE", None, 1, "forged"),
            ],
            key_params: Some(params()),
        };
        let s = OwnState::for_owner(&view, ROOM, OWNER, "APP", false);
        assert!(s.in_call);
        assert_eq!(s.devices.keys().collect::<Vec<_>>(), vec!["PHONE"]);
        let all = serde_json::to_string(&s.events_since(None).iter().map(|(_, v)| v.clone()).collect::<Vec<_>>()).unwrap();
        assert!(all.contains("own0"));
        assert!(!all.contains("theirs") && !all.contains("forged") && !all.contains("LAPTOP") && !all.contains("@other"));

        // Another user asking gets nothing of the owner's.
        let other = OwnState::for_owner(&view, ROOM, "@other:hs", "X", false);
        let theirs = serde_json::to_string(&other.events_since(None).iter().map(|(_, v)| v.clone()).collect::<Vec<_>>()).unwrap();
        assert!(!theirs.contains("own0") && !theirs.contains("PHONE"));
        assert!(theirs.contains("theirs"));
    }

    #[test]
    fn unencrypted_rooms_never_carry_a_key() {
        let view = RoomView { encrypted: false, members: vec![member(OWNER, "PHONE", "https://sfu.a")], keys: vec![key(OWNER, "PHONE", None, 0, "k")], key_params: Some(params()) };
        let s = OwnState::for_owner(&view, ROOM, OWNER, "APP", true);
        let ev = s.events_since(None);
        assert_eq!(ev.iter().map(|(n, _)| *n).collect::<Vec<_>>(), vec!["self_call", "paired"]);
    }

    #[test]
    fn keys_start_null_then_follow_rotation_and_hashed_forms_update_self_call() {
        let mut view = RoomView { encrypted: true, members: vec![member(OWNER, "PHONE", "https://sfu.a")], keys: vec![], key_params: Some(params()) };
        let s0 = OwnState::for_owner(&view, ROOM, OWNER, "APP", false);
        let ev0 = s0.events_since(None);
        let k0 = ev0.iter().find(|(n, _)| *n == "key").unwrap();
        assert!(k0.1["key"].is_null(), "none yet: the client waits");
        assert_eq!(k0.1["params"]["ratchet_salt"], "LKFrameEncryptionKey");

        // The first key brings the member id: self_call is re-sent with the hashed identities.
        view.keys.push(key(OWNER, "PHONE", Some("m1"), 0, "k0"));
        let s1 = OwnState::for_owner(&view, ROOM, OWNER, "APP", false);
        let ev1 = s1.events_since(Some(&s0));
        let names: Vec<&str> = ev1.iter().map(|(n, _)| *n).collect();
        assert_eq!(names, vec!["self_call", "key"]);
        assert_eq!(ev1[0].1["transports"][0]["identities"].as_array().unwrap().len(), 3);
        assert_eq!(ev1[1].1["key"], "k0");

        // A rotation is one more key event; nothing else changes.
        view.keys.push(key(OWNER, "PHONE", Some("m1"), 1, "k1"));
        let s2 = OwnState::for_owner(&view, ROOM, OWNER, "APP", false);
        let ev2 = s2.events_since(Some(&s1));
        assert_eq!(ev2.len(), 1);
        assert_eq!(ev2[0].1["index"], 1);
        assert!(s2.events_since(Some(&s2)).is_empty());
    }

    #[test]
    fn multi_sfu_lists_each_sfu_with_its_devices() {
        let view = RoomView {
            encrypted: false,
            members: vec![
                member(OWNER, "PHONE", "https://sfu.a"),
                CallMember { preferred: Some(t("https://sfu.b")), ..member(OWNER, "LAPTOP", "https://sfu.a") },
            ],
            keys: vec![],
            key_params: None,
        };
        let s = OwnState::for_owner(&view, ROOM, OWNER, "APP", false);
        let call = &s.events_since(None)[0].1;
        let ts = call["transports"].as_array().unwrap();
        assert_eq!(ts.len(), 2);
        assert_eq!(ts[0]["identities"].as_array().unwrap().len(), 2);
        assert_eq!(ts[1]["identities"], serde_json::json!(["@owner:hs:LAPTOP"]));
    }

    #[test]
    fn leaving_the_call_and_pairing_changes_are_events() {
        let view = RoomView { encrypted: false, members: vec![member(OWNER, "PHONE", "https://sfu.a")], keys: vec![], key_params: None };
        let a = OwnState::for_owner(&view, ROOM, OWNER, "APP", false);
        let b = OwnState::for_owner(&RoomView::default(), ROOM, OWNER, "APP", true);
        assert!(!b.in_call);
        let ev = b.events_since(Some(&a));
        assert_eq!(ev.iter().map(|(n, _)| *n).collect::<Vec<_>>(), vec!["self_call", "paired"]);
        assert_eq!(ev[0].1["in_call"], false);
    }
}
