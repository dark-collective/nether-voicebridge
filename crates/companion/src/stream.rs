//! `stream:attach` (ADR 0004 §5): the seam between the companion hub and the bridge's ghost driver.
//!
//! Each running bridge registers a [`StreamProvider`] for its room. The hub asks it to attach a stream device to the
//! ghost of a represented Discord member; the provider answers with a [`StreamGrant`]: the stream device's LiveKit
//! transport (kept fresh through a `watch`), its media key in an encrypted room, and a [`StreamGrant::live`] switch
//! the companion flips once its track is published (the device's call membership is posted only then: Element Call
//! subscribes reliably only to a track that is already live when it sees the membership). The provider tears the
//! device down when the lease's cancel token fires.
//!
//! This crate has no Matrix or LiveKit dependency; the provider lives in `nvb-matrix-rtc`.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, PoisonError, RwLock};

use tokio::sync::{broadcast, oneshot, watch, Notify};
use tokio_util::sync::CancellationToken;

/// Where and how the companion connects (one LiveKit identity, the stream device's).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamTransport {
    pub sfu_url: String,
    pub jwt: String,
    /// The LiveKit identity the token is for (`@ghost:server:S<…>`).
    pub identity: String,
    /// Unix seconds when this token was fetched (lk-jwt tokens live about an hour; a fresh one replaces it before).
    pub fetched_at: u64,
}

/// What an attached stream gives the companion.
#[derive(Debug, Clone)]
pub struct StreamGrant {
    pub ghost_mxid: String,
    pub device_id: String,
    /// Media is encrypted in this room: install [`StreamGrant::key`] (HKDF frame keys, index 0).
    pub e2ee: bool,
    pub key: Option<[u8; 16]>,
    /// Current transport; changes when the token is refreshed or the call moves to another SFU.
    pub transport: watch::Receiver<Option<StreamTransport>>,
    /// Notified once by the hub when the companion reports its track is published.
    pub live: Arc<Notify>,
}

/// Why an attach was refused, as a stable reason code (the hub maps it onto its HTTP answer).
pub type AttachError = &'static str;

pub type AttachReply = oneshot::Receiver<Result<StreamGrant, AttachError>>;

pub trait StreamProvider: Send + Sync {
    /// Attach a stream device to the ghost of `discord_user_id`. The device lives until `cancel` fires.
    fn attach(&self, discord_user_id: u64, cancel: CancellationToken) -> AttachReply;
}

type Providers = RwLock<HashMap<String, Arc<dyn StreamProvider>>>;

fn providers() -> &'static Providers {
    static P: OnceLock<Providers> = OnceLock::new();
    P.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Keeps a provider registered for a room; dropping it unregisters (if it is still the registered one).
pub struct ProviderRegistration {
    room_id: String,
    provider: Arc<dyn StreamProvider>,
}

impl Drop for ProviderRegistration {
    fn drop(&mut self) {
        let mut g = providers().write().unwrap_or_else(PoisonError::into_inner);
        if g.get(&self.room_id).is_some_and(|p| Arc::ptr_eq(p, &self.provider)) {
            g.remove(&self.room_id);
        }
    }
}

/// Register the bridge for `room_id` as its stream provider (a restart of that bridge replaces it).
pub fn register_provider(room_id: &str, provider: Arc<dyn StreamProvider>) -> ProviderRegistration {
    providers()
        .write()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(room_id.to_string(), provider.clone());
    ProviderRegistration { room_id: room_id.to_string(), provider }
}

pub fn provider_for(room_id: &str) -> Option<Arc<dyn StreamProvider>> {
    providers().read().unwrap_or_else(PoisonError::into_inner).get(room_id).cloned()
}

fn alerts() -> &'static broadcast::Sender<String> {
    static A: OnceLock<broadcast::Sender<String>> = OnceLock::new();
    A.get_or_init(|| broadcast::channel(64).0)
}

/// The bridge's subscriber saw a stream-device identity publish something other than a screen share (ADR 0004 §5.5):
/// the hub revokes the lease holding that identity.
pub fn report_disallowed_publication(identity: &str) {
    let _ = alerts().send(identity.to_string());
}

/// The hub's side of [`report_disallowed_publication`].
pub fn disallowed_publications() -> broadcast::Receiver<String> {
    alerts().subscribe()
}

/// Is this LiveKit identity (`@user:server:DEVICE`) a companion stream device (`S…`)?
pub fn is_stream_device_identity(identity: &str) -> bool {
    identity.starts_with('@') && identity.rsplit(':').next().is_some_and(|d| d.starts_with('S') && d.len() > 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fake;
    impl StreamProvider for Fake {
        fn attach(&self, _: u64, _: CancellationToken) -> AttachReply {
            let (tx, rx) = oneshot::channel();
            let _ = tx.send(Err("not_represented"));
            rx
        }
    }

    #[test]
    fn stream_device_identities_are_recognised() {
        assert!(is_stream_device_identity("@vb_123456789012345678:hs:S123456789012345678-ab12"));
        assert!(!is_stream_device_identity("@vb_123456789012345678:hs:D123456789012345678-ab12"));
        assert!(!is_stream_device_identity("@alice:hs"));
        assert!(!is_stream_device_identity("S123"));
    }

    #[test]
    fn registration_lives_as_long_as_its_guard() {
        let room = "!stream-test:hs";
        assert!(provider_for(room).is_none());
        let a = register_provider(room, Arc::new(Fake));
        assert!(provider_for(room).is_some());
        // A newer registration (bridge restart) replaces it; dropping the old guard leaves the new one in place.
        let b = register_provider(room, Arc::new(Fake));
        drop(a);
        assert!(provider_for(room).is_some());
        drop(b);
        assert!(provider_for(room).is_none());
    }
}
