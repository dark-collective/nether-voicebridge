//! The companion interface (ADR 0004): keys, leases and the `/_nvb/v1` HTTP +
//! SSE listener through which companion tools ask the bridge for them.
//!
//! Phase 1 serves the `observe` lease kind; keys for the other kinds can already
//! be issued. Everything here is off unless `[companions] enabled = true`.

pub mod audit;
pub mod http;
pub mod hub;
pub mod observe;
pub mod stream;
pub mod token;

pub use hub::CompanionHub;

/// Unix epoch seconds.
pub(crate) fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
