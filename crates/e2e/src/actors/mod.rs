//! Fake participants the harness drives against the bridge-under-test.
//!
//! D1 ships the Discord side ([`discord::DiscordSpeaker`]); the Matrix actors
//! (speaker/phantom/listener over the `nvb-matrix-rtc` testkit) arrive in D2.

pub mod discord;

use anyhow::Result;
use async_trait::async_trait;

/// Minimal lifecycle every fake participant supports; scenario code drives
/// richer actor-specific behavior (speaking probes, mutes) directly.
#[async_trait]
pub trait ParticipantDriver: Send {
    /// Enter the call/channel.
    async fn join(&mut self) -> Result<()>;
    /// Leave cleanly (the counterpart of a user hanging up properly).
    async fn leave(&mut self) -> Result<()>;
}
