//! Fake participants the harness drives against the bridge-under-test.
//!
//! Discord side: [`discord::DiscordSpeaker`] (D1) + [`discord::DiscordListener`]
//! (D2). Matrix side (D2, over the `nvb-matrix-rtc` testkit):
//! [`matrix::MatrixSpeaker`] / [`matrix::MatrixPhantom`] /
//! [`matrix::MatrixListener`].

pub mod discord;
pub mod matrix;

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
