//! Fake participants the harness drives against the bridge-under-test.
//!
//! Discord side: [`discord::DiscordSpeaker`] (D1) + [`discord::DiscordListener`]
//! (D2). Matrix side (D2, over the `nvb-matrix-rtc` testkit):
//! [`matrix::MatrixSpeaker`] / [`matrix::MatrixPhantom`] /
//! [`matrix::MatrixListener`].

pub mod crypto_dm;
pub mod discord;
pub mod matrix;
pub mod mgmt_client;

use anyhow::Result;
use async_trait::async_trait;

/// Warm-up lead-in every speaker plays BEFORE the probe: a quiet 440 Hz tone
/// with raised-cosine head/tail fades. The first ~350 ms of a fresh media path
/// carries spin-up artifacts that are not bridge regressions — sender silence
/// suppression eating the first frame, jitter/mixer priming, Opus/DAVE encoder
/// warm-up (live-measured 2026-07-07: a head dropout + a 60 ms glitch run,
/// then 5.6 s pristine). The lead-in absorbs all of that; it is NOT part of
/// the oracle's reference, so the strict zero-dropout/zero-glitch thresholds
/// apply to the probe alone.
pub fn warmup_lead_in() -> Vec<i16> {
    const MS: usize = 500;
    const FADE: usize = 10 * 48; // 10 ms
    let n = MS * 48;
    (0..n)
        .map(|i| {
            let fade = if i < FADE {
                0.5 - 0.5 * (std::f64::consts::PI * i as f64 / FADE as f64).cos()
            } else if i + FADE >= n {
                0.5 - 0.5 * (std::f64::consts::PI * (n - 1 - i) as f64 / FADE as f64).cos()
            } else {
                1.0
            };
            let phase = std::f64::consts::TAU * 440.0 * i as f64 / 48_000.0;
            (phase.sin() * fade * 0.2 * i16::MAX as f64) as i16
        })
        .collect()
}

/// Minimal lifecycle every fake participant supports; scenario code drives
/// richer actor-specific behavior (speaking probes, mutes) directly.
#[async_trait]
pub trait ParticipantDriver: Send {
    /// Enter the call/channel.
    async fn join(&mut self) -> Result<()>;
    /// Leave cleanly (the counterpart of a user hanging up properly).
    async fn leave(&mut self) -> Result<()>;
}
