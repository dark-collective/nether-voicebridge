//! Cross-side playback routing (Matrix → Discord attribution, Phase 7).
//!
//! The Matrix side (`nvb-matrix-rtc`) produces one audio stream per LiveKit
//! participant; the Discord side (`nvb-discord`) owns the puppet bots that give
//! each speaker their own named voice.  Neither crate depends on the other, so
//! the contract between them lives here in the SDK-free core that both already
//! depend on.
//!
//! * The Discord side implements [`PlaybackRouter`] (its `PuppetPool`).
//! * The Matrix side, for each subscribed track, calls [`PlaybackRouter::assign`]
//!   to bind the participant to an output, pushes frames to the returned
//!   [`ParticipantSink`], and calls [`PlaybackRouter::release`] when the track
//!   ends.
//!
//! SDK-free: only `std` + [`PlaybackMixer`], so it stays unit-testable.

use std::sync::Arc;

use crate::mixer::PlaybackMixer;

/// A sink for one Matrix participant's mono audio (48 kHz, 10 ms frames as they
/// arrive from libwebrtc).  Cheap to hold and `push` to from a per-track task.
pub trait ParticipantSink: Send + Sync {
    /// Append one participant frame to whatever output it's bound to.
    fn push(&self, mono: &[i16]);
}

/// Routes Matrix participants to Discord outputs.  Implemented by the Discord
/// puppet pool; consumed by the Matrix RTC side.
///
/// `assign`/`release` are intentionally **synchronous** (no async): an
/// implementation must do any slow work (joining voice, setting a nickname)
/// off-thread, so these can be called directly from the per-track audio tasks.
pub trait PlaybackRouter: Send + Sync {
    /// Bind a participant (`key` — a stable per-participant id) to an output and
    /// return its sink.  Idempotent: calling again with the same `key` returns a
    /// sink for the same output without re-allocating.  Always succeeds — when no
    /// dedicated puppet is free the participant falls back to the shared listener
    /// mix (overflow), so audio is never dropped, only un-attributed.
    fn assign(&self, key: &str, display_name: &str) -> Arc<dyn ParticipantSink>;

    /// Mirror the participant's mute state onto their Discord puppet (so a
    /// muted Matrix user shows muted in Discord).  A no-op for an unknown `key`
    /// or an overflow participant with no dedicated puppet.
    fn set_muted(&self, key: &str, muted: bool);

    /// Release the participant's output back to the pool (their track ended or
    /// they left the call).  A no-op for an unknown `key`.
    fn release(&self, key: &str);

    /// Release every participant (call teardown).  Belt-and-suspenders alongside
    /// per-track [`release`](Self::release) so a torn-down call leaves no puppet
    /// stuck in the channel.
    fn release_all(&self);
}

/// A [`ParticipantSink`] that appends to one key of a [`PlaybackMixer`].
///
/// Works for both shapes of output in Phase 7: a puppet's *single-participant*
/// mixer (one key) and the *shared listener* mixer (many keys, for overflow).
/// The mixer's own per-key jitter buffering and priming are reused unchanged.
pub struct MixerSink {
    mixer: Arc<PlaybackMixer>,
    key: String,
}

impl MixerSink {
    pub fn new(mixer: Arc<PlaybackMixer>, key: String) -> Self {
        Self { mixer, key }
    }
}

impl ParticipantSink for MixerSink {
    fn push(&self, mono: &[i16]) {
        self.mixer.push(&self.key, mono);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Samples a buffer must accumulate before the mixer releases it (priming).
    // Mirrors the mixer's 150 ms target at 48 kHz.
    const PRIME: usize = 150 * 48;

    #[test]
    fn mixer_sink_pushes_to_its_own_key() {
        let mixer = Arc::new(PlaybackMixer::new());
        let a = MixerSink::new(mixer.clone(), "alice".to_owned());
        let b = MixerSink::new(mixer.clone(), "bob".to_owned());

        // Each sink writes under a distinct key; both contribute to the mix.
        a.push(&vec![100i16; PRIME]);
        b.push(&vec![50i16; PRIME]);
        let mixed = mixer.pop_10ms_mixed();
        assert!(mixed.iter().all(|&s| s == 150), "both keys mixed");
    }
}
