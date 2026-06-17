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

/// A sink for one Discord speaker's mono audio (48 kHz, 20 ms frames as the
/// anchor's `VoiceTick` delivers them).  The opposite-direction twin of
/// [`ParticipantSink`]: the Discord capture side pushes a single speaker's
/// frames here, and the Matrix side reframes them into that speaker's ghost
/// LiveKit track.  Cheap to hold and `push` to from the capture hot path.
pub trait CaptureSink: Send + Sync {
    /// Append one Discord speaker frame to their ghost's publish path.
    fn push(&self, mono: &[i16]);
}

/// Routes Discord speakers to Matrix ghosts (Discord → Matrix attribution,
/// Phase 8).  The mirror of [`PlaybackRouter`]: implemented by the Matrix-side
/// ghost pool (which owns one LiveKit participant per Discord speaker) and
/// consumed by the Discord capture side.
///
/// `assign`/`release` are intentionally **synchronous** (no async), like
/// [`PlaybackRouter`]: an implementation must do its slow work (registering the
/// ghost, joining the call, connecting to LiveKit) off-thread so these can be
/// called directly from serenity's voice-state handler.
pub trait CaptureRouter: Send + Sync {
    /// Bind a Discord speaker (`key` — a stable per-speaker id, e.g. the Discord
    /// user id) to a ghost and return its sink.  Idempotent: calling again with
    /// the same `key` returns a sink for the same ghost without re-minting.
    /// `avatar_url`, when present, is the speaker's Discord avatar URL; the
    /// implementation mirrors it onto the ghost's Matrix profile.
    fn assign(&self, key: &str, display_name: &str, avatar_url: Option<&str>)
        -> Arc<dyn CaptureSink>;

    /// Mirror the Discord speaker's mute state onto their ghost's LiveKit track
    /// (so a muted Discord user shows muted in Element Call).  The mirror of
    /// [`PlaybackRouter::set_muted`].  A no-op for an unknown `key` (a speaker
    /// with no ghost yet).
    fn set_muted(&self, key: &str, muted: bool);

    /// Retire the speaker's ghost (they left the voice channel).  A no-op for an
    /// unknown `key`.
    fn release(&self, key: &str);

    /// Retire every ghost (call teardown).  Belt-and-suspenders alongside
    /// per-speaker [`release`](Self::release).
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

    // ── CaptureRouter contract (Phase 8) ────────────────────────────────────────

    use std::collections::HashMap;
    use std::sync::Mutex;

    /// Per-speaker sink that records the frames pushed to it.
    #[derive(Default)]
    struct RecordingSink {
        frames: Mutex<Vec<Vec<i16>>>,
    }
    impl CaptureSink for RecordingSink {
        fn push(&self, mono: &[i16]) {
            self.frames.lock().unwrap().push(mono.to_vec());
        }
    }

    /// Minimal `CaptureRouter` that hands out one `RecordingSink` per key,
    /// reusing it on re-assign (idempotence) and dropping it on release. Keeps
    /// the concrete `RecordingSink` handles so a test can inspect what was pushed.
    #[derive(Default)]
    struct StubRouter {
        sinks: Mutex<HashMap<String, Arc<RecordingSink>>>,
        muted: Mutex<HashMap<String, bool>>,
    }
    impl StubRouter {
        fn recorded(&self, key: &str) -> Vec<Vec<i16>> {
            self.sinks.lock().unwrap()[key].frames.lock().unwrap().clone()
        }
        fn len(&self) -> usize {
            self.sinks.lock().unwrap().len()
        }
        fn muted(&self, key: &str) -> Option<bool> {
            self.muted.lock().unwrap().get(key).copied()
        }
    }
    impl CaptureRouter for StubRouter {
        fn assign(
            &self,
            key: &str,
            _display_name: &str,
            _avatar_url: Option<&str>,
        ) -> Arc<dyn CaptureSink> {
            self.sinks
                .lock()
                .unwrap()
                .entry(key.to_owned())
                .or_insert_with(|| Arc::new(RecordingSink::default()))
                .clone()
        }
        fn set_muted(&self, key: &str, muted: bool) {
            self.muted.lock().unwrap().insert(key.to_owned(), muted);
        }
        fn release(&self, key: &str) {
            self.sinks.lock().unwrap().remove(key);
        }
        fn release_all(&self) {
            self.sinks.lock().unwrap().clear();
        }
    }

    #[test]
    fn capture_router_demuxes_per_speaker() {
        let router = StubRouter::default();
        let alice = router.assign("alice", "Alice", None);
        let bob = router.assign("bob", "Bob", None);

        alice.push(&[1, 1, 1]);
        bob.push(&[2, 2]);
        alice.push(&[3]);

        // Re-assign returns the *same* sink (idempotent): its history is intact.
        router.assign("alice", "Alice", None).push(&[4]);

        assert_eq!(router.recorded("alice"), vec![vec![1, 1, 1], vec![3], vec![4]]);
        assert_eq!(router.recorded("bob"), vec![vec![2, 2]]);
    }

    #[test]
    fn capture_router_release_drops_the_sink() {
        let router = StubRouter::default();
        router.assign("alice", "Alice", None);
        assert_eq!(router.len(), 1);
        router.release("alice");
        assert_eq!(router.len(), 0);
        router.assign("a", "A", None);
        router.assign("b", "B", None);
        router.release_all();
        assert_eq!(router.len(), 0);
    }

    #[test]
    fn capture_router_records_mute_state() {
        let router = StubRouter::default();
        router.assign("alice", "Alice", None);
        assert_eq!(router.muted("alice"), None); // not yet mirrored
        router.set_muted("alice", true);
        assert_eq!(router.muted("alice"), Some(true));
        router.set_muted("alice", false);
        assert_eq!(router.muted("alice"), Some(false));
    }
}
