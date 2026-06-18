//! Per-participant playback mixer for the Matrix → Discord direction.
//!
//! Mirror image of the Phase 5 capture path.  On the inbound side each Matrix
//! speaker arrives as an **independent** LiveKit track on its own clock, so —
//! unlike the Discord capture side, where songbird hands all speakers back
//! time-aligned at one tick — we cannot mix at the producer.  Each track gets
//! its own [`JitterBuffer`] (keyed by LiveKit track SID); the Discord playback
//! source pulls one mixed 10 ms mono frame at a time, and the mix happens lazily
//! at that pull.
//!
//! This module is SDK-free (no `livekit`/`songbird`/`tokio`) so it stays
//! unit-testable: the LiveKit consumer tasks call [`PlaybackMixer::push`], the
//! songbird playback source calls [`PlaybackMixer::pop_10ms_mixed`].
//!
//! No panics on the audio hot path: a poisoned lock is recovered via
//! `into_inner()` rather than unwrapped, so a panic elsewhere can never silence
//! playback for the rest of the process.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};

use super::jitter::JitterBuffer;
use super::{mix_mono, rms_dbfs, SAMPLES_10MS};

/// Jitter sizing for each inbound (LiveKit → Discord) participant stream.
///
/// `TARGET` is the pre-buffer cushion built once before playback starts; with
/// prime-once semantics (see [`JitterBuffer::pop_10ms`]) the depth then
/// oscillates around it.  songbird pulls this source in bursts, so the cushion
/// must exceed the burst amplitude or it underruns (crackle).  Live testing
/// showed depth oscillating ~140–320 ms at a 150 ms prime with zero underruns.
/// `MAX` is a safety cap (drop-oldest only on a genuine producer runaway, e.g.
/// clock drift) set above the observed peak so normal oscillation never drops,
/// while still bounding worst-case latency.
const JITTER_TARGET_MS: u32 = 150;
const JITTER_MAX_MS: u32 = 500;
const SAMPLE_RATE: u32 = 48_000;

/// Level (dBFS) above which a pushed frame counts as real speech rather than
/// silence/comfort-noise. True digital silence is −96 dBFS (the [`rms_dbfs`]
/// floor); DTX/comfort-noise and faint room tone sit low, while quiet speech is
/// typically above −45. −50 clears the noise floor without dropping soft speech.
/// Read by the Discord-side speaking gate (via [`PlaybackMixer::loud_frames`]) to
/// pause/resume the puppet's track so its Discord "speaking" ring tracks real
/// audio instead of staying permanently green.
const SPEAKING_FLOOR_DBFS: f32 = -50.0;

/// A set of per-participant jitter buffers that mix down to one mono stream.
///
/// Shared (via `Arc`) between the LiveKit consumer tasks (producers, one per
/// subscribed track) and the single songbird playback source (consumer).
/// Interior mutability via `Mutex`; critical sections are a buffer push or a
/// few 10 ms pops plus a mix — microseconds — so contention with the songbird
/// mixer thread is negligible.
pub struct PlaybackMixer {
    buffers: Mutex<HashMap<String, JitterBuffer>>,
    /// Count of pops that returned silence *while participants were present* —
    /// i.e. every active stream underran this tick. A mid-speech gap (crackle).
    /// Genuine silence (no participants) is not counted.
    underruns: AtomicU64,
    /// Monotonic count of pushed frames whose level exceeded [`SPEAKING_FLOOR_DBFS`]
    /// — i.e. frames carrying real speech. Drives the Discord speaking gate: while
    /// this stalls (only silence / no frames arriving) the gate pauses the track so
    /// the puppet stops signalling "speaking" to Discord.
    loud_frames: AtomicU64,
}

impl PlaybackMixer {
    pub fn new() -> Self {
        Self {
            buffers: Mutex::new(HashMap::new()),
            underruns: AtomicU64::new(0),
            loud_frames: AtomicU64::new(0),
        }
    }

    /// Append a participant's mono frame, creating its jitter buffer on first use.
    /// Each buffer pre-buffers to the target depth (priming) so the real-time
    /// playback consumer isn't fed silence on every small arrival stall.
    pub fn push(&self, key: &str, mono: &[i16]) {
        // Voice-activity tally for the speaking gate. Done on the push side (not at
        // pop) because a paused track stops being read, so the gate can only learn
        // that audio resumed from frames arriving here.
        if rms_dbfs(mono) > SPEAKING_FLOOR_DBFS {
            self.loud_frames.fetch_add(1, Ordering::Relaxed);
        }
        let mut map = self.buffers.lock().unwrap_or_else(PoisonError::into_inner);
        map.entry(key.to_owned())
            .or_insert_with(|| {
                JitterBuffer::new_primed(JITTER_TARGET_MS, JITTER_MAX_MS, SAMPLE_RATE)
            })
            .push(mono);
    }

    /// Read and reset the underrun counter (for periodic health logging).
    pub fn take_underruns(&self) -> u64 {
        self.underruns.swap(0, Ordering::Relaxed)
    }

    /// Monotonic count of pushed frames that carried real speech (above
    /// [`SPEAKING_FLOOR_DBFS`]). The speaking gate polls this: an advance since the
    /// last poll means audio is flowing (resume/keep the track playing); a stall
    /// past the hangover means silence (pause the track → Discord ring clears).
    pub fn loud_frames(&self) -> u64 {
        self.loud_frames.load(Ordering::Relaxed)
    }

    /// Largest queued depth (samples) across all participant buffers — for
    /// health logging of how the cushion is holding up against burst reads.
    pub fn max_depth_samples(&self) -> usize {
        self.buffers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .map(|jb| jb.depth_samples())
            .max()
            .unwrap_or(0)
    }

    /// Drop a participant's buffer (its track ended / it left the call).
    pub fn remove(&self, key: &str) {
        self.buffers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(key);
    }

    /// Drop all buffers (call teardown), so the next call starts clean.
    pub fn clear(&self) {
        self.buffers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }

    /// Pop one mixed 10 ms mono frame (480 samples at 48 kHz).
    ///
    /// Each participant that has a full 10 ms buffered contributes one frame;
    /// those underrunning are simply absent this tick (their jitter buffer holds
    /// the partial samples for next time).  When **nobody** has a frame ready the
    /// result is silence (zeros) — the playback clock must never stall, and
    /// returning a short/empty frame would desync the songbird mixer.
    pub fn pop_10ms_mixed(&self) -> Vec<i16> {
        let mut map = self.buffers.lock().unwrap_or_else(PoisonError::into_inner);
        let had_participants = !map.is_empty();
        let frames: Vec<Vec<i16>> = map.values_mut().filter_map(|jb| jb.pop_10ms()).collect();
        if frames.is_empty() {
            // Silence with participants present = every stream underran (a gap).
            if had_participants {
                self.underruns.fetch_add(1, Ordering::Relaxed);
            }
            return vec![0i16; SAMPLES_10MS];
        }
        let refs: Vec<&[i16]> = frames.iter().map(|f| f.as_slice()).collect();
        mix_mono(&refs)
    }
}

impl Default for PlaybackMixer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Samples a participant buffer must accumulate before it releases audio
    /// (priming cushion = the mixer's target depth).
    const PRIME: usize = (JITTER_TARGET_MS * SAMPLE_RATE / 1000) as usize;

    #[test]
    fn empty_mixer_yields_silence_frame() {
        let m = PlaybackMixer::new();
        let f = m.pop_10ms_mixed();
        assert_eq!(f.len(), SAMPLES_10MS);
        assert!(f.iter().all(|&s| s == 0));
        // No participants → genuine silence, not counted as an underrun.
        assert_eq!(m.take_underruns(), 0);
    }

    #[test]
    fn single_participant_passthrough_after_priming() {
        let m = PlaybackMixer::new();
        // Push a ramp longer than the cushion; once primed, frames drain oldest
        // first, in order.
        let frame: Vec<i16> = (0..(PRIME + SAMPLES_10MS) as i16).collect();
        m.push("p1", &frame);

        let a = m.pop_10ms_mixed();
        assert_eq!(a.len(), SAMPLES_10MS);
        assert_eq!(a[0], 0, "drains oldest sample first once primed");
        let b = m.pop_10ms_mixed();
        assert_eq!(b[0], SAMPLES_10MS as i16);
    }

    #[test]
    fn withholds_until_primed() {
        let m = PlaybackMixer::new();
        // Below the cushion: a full 10 ms is present but must not be released.
        m.push("p1", &vec![123i16; SAMPLES_10MS]);
        let f = m.pop_10ms_mixed();
        assert!(f.iter().all(|&s| s == 0), "withheld while priming");
        // Participant present but yielded nothing → counts as an underrun.
        assert_eq!(m.take_underruns(), 1);
    }

    #[test]
    fn two_participants_sum_sample_wise() {
        let m = PlaybackMixer::new();
        m.push("p1", &vec![100i16; PRIME]);
        m.push("p2", &vec![50i16; PRIME]);
        let mixed = m.pop_10ms_mixed();
        assert_eq!(mixed.len(), SAMPLES_10MS);
        assert!(mixed.iter().all(|&s| s == 150));
        assert_eq!(m.take_underruns(), 0);
    }

    #[test]
    fn primed_participant_mixes_while_other_still_priming() {
        let m = PlaybackMixer::new();
        m.push("p1", &vec![200i16; PRIME]); // primed
        m.push("p2", &vec![200i16; SAMPLES_10MS]); // still priming
        // Only p1 contributes; the mix equals p1's frame, not silence.
        let mixed = m.pop_10ms_mixed();
        assert!(mixed.iter().all(|&s| s == 200));
        // p1 produced audio, so this is not an underrun.
        assert_eq!(m.take_underruns(), 0);
    }

    #[test]
    fn remove_drops_participant_audio() {
        let m = PlaybackMixer::new();
        m.push("p1", &vec![100i16; PRIME]);
        m.remove("p1");
        assert!(m.pop_10ms_mixed().iter().all(|&s| s == 0));
    }

    #[test]
    fn loud_frames_counts_only_loud_pushes() {
        let m = PlaybackMixer::new();
        m.push("p1", &vec![0i16; SAMPLES_10MS]); // digital silence (−96 dBFS)
        assert_eq!(m.loud_frames(), 0);
        m.push("p1", &vec![8000i16; SAMPLES_10MS]); // ≈ −12 dBFS, real speech
        assert_eq!(m.loud_frames(), 1);
        m.push("p1", &vec![0i16; SAMPLES_10MS]); // silence again — no advance
        assert_eq!(m.loud_frames(), 1);
    }

    #[test]
    fn loud_frames_threshold_boundary() {
        // A constant-amplitude frame has RMS = 20*log10(a/32767) dBFS. The gate
        // floor is −50 dBFS → a ≈ 32767*10^(-50/20) ≈ 103. Straddle it.
        let m = PlaybackMixer::new();
        m.push("p1", &vec![90i16; SAMPLES_10MS]); // ≈ −51.2 dBFS → below floor
        assert_eq!(m.loud_frames(), 0, "−51 dBFS must not count");
        m.push("p1", &vec![120i16; SAMPLES_10MS]); // ≈ −48.7 dBFS → above floor
        assert_eq!(m.loud_frames(), 1, "−49 dBFS must count");
    }

    #[test]
    fn loud_frames_advances_per_push_across_keys() {
        // Overflow semantics: any loud speaker advances the shared counter, so the
        // overflow track stays "speaking" while anyone in the mix is talking.
        let m = PlaybackMixer::new();
        m.push("a", &vec![8000i16; SAMPLES_10MS]);
        m.push("b", &vec![8000i16; SAMPLES_10MS]);
        assert_eq!(m.loud_frames(), 2);
    }

    #[test]
    fn loud_frames_independent_of_underruns() {
        let m = PlaybackMixer::new();
        // A loud push (below the prime cushion) is withheld → counts as an underrun
        // on pop, but still tallies as a loud frame on push. The two are orthogonal.
        m.push("p1", &vec![8000i16; SAMPLES_10MS]);
        let _ = m.pop_10ms_mixed();
        assert_eq!(m.loud_frames(), 1);
        assert_eq!(m.take_underruns(), 1);
        assert_eq!(m.loud_frames(), 1, "reading underruns must not touch loud_frames");
    }
}
