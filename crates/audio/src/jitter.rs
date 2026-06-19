//! A simple mono PCM jitter buffer.
//!
//! Absorbs the skew between the Discord clock (which feeds it 20 ms frames in
//! real time) and the LiveKit clock (whose libwebrtc layer drains 10 ms frames
//! via `capture_frame` backpressure), plus Discord network jitter.
//!
//! Policy:
//! * **Overflow** — when the queued depth would exceed `max_samples`, drop the
//!   oldest samples so end-to-end latency stays bounded.
//! * **Underrun** — `pop_10ms` returns `None` rather than fabricating silence.
//!   The consumer then simply does not call `capture_frame`; libwebrtc injects
//!   its own 10 ms silence pad on underrun (see `rtc/tone.rs` doc), so emitting
//!   our own would double-pad and add latency.

use std::collections::VecDeque;

use super::SAMPLES_10MS;

/// A bounded FIFO of mono i16 samples, drained in fixed 10 ms chunks.
pub struct JitterBuffer {
    queue: VecDeque<i16>,
    /// Informational target depth (used for health logging, not enforced on push).
    target_samples: usize,
    /// Hard cap; pushing beyond this drops the oldest samples.
    max_samples: usize,
    /// Pre-buffer threshold. `0` disables priming (releases as soon as 10 ms is
    /// available — the original behavior). When non-zero, `pop_10ms` withholds
    /// frames until the queue first reaches this depth, then drains until empty
    /// before re-priming.  This keeps a cushion against arrival jitter so the
    /// consumer doesn't get silence injected mid-stream on every small stall.
    prime_samples: usize,
    /// Whether the cushion is currently filled (we may drain).  Starts `true`
    /// when priming is disabled, `false` when it is enabled (must fill first).
    primed: bool,
    /// Cumulative count of samples discarded by overflow drops (see `push`).
    /// Diagnostic only — surfaced via [`dropped_samples`](Self::dropped_samples)
    /// for health logging so a persistent backlog (a suspect for issue #32's
    /// "random audio") is visible instead of silent.
    dropped_samples: u64,
}

impl JitterBuffer {
    /// Create a buffer sized in milliseconds at the given sample rate.
    /// `target_ms` is the desired steady-state depth; `max_ms` the overflow cap.
    /// No pre-buffering: frames are released as soon as 10 ms is queued.
    pub fn new(target_ms: u32, max_ms: u32, sample_rate: u32) -> Self {
        Self::build(target_ms, max_ms, sample_rate, false)
    }

    /// Like [`new`](Self::new) but with **pre-buffering**: `pop_10ms` returns
    /// `None` until the queue first reaches `target_ms` of depth, building a
    /// cushion before playback starts (and re-builds it after a full underrun).
    /// Use this on the playback (LiveKit → Discord) path, where the consumer
    /// pulls in real time and an empty buffer would crackle.
    pub fn new_primed(target_ms: u32, max_ms: u32, sample_rate: u32) -> Self {
        Self::build(target_ms, max_ms, sample_rate, true)
    }

    fn build(target_ms: u32, max_ms: u32, sample_rate: u32, prime: bool) -> Self {
        let per_ms = sample_rate as usize / 1000;
        let max_samples = max_ms as usize * per_ms;
        let target_samples = target_ms as usize * per_ms;
        Self {
            queue: VecDeque::with_capacity(max_samples),
            target_samples,
            max_samples,
            prime_samples: if prime { target_samples } else { 0 },
            primed: !prime,
            dropped_samples: 0,
        }
    }

    /// Append a mono frame.  If this pushes the depth past `max_samples`, the
    /// oldest samples are dropped to keep latency bounded.
    pub fn push(&mut self, mono: &[i16]) {
        self.queue.extend(mono.iter().copied());
        if self.queue.len() > self.max_samples {
            let overflow = self.queue.len() - self.max_samples;
            self.dropped_samples = self.dropped_samples.saturating_add(overflow as u64);
            self.queue.drain(..overflow);
        }
    }

    /// Pop exactly one 10 ms mono frame (480 samples), or `None` on underrun.
    ///
    /// With priming enabled, returns `None` until the cushion is first built
    /// (depth reaches `prime_samples`).  It does **not** re-enter that filling
    /// state on a later transient underrun: a momentary miss conceals a single
    /// frame (`None`) and the next available frame is released immediately.
    /// Re-priming on every touch of empty was a bug — it forced a full
    /// cushion-rebuild from zero each time, which kept the mean depth far below
    /// target and made the buffer re-underrun continuously.  Since producer and
    /// consumer have no net rate drift, priming once and letting the depth
    /// oscillate around the target is stable.  `clear()` re-arms priming for a
    /// fresh call.
    pub fn pop_10ms(&mut self) -> Option<Vec<i16>> {
        if !self.primed {
            if self.queue.len() >= self.prime_samples {
                self.primed = true;
            } else {
                return None; // building the initial cushion
            }
        }

        if self.queue.len() < SAMPLES_10MS {
            return None; // transient underrun: conceal one frame, stay primed
        }
        Some(self.queue.drain(..SAMPLES_10MS).collect())
    }

    /// Drop all buffered samples (used on session activate/deactivate so a new
    /// call does not inherit stale audio).  Re-enters the priming state if
    /// priming is enabled, so the cushion is rebuilt before the next release.
    pub fn clear(&mut self) {
        self.queue.clear();
        self.primed = self.prime_samples == 0;
    }

    /// Current queued depth in samples (for health/level logging).
    pub fn depth_samples(&self) -> usize {
        self.queue.len()
    }

    /// The configured steady-state target depth in samples.
    pub fn target_samples(&self) -> usize {
        self.target_samples
    }

    /// Cumulative samples dropped to overflow since creation (diagnostic).
    /// A steadily climbing value means the consumer can't keep up and audio is
    /// being silently discarded mid-stream — a candidate cause of glitching.
    pub fn dropped_samples(&self) -> u64 {
        self.dropped_samples
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SR: u32 = 48_000;

    #[test]
    fn push_20ms_yields_two_10ms_frames() {
        let mut jb = JitterBuffer::new(60, 120, SR);
        let frame: Vec<i16> = (0..super::super::SAMPLES_20MS as i16).collect();
        jb.push(&frame);

        let a = jb.pop_10ms().expect("first 10ms");
        let b = jb.pop_10ms().expect("second 10ms");
        assert_eq!(a.len(), SAMPLES_10MS);
        assert_eq!(b.len(), SAMPLES_10MS);
        assert_eq!(a[0], 0);
        assert_eq!(b[0], SAMPLES_10MS as i16);
        assert!(jb.pop_10ms().is_none(), "buffer should now be empty");
    }

    #[test]
    fn underrun_returns_none_not_silence() {
        let mut jb = JitterBuffer::new(60, 120, SR);
        assert!(jb.pop_10ms().is_none());
        // Partial frame still underruns.
        jb.push(&vec![0i16; SAMPLES_10MS - 1]);
        assert!(jb.pop_10ms().is_none());
        assert_eq!(jb.depth_samples(), SAMPLES_10MS - 1);
    }

    #[test]
    fn overflow_drops_oldest_and_caps_depth() {
        // max = 20ms = 960 samples.
        let mut jb = JitterBuffer::new(10, 20, SR);
        // Push 40ms of a ramp; the first 20ms worth must be dropped.
        let frame: Vec<i16> = (0..(SAMPLES_10MS * 4) as i16).collect();
        jb.push(&frame);
        assert_eq!(jb.depth_samples(), SAMPLES_10MS * 2, "depth capped at max");

        // The oldest surviving sample is the first that was NOT dropped.
        let popped = jb.pop_10ms().expect("frame");
        let dropped = (SAMPLES_10MS * 4) - (SAMPLES_10MS * 2);
        assert_eq!(popped[0], dropped as i16);
        // The overflow counter records exactly what was discarded.
        assert_eq!(jb.dropped_samples(), dropped as u64);
    }

    #[test]
    fn dropped_samples_starts_zero_and_only_counts_overflow() {
        let mut jb = JitterBuffer::new(60, 120, SR);
        assert_eq!(jb.dropped_samples(), 0);
        // A normal push well under the cap drops nothing.
        jb.push(&vec![0i16; SAMPLES_10MS]);
        assert_eq!(jb.dropped_samples(), 0);
    }

    #[test]
    fn clear_empties_the_buffer() {
        let mut jb = JitterBuffer::new(60, 120, SR);
        jb.push(&vec![1i16; super::super::SAMPLES_20MS]);
        jb.clear();
        assert_eq!(jb.depth_samples(), 0);
        assert!(jb.pop_10ms().is_none());
    }

    #[test]
    fn primed_withholds_until_target_then_drains() {
        // 20 ms target → must buffer 960 samples before releasing anything.
        let mut jb = JitterBuffer::new_primed(20, 120, SR);

        // 10 ms queued: below the 20 ms cushion → still None even though a full
        // frame's worth is present.
        jb.push(&vec![7i16; SAMPLES_10MS]);
        assert!(jb.pop_10ms().is_none(), "should withhold while priming");

        // Reach the target: now it releases, and keeps releasing until empty.
        jb.push(&vec![7i16; SAMPLES_10MS]);
        assert_eq!(jb.depth_samples(), SAMPLES_10MS * 2);
        assert!(jb.pop_10ms().is_some(), "primed: first frame");
        assert!(jb.pop_10ms().is_some(), "primed: drains down to empty");
        assert!(jb.pop_10ms().is_none(), "empty");
    }

    #[test]
    fn primed_once_conceals_transient_underrun_without_reprime() {
        let mut jb = JitterBuffer::new_primed(20, 120, SR);
        // Prime (20 ms = 2 frames) and fully drain.
        jb.push(&vec![1i16; SAMPLES_10MS * 2]);
        assert!(jb.pop_10ms().is_some());
        assert!(jb.pop_10ms().is_some());
        assert!(jb.pop_10ms().is_none()); // transient underrun → conceal 1 frame

        // Stays primed: a single fresh frame is released immediately, NOT
        // withheld to rebuild the whole cushion (that re-prime behavior was the
        // crackle bug).
        jb.push(&vec![1i16; SAMPLES_10MS]);
        assert!(jb.pop_10ms().is_some(), "released immediately after underrun");
    }

    #[test]
    fn unprimed_releases_immediately() {
        // Regression: the default (Phase 5 outbound) path must be unchanged —
        // a single 10 ms frame is released right away, no pre-buffering.
        let mut jb = JitterBuffer::new(60, 120, SR);
        jb.push(&vec![3i16; SAMPLES_10MS]);
        assert!(jb.pop_10ms().is_some());
    }
}
