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
}

impl JitterBuffer {
    /// Create a buffer sized in milliseconds at the given sample rate.
    /// `target_ms` is the desired steady-state depth; `max_ms` the overflow cap.
    pub fn new(target_ms: u32, max_ms: u32, sample_rate: u32) -> Self {
        let per_ms = sample_rate as usize / 1000;
        let max_samples = max_ms as usize * per_ms;
        Self {
            queue: VecDeque::with_capacity(max_samples),
            target_samples: target_ms as usize * per_ms,
            max_samples,
        }
    }

    /// Append a mono frame.  If this pushes the depth past `max_samples`, the
    /// oldest samples are dropped to keep latency bounded.
    pub fn push(&mut self, mono: &[i16]) {
        self.queue.extend(mono.iter().copied());
        if self.queue.len() > self.max_samples {
            let overflow = self.queue.len() - self.max_samples;
            self.queue.drain(..overflow);
        }
    }

    /// Pop exactly one 10 ms mono frame (480 samples), or `None` on underrun.
    pub fn pop_10ms(&mut self) -> Option<Vec<i16>> {
        if self.queue.len() < SAMPLES_10MS {
            return None;
        }
        Some(self.queue.drain(..SAMPLES_10MS).collect())
    }

    /// Drop all buffered samples (used on session activate/deactivate so a new
    /// call does not inherit stale audio).
    pub fn clear(&mut self) {
        self.queue.clear();
    }

    /// Current queued depth in samples (for health/level logging).
    pub fn depth_samples(&self) -> usize {
        self.queue.len()
    }

    /// The configured steady-state target depth in samples.
    pub fn target_samples(&self) -> usize {
        self.target_samples
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
    }

    #[test]
    fn clear_empties_the_buffer() {
        let mut jb = JitterBuffer::new(60, 120, SR);
        jb.push(&vec![1i16; super::super::SAMPLES_20MS]);
        jb.clear();
        assert_eq!(jb.depth_samples(), 0);
        assert!(jb.pop_10ms().is_none());
    }
}
