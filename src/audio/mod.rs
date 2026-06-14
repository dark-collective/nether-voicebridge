//! SDK-free audio core: PCM reframing, downmix, mixing.
//!
//! This module deliberately imports **no** live SDKs (`livekit`, `songbird`,
//! `tokio`) so it can be unit-tested in isolation with the wav file-in/out
//! harness (`wav`).  The Discord side feeds it 20 ms interleaved-stereo i16
//! frames (48 kHz); it produces the mono frames the LiveKit publish path wants.
//!
//! Frame sizes used throughout (all at 48 kHz):
//! * 20 ms stereo  = 960 samples/channel = 1920 interleaved i16 (Discord tick)
//! * 20 ms mono    = 960 i16             (after downmix/mix, sent over the channel)
//! * 10 ms mono    = 480 i16             (one `capture_frame` to LiveKit)

pub mod jitter;
// Per-participant playback mixer for the Matrix → Discord direction (Phase 6).
pub mod mixer;
// WAV read/write — currently exercised by the file-in/out test harness and
// reused for the Phase 6 reverse direction; not yet called from the binary.
#[allow(dead_code)]
pub mod wav;

/// Samples per channel in a 10 ms frame at 48 kHz.
pub const SAMPLES_10MS: usize = 480;
/// Samples per channel in a 20 ms frame at 48 kHz.
pub const SAMPLES_20MS: usize = 960;

/// Average the L/R of an interleaved-stereo i16 buffer into mono.
///
/// A trailing odd sample (length not a multiple of 2) is ignored — never
/// panics, since this runs on the audio hot path. Averaging is done in i32 to
/// avoid overflow, then truncated back to i16.
pub fn downmix_stereo_to_mono(stereo: &[i16]) -> Vec<i16> {
    stereo
        .chunks_exact(2)
        .map(|lr| ((lr[0] as i32 + lr[1] as i32) / 2) as i16)
        .collect()
}

/// Sum N mono frames sample-wise with a saturating i16 clamp.
///
/// * Empty input → empty `Vec`.
/// * A single frame → a copy of that frame.
/// * **Frames may differ in length** — the output is as long as the longest
///   frame, and shorter frames contribute silence past their end. songbird can
///   hand back decoded frames of differing lengths when several speakers overlap
///   in one tick (a speaker just starting, packet loss/PLC), so this MUST NOT
///   assume equal lengths — an earlier `debug_assert` here panicked the songbird
///   event thread and silently killed capture.
///
/// Saturating addition means several simultaneously-loud speakers can clip;
/// this is an accepted Phase 5 limitation (no AGC/limiter yet).
pub fn mix_mono(frames: &[&[i16]]) -> Vec<i16> {
    let Some(first) = frames.first() else {
        return Vec::new();
    };
    if frames.len() == 1 {
        return first.to_vec();
    }
    let len = frames.iter().map(|f| f.len()).max().unwrap_or(0);
    (0..len)
        .map(|i| {
            let sum: i32 = frames.iter().map(|f| f.get(i).copied().unwrap_or(0) as i32).sum();
            sum.clamp(i16::MIN as i32, i16::MAX as i32) as i16
        })
        .collect()
}

// Note: reframing 20 ms → 10 ms is done by `JitterBuffer` (push a 20 ms frame,
// pop two 10 ms frames), so no standalone split function is needed here.

/// Compute dBFS RMS for a block of i16 samples.
/// Returns −96.0 (our silence floor) for silence or empty input.
///
/// Mirrors the level metric used by `CaptureSink`; lives here so the audio
/// core can log stream levels without depending on `capture`.
pub fn rms_dbfs(pcm: &[i16]) -> f32 {
    if pcm.is_empty() {
        return -96.0;
    }
    let mean_sq: f64 = pcm
        .iter()
        .map(|&s| {
            let f = s as f64 / i16::MAX as f64;
            f * f
        })
        .sum::<f64>()
        / pcm.len() as f64;
    if mean_sq <= 0.0 {
        return -96.0;
    }
    (10.0 * mean_sq.log10()) as f32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn downmix_averages_left_and_right() {
        assert_eq!(downmix_stereo_to_mono(&[100, 200, -100, -200]), vec![150, -150]);
    }

    #[test]
    fn downmix_empty_is_empty() {
        assert_eq!(downmix_stereo_to_mono(&[]), Vec::<i16>::new());
    }

    #[test]
    fn downmix_no_overflow_at_extremes() {
        // i16::MAX + i16::MAX would overflow i16; i32 averaging keeps it sane.
        assert_eq!(
            downmix_stereo_to_mono(&[i16::MAX, i16::MAX, i16::MIN, i16::MIN]),
            vec![i16::MAX, i16::MIN]
        );
    }

    #[test]
    fn mix_empty_is_empty() {
        assert_eq!(mix_mono(&[]), Vec::<i16>::new());
    }

    #[test]
    fn mix_single_frame_passthrough() {
        let a = [1i16, 2, 3];
        assert_eq!(mix_mono(&[&a]), vec![1, 2, 3]);
    }

    #[test]
    fn mix_sums_and_saturates() {
        let a = [30_000i16, -30_000, 100];
        let b = [10_000i16, -10_000, 100];
        // 40000 clamps to i16::MAX; -40000 clamps to i16::MIN; 200 fits.
        assert_eq!(mix_mono(&[&a, &b]), vec![i16::MAX, i16::MIN, 200]);
    }

    #[test]
    fn mix_unequal_lengths_pad_with_silence() {
        // Regression: songbird can hand back different-length frames when two
        // speakers overlap in one tick. mix_mono must NOT panic (it used to
        // `debug_assert` equal length, which killed the capture thread). Output
        // is as long as the longest frame; the short one contributes silence.
        let short = [10i16, 20];
        let long = [1i16, 2, 3, 4];
        assert_eq!(mix_mono(&[&short, &long]), vec![11, 22, 3, 4]);
        // Order-independent, and the longer-first case works too.
        assert_eq!(mix_mono(&[&long, &short]), vec![11, 22, 3, 4]);
    }

    #[test]
    fn rms_silence_floor() {
        assert_eq!(rms_dbfs(&[]), -96.0);
        assert_eq!(rms_dbfs(&[0, 0, 0]), -96.0);
    }
}
