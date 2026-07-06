//! Deterministic test-signal synthesis for the audio-integrity oracle (#38).
//!
//! Golden-file strategy: **no WAV blobs in git**. A probe is synthesized from a
//! `(seed, duration)` pair — the same inputs always produce a bit-identical
//! signal, so hermetic tests and the live E2E harness (which writes the probe
//! to a WAV at runtime via `wav::write_wav_mono_48k`) share one reference
//! without committing binaries.
//!
//! The signal is designed to arm every detector in [`crate::oracle`]:
//! * a **log chirp sweep** (spectrally distinctive — a codec that mangles it
//!   shows up in the correlation score),
//! * **tone bursts separated by true silence** (the dropout detector needs
//!   source-active regions *and* genuine silence to calibrate against),
//! * an **amplitude-ramped** passage (exercises gain normalization),
//! * short raised-cosine fades on every voiced segment so the *source* has no
//!   inherent discontinuities — any click in the captured side is then the
//!   pipeline's fault, not the probe's.

/// One probe cycle in milliseconds; longer durations repeat the cycle with
/// seed-varied phases/frequencies per repetition.
const CYCLE_MS: u32 = 1000;
/// Raised-cosine fade applied to each voiced segment's head and tail.
const FADE_MS: u32 = 5;

const SAMPLE_RATE: f64 = 48_000.0;
const SAMPLES_PER_MS: usize = 48;

/// xorshift64* — tiny, deterministic, good enough to decorrelate probe cycles.
/// (Not `rand`: zero deps, and reproducibility across platforms is the point.)
fn xorshift(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x.wrapping_mul(0x2545_F491_4F6C_DD1D)
}

/// A float in `[0, 1)` from the generator.
fn unit(state: &mut u64) -> f64 {
    (xorshift(state) >> 11) as f64 / (1u64 << 53) as f64
}

/// Synthesize a mono 48 kHz probe of `dur_ms` milliseconds.
///
/// Same `(seed, dur_ms)` ⇒ bit-identical output. Peak amplitude stays below
/// ~0.75 full-scale so a two-probe mix (overflow scenarios) has headroom
/// before `mix_mono`'s saturating clamp.
pub fn synth_probe_mono_48k(seed: u64, dur_ms: u32) -> Vec<i16> {
    let total = dur_ms as usize * SAMPLES_PER_MS;
    let mut out = vec![0i16; total];
    let mut rng = seed ^ 0x9E37_79B9_7F4A_7C15; // avoid the all-zero seed trap
    if rng == 0 {
        rng = 0x9E37_79B9_7F4A_7C15;
    }

    let mut cycle_start_ms = 0u32;
    while cycle_start_ms < dur_ms {
        // Per-cycle randomized parameters (deterministic from the seed stream).
        let chirp_f0 = 150.0 + unit(&mut rng) * 150.0; // 150–300 Hz
        let chirp_f1 = 2500.0 + unit(&mut rng) * 2500.0; // 2.5–5 kHz
        let tone_f = 300.0 + unit(&mut rng) * 600.0; // 300–900 Hz
        let phase0 = unit(&mut rng) * std::f64::consts::TAU;

        // Segment layout within the cycle, with per-cycle jittered lengths and
        // starts. The jitter is load-bearing: with a FIXED layout every cycle
        // shares one amplitude envelope, making envelope-based alignment
        // ambiguous at ±1 cycle (the oracle's coarse aligner would happily
        // lock a damaged capture onto the wrong cycle). Ranges below keep two
        // *guaranteed* digital-silence windows per cycle regardless of seed:
        // [430,470) and [980,1000) ms.
        // chirp [0, 350–430) | tone [470–520, +120–180) ends ≤700
        // | ramped chirp [700–740, +180–240) ends ≤980
        let chirp_len = 350 + (unit(&mut rng) * 80.0) as u32;
        let tone_start = 470 + (unit(&mut rng) * 50.0) as u32;
        let tone_len = 120 + (unit(&mut rng) * 60.0) as u32;
        let ramp_start = 700 + (unit(&mut rng) * 40.0) as u32;
        let ramp_len = 180 + (unit(&mut rng) * 60.0) as u32;

        write_chirp(&mut out, cycle_start_ms, chirp_len, chirp_f0, chirp_f1, phase0, 0.5, 0.5, dur_ms);
        write_tone(&mut out, cycle_start_ms + tone_start, tone_len, tone_f, phase0, 0.6, dur_ms);
        write_chirp(
            &mut out,
            cycle_start_ms + ramp_start,
            ramp_len,
            chirp_f0,
            chirp_f1,
            phase0,
            0.15,
            0.75,
            dur_ms,
        );

        cycle_start_ms += CYCLE_MS;
    }
    out
}

/// The stereo (interleaved L/R, L = R) variant, for chains that start with
/// `downmix_stereo_to_mono`. Identical channels make the downmix reference
/// exactly equal to [`synth_probe_mono_48k`] with the same inputs.
pub fn synth_probe_stereo_48k(seed: u64, dur_ms: u32) -> Vec<i16> {
    let mono = synth_probe_mono_48k(seed, dur_ms);
    let mut stereo = Vec::with_capacity(mono.len() * 2);
    for s in mono {
        stereo.push(s);
        stereo.push(s);
    }
    stereo
}

/// Raised-cosine fade factor for sample `i` of `n` in a voiced segment.
fn fade(i: usize, n: usize) -> f64 {
    let fade_n = FADE_MS as usize * SAMPLES_PER_MS;
    if i < fade_n {
        let x = i as f64 / fade_n as f64;
        0.5 - 0.5 * (std::f64::consts::PI * x).cos()
    } else if i + fade_n >= n {
        let x = (n - 1 - i) as f64 / fade_n as f64;
        0.5 - 0.5 * (std::f64::consts::PI * x).cos()
    } else {
        1.0
    }
}

/// Write a log chirp from `f0` to `f1` Hz over `len_ms`, with amplitude
/// linearly ramped `amp0 → amp1` across the segment. Clipped to `dur_ms`.
#[allow(clippy::too_many_arguments)]
fn write_chirp(
    out: &mut [i16],
    start_ms: u32,
    len_ms: u32,
    f0: f64,
    f1: f64,
    phase0: f64,
    amp0: f64,
    amp1: f64,
    dur_ms: u32,
) {
    if start_ms >= dur_ms {
        return;
    }
    let len_ms = len_ms.min(dur_ms - start_ms);
    let start = start_ms as usize * SAMPLES_PER_MS;
    let n = len_ms as usize * SAMPLES_PER_MS;
    let k = (f1 / f0).ln();
    for i in 0..n {
        let t = i as f64 / SAMPLE_RATE; // seconds into the segment
        let dur_s = n as f64 / SAMPLE_RATE;
        // Log-chirp instantaneous phase: 2π f0 dur/k (e^{k t/dur} − 1).
        let phase = phase0 + std::f64::consts::TAU * f0 * dur_s / k * ((k * t / dur_s).exp() - 1.0);
        let amp = amp0 + (amp1 - amp0) * (i as f64 / n as f64);
        let v = phase.sin() * amp * fade(i, n) * i16::MAX as f64;
        out[start + i] = v as i16;
    }
}

/// Write a constant tone at `f` Hz over `len_ms`. Clipped to `dur_ms`.
fn write_tone(out: &mut [i16], start_ms: u32, len_ms: u32, f: f64, phase0: f64, amp: f64, dur_ms: u32) {
    if start_ms >= dur_ms {
        return;
    }
    let len_ms = len_ms.min(dur_ms - start_ms);
    let start = start_ms as usize * SAMPLES_PER_MS;
    let n = len_ms as usize * SAMPLES_PER_MS;
    for i in 0..n {
        let t = i as f64 / SAMPLE_RATE;
        let v = (phase0 + std::f64::consts::TAU * f * t).sin() * amp * fade(i, n) * i16::MAX as f64;
        out[start + i] = v as i16;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rms_dbfs;

    #[test]
    fn deterministic_for_same_inputs() {
        assert_eq!(synth_probe_mono_48k(42, 1500), synth_probe_mono_48k(42, 1500));
    }

    #[test]
    fn different_seeds_differ() {
        assert_ne!(synth_probe_mono_48k(1, 1000), synth_probe_mono_48k(2, 1000));
    }

    #[test]
    fn length_matches_duration() {
        assert_eq!(synth_probe_mono_48k(7, 2000).len(), 2000 * SAMPLES_PER_MS);
    }

    #[test]
    fn has_voiced_and_true_silence_regions() {
        // The guaranteed windows must hold for EVERY seed (the layout jitter
        // ranges promise them) — spot-check a handful.
        for seed in [0u64, 3, 42, 0xDEAD_BEEF] {
            let p = synth_probe_mono_48k(seed, 1000);
            // Mid-chirp (100 ms) is inside the shortest possible chirp (350 ms).
            let voiced = &p[100 * SAMPLES_PER_MS..110 * SAMPLES_PER_MS];
            // [430,470) and [980,1000) are silence regardless of jitter.
            let gap = &p[432 * SAMPLES_PER_MS..468 * SAMPLES_PER_MS];
            let tail = &p[982 * SAMPLES_PER_MS..998 * SAMPLES_PER_MS];
            assert!(rms_dbfs(voiced) > -20.0, "seed {seed}: chirp region should be loud");
            assert_eq!(rms_dbfs(gap), -96.0, "seed {seed}: mid gap should be digital silence");
            assert_eq!(rms_dbfs(tail), -96.0, "seed {seed}: cycle tail should be digital silence");
        }
    }

    #[test]
    fn stereo_downmixes_to_the_mono_probe() {
        let mono = synth_probe_mono_48k(9, 1000);
        let stereo = synth_probe_stereo_48k(9, 1000);
        assert_eq!(crate::downmix_stereo_to_mono(&stereo), mono);
    }

    #[test]
    fn peak_leaves_mix_headroom() {
        // Two probes must be mixable without meaningful saturation.
        let p = synth_probe_mono_48k(5, 1000);
        let peak = p.iter().map(|&s| (s as i32).abs()).max().unwrap_or(0);
        assert!(peak <= (0.78 * i16::MAX as f64) as i32, "peak {peak} too hot");
    }
}
