//! WAV read/write helpers for the audio core's file-in/out test harness.
//!
//! `CaptureSink` (in `capture.rs`) only writes interleaved-stereo WAVs; here we
//! provide a mono 48 kHz reader and writer so a `.wav` can be fed through the
//! reframe/mix/jitter path with no live SDKs, and the result written back out
//! for inspection or assertion.

use anyhow::{Context, Result};
use hound::{SampleFormat, WavSpec, WavWriter};

use super::downmix_stereo_to_mono;

/// Mono 48 kHz 16-bit signed PCM — the audio core's canonical format.
const MONO_SPEC: WavSpec = WavSpec {
    channels: 1,
    sample_rate: 48_000,
    bits_per_sample: 16,
    sample_format: SampleFormat::Int,
};

/// Read a 16-bit PCM WAV into mono i16 samples.
///
/// * Mono input is returned as-is.
/// * Stereo input is downmixed (L/R averaged) via [`downmix_stereo_to_mono`].
/// * The sample rate is **not** resampled; a warning-worthy mismatch is the
///   caller's responsibility (test fixtures are authored at 48 kHz).
pub fn read_wav_mono_48k(path: &str) -> Result<Vec<i16>> {
    let mut reader = hound::WavReader::open(path)
        .with_context(|| format!("opening WAV '{path}'"))?;
    let spec = reader.spec();
    let samples: Vec<i16> = reader
        .samples::<i16>()
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("reading samples from '{path}'"))?;

    let mono = match spec.channels {
        1 => samples,
        2 => downmix_stereo_to_mono(&samples),
        n => anyhow::bail!("unsupported channel count {n} in '{path}'"),
    };
    Ok(mono)
}

/// Write mono i16 samples as a 48 kHz WAV.
pub fn write_wav_mono_48k(path: &str, samples: &[i16]) -> Result<()> {
    let mut writer = WavWriter::create(path, MONO_SPEC)
        .with_context(|| format!("creating WAV '{path}'"))?;
    for &s in samples {
        writer
            .write_sample(s)
            .with_context(|| format!("writing sample to '{path}'"))?;
    }
    writer.finalize().with_context(|| format!("finalising '{path}'"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audio::{jitter::JitterBuffer, mix_mono, rms_dbfs, SAMPLES_20MS};

    /// Build a temp path under the OS temp dir (no Date/rand: use the test name).
    fn tmp(name: &str) -> String {
        std::env::temp_dir()
            .join(format!("nvb_test_{name}.wav"))
            .to_string_lossy()
            .into_owned()
    }

    /// Generate `frames` worth of a 440 Hz mono sine at 48 kHz (20 ms frames).
    fn sine_mono(frames: usize) -> Vec<i16> {
        let total = frames * SAMPLES_20MS;
        (0..total)
            .map(|i| {
                let phase = std::f64::consts::TAU * 440.0 * i as f64 / 48_000.0;
                (phase.sin() * 0.3 * i16::MAX as f64) as i16
            })
            .collect()
    }

    #[test]
    fn wav_roundtrip_mono() {
        let path = tmp("roundtrip");
        let input = sine_mono(10);
        write_wav_mono_48k(&path, &input).expect("write");
        let read = read_wav_mono_48k(&path).expect("read");
        assert_eq!(read, input);
        let _ = std::fs::remove_file(&path);
    }

    /// The "feed a .wav to the publish path without Discord" harness:
    /// wav → jitter buffer → collected 10 ms frames → wav. Assert the audio
    /// survives intact (sample count and RMS preserved within tolerance).
    #[test]
    fn wav_through_jitter_path_preserves_audio() {
        let in_path = tmp("jitter_in");
        let out_path = tmp("jitter_out");
        let input = sine_mono(50); // 1 second
        write_wav_mono_48k(&in_path, &input).expect("write input");

        let mono = read_wav_mono_48k(&in_path).expect("read input");
        let mut jb = JitterBuffer::new(60, 120, 48_000);
        let mut out = Vec::new();
        // Feed 20 ms at a time, draining all available 10 ms frames each step.
        for chunk in mono.chunks(SAMPLES_20MS) {
            jb.push(chunk);
            while let Some(f) = jb.pop_10ms() {
                out.extend_from_slice(&f);
            }
        }
        write_wav_mono_48k(&out_path, &out).expect("write output");

        // No overflow at this depth, so every sample should survive.
        assert_eq!(out.len(), input.len(), "sample count preserved");
        let din = rms_dbfs(&input);
        let dout = rms_dbfs(&out);
        assert!((din - dout).abs() < 0.1, "RMS preserved: {din} vs {dout}");

        let _ = std::fs::remove_file(&in_path);
        let _ = std::fs::remove_file(&out_path);
    }

    /// Two offset mono fixtures mixed: the sum (saturating) is what the tick
    /// mixer produces when two speakers talk at once.
    #[test]
    fn two_streams_mix() {
        let a = sine_mono(5);
        let b: Vec<i16> = sine_mono(5).iter().map(|&s| s / 2).collect();
        let mixed = mix_mono(&[&a, &b]);
        assert_eq!(mixed.len(), a.len());
        // Mixed level is louder than either input (constructive, same phase).
        assert!(rms_dbfs(&mixed) > rms_dbfs(&a));
    }
}
