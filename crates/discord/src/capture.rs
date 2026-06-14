//! `CaptureSink` — SDK-agnostic per-SSRC WAV writer.
//!
//! Receives interleaved stereo i16 PCM frames (48 kHz, 20 ms per `VoiceTick`)
//! and writes them lazily to `capture_<ssrc>.wav`.  Decoupled from songbird so
//! it can be unit-tested with raw PCM and reused by the audio core in Phase 5.

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::BufWriter,
    sync::Mutex,
};

use hound::{SampleFormat, WavSpec, WavWriter};

/// Frames per second at 48 kHz with 20 ms ticks: 50 ticks/s.
/// Log RMS roughly every second (every 50 frames per SSRC).
const RMS_LOG_INTERVAL: u32 = 50;

/// WAV format used for all captures: stereo, 48 kHz, 16-bit signed PCM.
const WAV_SPEC: WavSpec = WavSpec {
    channels: 2,
    sample_rate: 48_000,
    bits_per_sample: 16,
    sample_format: SampleFormat::Int,
};

struct SsrcState {
    writer: WavWriter<BufWriter<File>>,
    /// How many 20 ms frames have been written for this SSRC.
    frame_count: u32,
}

/// Shared, thread-safe capture sink.  Wrap in `Arc` and clone freely.
pub struct CaptureSink {
    inner: Mutex<HashMap<u32, SsrcState>>,
}

impl Default for CaptureSink {
    fn default() -> Self {
        Self::new()
    }
}

impl CaptureSink {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Write one 20 ms PCM frame for the given SSRC.
    ///
    /// Creates `capture_<ssrc>.wav` on first call for that SSRC.
    /// Computes RMS and logs it roughly once per second.
    /// All errors are logged as warnings; the audio path never panics.
    pub fn write_frame(&self, ssrc: u32, pcm: &[i16]) {
        let mut map = self.inner.lock().expect("CaptureSink mutex poisoned");

        let state = map.entry(ssrc).or_insert_with(|| {
            let path = format!("capture_{ssrc}.wav");
            match Self::open_writer(&path) {
                Ok(w) => {
                    tracing::info!(ssrc, path, "capturing SSRC → WAV file");
                    SsrcState { writer: w, frame_count: 0 }
                }
                Err(e) => {
                    // This should be unreachable in practice, but we can't
                    // return an error from or_insert_with.  Log and write to a
                    // temp path so the caller doesn't panic.
                    let fallback = format!("capture_{ssrc}_err.wav");
                    tracing::warn!(ssrc, error = %e, fallback, "failed to open WAV; using fallback");
                    let w = Self::open_writer(&fallback)
                        .expect("even fallback WAV open failed — filesystem error");
                    SsrcState { writer: w, frame_count: 0 }
                }
            }
        });

        // Write samples; skip the rest of the frame on any individual error.
        for &sample in pcm {
            if let Err(e) = state.writer.write_sample(sample) {
                tracing::warn!(ssrc, error = %e, "WAV write_sample failed; dropping frame tail");
                break;
            }
        }

        state.frame_count += 1;

        // Log RMS once per second.
        if state.frame_count.is_multiple_of(RMS_LOG_INTERVAL) {
            let rms = compute_rms(pcm);
            tracing::info!(ssrc, rms_db = format_args!("{rms:.1}"), frame = state.frame_count, "audio level");
        }
    }

    /// Finalise all open WAV files.
    ///
    /// **Must** be called before exit — `hound::WavWriter` does not write the
    /// correct RIFF/data header lengths on drop.  Calling this produces files
    /// that every player can open; skipping it leaves truncated headers.
    pub fn finalize_all(&self) {
        let mut map = self.inner.lock().expect("CaptureSink mutex poisoned");
        for (ssrc, state) in map.drain() {
            let frames = state.frame_count;
            match state.writer.finalize() {
                Ok(()) => tracing::info!(ssrc, frames, "WAV finalised"),
                Err(e) => tracing::warn!(ssrc, error = %e, "WAV finalise failed"),
            }
        }
    }

    fn open_writer(path: &str) -> hound::Result<WavWriter<BufWriter<File>>> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        WavWriter::new(BufWriter::new(file), WAV_SPEC)
    }
}

/// Compute dBFS RMS for a block of i16 samples.
/// Returns −∞ (represented as −96.0) for silence.
fn compute_rms(pcm: &[i16]) -> f32 {
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
    fn rms_silence_is_negative_infinity_floor() {
        assert_eq!(compute_rms(&[]), -96.0);
        assert_eq!(compute_rms(&[0, 0, 0]), -96.0);
    }

    #[test]
    fn rms_full_scale_sine_approaches_minus_3db() {
        // A square wave at ±i16::MAX would be exactly 0 dBFS.
        // A sine at full scale is ≈ −3 dBFS.
        let samples: Vec<i16> = (0..480)
            .map(|i| {
                let phase = 2.0 * std::f64::consts::PI * i as f64 / 48.0;
                (phase.sin() * i16::MAX as f64) as i16
            })
            .collect();
        let db = compute_rms(&samples);
        // Sine RMS = 1/√2, so ≈ −3.01 dBFS.
        assert!((db - (-3.01)).abs() < 0.2, "rms = {db}");
    }
}
