//! Tier-1 hermetic audio-integrity regression tests (#38 Phase B).
//!
//! Feeds deterministic probe signals through the real in-process audio chains
//! and asserts intactness with the oracle. Thresholds here are deliberately
//! much stricter than [`OracleConfig::default`] (which is sized for the live
//! Opus path): these chains are lossless pass-throughs, so anything below
//! near-perfect is a regression.
//!
//! Also contains the oracle's **negative self-tests**: every detector must
//! fire on its injected fault, or the oracle is a useless always-green.

use nvb_audio::jitter::JitterBuffer;
use nvb_audio::mixer::PlaybackMixer;
use nvb_audio::oracle::{compare_mono_48k, OracleConfig, OracleReport};
use nvb_audio::probe::{synth_probe_mono_48k, synth_probe_stereo_48k};
use nvb_audio::{downmix_stereo_to_mono, mix_mono, SAMPLES_10MS, SAMPLES_20MS};

/// Strict thresholds for lossless in-process chains.
fn strict() -> OracleConfig {
    OracleConfig { min_ncc: 0.99, min_seg_snr_db: 40.0, ..OracleConfig::default() }
}

fn assert_pass(report: &OracleReport, what: &str) {
    assert!(
        report.pass,
        "{what} should pass, got ncc={} seg_snr={}dB gain={} delay={} dropouts={:?} glitches={:?}",
        report.ncc, report.seg_snr_db, report.gain, report.delay_samples, report.dropouts,
        report.glitches
    );
}

/// Drive mono probes through the Matrix→Discord playback shape in real-time
/// lockstep: per 20 ms tick, push each participant's frame into the
/// [`PlaybackMixer`] and pop two mixed 10 ms frames. Lockstep matters — the
/// mixer's per-participant jitter buffers cap depth at 500 ms, so pushing the
/// whole signal up front would overflow-drop the head.
///
/// Returns the captured mix (leading prime-cushion silence included — the
/// oracle's alignment absorbs it).
fn run_mixer_chain(participants: &[&[i16]]) -> Vec<i16> {
    let mixer = PlaybackMixer::new();
    let len = participants.iter().map(|p| p.len()).max().unwrap_or(0);
    let mut captured = Vec::with_capacity(len + SAMPLES_10MS * 40);
    let mut pos = 0usize;
    while pos < len {
        for (i, p) in participants.iter().enumerate() {
            if pos < p.len() {
                let end = (pos + SAMPLES_20MS).min(p.len());
                mixer.push(&format!("p{i}"), &p[pos..end]);
            }
        }
        pos += SAMPLES_20MS;
        captured.extend(mixer.pop_10ms_mixed());
        captured.extend(mixer.pop_10ms_mixed());
    }
    // Drain what the prime cushion is still holding (150 ms) plus slack.
    for _ in 0..25 {
        captured.extend(mixer.pop_10ms_mixed());
    }
    captured
}

/// The Discord→Matrix in-process shape: 20 ms interleaved stereo → downmix →
/// jitter reframe (20 ms in / 10 ms out) → mixer → 10 ms drain.
#[test]
fn chain_downmix_jitter_mix_is_intact() {
    let stereo = synth_probe_stereo_48k(0xD15C, 2000);
    let reference = downmix_stereo_to_mono(&stereo);

    // Stage 1: downmix + jitter reframe, tick by tick.
    let mut jb = JitterBuffer::new(60, 120, 48_000);
    let mut reframed = Vec::with_capacity(reference.len());
    for frame in stereo.chunks(SAMPLES_20MS * 2) {
        jb.push(&downmix_stereo_to_mono(frame));
        while let Some(f) = jb.pop_10ms() {
            reframed.extend(f);
        }
    }
    // Stage 2: playback mixer as the single participant.
    let captured = run_mixer_chain(&[&reframed]);

    let report = compare_mono_48k(&reference, &captured, &strict());
    assert_pass(&report, "downmix→jitter→mix chain");
    assert!(report.delay_samples > 0, "prime cushion should show up as delay");
    assert!((report.gain - 1.0).abs() < 0.01, "pass-through gain ≈ 1, got {}", report.gain);
}

/// Overflow-mix integrity: two speakers through one mixer must equal the
/// saturating reference mix of the two probes.
#[test]
fn chain_overflow_mix_is_intact() {
    let a = synth_probe_mono_48k(0xA, 2000);
    let b = synth_probe_mono_48k(0xB, 2000);
    let reference = mix_mono(&[&a, &b]);

    let captured = run_mixer_chain(&[&a, &b]);

    let report = compare_mono_48k(&reference, &captured, &strict());
    assert_pass(&report, "two-speaker overflow mix");
}

/// The WAV round-trip used by the Tier-3 harness artifacts must itself be
/// lossless — otherwise every live comparison inherits its noise.
#[test]
fn chain_wav_roundtrip_is_lossless() {
    let probe = synth_probe_mono_48k(0x3AF, 1500);
    let path = std::env::temp_dir().join(format!("nvb_oracle_roundtrip_{}.wav", std::process::id()));
    let path = path.to_string_lossy().into_owned();

    nvb_audio::wav::write_wav_mono_48k(&path, &probe).expect("write");
    let read_back = nvb_audio::wav::read_wav_mono_48k(&path).expect("read");
    let _ = std::fs::remove_file(&path);

    assert_eq!(probe, read_back, "WAV round-trip must be bit-exact");
    let report = compare_mono_48k(&probe, &read_back, &strict());
    assert_pass(&report, "WAV round-trip");
    assert_eq!(report.delay_samples, 0);
}

/// Negative: a 200 ms hole in a voiced region must trip the dropout detector.
#[test]
fn detector_fires_on_injected_dropout() {
    let probe = synth_probe_mono_48k(0xD0, 2000);
    let mut mangled = probe.clone();
    // [1200, 1400) ms sits inside the second cycle's chirp — source-active.
    mangled[1200 * 48..1400 * 48].fill(0);

    let report = compare_mono_48k(&probe, &mangled, &OracleConfig::default());
    assert!(!report.pass, "dropout must fail the oracle");
    assert!(
        report.dropouts.iter().any(|a| a.start_ms < 1400 && a.end_ms > 1200),
        "dropout anomaly should cover the injected hole, got {:?}",
        report.dropouts
    );
}

/// Negative: an injected broadband click must trip the glitch detector.
#[test]
fn detector_fires_on_injected_glitch() {
    let probe = synth_probe_mono_48k(0x61, 2000);
    let mut mangled = probe.clone();
    // Alternating full-scale samples mid-tone ([500,650) ms is the tone burst):
    // a discontinuity far beyond anything the source's ~900 Hz content explains.
    let at = 560 * 48;
    for (i, s) in mangled[at..at + 6].iter_mut().enumerate() {
        *s = if i % 2 == 0 { 30_000 } else { -30_000 };
    }

    let report = compare_mono_48k(&probe, &mangled, &OracleConfig::default());
    assert!(!report.pass, "glitch must fail the oracle");
    assert!(
        report.glitches.iter().any(|a| a.start_ms <= 560 && a.end_ms > 560),
        "glitch anomaly should cover the injected click, got {:?}",
        report.glitches
    );
}

/// Calibration guard: a time-shifted, gain-scaled but otherwise clean copy
/// must still PASS — the oracle absorbs latency and level differences.
#[test]
fn shifted_and_scaled_copy_still_passes() {
    let probe = synth_probe_mono_48k(0x5C, 2000);
    let mut shifted = vec![0i16; 300 * 48]; // 300 ms of leading silence
    shifted.extend(probe.iter().map(|&s| (s as f64 * 0.5) as i16));

    let report = compare_mono_48k(&probe, &shifted, &OracleConfig::default());
    assert_pass(&report, "shifted+scaled clean copy");
    assert!(
        (report.delay_samples - 300 * 48).abs() <= 48,
        "delay should be ≈ 300 ms, got {} samples",
        report.delay_samples
    );
    assert!((report.gain - 0.5).abs() < 0.02, "gain should be ≈ 0.5, got {}", report.gain);
}

/// Long-probe sweep for intermittent (#32-class) faults — minutes of signal so
/// a once-a-minute glitch has somewhere to land. `#[ignore]`d: the nightly
/// runs it via `cargo test -- --ignored`.
#[test]
#[ignore = "long-running nightly sweep (minutes of audio)"]
fn long_probe_sweep_stays_clean() {
    let stereo = synth_probe_stereo_48k(0x1096, 180_000); // 3 minutes
    let reference = downmix_stereo_to_mono(&stereo);

    let mut jb = JitterBuffer::new(60, 120, 48_000);
    let mut reframed = Vec::with_capacity(reference.len());
    for frame in stereo.chunks(SAMPLES_20MS * 2) {
        jb.push(&downmix_stereo_to_mono(frame));
        while let Some(f) = jb.pop_10ms() {
            reframed.extend(f);
        }
    }
    let captured = run_mixer_chain(&[&reframed]);

    let report = compare_mono_48k(&reference, &captured, &strict());
    assert_pass(&report, "3-minute sweep");
}

/// Deterministic low-level noise (±3 LSB), standing in for the -100 dBFS
/// dither a live capture carries where nothing was sent.
fn dither(len: usize, mut seed: u64) -> Vec<i16> {
    (0..len)
        .map(|_| {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            ((seed >> 33) % 7) as i16 - 3
        })
        .collect()
}

/// Stage-1 alignment must not be fooled by a capture whose FIRST second is
/// damaged. Nightly #418 (2026-09-29): a slip inside cycle 0 made the true
/// lag's refined score ≈ 0, so a candidate overlapping ~120 ms of tail dither
/// won the global alignment and every cycle was then scored against silence
/// (mean ncc 0.009) although cycles 2–7 were intact at ≥ 0.98. Long lead-in,
/// long noisy tail, a bad first cycle: the delay must still land on the probe.
#[test]
fn alignment_survives_a_damaged_first_second() {
    let probe = synth_probe_mono_48k(0xD2B2, 8_000);
    let lead = 3_800 * 48;
    let tail = 4_000 * 48;
    let cycle = &probe[..48_000];
    let slipped: Vec<i16> = {
        // 35 ms of the first cycle arrives late: silence, then the cycle cut short.
        let mut v = vec![0i16; 35 * 48];
        v.extend_from_slice(&cycle[..48_000 - 35 * 48]);
        v
    };
    let inverted: Vec<i16> = cycle.iter().map(|s| s.saturating_neg()).collect();
    for (what, first_cycle) in [("35 ms slip", slipped), ("phase-inverted", inverted)] {
        let mut captured = vec![0i16; lead];
        captured.extend(first_cycle);
        captured.extend_from_slice(&probe[48_000..]);
        captured.extend(dither(tail, 0x5EED));
        let rep = compare_mono_48k(
            &probe,
            &captured,
            &OracleConfig { max_align_ms: 60_000, ..OracleConfig::default() },
        );
        assert!(
            (rep.delay_samples - lead as isize).abs() <= 480,
            "{what}: delay {} should be within one hop of {lead} (ncc={})",
            rep.delay_samples,
            rep.ncc
        );
    }
}
