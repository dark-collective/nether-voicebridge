//! Offline replay of the e2e audio oracle (stage 1 global align + stage 2
//! per-cycle scoring, mirroring `nvb_e2e::scenarios::audio::oracle_assert`)
//! against a probe/captured WAV pair from a run artifact.
//!
//! usage: cargo run -p nvb-audio --example oracle_replay -- <probe.wav> <captured.wav> <stage1_max_align_ms>...
//! (WAVs: `d2m_probe.wav` / `d2m_captured.wav` from `e2e-artifacts/<scenario>/`)
use nvb_audio::oracle::{compare_mono_48k, OracleConfig};
use nvb_audio::wav::read_wav_mono_48k;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let probe = read_wav_mono_48k(&args[1]).expect("probe wav");
    let captured = read_wav_mono_48k(&args[2]).expect("captured wav");
    println!(
        "probe {:.2}s captured {:.2}s",
        probe.len() as f64 / 48000.0,
        captured.len() as f64 / 48000.0
    );
    for b in args[3..].iter().map(|s| s.parse::<u32>().expect("bound ms")) {
        let global =
            compare_mono_48k(&probe, &captured, &OracleConfig { max_align_ms: b, ..Default::default() });
        println!(
            "stage1 bound={b}ms: delay={} ({:.3}s) ncc={:.3}",
            global.delay_samples,
            global.delay_samples as f64 / 48000.0,
            global.ncc
        );
        let base = global.delay_samples;
        let cycle_len = 48_000usize;
        let pad = 24_000isize;
        let cycles = probe.len() / cycle_len;
        let (mut sum, mut good, mut drops) = (0.0f64, 0usize, 0usize);
        for k in 0..cycles {
            let src = &probe[k * cycle_len..((k + 1) * cycle_len).min(probe.len())];
            let start = (base + (k * cycle_len) as isize - pad).max(0) as usize;
            let end = ((base + ((k + 1) * cycle_len) as isize + pad).max(0) as usize).min(captured.len());
            let window = if start < end { &captured[start..end] } else { &[][..] };
            let rep = compare_mono_48k(
                src,
                window,
                &OracleConfig { max_align_ms: 600, ..Default::default() },
            );
            println!(
                "  cycle {k}: ncc={:.3} seg_snr={:.1}dB gain={:.2} delay={} dropouts={} glitches={}{}",
                rep.ncc,
                rep.seg_snr_db,
                rep.gain,
                rep.delay_samples,
                rep.dropouts.len(),
                rep.glitches.len(),
                if rep.ncc >= 0.85 { "" } else { "  <-- rough" }
            );
            sum += rep.ncc as f64;
            if rep.ncc >= 0.85 {
                good += 1;
            }
            drops += rep.dropouts.len();
        }
        println!(
            "  => mean_ncc={:.3} good_cycles={good}/{cycles} dropouts={drops} → {}",
            sum / cycles as f64,
            if sum / cycles as f64 >= 0.75 && good * 2 > cycles { "PASS" } else { "FAIL" }
        );
    }
}
