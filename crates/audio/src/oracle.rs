//! The audio-integrity oracle (#38): tolerance-based comparison of a captured
//! signal against its known source.
//!
//! Bit-exact assertion is impossible across the bridge (Opus, resampling,
//! 20 ms ↔ 10 ms reframing, jitter concealment), so the oracle instead
//! answers "did the audio cross *intact*?" with a fixed pipeline:
//!
//! 1. **Align** — find the capture delay via cross-correlation of 10 ms RMS
//!    envelopes (cheap: 100 fps, not 48 kHz), refined to sample precision.
//! 2. **Normalize** — least-squares gain fit over the aligned overlap.
//! 3. **Score** — normalized cross-correlation (NCC) + segmental SNR over
//!    source-active 10 ms segments. Thresholds gate pass/fail, but the
//!    continuous scores are always reported so nightly runs can trend them —
//!    a slow drift downward is a regression even while still "passing".
//! 4. **Dropout detector** — source-active segments that arrive silent
//!    (issue #32's symptom: gaps where there was speech).
//! 5. **Glitch detector** — sample-to-sample discontinuities in the capture
//!    that the source can't explain (issue #36's symptom: broadband clicks).
//!
//! Everything is pure `&[i16]` math (f64 accumulators), zero dependencies, no
//! panics — degenerate inputs yield a failing report, never an abort.

use super::rms_dbfs;

/// Segment size for envelopes / segmental metrics: 10 ms at 48 kHz.
const HOP: usize = 480;
/// A source segment counts as *active* (something audible was sent) above this.
const ACTIVE_FLOOR_DBFS: f32 = -40.0;
/// A captured segment counts as a *dropout* (arrived silent) below this, when
/// the source segment was active.
const DROPOUT_FLOOR_DBFS: f32 = -60.0;
/// Per-segment SNR clamp, in dB. The lower clamp keeps one disastrous segment
/// from dragging the mean below what the detectors already report; the upper
/// keeps near-lossless segments (residual ≈ 0) from inflating it to infinity.
const SEG_SNR_CLAMP_DB: (f64, f64) = (0.0, 60.0);
/// Glitch detection: a captured segment is flagged when its max sample-to-sample
/// jump exceeds `gain × source-jump × RATIO + FLOOR`. The ratio absorbs codec
/// ringing around legitimate transients; the floor absorbs quantization noise
/// in near-silent passages. Calibrated by the negative self-tests in
/// `tests/oracle_pipeline.rs` — an injected click must fire, a clean
/// pass-through must not.
const GLITCH_JUMP_RATIO: f64 = 3.0;
const GLITCH_JUMP_FLOOR: f64 = 3000.0;
/// Minimum envelope overlap (in 10 ms hops) for an alignment lag to be
/// considered at all — correlating a handful of segments is noise.
const MIN_ALIGN_OVERLAP_HOPS: usize = 10;

/// Pass/fail thresholds. `Default` is tuned for the *live* path (Opus + jitter
/// in the loop); hermetic in-process chains should assert with much stricter
/// values (see `tests/oracle_pipeline.rs`).
#[derive(Debug, Clone)]
pub struct OracleConfig {
    /// Normalized cross-correlation floor over the aligned overlap.
    pub min_ncc: f32,
    /// Mean segmental-SNR floor (dB) over source-active segments.
    pub min_seg_snr_db: f32,
    /// Maximum tolerated dropout events (merged runs, not segments).
    pub max_dropouts: usize,
    /// Maximum tolerated glitch events (merged runs, not segments).
    pub max_glitches: usize,
    /// Sanity bound on the discovered capture delay, in ms (either direction).
    pub max_align_ms: u32,
}

impl Default for OracleConfig {
    fn default() -> Self {
        Self {
            min_ncc: 0.85,
            min_seg_snr_db: 10.0,
            max_dropouts: 0,
            max_glitches: 0,
            max_align_ms: 2000,
        }
    }
}

/// A contiguous run of anomalous 10 ms segments, in ms relative to the
/// **source** timeline (post-alignment).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Anomaly {
    pub start_ms: u32,
    pub end_ms: u32,
}

/// The oracle's verdict plus every continuous score behind it (for trending).
#[derive(Debug, Clone)]
pub struct OracleReport {
    /// Discovered capture delay in samples (positive = captured lags source).
    pub delay_samples: isize,
    /// Least-squares gain of the capture relative to the source.
    pub gain: f32,
    /// Normalized cross-correlation over the aligned overlap.
    pub ncc: f32,
    /// Mean segmental SNR (dB) over source-active segments.
    pub seg_snr_db: f32,
    pub dropouts: Vec<Anomaly>,
    pub glitches: Vec<Anomaly>,
    pub pass: bool,
}

impl OracleReport {
    fn failed() -> Self {
        Self {
            delay_samples: 0,
            gain: 0.0,
            ncc: 0.0,
            seg_snr_db: 0.0,
            dropouts: Vec::new(),
            glitches: Vec::new(),
            pass: false,
        }
    }
}

/// Compare a captured mono 48 kHz signal against its known source.
///
/// `source` is what was sent (e.g. a [`crate::probe`] signal); `captured` is
/// what a far-side listener recorded. Returns a full report; `report.pass`
/// applies `cfg`'s thresholds.
pub fn compare_mono_48k(source: &[i16], captured: &[i16], cfg: &OracleConfig) -> OracleReport {
    // Need at least a correlatable overlap on both sides.
    if source.len() < HOP * MIN_ALIGN_OVERLAP_HOPS || captured.len() < HOP * MIN_ALIGN_OVERLAP_HOPS
    {
        return OracleReport::failed();
    }

    // 1. Align.
    let max_lag_samples = cfg.max_align_ms as usize * 48;
    let Some(delay) = align(source, captured, max_lag_samples) else {
        return OracleReport::failed();
    };

    // Aligned overlap: positive delay = captured lags the source.
    let (s, c) = if delay >= 0 {
        let d = delay as usize;
        if d >= captured.len() {
            return OracleReport::failed();
        }
        (source, &captured[d..])
    } else {
        let d = (-delay) as usize;
        if d >= source.len() {
            return OracleReport::failed();
        }
        (&source[d..], captured)
    };
    let n = s.len().min(c.len());
    if n < HOP * MIN_ALIGN_OVERLAP_HOPS {
        return OracleReport::failed();
    }
    let (s, c) = (&s[..n], &c[..n]);

    // 2. Normalize: least-squares gain g minimizing Σ(c − g·s)².
    let mut ss = 0f64;
    let mut sc = 0f64;
    let mut cc = 0f64;
    for i in 0..n {
        let sf = s[i] as f64;
        let cf = c[i] as f64;
        ss += sf * sf;
        sc += sf * cf;
        cc += cf * cf;
    }
    if ss <= 0.0 {
        return OracleReport::failed(); // all-silent source: nothing to verify
    }
    let gain = sc / ss;

    // 3. Global score: NCC over the overlap.
    let ncc = if cc > 0.0 { sc / (ss.sqrt() * cc.sqrt()) } else { 0.0 };

    // Segmental metrics over 10 ms hops.
    let mut snr_sum = 0f64;
    let mut active_segs = 0usize;
    let mut dropout_segs = Vec::new();
    let mut glitch_segs = Vec::new();
    for (seg, (s_seg, c_seg)) in
        s.as_chunks::<HOP>().0.iter().zip(c.as_chunks::<HOP>().0.iter()).enumerate()
    {
        let src_active = rms_dbfs(s_seg) > ACTIVE_FLOOR_DBFS;

        if src_active {
            active_segs += 1;
            // Segmental SNR of the gain-normalized residual.
            let mut sig = 0f64;
            let mut noise = 0f64;
            for i in 0..HOP {
                let ideal = gain * s_seg[i] as f64;
                let resid = c_seg[i] as f64 - ideal;
                sig += ideal * ideal;
                noise += resid * resid;
            }
            let snr = if noise > 0.0 {
                (10.0 * (sig / noise).log10()).clamp(SEG_SNR_CLAMP_DB.0, SEG_SNR_CLAMP_DB.1)
            } else {
                SEG_SNR_CLAMP_DB.1
            };
            snr_sum += snr;

            // 4. Dropout: audible was sent, silence arrived.
            if rms_dbfs(c_seg) < DROPOUT_FLOOR_DBFS {
                dropout_segs.push(seg);
            }
        }

        // 5. Glitch: a discontinuity the (gain-scaled) source can't explain.
        let src_jump = max_jump(s_seg) * gain.abs();
        let cap_jump = max_jump(c_seg);
        if cap_jump > src_jump * GLITCH_JUMP_RATIO + GLITCH_JUMP_FLOOR {
            glitch_segs.push(seg);
        }
    }
    if active_segs == 0 {
        return OracleReport::failed(); // no active audio inside the overlap
    }
    let seg_snr_db = (snr_sum / active_segs as f64) as f32;

    let dropouts = merge_runs(&dropout_segs);
    let glitches = merge_runs(&glitch_segs);

    let pass = ncc as f32 >= cfg.min_ncc
        && seg_snr_db >= cfg.min_seg_snr_db
        && dropouts.len() <= cfg.max_dropouts
        && glitches.len() <= cfg.max_glitches
        && delay.unsigned_abs() <= max_lag_samples;

    OracleReport {
        delay_samples: delay,
        gain: gain as f32,
        ncc: ncc as f32,
        seg_snr_db,
        dropouts,
        glitches,
        pass,
    }
}

/// Max absolute sample-to-sample jump within a segment.
fn max_jump(seg: &[i16]) -> f64 {
    seg.windows(2).map(|w| (w[1] as f64 - w[0] as f64).abs()).fold(0.0, f64::max)
}

/// Merge consecutive flagged 10 ms segments into [`Anomaly`] runs.
fn merge_runs(segs: &[usize]) -> Vec<Anomaly> {
    let mut out: Vec<Anomaly> = Vec::new();
    for &seg in segs {
        let start_ms = (seg * 10) as u32;
        let end_ms = start_ms + 10;
        match out.last_mut() {
            Some(last) if last.end_ms == start_ms => last.end_ms = end_ms,
            _ => out.push(Anomaly { start_ms, end_ms }),
        }
    }
    out
}

/// How many coarse alignment candidates get a sample-precision look. Damaged
/// captures (a dropout, a loud glitch) can make a WRONG envelope lag outscore
/// the true one at 10 ms resolution — but at sample resolution the true lag
/// wins by an order of magnitude, so refining a top-K and keeping the best
/// refined score makes alignment robust to the very faults being detected.
const ALIGN_CANDIDATES: usize = 5;
/// Minimum spacing between coarse candidates, in hops — adjacent lags from the
/// same correlation peak would waste the K slots.
const ALIGN_CANDIDATE_SPACING_HOPS: isize = 5;

/// Find the capture delay (samples; positive = captured lags the source), or
/// `None` when no lag in range produces a meaningful correlation.
///
/// Two stages: coarse search over 10 ms RMS envelopes (O(hops × lags) at
/// 100 fps), then sample-precision refinement of the top-K coarse candidates —
/// the best *refined* correlation wins (see [`ALIGN_CANDIDATES`]).
fn align(source: &[i16], captured: &[i16], max_lag_samples: usize) -> Option<isize> {
    let env_s = envelope(source);
    let env_c = envelope(captured);
    let max_lag_hops = (max_lag_samples / HOP) as isize;

    let mut scored: Vec<(isize, f64)> = Vec::new();
    for lag in -max_lag_hops..=max_lag_hops {
        // env_c[i + lag] pairs with env_s[i].
        let (s_off, c_off) = if lag >= 0 { (0usize, lag as usize) } else { ((-lag) as usize, 0) };
        if s_off >= env_s.len() || c_off >= env_c.len() {
            continue;
        }
        let n = (env_s.len() - s_off).min(env_c.len() - c_off);
        if n < MIN_ALIGN_OVERLAP_HOPS {
            continue;
        }
        let mut dot = 0f64;
        let mut es = 0f64;
        let mut ec = 0f64;
        for i in 0..n {
            let a = env_s[s_off + i];
            let b = env_c[c_off + i];
            dot += a * b;
            es += a * a;
            ec += b * b;
        }
        if es <= 0.0 || ec <= 0.0 {
            continue;
        }
        scored.push((lag, dot / (es.sqrt() * ec.sqrt())));
    }
    scored.sort_by(|a, b| b.1.total_cmp(&a.1));

    let mut candidates: Vec<isize> = Vec::new();
    for &(lag, _) in &scored {
        if candidates.iter().all(|&c| (c - lag).abs() >= ALIGN_CANDIDATE_SPACING_HOPS) {
            candidates.push(lag);
        }
        if candidates.len() == ALIGN_CANDIDATES {
            break;
        }
    }

    let mut best: Option<(isize, f64)> = None;
    for cand in candidates {
        if let Some((delay, score)) = refine(source, captured, cand * HOP as isize) {
            if best.is_none_or(|(_, b)| score > b) {
                best = Some((delay, score));
            }
        }
    }
    best.map(|(delay, _)| delay)
}

/// Sample-precision refinement of a coarse lag: search ±1 hop around it in two
/// passes (stride 6, then stride 1 around that winner), correlating over a
/// bounded window so long captures stay cheap. Returns `(delay, ncc)`.
fn refine(source: &[i16], captured: &[i16], coarse: isize) -> Option<(isize, f64)> {
    let score_at = |delay: isize| -> Option<f64> {
        let (s_off, c_off) =
            if delay >= 0 { (0usize, delay as usize) } else { ((-delay) as usize, 0) };
        if s_off >= source.len() || c_off >= captured.len() {
            return None;
        }
        let n = (source.len() - s_off).min(captured.len() - c_off).min(48_000);
        if n < HOP {
            return None;
        }
        let mut dot = 0f64;
        let mut es = 0f64;
        let mut ec = 0f64;
        for i in 0..n {
            let a = source[s_off + i] as f64;
            let b = captured[c_off + i] as f64;
            dot += a * b;
            es += a * a;
            ec += b * b;
        }
        if es <= 0.0 || ec <= 0.0 {
            return None;
        }
        Some(dot / (es.sqrt() * ec.sqrt()))
    };

    let mut best: Option<(isize, f64)> = None;
    let consider = |delay: isize, best: &mut Option<(isize, f64)>| {
        if let Some(score) = score_at(delay) {
            if best.is_none_or(|(_, b)| score > b) {
                *best = Some((delay, score));
            }
        }
    };
    let mut d = coarse - HOP as isize;
    while d <= coarse + HOP as isize {
        consider(d, &mut best);
        d += 6;
    }
    let (center, _) = best?;
    for d in (center - 6)..=(center + 6) {
        consider(d, &mut best);
    }
    best
}

/// Linear RMS per 10 ms hop.
fn envelope(pcm: &[i16]) -> Vec<f64> {
    pcm.as_chunks::<HOP>()
        .0
        .iter()
        .map(|seg| {
            let sum: f64 = seg.iter().map(|&s| (s as f64) * (s as f64)).sum();
            (sum / HOP as f64).sqrt()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn too_short_input_fails_without_panic() {
        let r = compare_mono_48k(&[0i16; 100], &[0i16; 100], &OracleConfig::default());
        assert!(!r.pass);
    }

    #[test]
    fn all_silent_source_fails() {
        let silent = vec![0i16; 48_000];
        let r = compare_mono_48k(&silent, &silent, &OracleConfig::default());
        assert!(!r.pass, "an all-silent source verifies nothing");
    }

    #[test]
    fn merge_runs_merges_adjacent_only() {
        let runs = merge_runs(&[3, 4, 5, 9]);
        assert_eq!(
            runs,
            vec![Anomaly { start_ms: 30, end_ms: 60 }, Anomaly { start_ms: 90, end_ms: 100 }]
        );
    }
}
