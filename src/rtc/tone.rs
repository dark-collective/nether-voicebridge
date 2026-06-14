//! Publish a 440 Hz sine tone from the bridge into the LiveKit room.
//!
//! Creates a NativeAudioSource → LocalAudioTrack → publishes as Microphone.
//! Feeds 10 ms frames (480 samples/channel at 48 kHz, stereo) in a tight loop
//! using `capture_frame`'s built-in backpressure as the clock.
//!
//! # Why tight loop, not a tokio interval
//!
//! `NativeAudioSource::new(…, queue_size_ms=1000)` enables the *buffered* path in
//! libwebrtc (`webrtc-sys-0.3.34/src/audio_track.cpp`). The C++ side runs its own
//! `RepeatingTaskHandle` every 10 ms that drains 960 samples to the codec sinks; if
//! the buffer holds fewer than 960 samples it injects a 10 ms **silence frame**
//! ("Always provide a 10ms frame to avoid playout underruns", audio_track.cpp:174).
//! `capture_frame` completes the caller **immediately** while
//! `buffer.size() <= notify_threshold_samples` (96000 = 1000 ms of samples); real
//! backpressure only kicks in once the buffer exceeds that threshold.
//!
//! A second external 10 ms tokio interval keeps the buffer hovering near empty: the
//! two 10 ms clocks race, and whenever the C++ drain tick leads our producer tick the
//! C++ layer inserts a silence frame. `MissedTickBehavior::Skip` compounds it — any
//! late producer tick is dropped outright. Intermittent silence in a 440 Hz tone =
//! the "garbled, not a consistent tone" symptom.
//!
//! The fix: remove the external interval. Fill the buffer in a tight loop; once it
//! reaches 1000 ms `capture_frame` naturally blocks until the C++ drain tick
//! consumes one 10 ms chunk, providing gapless self-pacing with a healthy buffer
//! margin above the drain threshold.

use anyhow::{Context, Result};
use livekit::{
    Room,
    options::TrackPublishOptions,
    track::{LocalAudioTrack, LocalTrack, TrackSource},
    webrtc::audio_frame::AudioFrame,
    webrtc::audio_source::{
        AudioSourceOptions,
        RtcAudioSource,
        native::NativeAudioSource,
    },
};
use std::borrow::Cow;
use std::f32::consts::TAU;
use tokio_util::sync::CancellationToken;
use tokio::task;

const SAMPLE_RATE: u32 = 48_000;
const NUM_CHANNELS: u32 = 2;
// 10 ms at 48 kHz = 480 samples per channel.
const SAMPLES_PER_CHANNEL: u32 = 480;
// Total i16 values per frame (interleaved stereo).
const FRAME_SAMPLES: usize = (SAMPLES_PER_CHANNEL * NUM_CHANNELS) as usize;
const TONE_HZ: f32 = 440.0;
const TONE_AMP: f32 = 0.3; // 30% amplitude to avoid clipping

/// Publish a 440 Hz sine tone into the LiveKit room and run until cancelled.
///
/// This function publishes the track and then spawns a task that feeds audio
/// frames until the token is cancelled. The returned handle can be awaited to
/// wait for the tone task to finish.
pub async fn publish_and_run(room: &Room, cancel: CancellationToken) -> Result<tokio::task::JoinHandle<()>> {
    let source = NativeAudioSource::new(
        AudioSourceOptions::default(),
        SAMPLE_RATE,
        NUM_CHANNELS,
        1000, // queue_size_ms
    );

    let track = LocalAudioTrack::create_audio_track(
        "bridge-tone",
        RtcAudioSource::Native(source.clone()),
    );

    let options = TrackPublishOptions {
        source: TrackSource::Microphone,
        ..Default::default()
    };

    room.local_participant()
        .publish_track(LocalTrack::Audio(track), options)
        .await
        .context("publishing audio track to LiveKit room")?;

    tracing::info!("440 Hz sine tone track published");

    // Spawn the frame-feeder task.
    let handle = tokio::spawn(async move {
        run_tone_loop(source, cancel).await;
    });

    Ok(handle)
}

/// Feed audio frames into the source until cancelled.
///
/// Tight loop: generate a frame, then race `capture_frame` against the cancellation
/// token. `capture_frame` self-paces via the C++ buffer's drain-callback — once the
/// 1000 ms buffer fills it blocks ≤10 ms per call, providing gapless delivery with
/// no secondary clock. No `unwrap`/`expect` (audio hot path).
async fn run_tone_loop(source: NativeAudioSource, cancel: CancellationToken) {
    let mut phase: f32 = 0.0;
    let phase_inc = TAU * TONE_HZ / SAMPLE_RATE as f32;

    tracing::info!("tone loop started");

    loop {
        // Generate one 10 ms frame of 440 Hz sine, interleaved stereo.
        // Build the frame *before* awaiting so there is no gap between calls.
        let mut buf = Vec::with_capacity(FRAME_SAMPLES);
        for _ in 0..SAMPLES_PER_CHANNEL {
            let sample = (phase.sin() * TONE_AMP * i16::MAX as f32) as i16;
            buf.push(sample); // left
            buf.push(sample); // right
            phase = (phase + phase_inc) % TAU;
        }

        let frame = AudioFrame {
            data: Cow::Owned(buf),
            sample_rate: SAMPLE_RATE,
            num_channels: NUM_CHANNELS,
            samples_per_channel: SAMPLES_PER_CHANNEL,
        };

        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::info!("tone loop cancelled");
                break;
            }
            res = source.capture_frame(&frame) => {
                if let Err(e) = res {
                    // Buffer full or stalled drain — non-fatal. Yield to avoid
                    // a hot spin while the C++ side catches up.
                    tracing::warn!(error = %e, "tone frame capture error — yielding");
                    task::yield_now().await;
                }
            }
        }
    }
    tracing::info!("tone loop exited");
}
