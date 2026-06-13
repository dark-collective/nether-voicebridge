//! Publish a 440 Hz sine tone from the bridge into the LiveKit room.
//!
//! Creates a NativeAudioSource → LocalAudioTrack → publishes as Microphone.
//! Feeds 10 ms frames (480 samples/channel at 48 kHz, stereo) on a tokio interval
//! until the CancellationToken is cancelled.

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
async fn run_tone_loop(source: NativeAudioSource, cancel: CancellationToken) {
    let mut interval =
        tokio::time::interval(std::time::Duration::from_millis(10));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut phase: f32 = 0.0;
    let phase_inc = TAU * TONE_HZ / SAMPLE_RATE as f32;

    tracing::info!("tone loop started");

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!("tone loop cancelled");
                break;
            }
            _ = interval.tick() => {}
        }

        // Generate one 10 ms frame of 440 Hz sine, interleaved stereo.
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

        if let Err(e) = source.capture_frame(&frame).await {
            // Non-fatal: log and continue. Frame drops under load are recoverable.
            tracing::warn!(error = %e, "tone frame capture error");
        }
    }
    tracing::info!("tone loop exited");
}
