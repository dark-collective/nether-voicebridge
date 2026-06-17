//! Publish the bridge's outbound audio track into the LiveKit room.
//!
//! Creates a **mono** `NativeAudioSource` (Element Call / Opus upmix mono on
//! playout, so mono halves bandwidth and makes the stereo→mono downmix the only
//! channel-reduction point), publishes it as a Microphone track, and returns
//! the source.  Frames are fed into the returned source by the long-lived
//! audio-consumer in `rtc::mod` — unlike the Phase 4 tone, there is no feeder
//! loop here: Discord's 20 ms tick is the producer clock.
//!
//! `capture_frame`'s self-pacing (the libwebrtc 1000 ms buffer) is documented
//! at length in `rtc/tone.rs`; the same source configuration is used here.

use anyhow::{Context, Result};
use livekit::{
    Room,
    options::TrackPublishOptions,
    track::{LocalAudioTrack, LocalTrack, TrackSource},
    webrtc::audio_source::{AudioSourceOptions, RtcAudioSource, native::NativeAudioSource},
};

const SAMPLE_RATE: u32 = 48_000;
/// Mono: the audio core downmixes Discord's stereo capture before it reaches us.
const NUM_CHANNELS: u32 = 1;
/// libwebrtc buffer depth; see the `rtc/tone.rs` doc for why 1000 ms self-paces.
const QUEUE_SIZE_MS: u32 = 1000;

/// Create a mono audio source, publish it to `room`, and return the source (to
/// feed `capture_frame` 10 ms mono frames into) plus the published track (so the
/// caller can `mute()`/`unmute()` it — used to mirror a Discord speaker's mute
/// state onto their ghost). The track is cheap to clone and `Arc`-backed.
pub async fn publish(room: &Room) -> Result<(NativeAudioSource, LocalAudioTrack)> {
    let source = NativeAudioSource::new(
        AudioSourceOptions::default(),
        SAMPLE_RATE,
        NUM_CHANNELS,
        QUEUE_SIZE_MS,
    );

    let track =
        LocalAudioTrack::create_audio_track("bridge-audio", RtcAudioSource::Native(source.clone()));

    let options = TrackPublishOptions {
        source: TrackSource::Microphone,
        ..Default::default()
    };

    room.local_participant()
        .publish_track(LocalTrack::Audio(track.clone()), options)
        .await
        .context("publishing bridge audio track to LiveKit room")?;

    tracing::info!("bridge audio track published (mono, 48 kHz)");
    Ok((source, track))
}
