//! Playback routing sinks and the per-bridge [`ChannelView`] router impl.

use super::*;
use nvb_audio::routing::{ParticipantSink, PlaybackRouter};

/// A no-op sink for the (unreachable) case where a channel vanished mid-assign.
pub(super) struct NullSink;
impl ParticipantSink for NullSink {
    fn push(&self, _mono: &[i16]) {}
}

/// A [`ParticipantSink`] whose target mixer the pool can swap (for promotion/demotion).
pub(super) struct RoutedSink {
    pub(super) route: Route,
    pub(super) key: String,
}

impl ParticipantSink for RoutedSink {
    fn push(&self, mono: &[i16]) {
        let mixer = self.route.lock().unwrap_or_else(PoisonError::into_inner).clone();
        mixer.push(&self.key, mono);
    }
}

impl PlaybackRouter for ChannelView {
    fn assign(&self, key: &str, display_name: &str) -> Arc<dyn ParticipantSink> {
        self.pool.assign(&self.key, key, display_name)
    }
    fn set_muted(&self, key: &str, muted: bool) {
        self.pool.set_muted(&self.key, key, muted)
    }
    fn release(&self, key: &str) {
        self.pool.release(&self.key, key)
    }
    fn release_all(&self) {
        self.pool.release_all(&self.key)
    }
}
