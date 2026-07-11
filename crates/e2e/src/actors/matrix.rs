//! Fake Matrix call participants, driven through the `nvb-matrix-rtc` testkit
//! (D2): real appservice users of the SEPARATE `nvbtest` registration that
//! join real Element-Call sessions in the standing test rooms.
//!
//! * [`MatrixSpeaker`] — joins the call, publishes a LiveKit track, and speaks
//!   the Phase-B probe (the signal the audio oracle aligns against).
//! * [`MatrixPhantom`] — membership only, no LiveKit; its `kill()` leaves the
//!   membership behind (the reaper-feed injector).
//! * [`MatrixListener`] — joins the call and records every subscribed track;
//!   what a Discord speaker's ghost publishes comes back as PCM for the oracle.

use crate::config::E2eConfig;
use anyhow::{Context as _, Result};
use nvb_matrix_rtc::testkit::{
    self, TestCallConfig, TestCallHandle, TestListenerHandle, TestUser, TrackRecording,
};
use tokio::sync::mpsc;

/// 20 ms of mono 48 kHz — the chunk size the speaker feeds the testkit (which
/// reframes to LiveKit's 10 ms internally; `capture_frame` self-paces).
const SAMPLES_20MS: usize = 960;

/// Build the testkit config for one room. `media_e2ee` must match the room's
/// actual encryption state (the bridge auto-detects the same way).
fn call_config(cfg: &E2eConfig, room_id: &str, media_e2ee: bool) -> Result<TestCallConfig> {
    Ok(TestCallConfig {
        homeserver_url: cfg.homeserver_url.clone(),
        as_token: cfg.test_as_token()?.to_owned(),
        room_id: room_id.to_owned(),
        server_name: cfg.server_domain(),
        media_e2ee,
        bridge_ghost_prefix: cfg.ghost_prefix(),
    })
}

/// A fake Matrix user speaking into the call.
pub struct MatrixSpeaker {
    handle: TestCallHandle,
    frames_tx: mpsc::Sender<Vec<i16>>,
}

impl MatrixSpeaker {
    /// Join `room_id` as `@{localpart}:server` and go live (track published +
    /// membership posted — the bridge sees a Matrix human and opens).
    pub async fn join(
        cfg: &E2eConfig,
        room_id: &str,
        media_e2ee: bool,
        localpart: &str,
        display_name: &str,
    ) -> Result<Self> {
        let tcfg = call_config(cfg, room_id, media_e2ee)?;
        let user = TestUser::new(localpart, display_name)?;
        // Depth 64 ≈ 1.28 s of audio in flight; the testkit side drains it at
        // LiveKit's real-time pace, so `send().await` provides backpressure.
        let (frames_tx, frames_rx) = mpsc::channel::<Vec<i16>>(64);
        let handle = testkit::join_and_publish(&tcfg, &user, frames_rx)
            .await
            .context("matrix speaker join_and_publish")?;
        Ok(Self { handle, frames_tx })
    }

    /// The MXID this speaker publishes as (the bridge keys its puppet on it).
    #[allow(dead_code)] // first caller: the D3 multi-speaker scenarios
    pub fn mxid(&self) -> &str {
        self.handle.mxid()
    }

    /// Keep the published track carrying real frames (the warm-up tone,
    /// looped) until [`Filler::stop`] — the #73 fresh-join scenario needs
    /// audio FLOWING while the bridge joins, so its first subscribed frames
    /// deterministically beat the Olm-delivered media key.
    pub fn start_filler(&self) -> Filler {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        let tx = self.frames_tx.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = stop.clone();
        let task = tokio::spawn(async move {
            let tone = super::warmup_lead_in();
            'feed: loop {
                for chunk in tone.chunks(SAMPLES_20MS) {
                    if stop_flag.load(Ordering::Relaxed) {
                        break 'feed;
                    }
                    // Send failure = the speaker left (testkit dropped the
                    // receiver) — the filler just winds down.
                    if tx.send(chunk.to_vec()).await.is_err() {
                        break 'feed;
                    }
                }
            }
        });
        Filler { stop, task }
    }

    /// Speak the warm-up lead-in then the seeded deterministic probe. Returns
    /// once the last chunk is queued; LiveKit buffers up to ~1 s ahead, so
    /// callers should allow that much (plus pipeline latency) before asserting
    /// on the far side.
    pub async fn speak_probe(&self, seed: u64, dur_ms: u32) -> Result<()> {
        let mut samples = super::warmup_lead_in();
        samples.extend(nvb_audio::probe::synth_probe_mono_48k(seed, dur_ms));
        for chunk in samples.chunks(SAMPLES_20MS) {
            self.frames_tx
                .send(chunk.to_vec())
                .await
                .context("matrix speaker feed closed")?;
        }
        Ok(())
    }

    /// Leave cleanly (stop publishing, retract membership).
    pub async fn leave(self) {
        self.handle.leave_clean().await;
    }
}

/// A running [`MatrixSpeaker::start_filler`] feed. Up to ~1.3 s of queued
/// filler (the channel's depth) still drains at LiveKit pace after `stop`,
/// so leave a beat before playing anything the oracle must align on.
pub struct Filler {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl Filler {
    /// Stop feeding and wait for the feed task to wind down.
    pub async fn stop(self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let _ = self.task.await;
    }
}

/// A membership-only fake participant (no LiveKit).
#[allow(dead_code)] // first caller: the D3 reaper scenarios
pub struct MatrixPhantom {
    handle: TestCallHandle,
}

impl MatrixPhantom {
    /// Post `m.call.member` in `room_id`. With `with_crypto` the membership
    /// carries a real (ephemeral) crypto device, so publishers' Olm key sends
    /// targeting it succeed.
    #[allow(dead_code)] // first caller: the D3 reaper scenarios
    pub async fn join(
        cfg: &E2eConfig,
        room_id: &str,
        media_e2ee: bool,
        with_crypto: bool,
        localpart: &str,
        display_name: &str,
    ) -> Result<Self> {
        let tcfg = call_config(cfg, room_id, media_e2ee)?;
        let user = TestUser::new(localpart, display_name)?;
        let handle = testkit::join_presence(&tcfg, &user, with_crypto)
            .await
            .context("matrix phantom join_presence")?;
        Ok(Self { handle })
    }

    #[allow(dead_code)] // first caller: the D3 reaper scenarios
    pub fn mxid(&self) -> &str {
        self.handle.mxid()
    }

    /// Leave cleanly (retract the membership).
    #[allow(dead_code)] // first caller: the D3 reaper scenarios
    pub async fn leave(self) {
        self.handle.leave_clean().await;
    }

    /// Die WITHOUT retracting — the stale-membership injector the reaper
    /// scenarios (D3) feed on.
    #[allow(dead_code)] // first caller: the D3 reaper scenarios
    pub async fn kill(self) {
        self.handle.kill().await;
    }
}

/// A fake Matrix user listening in the call, recording every subscribed track.
pub struct MatrixListener {
    handle: TestListenerHandle,
}

impl MatrixListener {
    /// Join `room_id` and start recording. Its membership makes the bridge's
    /// call active, so present Discord speakers' ghosts connect and publish.
    /// In an encrypted room the recordings are UNDECRYPTED noise (decrypting
    /// listener = D2.5/D3) — the listener is then only the key-delivery target
    /// and call-activator; don't oracle its output.
    pub async fn join(
        cfg: &E2eConfig,
        room_id: &str,
        media_e2ee: bool,
        localpart: &str,
        display_name: &str,
    ) -> Result<Self> {
        let tcfg = call_config(cfg, room_id, media_e2ee)?;
        let user = TestUser::new(localpart, display_name)?;
        let handle = testkit::join_and_subscribe(&tcfg, &user)
            .await
            .context("matrix listener join_and_subscribe")?;
        Ok(Self { handle })
    }

    /// Join an ENCRYPTED `room_id` as a DECRYPTING listener: runs a full synced
    /// crypto Client so the ghost's Olm-delivered media key is decrypted and fed
    /// to a LiveKit KeyProvider, making the recorded frames cleartext. Unlike
    /// [`Self::join`] in an encrypted room, its recordings ARE oracle-able.
    pub async fn join_decrypting(
        cfg: &E2eConfig,
        room_id: &str,
        localpart: &str,
        display_name: &str,
    ) -> Result<Self> {
        let tcfg = call_config(cfg, room_id, true)?;
        let user = TestUser::new(localpart, display_name)?;
        let handle = testkit::join_and_subscribe_decrypting(&tcfg, &user)
            .await
            .context("matrix listener join_and_subscribe_decrypting")?;
        Ok(Self { handle })
    }

    /// Stop, leave cleanly, and return everything recorded.
    pub async fn stop(self) -> Result<Vec<TrackRecording>> {
        self.handle.stop().await
    }
}
