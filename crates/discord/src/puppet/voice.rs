//! Per-puppet async command loop and voice operations: join / leave / rejoin,
//! anchor activation, mute mirroring, and the speaking-gate task.

use super::*;
use super::ghosts::ensure_ghost;
use super::sinks::{DriverFaultHandler, PuppetSsrcRegistrar};
use crate::{
    ConnectionLogger, PlaybackSource, SpeakingLogger, VoiceReceiver, PLAYBACK_CHANNELS,
    PLAYBACK_SAMPLE_RATE,
};
use songbird::input::{Input, RawAdapter};
use songbird::tracks::TrackHandle;
use songbird::CoreEvent;

/// A speaking-gate task handle: cancel token + join handle (see
/// [`spawn_speaking_gate`]).
type Gate = (CancellationToken, tokio::task::JoinHandle<()>);

/// Cancel a speaking gate, if armed. The task self-terminates within one
/// `GATE_POLL`; not awaited.
fn cancel_gate(g: &mut Option<Gate>) {
    if let Some((tok, _jh)) = g.take() {
        tok.cancel();
    }
}

/// One live per-guild voice engagement of a bot, as seen by its command loop:
/// the channel context, this session's playback mixer and connection SSRC, the
/// join generation, and the speaking gates for its tracks. The loop holds one
/// of these per guild (#46 — a bot multiplexes at most one voice Call per
/// guild; songbird keys Calls by `GuildId`).
struct GuildConn {
    chan: ChannelCtx,
    /// The session's playback buffer (received with the `Assign`).
    mixer: Arc<PlaybackMixer>,
    /// This connection's own SSRC (registered into the channel's exclude set on
    /// `DriverConnect`; removed again on leave/rejoin). Minted fresh per `Assign`.
    current_ssrc: Arc<StdMutex<Option<u32>>>,
    /// Join generation of this connection (stamped from the loop's monotonic
    /// counter): a `DriverDropped` tagged with an older epoch is stale — the
    /// connection it refers to was already torn down or replaced.
    epoch: u64,
    /// Speaking gate for this bot's own dedicated playback track here.
    puppet_gate: Option<Gate>,
    /// Speaking gate for the channel overflow track (armed while anchor here).
    overflow_gate: Option<Gate>,
}

impl GuildConn {
    fn cancel_gates(&mut self) {
        cancel_gate(&mut self.puppet_gate);
        cancel_gate(&mut self.overflow_gate);
    }
}

/// Drain commands for one puppet, driving its voice connections. The loop is
/// channel-agnostic: each `Assign` carries the channel context, stored per
/// guild in `active`; guild-qualified commands act on that guild's engagement.
pub(super) async fn puppet_command_loop(
    ctx: Context,
    idx: usize,
    nickname_prefix: Arc<str>,
    pool: Arc<SharedPool>,
    mut cmd_rx: mpsc::Receiver<PuppetCommand>,
) {
    // One live engagement per guild (at most one voice Call per guild per bot).
    let mut active: HashMap<GuildId, GuildConn> = HashMap::new();
    // Monotonic join generation shared across this bot's guilds; each (re)join
    // stamps its conn, so a stale `DriverDropped` is recognisable per guild.
    let mut epoch: u64 = 0;
    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            PuppetCommand::Assign { ctx: chan, display_name, mixer } => {
                let chan = *chan;
                let guild = chan.guild_id;
                // A same-guild re-assign replaces the engagement (its Release, if
                // any, was ordered ahead of this on the FIFO command channel);
                // the fresh join below replaces the guild's Call. Engagements in
                // OTHER guilds are untouched.
                if let Some(mut old) = active.remove(&guild) {
                    if old.chan.channel_id != chan.channel_id {
                        tracing::warn!(
                            idx,
                            guild = guild.get(),
                            old_channel = old.chan.channel_id.get(),
                            new_channel = chan.channel_id.get(),
                            "re-assign replaces an engagement for a different channel in this guild without a prior release"
                        );
                    }
                    old.cancel_gates();
                }
                epoch += 1;
                // Fresh per engagement: each voice connection has its own SSRC (a
                // prior channel's entry was already removed by its Release).
                let current_ssrc = Arc::new(StdMutex::new(None));
                let handle = puppet_join(
                    &chan,
                    idx,
                    &ctx,
                    &mixer,
                    &current_ssrc,
                    &nickname_prefix,
                    &display_name,
                    epoch,
                    &pool.slots[idx].cmd_tx,
                )
                .await;
                let mut conn = GuildConn {
                    chan,
                    mixer,
                    current_ssrc,
                    epoch,
                    puppet_gate: None,
                    overflow_gate: None,
                };
                if let Some(handle) = handle {
                    let tok = CancellationToken::new();
                    let jh = spawn_speaking_gate(
                        handle,
                        conn.mixer.clone(),
                        tok.clone(),
                        conn.chan.bridge.clone(),
                        "puppet",
                    );
                    conn.puppet_gate = Some((tok, jh));
                    active.insert(guild, conn);
                    // Activate this bot as anchor iff the allocator elected it.
                    pool.on_join_succeeded(idx, guild);
                } else {
                    // Keep the failed engagement so the failover's queued Release
                    // still runs the full leave/cleanup path for this guild.
                    active.insert(guild, conn);
                    // Quarantine this bot + fail the participant over to another,
                    // then schedule the bot's recovery after the (backoff) cooldown.
                    let backoff = pool.on_join_failed(idx, guild);
                    let pool = pool.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(backoff).await;
                        pool.recover_bot(idx, guild);
                    });
                }
            }
            PuppetCommand::BecomeAnchor { guild } => {
                if let Some(conn) = active.get_mut(&guild) {
                    // Idempotent: drop any prior overflow gate before (re)anchoring.
                    cancel_gate(&mut conn.overflow_gate);
                    if let Some(handle) = puppet_become_anchor(&conn.chan, idx, &ctx).await {
                        let tok = CancellationToken::new();
                        let jh = spawn_speaking_gate(
                            handle,
                            conn.chan.overflow_mixer.clone(),
                            tok.clone(),
                            conn.chan.bridge.clone(),
                            "overflow",
                        );
                        conn.overflow_gate = Some((tok, jh));
                    }
                } else {
                    tracing::warn!(idx, guild = guild.get(), "BecomeAnchor with no engagement in this guild");
                }
            }
            PuppetCommand::SetMuted { guild, muted } => {
                if active.contains_key(&guild) {
                    puppet_set_muted(&ctx, idx, guild, muted).await;
                }
            }
            PuppetCommand::SetNickname { guild, name } => {
                if active.contains_key(&guild) {
                    let nick = build_nickname(&nickname_prefix, &name);
                    if let Err(e) = guild.edit_nickname(&ctx.http, Some(nick.as_str())).await {
                        tracing::warn!(idx, error = %e, "failed to relabel puppet nickname");
                    } else {
                        tracing::info!(idx, nick = %nick, "puppet relabeled");
                    }
                }
            }
            PuppetCommand::SetStandbyNickname { guild } => {
                if active.contains_key(&guild) {
                    // The label is configured whole — no prefix; reuse
                    // build_nickname only for its safe char truncation.
                    let nick = build_nickname("", &pool.standby_nickname);
                    if let Err(e) = guild.edit_nickname(&ctx.http, Some(nick.as_str())).await {
                        tracing::warn!(idx, error = %e, "failed to set standby nickname");
                    } else {
                        tracing::info!(idx, nick = %nick, "puppet relabeled (standby)");
                    }
                }
            }
            PuppetCommand::Release { guild } => {
                if let Some(mut conn) = active.remove(&guild) {
                    conn.cancel_gates();
                    puppet_leave(&ctx, idx, &conn.chan, &conn.current_ssrc).await;
                }
            }
            PuppetCommand::ReleaseAndAck { done } => {
                for (_guild, mut conn) in active.drain() {
                    conn.cancel_gates();
                    puppet_leave(&ctx, idx, &conn.chan, &conn.current_ssrc).await;
                }
                let _ = done.send(());
            }
            // An unexpected voice drop (event-driven) or a reconcile probe finding
            // the bot out of voice → rejoin the SAME bot in that guild, keeping
            // its participant + anchor role. `should_recover_disconnect` gates on
            // the (bot, guild) session (a deliberate leave already removed it)
            // and claims the recovery latch.
            cmd @ (PuppetCommand::DriverDropped { .. } | PuppetCommand::VerifyConnected { .. }) => {
                let (guild, proceed) = match &cmd {
                    PuppetCommand::DriverDropped { guild, epoch: ev } => {
                        // Ignore a stale disconnect from a replaced connection.
                        let live = active.get(guild).is_some_and(|c| c.epoch == *ev);
                        (*guild, live && pool.should_recover_disconnect(idx, *guild))
                    }
                    PuppetCommand::VerifyConnected { guild } => {
                        let proceed = pool.should_recover_disconnect(idx, *guild)
                            && if let Some(assigned) =
                                active.get(guild).map(|c| c.chan.channel_id)
                            {
                                // Displaced = out of voice entirely (silent
                                // death) OR in the wrong channel (#62 force-
                                // move), per songbird's CURRENT view.
                                let displaced = voice_displaced(&ctx, *guild, assigned).await;
                                if !displaced {
                                    pool.finish_recovery(idx, *guild);
                                }
                                displaced
                            } else {
                                pool.finish_recovery(idx, *guild);
                                false
                            };
                        (*guild, proceed)
                    }
                    _ => unreachable!(),
                };
                if proceed {
                    // Extract what the rejoin needs, ending the map borrow before
                    // the awaits; the new gate is stored on a fresh lookup.
                    let pieces = active.get_mut(&guild).map(|conn| {
                        conn.cancel_gates();
                        (conn.chan.clone(), conn.mixer.clone(), conn.current_ssrc.clone())
                    });
                    if let Some((chan, mixer, ssrc)) = pieces {
                        tracing::warn!(idx, guild = guild.get(), "puppet voice dropped unexpectedly — recovering (rejoin)");
                        epoch += 1;
                        if let Some(conn) = active.get_mut(&guild) {
                            conn.epoch = epoch;
                        }
                        let handle = rejoin_puppet(
                            &ctx,
                            idx,
                            &mixer,
                            &ssrc,
                            &nickname_prefix,
                            &pool,
                            &chan,
                            epoch,
                        )
                        .await;
                        if let Some(handle) = handle {
                            let tok = CancellationToken::new();
                            let jh = spawn_speaking_gate(
                                handle,
                                mixer,
                                tok.clone(),
                                chan.bridge.clone(),
                                "puppet",
                            );
                            if let Some(conn) = active.get_mut(&guild) {
                                conn.puppet_gate = Some((tok, jh));
                            }
                        }
                    } else {
                        // Latch was claimed but there's no engagement to rejoin.
                        pool.finish_recovery(idx, guild);
                    }
                }
            }
        }
    }
    tracing::info!(idx, "puppet command loop exited");
}

/// Register the Discord→Matrix capture handlers + overflow playback on this bot's
/// voice connection (it has become the anchor), then seed ghosts for members
/// already present in the channel.
async fn puppet_become_anchor(
    channel: &ChannelCtx,
    idx: usize,
    ctx: &Context,
) -> Option<TrackHandle> {
    let manager = songbird::get(ctx).await?;
    let Some(call_lock) = manager.get(channel.guild_id) else {
        tracing::warn!(idx, "anchor command but puppet not in voice yet");
        return None;
    };
    let overflow_handle;
    {
        let mut call = call_lock.lock().await;
        call.add_global_event(
            CoreEvent::VoiceTick.into(),
            VoiceReceiver::new(
                channel.bridge.clone(),
                channel.exclude.clone(),
                channel.ssrc_map.clone(),
                channel.user_sinks.clone(),
                channel.capture_sink.clone(),
                channel.optout.clone(),
                channel.anchor.clone(),
                idx,
            ),
        );
        // The anchor also plays the channel's overflow mix (un-slotted speakers).
        let overflow_src = PlaybackSource::new(channel.overflow_mixer.clone());
        let overflow_input =
            Input::from(RawAdapter::new(overflow_src, PLAYBACK_SAMPLE_RATE, PLAYBACK_CHANNELS));
        overflow_handle = call.play_input(overflow_input);
    }
    tracing::info!(idx, bridge = %channel.bridge, "anchor capture active — Discord→Matrix demux from this puppet");

    // Seed ghosts for everyone already in the voice channel — voice_state_update
    // only fires on later changes. Read the cached voice states (populated via the
    // GUILD_VOICE_STATES intent); ensure_ghost is idempotent.
    let present: Vec<u64> = match ctx.cache.guild(channel.guild_id) {
        Some(guild) => guild
            .voice_states
            .iter()
            .filter(|(_, vs)| vs.channel_id == Some(channel.channel_id))
            .map(|(uid, _)| uid.get())
            .collect(),
        None => Vec::new(),
    };
    if !present.is_empty() {
        tracing::info!(count = present.len(), bridge = %channel.bridge, "seeding ghosts for existing voice members");
    }
    for uid in present {
        ensure_ghost(ctx, channel, uid, None).await;
    }
    Some(overflow_handle)
}

/// Self-mute/unmute this puppet's voice connection to mirror the Matrix
/// participant's mute state (shows as muted in the Discord client).
async fn puppet_set_muted(ctx: &Context, idx: usize, guild_id: GuildId, muted: bool) {
    let Some(manager) = songbird::get(ctx).await else { return };
    if let Some(call_lock) = manager.get(guild_id) {
        let mut call = call_lock.lock().await;
        if let Err(e) = call.mute(muted).await {
            tracing::warn!(idx, error = %e, muted, "failed to set puppet mute state");
        } else {
            tracing::info!(idx, muted, "puppet mute state mirrored");
        }
    }
}

/// Drive a playback track's Discord "speaking" ring from real audio activity.
///
/// songbird marks a track "speaking" whenever it yields a full frame, and our
/// [`PlaybackSource`] always yields one (silence on underrun, never EOF — EOF would
/// end the track permanently), so the ring would stay green forever. This task
/// **pauses** the track after [`GATE_HANGOVER`] with no loud frames (songbird then
/// stops signalling "speaking" → the ring clears) and **resumes** it the moment
/// audio returns. Activity is observed on the push side via
/// [`PlaybackMixer::loud_frames`], because a paused track is no longer read.
///
/// `play`/`pause` errors are ignored: after a leave the track is gone and the next
/// `cancel` wins. The task exits within one [`GATE_POLL`] of cancellation.
fn spawn_speaking_gate(
    handle: TrackHandle,
    mixer: Arc<PlaybackMixer>,
    cancel: CancellationToken,
    bridge: Arc<str>,
    label: &'static str,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_loud = mixer.loud_frames();
        let mut last_active = tokio::time::Instant::now();
        let mut playing = true; // a freshly `play_input`'d track starts Playing
        let mut ticker = tokio::time::interval(GATE_POLL);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = ticker.tick() => {
                    let now = mixer.loud_frames();
                    if now != last_loud {
                        last_loud = now;
                        last_active = tokio::time::Instant::now();
                        if !playing {
                            // Frames arrive continuously (no DTX), so the buffer
                            // filled to its cap while paused — drop that stale
                            // backlog and re-prime from live audio, else playback
                            // runs pinned at JITTER_MAX (drop-oldest → choppy).
                            mixer.clear();
                            let _ = handle.play();
                            playing = true;
                            tracing::debug!(bridge = %bridge, label, "speaking gate: resume (audio)");
                        }
                    } else if playing && last_active.elapsed() >= GATE_HANGOVER {
                        let _ = handle.pause();
                        playing = false;
                        tracing::debug!(bridge = %bridge, label, "speaking gate: pause (silence)");
                    }
                }
            }
        }
        tracing::debug!(bridge = %bridge, label, "speaking gate stopped");
    })
}

/// Returns `true` if the bot actually joined the voice channel. `false` on a join
/// failure (e.g. the gateway timed out after a channel hop) — the caller fails the
/// participant over to another pool bot.
#[allow(clippy::too_many_arguments)]
async fn puppet_join(
    channel: &ChannelCtx,
    idx: usize,
    ctx: &Context,
    mixer: &Arc<PlaybackMixer>,
    current_ssrc: &Arc<StdMutex<Option<u32>>>,
    nickname_prefix: &str,
    display_name: &str,
    epoch: u64,
    cmd_tx: &mpsc::Sender<PuppetCommand>,
) -> Option<TrackHandle> {
    let Some(manager) = songbird::get(ctx).await else {
        tracing::error!(idx, "songbird not registered on puppet client");
        return None;
    };

    // Register handlers and start playback on the Call *before* connecting: two
    // events fire *during* `manager.join().await` and are missed if a handler is
    // attached after the await — `DriverConnect` (our own SSRC, echo-exclusion)
    // and `SpeakingStateUpdate` (others' SSRC→user; emitted around connect).
    let call_lock = manager.get_or_insert(channel.guild_id);
    let handle: Option<TrackHandle>;
    {
        let mut call = call_lock.lock().await;
        call.add_global_event(
            CoreEvent::DriverConnect.into(),
            PuppetSsrcRegistrar {
                bridge: channel.bridge.clone(),
                idx,
                exclude: channel.exclude.clone(),
                current_ssrc: current_ssrc.clone(),
            },
        );
        call.add_global_event(
            CoreEvent::SpeakingStateUpdate.into(),
            SpeakingLogger::new(channel.bridge.clone(), channel.ssrc_map.clone()),
        );
        // An unexpected DriverDisconnect drives voice recovery (rejoin), tagged
        // with this join's `epoch` so a stale disconnect from a replaced
        // connection is ignored. DriverReconnect/ClientDisconnect stay log-only.
        call.add_global_event(
            CoreEvent::DriverDisconnect.into(),
            DriverFaultHandler {
                bridge: channel.bridge.clone(),
                idx,
                guild: channel.guild_id,
                epoch,
                cmd_tx: cmd_tx.clone(),
            },
        );
        for ev in [CoreEvent::DriverReconnect, CoreEvent::ClientDisconnect] {
            call.add_global_event(
                ev.into(),
                ConnectionLogger::new(channel.bridge.clone(), channel.ssrc_map.clone()),
            );
        }
        let source = PlaybackSource::new(mixer.clone());
        let input = Input::from(RawAdapter::new(source, PLAYBACK_SAMPLE_RATE, PLAYBACK_CHANNELS));
        handle = Some(call.play_input(input));
    }

    if let Err(e) = manager.join(channel.guild_id, channel.channel_id).await {
        tracing::error!(idx, error = %e, "puppet failed to join voice channel");
        // Reset the (possibly half-open) Call so a later reuse of this bot starts
        // clean once it recovers from quarantine.
        let _ = manager.remove(channel.guild_id).await;
        return None;
    }

    let nick = build_nickname(nickname_prefix, display_name);
    if let Err(e) = channel.guild_id.edit_nickname(&ctx.http, Some(nick.as_str())).await {
        tracing::warn!(idx, error = %e, "failed to set puppet nickname");
    }
    tracing::info!(idx, bridge = %channel.bridge, display_name, "puppet joined voice and assigned");
    handle
}

async fn puppet_leave(
    ctx: &Context,
    idx: usize,
    channel: &ChannelCtx,
    current_ssrc: &Arc<StdMutex<Option<u32>>>,
) {
    if let Err(e) = channel.guild_id.edit_nickname(&ctx.http, None).await {
        tracing::warn!(idx, error = %e, "failed to reset puppet nickname");
    }
    if let Some(manager) = songbird::get(ctx).await {
        if let Err(e) = manager.remove(channel.guild_id).await {
            tracing::warn!(idx, error = %e, "puppet failed to leave voice channel");
        }
    }
    // Stop excluding our (now-gone) SSRC so a recycled SSRC isn't wrongly muted.
    let ssrc = current_ssrc.lock().unwrap_or_else(PoisonError::into_inner).take();
    if let Some(s) = ssrc {
        channel
            .exclude
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&s);
        tracing::info!(idx, ssrc = s, "puppet SSRC removed from echo-exclusion");
    }
    tracing::info!(idx, bridge = %channel.bridge, "puppet left voice and released");
}

/// Recover a bot whose voice connection dropped mid-session: drop the dead Call,
/// rejoin the SAME bot to its current channel (re-registering handlers + replaying
/// playback), and re-activate its anchor role if it was the anchor. Unlike
/// `on_join_failed`, this keeps the bot serving its participant and keeps its
/// route/anchor — the only correct recovery when the bot is the sole one serving
/// the channel (no spare to fail over to). On a failed rejoin it falls back to the
/// existing quarantine→failover→recover machinery. Returns the new playback handle
/// (so the caller respawns the speaking gate). Clears the recovery latch.
#[allow(clippy::too_many_arguments)]
async fn rejoin_puppet(
    ctx: &Context,
    idx: usize,
    mixer: &Arc<PlaybackMixer>,
    current_ssrc: &Arc<StdMutex<Option<u32>>>,
    nickname_prefix: &str,
    pool: &Arc<SharedPool>,
    chan: &ChannelCtx,
    epoch: u64,
) -> Option<TrackHandle> {
    // Drop the dead Call so the rejoin starts clean (mirrors puppet_join's reset
    // on a failed join).
    if let Some(manager) = songbird::get(ctx).await {
        // A LIVE connection (the #62 force-move case) needs a real gateway
        // leave here, and its confirmation echo cancels a join issued right
        // after (live-observed: "request was cancelled/dropped" → needless
        // quarantine + failover to another bot). Let the leave settle before
        // rejoining. A dead connection (the classic disconnect recovery) has
        // no echo pending — remove locally and rejoin at full speed, as
        // proven by #46.
        let was_live = match manager.get(chan.guild_id) {
            Some(call) => call.lock().await.current_channel().is_some(),
            None => false,
        };
        let _ = manager.remove(chan.guild_id).await;
        if was_live {
            tokio::time::sleep(Duration::from_millis(1500)).await;
        }
    }
    // Drop the dead connection's SSRC from echo-exclusion; the fresh join
    // re-registers a new one via DriverConnect (mirrors puppet_leave's cleanup,
    // which this path bypasses).
    let stale_ssrc = current_ssrc.lock().unwrap_or_else(PoisonError::into_inner).take();
    if let Some(s) = stale_ssrc {
        chan.exclude.write().unwrap_or_else(PoisonError::into_inner).remove(&s);
    }
    let display_name = pool.session_display_name(idx, chan.guild_id);
    let handle = puppet_join(
        chan,
        idx,
        ctx,
        mixer,
        current_ssrc,
        nickname_prefix,
        &display_name,
        epoch,
        &pool.slots[idx].cmd_tx,
    )
    .await;
    if handle.is_some() {
        tracing::info!(idx, bridge = %chan.bridge, "puppet voice connection recovered — rejoined");
        // Re-activate the anchor role if this bot is still the channel's anchor.
        pool.on_join_succeeded(idx, chan.guild_id);
        pool.finish_recovery(idx, chan.guild_id);
    } else {
        tracing::warn!(idx, bridge = %chan.bridge, "puppet rejoin failed — quarantining + failing over");
        pool.finish_recovery(idx, chan.guild_id);
        let guild = chan.guild_id;
        let backoff = pool.on_join_failed(idx, guild);
        let pool = pool.clone();
        tokio::spawn(async move {
            tokio::time::sleep(backoff).await;
            pool.recover_bot(idx, guild);
        });
    }
    handle
}

/// Whether this bot is genuinely out of the voice channel. `current_channel()` is
/// `None` only when not joined / kicked and STAYS `Some` through a transient
/// reconnect — so this never false-fires on a benign blip (unlike
/// `current_connection()`). Returns `true` (treat as gone) if songbird/the Call is
/// missing entirely.
async fn voice_displaced(ctx: &Context, guild_id: GuildId, assigned: ChannelId) -> bool {
    let Some(manager) = songbird::get(ctx).await else {
        return true;
    };
    match manager.get(guild_id) {
        Some(call) => match call.lock().await.current_channel() {
            // In voice, but is it the ASSIGNED channel? (#62: after a force-
            // move the connection is alive, so "connected" alone is not
            // "healthy".)
            Some(current) => current.0.get() != assigned.get(),
            None => true,
        },
        None => true,
    }
}
