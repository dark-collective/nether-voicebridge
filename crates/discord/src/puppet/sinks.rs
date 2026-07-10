//! Songbird driver event handlers registered by a puppet's voice connection:
//! SSRC echo-exclusion registration and driver-fault (disconnect) signalling.

use super::*;
use serenity::async_trait;
use songbird::{Event, EventContext, EventHandler as SongbirdEventHandler};

/// Songbird handler that records a puppet's own SSRC into its channel's
/// echo-exclusion set as soon as its voice driver connects.
pub(super) struct PuppetSsrcRegistrar {
    pub(super) bridge: Arc<str>,
    pub(super) idx: usize,
    pub(super) exclude: SharedExclude,
    pub(super) current_ssrc: Arc<StdMutex<Option<u32>>>,
}

#[async_trait]
impl SongbirdEventHandler for PuppetSsrcRegistrar {
    #[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %self.bridge))]
    async fn act(&self, ctx: &EventContext<'_>) -> Option<Event> {
        if let EventContext::DriverConnect(c) = ctx {
            self.exclude
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(c.ssrc);
            *self.current_ssrc.lock().unwrap_or_else(PoisonError::into_inner) = Some(c.ssrc);
            tracing::info!(idx = self.idx, ssrc = c.ssrc, "puppet SSRC registered for echo-exclusion");
        }
        None
    }
}

/// Songbird handler that turns an UNEXPECTED voice `DriverDisconnect` into a
/// rejoin request for this bot. songbird only emits `DriverDisconnect` once it
/// has given up its own reconnect attempts, so it is the right moment to
/// re-establish the connection at the application level. `epoch` ties the signal
/// to the join generation that registered this handler, so a disconnect from a
/// connection we have already torn down/replaced is ignored by the command loop.
/// Logs the event (replacing `ConnectionLogger` for this one event) and signals
/// the bot's loop; the loop's `busy`-gate decides expected-vs-unexpected.
pub(super) struct DriverFaultHandler {
    pub(super) bridge: Arc<str>,
    pub(super) idx: usize,
    /// The guild whose Call this handler is registered on — the loop routes the
    /// recovery to that guild's engagement.
    pub(super) guild: GuildId,
    pub(super) epoch: u64,
    pub(super) cmd_tx: mpsc::Sender<PuppetCommand>,
}

#[async_trait]
impl SongbirdEventHandler for DriverFaultHandler {
    #[tracing::instrument(parent = None, name = "bridge", skip_all, fields(name = %self.bridge))]
    async fn act(&self, ctx: &EventContext<'_>) -> Option<Event> {
        if let EventContext::DriverDisconnect(d) = ctx {
            // Diagnostic only — fires on EVERY disconnect incl. deliberate leaves. The
            // command loop's `busy`-gate decides expected-vs-unexpected and logs the
            // actual recovery at the decision point.
            tracing::debug!(
                idx = self.idx, kind = ?d.kind, reason = ?d.reason, channel_id = ?d.channel_id,
                "songbird DriverDisconnect observed"
            );
            let _ = self.cmd_tx.try_send(PuppetCommand::DriverDropped {
                guild: self.guild,
                epoch: self.epoch,
            });
        }
        None
    }
}
