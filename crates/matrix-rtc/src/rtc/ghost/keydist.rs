//! E2EE media-key distribution (Path 2 plaintext / Path 3 Olm).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use matrix_sdk::{
    deserialized_responses::SyncOrStrippedState,
    ruma::{
        events::{SyncStateEvent, call::member::CallMemberEventContent},
        RoomId,
    },
    Client,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use nvb_config::KeyTransport as KeyTransportMode;

use super::super::key_transport::{
    GhostCrypto, GhostCryptoCache, KeyRecipient, KeyTransport, OlmTransport, PlaintextTransport,
    RefreshMode,
};
use super::super::keys;
use super::identity::is_bridge_excluded_identity;
use super::GhostConfig;

/// Number of key re-sends in the burst that follows a membership change.
const KEY_BURST_COUNT: u32 = 6;
/// Spacing between burst re-sends (~`COUNT * INTERVAL` = the covered window).
pub(super) const KEY_BURST_INTERVAL: Duration = Duration::from_secs(3);
/// Steady-state re-send period once a burst is done — a slow safety net for a key
/// dropped even by the burst. Mild (≈1/ghost/min) vs. the old 5s spam.
const KEY_HEARTBEAT: Duration = Duration::from_secs(60);

/// Minimum spacing between Olm session rotations (#52). Membership-driven rotations
/// are rate-limited to this so Element Call's ~90s membership flaps don't churn
/// one-time keys on every flap.
pub(super) const SESSION_ROTATE_MIN_INTERVAL: Duration = Duration::from_secs(90);
/// Maximum age of a ghost's outbound Olm sessions before the heartbeat rotates them
/// anyway (#52). This is what bounds a listener-side dead-session wedge in a static
/// call (no membership changes): worst-case inaudibility ≈ this + one heartbeat,
/// instead of "until the bridge restarts".
pub(super) const SESSION_ROTATE_MAX_AGE: Duration = Duration::from_secs(15 * 60);

/// Olm crypto-client bootstrap attempts before giving up (no plaintext downgrade —
/// see [`distribute_key_task`]).
const OLM_BOOTSTRAP_ATTEMPTS: u32 = 3;
/// Backoff between Olm bootstrap attempts.
const OLM_BOOTSTRAP_RETRY: Duration = Duration::from_secs(5);

/// Build the configured key transport, then run [`distribute_key_loop`]. Spawned so
/// the (potentially slow) Olm crypto-client bootstrap never blocks the ghost's audio
/// pump.
///
/// **No silent downgrade.** When `key_transport = "olm"` and bootstrap ultimately
/// fails, we give up *without* falling back to plaintext: in an encrypted room a
/// plaintext key send would leak the frame key to the homeserver, defeating the
/// whole point of Path 3. The operator chooses plaintext explicitly via config; the
/// bridge never chooses it for them at runtime.
#[allow(clippy::too_many_arguments)]
pub(super) async fn distribute_key_task(
    client: Client,
    cfg: GhostConfig,
    localpart: String,
    ghost_mxid: String,
    device_id: String,
    key: [u8; 16],
    members_gen: watch::Receiver<u64>,
    cancel: CancellationToken,
    crypto_cache: GhostCryptoCache,
) {
    let transport: Arc<dyn KeyTransport> = match cfg.key_transport {
        KeyTransportMode::Plaintext => Arc::new(PlaintextTransport::new(
            cfg.http.clone(),
            cfg.homeserver_url.clone(),
            cfg.as_token.clone(),
            cfg.room_id.clone(),
            ghost_mxid.clone(),
            device_id.clone(),
        )),
        KeyTransportMode::Olm => {
            match bootstrap_olm_transport(
                &cfg,
                &localpart,
                &ghost_mxid,
                &device_id,
                &cancel,
                &crypto_cache,
            )
            .await
            {
                Some(t) => t,
                None => return, // bootstrap failed or cancelled; already logged.
            }
        }
    };
    // Session rotation (#52) only applies to the Olm transport (plaintext has no
    // sessions); the config knob can turn it off entirely.
    let rotate_sessions =
        cfg.rotate_ghost_sessions && matches!(cfg.key_transport, KeyTransportMode::Olm);
    let source: Arc<dyn RecipientSource> = Arc::new(CallRecipients { client, cfg });
    distribute_key_loop(source, key, transport, rotate_sessions, members_gen, cancel).await;
}

/// Source of the current key-send recipients (the call's human members). Abstracted
/// from [`distribute_key_loop`] so the loop's burst/heartbeat policy is unit-testable
/// without a live homeserver (tests supply a scripted source).
#[async_trait]
pub(super) trait RecipientSource: Send + Sync {
    async fn recipients(&self) -> Result<Vec<KeyRecipient>>;
}

/// Production source: reads call memberships via the bridge bot's joined client.
struct CallRecipients {
    client: Client,
    cfg: GhostConfig,
}

#[async_trait]
impl RecipientSource for CallRecipients {
    async fn recipients(&self) -> Result<Vec<KeyRecipient>> {
        enumerate_key_recipients(&self.client, &self.cfg).await
    }
}

/// Bootstrap an [`OlmTransport`] with bounded retries. Returns `None` if cancelled
/// or if every attempt fails (caller must NOT downgrade to plaintext).
async fn bootstrap_olm_transport(
    cfg: &GhostConfig,
    localpart: &str,
    ghost_mxid: &str,
    device_id: &str,
    cancel: &CancellationToken,
    crypto_cache: &GhostCryptoCache,
) -> Option<Arc<dyn KeyTransport>> {
    // Reuse this ghost's crypto across rejoins: a respawn with a NEW crypto device
    // would not be picked up by Element Call for an already-keyed participant (the
    // rejoiner goes silent). The cache keeps the device + Olm sessions stable for the
    // process lifetime — see [`GhostCryptoCache`].
    if let Some(crypto) = crypto_cache
        .lock()
        .await
        .get(ghost_mxid)
        .cloned()
    {
        tracing::info!("ghost Olm key transport ready (reused cached OlmMachine across rejoin)");
        return Some(Arc::new(OlmTransport::new(
            crypto,
            cfg.room_id.clone(),
            device_id.to_owned(),
        )));
    }

    for attempt in 1..=OLM_BOOTSTRAP_ATTEMPTS {
        if cancel.is_cancelled() {
            return None;
        }
        match GhostCrypto::bootstrap(
            cfg.http.clone(),
            &cfg.homeserver_url,
            &cfg.as_token,
            localpart,
            ghost_mxid,
            &cfg.store_path,
            cfg.prune_stale_devices,
        )
        .await
        {
            Ok(crypto) => {
                tracing::info!("ghost Olm key transport ready (send-only OlmMachine)");
                let crypto = Arc::new(crypto);
                // Cache for this ghost's future rejoins (insert-if-absent; a racing
                // insert would only have built an equivalent fresh device).
                crypto_cache
                    .lock()
                    .await
                    .entry(ghost_mxid.to_owned())
                    .or_insert_with(|| crypto.clone());
                return Some(Arc::new(OlmTransport::new(
                    crypto,
                    cfg.room_id.clone(),
                    device_id.to_owned(),
                )));
            }
            Err(e) => {
                tracing::warn!(attempt, error = %e, "ghost Olm transport bootstrap failed; retrying");
                tokio::select! {
                    _ = cancel.cancelled() => return None,
                    _ = tokio::time::sleep(OLM_BOOTSTRAP_RETRY) => {}
                }
            }
        }
    }
    tracing::error!(
        attempts = OLM_BOOTSTRAP_ATTEMPTS,
        "ghost Olm key transport bootstrap failed — media key NOT distributed (refusing to \
         downgrade to plaintext in an encrypted room); this ghost is inaudible in Element Call \
         until it reconnects"
    );
    None
}

// The send-only Olm crypto engine (`GhostCrypto`, in `key_transport.rs`) replaced
// the old per-ghost full `matrix_sdk::Client` here (its SQLite crypto store,
// `sync_once`, and perpetual `/sync`) — issue #44. The bootstrap now lives in
// `GhostCrypto::bootstrap` and uses an in-memory `OlmMachine` with on-demand
// `/keys/query`, so there is no per-ghost crypto store or sync loop.

/// Distribute this ghost's media key to the call's humans until cancelled, over the
/// given [`KeyTransport`].
///
/// **Burst-on-change + slow heartbeat.** A one-shot send is fragile: a recipient
/// whose Element Call client is still completing call setup drops the to-device key
/// and (with naive "send only to new recipients") never gets re-keyed — so a Matrix
/// listener can't hear ghosts that were already live when it joined. Instead, each
/// membership change (a new human, or this ghost going live with humans present)
/// triggers a short **burst** of re-sends to ALL current humans, covering that
/// setup window; between bursts a slow **heartbeat** re-send is a backstop. The
/// `members_gen` watch (bumped on each join) wakes the loop immediately, so a new
/// joiner is keyed without waiting for the heartbeat, while a stable call only
/// re-sends about once a minute. The transport is opaque here: plaintext (Path 2)
/// and Olm (Path 3) share this exact retry policy.
pub(super) async fn distribute_key_loop(
    source: Arc<dyn RecipientSource>,
    key: [u8; 16],
    transport: Arc<dyn KeyTransport>,
    rotate_sessions: bool,
    mut members_gen: watch::Receiver<u64>,
    cancel: CancellationToken,
) {
    // Recipients keyed so far — used only to detect a NEW human (which restarts a
    // burst); the sends themselves are not gated by it (we re-send to all current).
    let mut keyed: HashSet<KeyRecipient> = HashSet::new();
    // Burst re-sends still owed after the last membership change. >0 ⇒ wake fast.
    let mut burst: u32 = 0;
    // Whether the NEXT send forces a fresh /keys/query for the recipients (issue #51).
    // The first send forces (humans may already be present); thereafter only a
    // membership-generation bump re-arms it — one forced query per membership change,
    // not per 3s burst tick.
    let mut refresh = RefreshMode::Force;
    // When the current re-key attempt began (a new joiner or a membership bump), so we
    // can log per-ghost "membership change → key delivered" latency (the #51 symptom).
    let mut rekey_started: Option<tokio::time::Instant> = None;
    // Olm session rotation (#52). A membership bump requests a rotation (a wedged
    // listener's natural remedy is rejoining the call — rotation is what actually
    // heals them, unlike today where a rejoin keeps the dead session); the max-age
    // check bounds a wedge in a static call. Bootstrap/cache-reuse just (re)built or
    // reused a machine, so the loop starts with the clock at "now".
    let mut rotate_pending = false;
    let mut last_rotation = tokio::time::Instant::now();
    loop {
        // Did this iteration deliver the key to at least one device? A send that
        // reaches NOBODY is almost always a cold-start race: this ghost's own crypto
        // client hasn't yet tracked the recipients' devices (its background sync /
        // `/keys/query` lags the first send), so `get_user_devices` returns empty.
        // Such an iteration must keep retrying at the FAST burst interval until sync
        // catches up — NOT drain the burst, mark recipients keyed, and drop to the
        // 60s heartbeat, which would leave that speaker silent in an encrypted call
        // for up to a minute (observed on a busy VPS: 57 such failures, multi-minute
        // gaps). Defaults true so empty / enumeration-error iterations idle normally.
        let mut reached_someone = true;
        match source.recipients().await {
            Ok(current) => {
                // A newly-seen recipient (re)starts the burst to cover its client's
                // call-setup window, during which it may drop the to-device key.
                if current.iter().any(|r| !keyed.contains(r)) {
                    burst = KEY_BURST_COUNT;
                    if rekey_started.is_none() {
                        rekey_started = Some(tokio::time::Instant::now());
                    }
                }
                if current.is_empty() {
                    keyed.clear();
                } else {
                    // Rotate BEFORE the send so the fresh sessions carry this very
                    // key delivery. Membership-driven rotations wait out the
                    // min-interval (`rotate_pending` persists, so a deferred one
                    // fires on a later tick); the max-age path needs no pending flag.
                    if rotate_sessions {
                        let elapsed = last_rotation.elapsed();
                        let reason = if rotate_pending && elapsed >= SESSION_ROTATE_MIN_INTERVAL {
                            Some("membership")
                        } else if elapsed >= SESSION_ROTATE_MAX_AGE {
                            Some("max-age")
                        } else {
                            None
                        };
                        if let Some(reason) = reason {
                            if transport.rotate_sessions(reason).await {
                                last_rotation = tokio::time::Instant::now();
                                rotate_pending = false;
                            }
                        }
                    }
                    reached_someone = match transport.send_key(&current, &key, 0, refresh).await {
                        // Every targeted device got the key.
                        Ok(o) if o.failed.is_empty() => {
                            tracing::info!(
                                recipients = ?current.iter().map(|r| format!("{}:{}", r.user_id, r.device_id)).collect::<Vec<_>>(),
                                transport = transport.kind(),
                                delivered = o.delivered,
                                key_fp = %keys::key_fp(&key),
                                burst,
                                "ghost media key sent"
                            );
                            true
                        }
                        // Partial success — the key reached the live device(s); the
                        // unreachable ones are almost always stale/offline sessions
                        // with no one-time keys (can't be fixed from the bridge). This
                        // is the normal, healthy case, so it's INFO not WARN. The
                        // per-device id list is DEBUG to keep the default log quiet;
                        // burst/heartbeat keeps retrying in case any are transient.
                        Ok(o) if o.delivered > 0 => {
                            tracing::info!(
                                transport = transport.kind(),
                                delivered = o.delivered,
                                unreachable = o.failed.len(),
                                burst,
                                "ghost media key delivered (some recipient devices unreachable — likely stale/offline sessions)"
                            );
                            tracing::debug!(
                                unreachable_devices = ?o.failed.iter().map(|f| format!("{}:{}", f.user_id, f.device_id)).collect::<Vec<_>>(),
                                "ghost media key: unreachable devices"
                            );
                            true
                        }
                        // Reached NO devices — the real failure (e.g. the live device
                        // is out of one-time keys, or the ghost's sync hasn't tracked
                        // anyone yet). Keep fast-retrying (see `reached_someone`).
                        Ok(o) => {
                            tracing::warn!(
                                transport = transport.kind(),
                                unreachable = o.failed.len(),
                                burst,
                                "ghost media key reached NO recipient devices — nobody can decrypt this ghost (will retry)"
                            );
                            false
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                transport = transport.kind(),
                                "ghost media-key send failed"
                            );
                            false
                        }
                    };
                    // Whether it reached anyone or not, one send has now consumed the
                    // forced refresh: a genuine cold start is re-covered by the burst,
                    // and re-forcing every 3s tick would multiply /keys/query needlessly.
                    // The members_gen bump arm below re-arms it on the next membership change.
                    refresh = RefreshMode::Steady;
                    if reached_someone {
                        // Progress: remember who we've keyed and spend one burst slot.
                        keyed = current.into_iter().collect();
                        burst = burst.saturating_sub(1);
                        if let Some(t) = rekey_started.take() {
                            tracing::info!(
                                latency_ms = t.elapsed().as_millis() as u64,
                                transport = transport.kind(),
                                "ghost re-key delivered after membership change"
                            );
                        }
                    }
                    // else: reached nobody — leave `keyed` and `burst` untouched, so the
                    // new-recipient check re-arms the burst next loop and we retry at the
                    // fast interval until this ghost's crypto sync catches up.
                }
            }
            Err(e) => tracing::warn!(error = %e, "ghost media key: recipient enumeration failed"),
        }
        // Fast cadence while bursting OR while we've reached nobody (cold-sync recovery).
        let wait = if burst > 0 || !reached_someone {
            KEY_BURST_INTERVAL
        } else {
            KEY_HEARTBEAT
        };
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            // A human joined (generation bumped) → re-evaluate and burst-key them.
            res = members_gen.changed() => {
                if res.is_err() {
                    // Sender dropped (shutdown). cancel will fire imminently; wait
                    // for it rather than busy-looping on a closed channel.
                    cancel.cancelled().await;
                    break;
                }
                // Force ONE fresh /keys/query on the next send so a same-user rejoin's
                // (possibly rotated) returning device is re-fetched and re-keyed at once
                // rather than after the slow burst/heartbeat convergence (issue #51).
                refresh = RefreshMode::Force;
                // And request a session rotation (#52): a rejoining listener may hold
                // a dead inbound session for this ghost that only fresh sessions fix.
                rotate_pending = true;
                if rekey_started.is_none() {
                    rekey_started = Some(tokio::time::Instant::now());
                }
            }
            _ = tokio::time::sleep(wait) => {}
        }
    }
    tracing::debug!("ghost media-key distribution stopped");
}

/// Enumerate the call's human members (key-send recipients), read via the bridge
/// bot's joined client. Excludes our ghosts ([`is_ghost_identity`], which also
/// drops *this* ghost) and the bridge bot itself (the subscriber skips ghost
/// tracks, so it never needs ghost keys — constraint #5). Deduplicated by MXID.
async fn enumerate_key_recipients(client: &Client, cfg: &GhostConfig) -> Result<Vec<KeyRecipient>> {
    let room_id = RoomId::parse(&cfg.room_id)
        .with_context(|| format!("parsing room_id '{}'", cfg.room_id))?;
    let Some(room) = client.get_room(&room_id) else {
        return Ok(Vec::new());
    };
    let raws = room
        .get_state_events_static::<CallMemberEventContent>()
        .await
        .context("reading call memberships for key distribution")?;

    // One recipient per ACTIVE membership = one (user, device) actually in the call.
    // m.call.member is per-device, so this targets exactly the in-call device(s) — not
    // every device the user owns (which would waste Olm OTK claims on offline sessions).
    let mut recipients: Vec<KeyRecipient> = Vec::new();
    for raw in raws {
        let Ok(ev) = raw.deserialize() else { continue };
        let orig = match ev {
            SyncOrStrippedState::Sync(SyncStateEvent::Original(o)) => o,
            _ => continue,
        };
        let sender = orig.sender;
        if is_bridge_excluded_identity(sender.as_str(), &cfg.ghost_localpart_prefix) || sender.as_str() == cfg.bot_user_id {
            continue;
        }
        for m in orig.content.active_memberships(Some(orig.origin_server_ts)) {
            let r = KeyRecipient {
                user_id: sender.clone(),
                device_id: m.device_id().to_owned(),
            };
            if !recipients.contains(&r) {
                recipients.push(r);
            }
        }
    }
    Ok(recipients)
}
