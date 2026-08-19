//! #99 — the invite/DM lifecycle scenarios the `@dau:tinier.town` incident
//! (2026-07-12) showed the suite was blind to.
//!
//! * [`MGMT_INVITE_OFFLINE`] (`Pass`) — a DM invite that arrives while the
//!   bridge is DOWN must produce exactly ONE accept when it starts: the
//!   pre-#97-fix `handle_invite` stamps the #89a per-sender cooldown BEFORE
//!   its join, so a redelivery of the still-pending invite hits the stamp its
//!   own first pass set and DECLINES → join-then-leave. Arming detail:
//!   `mgmt_cooldown_secs` must be SET (prod always had it; the harness's
//!   standing block leaves it unset — which is exactly why 20 scenarios never
//!   reproduced #97). **Live finding (2026-07-12, pre-fix bridge):** this
//!   ships as `Pass`, not `KnownFail` — the trigger needs the seconds-long
//!   federated-join window, and with a same-server inviter Tuwunel does NOT
//!   redeliver the invite (verified: armed cooldown, exactly one accept, zero
//!   declines). Same-server = the only thing the harness can mint (see the
//!   testing-design doc's federation-limitation note), so here it guards the
//!   post-fix contract as a regression test rather than reproducing the bug.
//! * [`MGMT_DM_PENDING_REAP`] (`KnownFail{#104}` — mode 1, the reap race,
//!   is FIXED; the residual is the cold-outreach first-message UTD) — a
//!   DM the BOT initiated (operator `msg` → `create_dm`) has the bot joined
//!   and the target merely invited, so `joined_members_count() == 1`: the
//!   #89b empty-room sweep reaps it before the target can accept, killing
//!   the outreach. Pre-fix this reproduced ~100 ms after creation (the
//!   operator's message 403'd mid-send). **alpha.25 did NOT fully close
//!   it** — three live post-fix runs, two failure modes, one root (the SDK
//!   store lags `create_dm`):
//!   1. *reap race* (2×): the exemption's `invited_members_count()` still
//!      read 0 a full 3 s after creation → the sweep reaped the room with
//!      the invite pending server-side the whole time;
//!   2. *first-message UTD* (1×): the sweep spared the room, but the 📣
//!      sent immediately after `create_dm` was never decryptable by the
//!      invited target (megolm recipients computed before the invite
//!      reached the bot's local store — no key ever shared).
//!
//!   Both fixes live in `crates/matrix-rtc` (the other instance's file): a
//!   non-lagging membership source for the exemption, and deferring the
//!   outreach send until the room state has caught up. Evidence on #100.
//!   Flip to `Pass` when that lands.
//!
//!   The target is a CRYPTO client, not the raw-CS one: live testing showed
//!   the SDK's `create_dm` mints the operator-msg DM **encrypted** (initial
//!   `m.room.encryption`), and the 📣 is sent while the target is still
//!   invited — so reading it requires a device that existed at send time
//!   (the SDK pre-shares the megolm session to invited members' devices).
//!   A deviceless raw-CS user sees only ciphertext, exactly like a real
//!   `msg` recipient on a non-E2EE client would.

use anyhow::{Context as _, Result, bail};
use std::sync::Arc;
use std::time::Duration;

use crate::actors::crypto_dm::MatrixCryptoDmClient;
use crate::actors::mgmt_client::MatrixManagementClient;
use crate::bridge_proc::{BridgeUnderTest, tenant_entry};
use crate::scenario::{Ctx, Expectation, ScenarioDef};

pub static MGMT_INVITE_OFFLINE: ScenarioDef = ScenarioDef {
    name: "mgmt_invite_offline",
    expectation: Expectation::Pass,
    run: boxed_offline,
};

pub static MGMT_DM_PENDING_REAP: ScenarioDef = ScenarioDef {
    name: "mgmt_dm_pending_reap",
    // Was KnownFail{#104} (local cold-outreach UTD): Tuwunel v1.8.2 fixed the
    // local leg — the scenario unexpected-PASSED nightlies #327/#328, and its
    // #329 "failure" was the harness txn-dedup swallow, not #104 (the federated
    // leg, `fed_cold_outreach_utd`, stays KnownFail). Flipped 2026-08-19 (#100).
    expectation: Expectation::Pass,
    run: boxed_reap,
};

fn boxed_offline(ctx: Arc<Ctx>) -> crate::scenario::ScenarioFuture {
    Box::pin(run_offline(ctx))
}

fn boxed_reap(ctx: Arc<Ctx>) -> crate::scenario::ScenarioFuture {
    Box::pin(run_reap(ctx))
}

/// Idle no-owner tenant: gives `await_ready` a bridge to anchor on, and (for
/// the reap scenario) populates `safe_rooms` the way a real deployment's
/// tenant rooms do — the sweep must reap the pending DM *despite* having
/// legitimate rooms to protect.
const IDLE_TENANT: &str = "Invite Drill Anchor";

/// Compressed sweep tick (min the bridge accepts is 5).
const SWEEP_SECS: &str = "5";

/// The `/bridge` command-registration ERROR is expected on the shared test
/// guild; everything else is a failure (same filter as every mgmt scenario).
fn unexpected_errors(bridge: &BridgeUnderTest) -> Vec<String> {
    bridge
        .scraper
        .error_events()
        .into_iter()
        .filter(|e| !e.message.contains("register the /bridge command"))
        .map(|e| e.message.clone())
        .collect()
}

// ─── #97a: invite while the bridge is down → exactly one accept ────────────

async fn run_offline(ctx: Arc<Ctx>) -> Result<()> {
    let cfg = &ctx.cfg;
    cfg.test_as_token()?;

    // Everything user-side happens BEFORE the bridge starts: the DM + pending
    // invite (the offline case under test) and the marker the greeting scan
    // reads relative to (the greeting can beat any post-start marker).
    let user =
        MatrixManagementClient::register(cfg, "nvbtest_offline", "NVB Test Offline Invite").await?;
    let dm = user.create_dm().await.context("creating pre-start DM")?;
    let marker = user.send(&dm, "drill: invited while bridge down").await?;

    // Arm the #89a cooldown exactly as prod ran it — unset (the standing
    // harness block) makes the cooldown path inert and hides #97 entirely.
    let tenants = tenant_entry(IDLE_TENANT, &cfg.room_a, &cfg.guild_id, &cfg.channel_a);
    let bridge = BridgeUnderTest::start_with_management_opts(
        cfg,
        ctx.profile,
        &tenants,
        &ctx.artifacts.join("bridge"),
        &[],
        "mgmt_cooldown_secs = 60\n",
    )
    .await
    .context("starting bridge-under-test")?;

    let result = drive_offline(&user, &dm, &marker, &bridge).await;

    let teardown = match result {
        Ok(()) => bridge.sigterm_and_wait(Duration::from_secs(20)).await.context("SIGTERM drain"),
        Err(e) => {
            bridge.kill().await;
            Err(e)
        }
    };
    if let Err(e) = user.leave_as(&dm, user.mxid()).await {
        eprintln!("offline-drill user leave failed (non-fatal): {e}");
    }
    teardown
}

async fn drive_offline(
    user: &MatrixManagementClient,
    dm: &str,
    marker: &str,
    bridge: &BridgeUnderTest,
) -> Result<()> {
    bridge.await_ready(&[IDLE_TENANT], Duration::from_secs(120)).await?;
    bridge
        .scraper
        .await_event(Duration::from_secs(120), |e| e.message.contains("management bot ready"))
        .await
        .context("management bot never became ready")?;

    // The startup sweep must accept the offline invite and greet.
    user.await_bot_joined(dm, Duration::from_secs(60)).await?;
    user.wait_for_bot_message_containing(
        dm,
        marker,
        "I link Discord voice channels",
        Duration::from_secs(30),
    )
    .await
    .context("no greeting after the offline-invite accept")?;

    // Settle window: the pre-fix double-processing (sync redelivery of the
    // still-pending invite vs. the raw join that leaves local state Invited)
    // plays out within the first sync cycles after the accept.
    tokio::time::sleep(Duration::from_secs(15)).await;

    // POST-FIX CONTRACT — pre-fix each of these is exactly the #97a failure.
    // Both scans are scoped to THIS room via the log's `room_id` field, so a
    // stale invite left by an aborted earlier run can't skew the counts.
    // (a) the same invite must not be processed into a second accept…
    let accepts = bridge
        .scraper
        .events_matching(|e| {
            e.message.contains("accepting DM invite from self-serve user")
                && e.field("room_id") == Some(dm)
        })
        .len();
    if accepts != 1 {
        bail!("expected exactly ONE invite accept for {dm}, saw {accepts} (#97 double-processing)");
    }
    // (b) …and never into a cooldown DECLINE of the just-joined room…
    let declines = bridge
        .scraper
        .events_matching(|e| {
            e.message.contains("declining self-serve invite") && e.field("room_id") == Some(dm)
        })
        .len();
    if declines != 0 {
        bail!(
            "the redelivered invite hit the just-stamped #89a cooldown and was declined \
             (join-then-leave, #97): {declines} decline(s)"
        );
    }
    // (c) …so the bot is still a member once the dust settles.
    let members = user.joined_members(dm).await?;
    if !members.iter().any(|m| m != user.mxid()) {
        bail!("bot no longer joined to the DM after the settle window (join-then-leave, #97)");
    }

    let errors = unexpected_errors(bridge);
    if !errors.is_empty() {
        bail!("bridge logged {} ERROR event(s); first: {}", errors.len(), errors[0]);
    }
    Ok(())
}

// ─── #97b: bot-initiated DM must survive the sweep until accepted ──────────

async fn run_reap(ctx: Arc<Ctx>) -> Result<()> {
    let cfg = &ctx.cfg;
    cfg.test_as_token()?;

    let op = MatrixManagementClient::register(cfg, "nvbtest_operator", "NVB Test Operator")
        .await
        .context("registering operator")?;
    // Crypto target, registered (device keys + OTKs uploaded) BEFORE the
    // operator's `msg`: the bot's `create_dm` DM is encrypted and the megolm
    // session is shared to devices present at send time.
    let target = MatrixCryptoDmClient::register(cfg, "nvbtest_dmtarget", "NVB Test DM Target")
        .await
        .context("registering crypto target")?;
    // Stale pending invites from earlier (failed) runs would confuse the
    // which-room-did-the-bot-just-open scan — reject them first.
    for stale in target.invited_rooms() {
        if let Err(e) = target.reject_invite(&stale).await {
            eprintln!("rejecting stale invite {stale} failed (non-fatal): {e}");
        }
    }

    let tenants = tenant_entry(IDLE_TENANT, &cfg.room_a, &cfg.guild_id, &cfg.channel_a);
    let bridge = BridgeUnderTest::start_with_management_env(
        cfg,
        ctx.profile,
        &tenants,
        &ctx.artifacts.join("bridge"),
        &[("NVB_MAINTENANCE_INTERVAL_SECS", SWEEP_SECS)],
    )
    .await
    .context("starting bridge-under-test")?;

    let mut pending_dm = None;
    let mut op_dm = None;
    let result = drive_reap(&op, &target, &bridge, &mut pending_dm, &mut op_dm).await;

    let teardown = match result {
        Ok(()) => bridge.sigterm_and_wait(Duration::from_secs(20)).await.context("SIGTERM drain"),
        Err(e) => {
            bridge.kill().await;
            Err(e)
        }
    };
    // Drill rooms must not accumulate: the target leaves (or rejects) the
    // bot-initiated DM and logs out its ephemeral device, the operator
    // leaves their command DM. The bot's own memberships are its #89b reap's
    // problem.
    if let Err(e) = target.teardown(pending_dm.as_deref()).await {
        eprintln!("reap-drill target teardown failed (non-fatal): {e}");
    }
    if let Some(room) = &op_dm {
        if let Err(e) = op.leave_as(room, op.mxid()).await {
            eprintln!("reap-drill operator leave failed (non-fatal): {e}");
        }
    }
    teardown
}

async fn drive_reap(
    op: &MatrixManagementClient,
    target: &MatrixCryptoDmClient,
    bridge: &BridgeUnderTest,
    pending_dm: &mut Option<String>,
    op_dm: &mut Option<String>,
) -> Result<()> {
    bridge.await_ready(&[IDLE_TENANT], Duration::from_secs(120)).await?;
    bridge
        .scraper
        .await_event(Duration::from_secs(120), |e| e.message.contains("management bot ready"))
        .await
        .context("management bot never became ready")?;

    // Operator opens their command DM and makes the bot INITIATE a DM to the
    // target (#94 `msg` → `create_dm` with the target merely invited).
    let opdm = op.create_dm().await.context("opening operator DM")?;
    *op_dm = Some(opdm.clone());
    op.await_bot_joined(&opdm, Duration::from_secs(60)).await?;
    op.await_greeting(&opdm, Duration::from_secs(30)).await?;
    let ev = op
        .send(&opdm, &format!("msg {} nvb e2e pending-reap drill", target.mxid()))
        .await?;
    let reply = op.wait_for_reply(&opdm, &ev, Duration::from_secs(45)).await?;
    if !reply.contains("✅ Sent to") {
        bail!("operator msg was not delivered: {reply:?}");
    }

    // Find the bot-initiated DM via the target's pending invites.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let room = loop {
        if let Some(room) = target.invited_rooms().into_iter().next() {
            break room;
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("target never saw the bot-initiated DM invite");
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    *pending_dm = Some(room.clone());

    // Hold WITHOUT accepting across several compressed sweep ticks — the real
    // incident window is a user who doesn't touch the invite for minutes.
    tokio::time::sleep(Duration::from_secs(20)).await;

    // POST-FIX CONTRACT — pre-fix the sweep deterministically reaps here:
    // (a) the sweep must not have left the pending-invite DM…
    let reaped = bridge.scraper.events_matching(|e| {
        e.message.contains("maintenance: left room")
            && e.field("room_id") == Some(room.as_str())
            && e.field("reason").is_some_and(|r| r.contains("#89b"))
    });
    if !reaped.is_empty() {
        bail!(
            "the #89b empty-room sweep reaped the bot-initiated DM {room} while its invite \
             was still pending (#97)"
        );
    }
    // (b) …the target can still accept it…
    target.join(&room).await.context("accepting the pending DM invite")?;
    // (c) …into a room the bot still inhabits, with the operator message
    // READABLE — the DM is encrypted (SDK `create_dm`), so this decrypts the
    // pre-join 📣 via the megolm session the bot shared to our device while
    // we were still invited.
    target
        .wait_for_bot_message_containing(
            &room,
            "Message from the bridge operator",
            Duration::from_secs(30),
        )
        .await
        .context("operator message not readable after accepting the invite")?;
    let members = target.joined_members(&room).await?;
    if !members.iter().any(|m| m != target.mxid()) {
        bail!("bot not joined to the DM after the target accepted (reaped, #97)");
    }

    let errors = unexpected_errors(bridge);
    if !errors.is_empty() {
        bail!("bridge logged {} ERROR event(s); first: {}", errors.len(), errors[0]);
    }
    Ok(())
}
