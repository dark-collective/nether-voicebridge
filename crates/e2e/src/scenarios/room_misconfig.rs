//! #95 tiers 1+2 — `room_misconfig_notice`: a bridged room WITHOUT the
//! `m.call.member` PL-0 override (the single most common real-world
//! bridge-breaker; it silently kills Discord→Matrix) is detected by the
//! maintenance sweep and announced.
//!
//! Setup exploits room defaults: a freshly created room has NO
//! `events["org.matrix.msc3401.call.member"]` override and `state_default`
//! 50, so it is misconfigured **by construction**. The OPERATOR test user
//! creates it (a self-serve user's non-direct invite would be rejected by
//! the #71/#85 invite gate) and it is seeded as a tenant, so the sweep —
//! compressed to seconds via `NVB_MAINTENANCE_INTERVAL_SECS` — must:
//!
//! 1. post the fix-it notice into the room itself,
//! 2. append a `room_misconfigured` audit event,
//! 3. WARN (`bridged room misconfigured`) for the ops tooling,
//! 4. NOT repeat the notice on the next sweeps (24 h per-room cooldown).

use anyhow::{Context as _, Result, bail};
use std::sync::Arc;
use std::time::Duration;

use crate::actors::mgmt_client::MatrixManagementClient;
use crate::bridge_proc::{BridgeUnderTest, tenant_entry};
use crate::scenario::{Ctx, Expectation, ScenarioDef};

pub static ROOM_MISCONFIG_NOTICE: ScenarioDef = ScenarioDef {
    name: "room_misconfig_notice",
    expectation: Expectation::Pass,
    run: boxed,
};

fn boxed(ctx: Arc<Ctx>) -> crate::scenario::ScenarioFuture {
    Box::pin(run(ctx))
}

const BRIDGE_NAME: &str = "Misconfig Drill";
/// Compressed sweep tick for the drill (min the bridge accepts is 5).
const SWEEP_SECS: &str = "5";

async fn run(ctx: Arc<Ctx>) -> Result<()> {
    let cfg = &ctx.cfg;
    cfg.test_as_token()?;

    // 1. Operator creates a fresh room (misconfigured by construction: no
    //    call.member override, state_default 50) with the bot invited.
    let op = MatrixManagementClient::register(cfg, "nvbtest_operator", "NVB Test Operator")
        .await
        .context("registering operator")?;
    let room = op.create_room_nondirect().await.context("creating drill room")?;

    // 2. Seed it as a tenant and start the bridge with a compressed sweep.
    let tenants = tenant_entry(BRIDGE_NAME, &room, &cfg.guild_id, &cfg.channel_a);
    let bridge = BridgeUnderTest::start_with_management_env(
        cfg,
        ctx.profile,
        &tenants,
        &ctx.artifacts.join("bridge"),
        &[("NVB_MAINTENANCE_INTERVAL_SECS", SWEEP_SECS)],
    )
    .await
    .context("starting bridge-under-test")?;

    let result = drive(&ctx, &op, &room, &bridge).await;

    // Teardown either way: drain the bridge, then both test users leave the
    // drill room so it doesn't accumulate across runs (best-effort).
    let teardown = match result {
        Ok(()) => bridge.sigterm_and_wait(Duration::from_secs(20)).await.context("SIGTERM drain"),
        Err(e) => {
            bridge.kill().await;
            Err(e)
        }
    };
    for mxid in [cfg.bot_user_id(), op.mxid().to_string()] {
        if let Err(e) = op.leave_as(&room, &mxid).await {
            eprintln!("drill-room leave for {mxid} failed (non-fatal): {e}");
        }
    }
    teardown
}

async fn drive(
    ctx: &Ctx,
    op: &MatrixManagementClient,
    room: &str,
    bridge: &BridgeUnderTest,
) -> Result<()> {
    bridge.await_ready(&[BRIDGE_NAME], Duration::from_secs(120)).await?;
    bridge
        .scraper
        .await_event(Duration::from_secs(120), |e| e.message.contains("management bot ready"))
        .await
        .context("management bot never became ready")?;

    // Marker event so the notice-wait only scans messages from here on.
    let marker = op.send(room, "drill: awaiting misconfig notice").await?;

    // 3. The sweep must post the fix-it notice into the room itself.
    let notice = op
        .wait_for_bot_message_containing(
            room,
            &marker,
            "org.matrix.msc3401.call.member",
            Duration::from_secs(60),
        )
        .await
        .context("maintenance sweep never posted the in-room misconfig notice")?;
    if !notice.contains("blocking the voice bridge") || !notice.contains("/devtools") {
        bail!("notice lacks the expected fix instructions: {notice:?}");
    }

    // 4. The WARN for ops tooling fired…
    bridge
        .scraper
        .await_event(Duration::from_secs(30), |e| {
            e.message.contains("bridged room misconfigured")
        })
        .await
        .context("no 'bridged room misconfigured' WARN")?;
    // …and the audit trail recorded it.
    let audit_path = ctx.artifacts.join("bridge").join("data").join("audit.jsonl");
    let audit = tokio::fs::read_to_string(&audit_path)
        .await
        .with_context(|| format!("reading {}", audit_path.display()))?;
    if !audit.contains("\"room_misconfigured\"") || !audit.contains(room) {
        bail!("audit.jsonl lacks the room_misconfigured event for {room}");
    }

    // 5. Cooldown: across the next few sweep ticks (~4 × 5 s) the notice must
    //    NOT repeat — the per-room 24 h stamp gates it. (Needle-based: the
    //    bot's greeting / "Unrecognized command" chatter in this 2-member
    //    room is expected and ignored.)
    let cooldown_marker = op.send(room, "drill: cooldown window").await?;
    if op
        .wait_for_bot_message_containing(
            room,
            &cooldown_marker,
            "org.matrix.msc3401.call.member",
            Duration::from_secs(20),
        )
        .await
        .is_ok()
    {
        bail!("misconfig notice repeated within the cooldown window");
    }

    // ── No unexpected ERROR events ────────────────────────────────────────
    let errors: Vec<_> = bridge
        .scraper
        .error_events()
        .into_iter()
        .filter(|e| !e.message.contains("register the /bridge command"))
        .collect();
    if !errors.is_empty() {
        bail!("bridge logged {} ERROR event(s); first: {}", errors.len(), errors[0].message);
    }
    Ok(())
}
