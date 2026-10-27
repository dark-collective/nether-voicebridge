//! #150 — `mgmt_dm_reuse`: bot→user notices land in the DM the user already
//! has with the bot, instead of a fresh room each.
//!
//! Before #150 every notice (`msg`, `announce`, the pairing confirmation, the
//! failover notice) called the SDK's `create_dm`, which ALWAYS creates a
//! room — and the DM a user opens is joined over the raw appservice path, so
//! it never reached the bot's `m.direct` either. One live user ended up with
//! three DMs with the bot. This scenario:
//!
//! 1. has a crypto user open an encrypted DM with the bot and talk in it
//!    (the room the bot must now prefer);
//! 2. has the operator `msg` that user TWICE;
//! 3. asserts both 📣 messages arrive, decryptable, in the user's own DM,
//!    and that the user never received an invite to a second room.

use anyhow::{Context as _, Result, bail};
use std::sync::Arc;
use std::time::Duration;

use crate::actors::crypto_dm::MatrixCryptoDmClient;
use crate::actors::mgmt_client::MatrixManagementClient;
use crate::bridge_proc::{BridgeUnderTest, tenant_entry};
use crate::scenario::{Ctx, Expectation, ScenarioDef};

pub static MGMT_DM_REUSE: ScenarioDef = ScenarioDef {
    name: "mgmt_dm_reuse",
    expectation: Expectation::Pass,
    run: boxed,
};

fn boxed(ctx: Arc<Ctx>) -> crate::scenario::ScenarioFuture {
    Box::pin(run(ctx))
}

/// Idle no-owner anchor tenant (same pattern as `mgmt_dm_e2ee`).
const IDLE_TENANT: &str = "DM Reuse Idle Anchor";

async fn run(ctx: Arc<Ctx>) -> Result<()> {
    let cfg = &ctx.cfg;
    cfg.test_as_token()?;

    let op = MatrixManagementClient::register(cfg, "nvbtest_operator", "NVB Test Operator")
        .await
        .context("registering operator")?;
    let user = MatrixCryptoDmClient::register(cfg, "nvbtest_dmreuse", "NVB Test DM Reuse")
        .await
        .context("registering crypto DM user")?;
    // Stale invites from an aborted earlier run would read as a "second DM".
    for stale in user.invited_rooms() {
        if let Err(e) = user.reject_invite(&stale).await {
            eprintln!("rejecting stale invite {stale} failed (non-fatal): {e}");
        }
    }

    let tenants = tenant_entry(IDLE_TENANT, &cfg.room_a, &cfg.guild_id, &cfg.channel_a);
    let bridge = BridgeUnderTest::start_with_management(
        cfg,
        ctx.profile,
        &tenants,
        &ctx.artifacts.join("bridge"),
    )
    .await
    .context("starting bridge-under-test")?;

    let mut user_dm = None;
    let mut op_dm = None;
    let result = drive(&op, &user, &bridge, &mut user_dm, &mut op_dm).await;

    let teardown = match result {
        Ok(()) => bridge.sigterm_and_wait(Duration::from_secs(20)).await.context("SIGTERM drain"),
        Err(e) => {
            bridge.kill().await;
            Err(e)
        }
    };
    // A bot-created stray DM (the failure case) is left by rejecting it here.
    for stray in user.invited_rooms() {
        let _ = user.reject_invite(&stray).await;
    }
    if let Err(e) = user.teardown(user_dm.as_deref()).await {
        eprintln!("dm-reuse user teardown failed (non-fatal): {e}");
    }
    if let Some(room) = &op_dm {
        if let Err(e) = op.leave_as(room, op.mxid()).await {
            eprintln!("dm-reuse operator leave failed (non-fatal): {e}");
        }
    }
    teardown
}

async fn drive(
    op: &MatrixManagementClient,
    user: &MatrixCryptoDmClient,
    bridge: &BridgeUnderTest,
    user_dm: &mut Option<String>,
    op_dm: &mut Option<String>,
) -> Result<()> {
    bridge.await_ready(&[IDLE_TENANT], Duration::from_secs(120)).await?;
    bridge
        .scraper
        .await_event(Duration::from_secs(120), |e| e.message.contains("management bot ready"))
        .await
        .context("management bot never became ready")?;

    // 1. The user's own DM, greeted and used.
    let dm = user.create_encrypted_dm().await.context("creating the user's DM")?;
    *user_dm = Some(dm.clone());
    user.wait_for_bot_message_containing(&dm, "I link Discord voice channels", Duration::from_secs(60))
        .await
        .context("bot greeting never arrived in the user's DM")?;
    user.send(&dm, "help").await.context("sending help")?;
    user.wait_for_bot_message_containing(&dm, "remove a bridge you created", Duration::from_secs(45))
        .await
        .context("no help reply in the user's DM")?;

    // 2. Two operator notices to the user.
    let opdm = op.create_dm().await.context("opening operator DM")?;
    *op_dm = Some(opdm.clone());
    op.await_bot_joined(&opdm, Duration::from_secs(60)).await?;
    op.await_greeting(&opdm, Duration::from_secs(30)).await?;
    for tag in ["dm-reuse one", "dm-reuse two"] {
        let ev = op.send(&opdm, &format!("msg {} nvb e2e {tag}", user.mxid())).await?;
        let reply = op.wait_for_reply(&opdm, &ev, Duration::from_secs(45)).await?;
        if !reply.contains("✅ Sent to") {
            bail!("operator msg ({tag}) was not delivered: {reply:?}");
        }
    }

    // 3. Both land in the user's DM…
    for tag in ["dm-reuse one", "dm-reuse two"] {
        user.wait_for_bot_message_containing(&dm, tag, Duration::from_secs(30))
            .await
            .with_context(|| format!("operator notice {tag:?} did not land in the user's existing DM"))?;
    }
    // …and no second room was opened for them.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let strays = user.invited_rooms();
    if !strays.is_empty() {
        bail!("the bot opened {} new DM(s) instead of reusing {dm}: {strays:?} (#150)", strays.len());
    }

    let errors: Vec<_> = bridge
        .scraper
        .error_events()
        .into_iter()
        .filter(|e| !e.message.contains("register the /bridge command"))
        .map(|e| e.message.clone())
        .collect();
    if !errors.is_empty() {
        bail!("bridge logged {} ERROR event(s); first: {}", errors.len(), errors[0]);
    }
    Ok(())
}
