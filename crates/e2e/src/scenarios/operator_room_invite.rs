//! `mgmt_operator_room_invite` — the #120 repro: an OPERATOR invites the bot
//! into a populated (>2-member) room, the documented manual first step for
//! bridging a restricted room.
//!
//! Suspected bug (found building #111, see PR #119): the invite gate accepts
//! the operator's invite unconditionally, but `greet_when_joined`'s #85
//! anti-spoof check — meant to stop *self-serve* users pulling the bot into
//! group rooms via a forged `is_direct` — runs after EVERY accepted-invite
//! join and LEAVES any room that isn't a ≤2-member DM. If that holds, the bot
//! joins and then leaves within ~14 s, silently breaking the manual
//! invite-then-relink flow (and consuming the invite, so a follow-up `link`
//! probe cannot rejoin an invite-only room).
//!
//! Reproduced live 2026-07-13 (`leave_logged=true bot_still_joined=false`,
//! issue #120 comment 1264), then FIXED: `greet_when_joined` now keeps
//! operator-invited group rooms joined (skipping only the greeting); the #85
//! leave still applies to every self-serve join. This scenario asserts the
//! fixed contract — the bot stays joined — and guards it as a regression.

use crate::actors::mgmt_client::{MatrixManagementClient, register_and_join};
use crate::bridge_proc::{BridgeUnderTest, tenant_entry};
use crate::config::E2eConfig;
use crate::scenario::{Ctx, Expectation, ScenarioDef};
use anyhow::{Context as _, Result, bail};
use std::sync::Arc;
use std::time::Duration;

pub static MGMT_OPERATOR_ROOM_INVITE: ScenarioDef = ScenarioDef {
    name: "mgmt_operator_room_invite",
    expectation: Expectation::Pass,
    run: boxed,
};

fn boxed(ctx: Arc<Ctx>) -> crate::scenario::ScenarioFuture {
    Box::pin(run(ctx))
}

const IDLE_TENANT: &str = "Op Invite Anchor";

async fn run(ctx: Arc<Ctx>) -> Result<()> {
    let cfg = &ctx.cfg;
    cfg.test_as_token()?;

    let tenants = tenant_entry(IDLE_TENANT, &cfg.room_a, &cfg.guild_id, &cfg.channel_a);
    let bridge = BridgeUnderTest::start_with_management(
        cfg,
        ctx.profile,
        &tenants,
        &ctx.artifacts.join("bridge"),
    )
    .await
    .context("starting bridge-under-test")?;

    let result = drive(cfg, &bridge).await;
    match result {
        Ok(()) => {
            bridge.sigterm_and_wait(Duration::from_secs(20)).await.context("SIGTERM drain")?;
            Ok(())
        }
        Err(e) => {
            bridge.kill().await;
            Err(e)
        }
    }
}

async fn drive(cfg: &E2eConfig, bridge: &BridgeUnderTest) -> Result<()> {
    bridge.await_ready(&[IDLE_TENANT], Duration::from_secs(120)).await?;
    bridge
        .scraper
        .await_event(Duration::from_secs(120), |e| e.message.contains("management bot ready"))
        .await
        .context("management bot never became ready")?;

    // Operator (harness allowlist) + a populated invite-only room: creator +
    // two extra members = 3 joined humans, safely past the ≤2 DM threshold.
    let op = MatrixManagementClient::register(cfg, "nvbtest_operator", "NVB Test Operator")
        .await
        .context("registering operator")?;
    let room = op.create_private_room_prepped().await.context("create restricted room")?;
    for lp in ["nvbtest_opx1", "nvbtest_opx2"] {
        let mxid = format!("@{lp}:{}", cfg.server_domain());
        op.invite(&room, &mxid).await.with_context(|| format!("inviting {lp}"))?;
        register_and_join(cfg, lp, &room).await.with_context(|| format!("joining {lp}"))?;
    }

    // The documented manual step: the operator invites the bot.
    op.invite(&room, &cfg.bot_user_id()).await.context("operator inviting the bot")?;
    bridge
        .scraper
        .await_event(Duration::from_secs(60), |e| {
            e.message.contains("accepting invite from allowlisted operator")
        })
        .await
        .context("operator invite was not accepted")?;

    // The greeting task probes joined-state at +1/+3/+10 s; give the whole
    // window (plus sync lag) to act, then assert the DESIRED contract.
    tokio::time::sleep(Duration::from_secs(25)).await;
    let left = bridge.scraper.events_matching(|e| {
        e.message.contains("invited room is not a DM")
    });
    let members = op.joined_members(&room).await?;
    let bot_joined = members.iter().any(|m| m == &cfg.bot_user_id());
    // Cleanup before verdict (drill room; #89b handles the rest).
    for m in ["nvbtest_opx1", "nvbtest_opx2"] {
        let mxid = format!("@{m}:{}", cfg.server_domain());
        let _ = op.leave_as(&room, &mxid).await;
    }
    let _ = op.leave_as(&room, op.mxid()).await;
    if !left.is_empty() || !bot_joined {
        bail!(
            "the #85 anti-spoof greet-leave evicted the bot from an operator-invited room \
             (#120): leave_logged={} bot_still_joined={bot_joined} — the manual \
             invite-then-relink flow is broken for populated rooms",
            !left.is_empty(),
        );
    }
    Ok(())
}
