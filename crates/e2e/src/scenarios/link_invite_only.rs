//! `mgmt_link_invite_only` — the #111 awaiting-invite exemption, end to end.
//!
//! The gap this closes: a self-serve `link` of an invite-only room used to be
//! a dead end — the probe join 403s, and the invite the (pre-beta.3) reply
//! instructed was auto-rejected by the DMs-only invite gate. With #111 the
//! failed join arms a single-use **awaiting-invite intent**, and an invite
//! from EXACTLY the requester for EXACTLY that room resumes the link.
//!
//! Driven live against the real bridge:
//!
//! * self-serve `link` of a fresh **invite-only** room → the reply arms the
//!   intent ("invite ... within N minutes"), nothing written;
//! * `list` surfaces the in-flight state ("awaiting your invite" — the
//!   avoid-confusion status contract);
//! * the requester invites the bot from the room → the invite gate accepts it
//!   (`link_intent` — asserted via the bridge log) **despite** `mgmt_cooldown_secs`
//!   being armed here: this user's DM-creation invite-join stamped the #89a
//!   cooldown moments earlier, so the room invite lands inside it and would be
//!   DECLINED without the matched-intent bypass — this scenario exists to
//!   prove that bypass;
//! * the resumed link probes and lands at a pending Discord-approval code in
//!   the origin DM, with the bot a joined member of the room.
//!
//! The negative twin (a non-direct invite with NO matching intent is still
//! rejected) stays covered by `mgmt_self_serve_link`, which runs the same
//! gate with an intent-less user.

use crate::actors::mgmt_client::MatrixManagementClient;
use crate::bridge_proc::{BridgeUnderTest, tenant_entry};
use crate::config::E2eConfig;
use crate::scenario::{Ctx, Expectation, ScenarioDef};
use anyhow::{Context as _, Result, bail};
use std::sync::Arc;
use std::time::Duration;

pub static MGMT_LINK_INVITE_ONLY: ScenarioDef = ScenarioDef {
    name: "mgmt_link_invite_only",
    expectation: Expectation::Pass,
    run: boxed,
};

fn boxed(ctx: Arc<Ctx>) -> crate::scenario::ScenarioFuture {
    Box::pin(run(ctx))
}

/// Idle tenant so `await_ready` has a bridge to wait for; points at the
/// standing Auto Test 01 pair and stays closed (no participants).
const IDLE_TENANT: &str = "Invite Drill Anchor";

async fn run(ctx: Arc<Ctx>) -> Result<()> {
    let cfg = &ctx.cfg;
    cfg.test_as_token()?;

    let tenants = tenant_entry(IDLE_TENANT, &cfg.room_a, &cfg.guild_id, &cfg.channel_a);
    // Arm the #89a cooldown: the whole point (see module doc) is proving the
    // matched-intent invite is exempt from the cooldown the user's own
    // DM-creation join stamped moments earlier.
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
    let reply_to = Duration::from_secs(45);
    bridge.await_ready(&[IDLE_TENANT], Duration::from_secs(120)).await?;
    bridge
        .scraper
        .await_event(Duration::from_secs(120), |e| e.message.contains("management bot ready"))
        .await
        .context("management bot never became ready")?;

    let user =
        MatrixManagementClient::register(cfg, "nvbtest_inv", "NVB Invite Drill User").await?;
    let dm = user.create_dm().await.context("open DM")?;
    user.await_bot_joined(&dm, Duration::from_secs(60)).await?;
    user.await_greeting(&dm, Duration::from_secs(30)).await?;

    // A fresh invite-only room, PL-prepped, bot NOT invited yet.
    let room = user.create_private_room_prepped().await.context("create invite-only room")?;

    // 1. `link` → probe join fails → the reply arms the awaiting-invite intent.
    let ev = user
        .send(&dm, &format!("link InviteDrill {} {} {}", room, cfg.guild_id, cfg.channel_a))
        .await?;
    let reply = user.wait_for_reply(&dm, &ev, reply_to).await?;
    if !reply.contains("could not join") {
        bail!("link of an invite-only room did not fail at the join: {reply:?}");
    }
    if !(reply.contains("invite") && reply.contains("minutes")) {
        bail!("join-failure reply did not arm the awaiting-invite intent: {reply:?}");
    }

    // 2. `list` surfaces the in-flight intent (the status contract).
    let ev_list = user.send(&dm, "list").await?;
    let listing = user.wait_for_reply(&dm, &ev_list, reply_to).await?;
    if !listing.contains("awaiting your invite") {
        bail!("`list` does not show the awaiting-invite intent: {listing:?}");
    }

    // 3. Invite the bot — inside the cooldown window stamped by this user's
    //    DM join above. Without the #111 bypass this is declined
    //    ("invite cooldown").
    user.invite(&room, &cfg.bot_user_id()).await.context("inviting the bot")?;
    bridge
        .scraper
        .await_event(Duration::from_secs(60), |e| {
            e.message.contains("accepting invite matching an awaiting-invite link intent")
        })
        .await
        .context("the matched-intent invite was not accepted (#111 bypass)")?;

    // 4. The resumed link lands at a pending Discord-approval code in the DM.
    let outcome = user
        .wait_for_bot_message_containing(&dm, &ev_list, "approval required", Duration::from_secs(60))
        .await
        .context("resumed link never reached the pending-approval reply")?;
    if !outcome.contains("/bridge approve") {
        bail!("pending-approval reply carries no approval command: {outcome:?}");
    }

    // 5. The bot is a joined member of the invite-only room.
    let members = user.joined_members(&room).await?;
    if !members.iter().any(|m| m == &cfg.bot_user_id()) {
        bail!("bot is not a joined member of the invite-only room after the resume");
    }

    // Teardown hygiene: the user leaves; the bot-only drill room is #89b food
    // for a future run's maintenance sweep.
    if let Err(e) = user.leave_as(&room, user.mxid()).await {
        eprintln!("drill-room leave failed (non-fatal): {e}");
    }
    Ok(())
}
