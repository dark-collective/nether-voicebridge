//! #109 — federated scenarios against the `nether.autos` fixture homeserver.
//!
//! The whole #96/#97/#103 incident class was invisible to the suite because
//! every actor it could mint lived on the SAME homeserver as the bridge
//! (see `invite_lifecycle`'s module doc). These scenarios drive genuinely
//! REMOTE `@autotest_*:nether.autos` users — federated invites with real
//! multi-second join windows, cross-server E2EE device fan-out, and v12
//! (domainless-id) rooms minted on another server. They all gate on
//! [`crate::config::E2eConfig::federation_or_skip`]: without the `fed_*`
//! config group they report `skipped`, so the suite still runs anywhere.
//! Fixture provisioning: `docs/e2e-homeserver.md`.
//!
//! * [`FED_DM_INVITE_LIFECYCLE`] (`Pass`) — the #97 regression guard over
//!   REAL federation: a remote user DMs the running bot; the invite arrives
//!   over federation and the bot's accept-join takes actual seconds — the
//!   window where pre-fix invite redelivery hit the freshly-stamped #89a
//!   cooldown and join-then-left. Same-server variant: `mgmt_invite_offline`.
//! * [`FED_COLD_OUTREACH_UTD`] (`KnownFail{#104}`) — operator `msg` to a
//!   remote crypto user: the bot's `create_dm` + immediate 📣 must reach a
//!   recipient whose devices/OTKs live on ANOTHER server. #104's residual
//!   (megolm session computed before the invited member's devices are known
//!   → first message UTD) is expected to reproduce harder over federation
//!   than the same-server `mgmt_dm_pending_reap` twin.
//! * [`FED_LINK_V12`] (`Pass`) — the #103 acceptance test: a remote user
//!   mints a **v12 (domainless-id) room on the fixture**, then self-serve
//!   `link`s it. Exercises the v12 room-id handling in `link` and the
//!   via-hinted first-contact probe join that #103 shipped (v0.3.0-beta.1).
//!   Deliberately invites the bot at creation — the federated invite is what
//!   teaches nether.im the room; an UNINVITED variant (pure via-discovery
//!   join) is follow-up hardening.
//! * `fed_slow_join_greeting` — DEFERRED: [`FED_DM_INVITE_LIFECYCLE`]
//!   already proves the greeting lands after a real federated join; a
//!   dedicated scenario only adds value with injected federation latency
//!   beyond `greet_when_joined`'s 1/3/10 s retry ladder (mgmt.rs), which
//!   needs a chaos proxy the fixture doesn't have yet (#106's later tier).

use anyhow::{Context as _, Result, bail};
use std::sync::Arc;
use std::time::Duration;

use crate::actors::crypto_dm::MatrixCryptoDmClient;
use crate::actors::mgmt_client::MatrixManagementClient;
use crate::bridge_proc::{BridgeUnderTest, tenant_entry};
use crate::scenario::{Ctx, Expectation, ScenarioDef};

pub static FED_DM_INVITE_LIFECYCLE: ScenarioDef = ScenarioDef {
    name: "fed_dm_invite_lifecycle",
    expectation: Expectation::Pass,
    run: boxed_invite,
};

pub static FED_COLD_OUTREACH_UTD: ScenarioDef = ScenarioDef {
    name: "fed_cold_outreach_utd",
    expectation: Expectation::KnownFail { issue: "#104" },
    run: boxed_cold,
};

pub static FED_LINK_V12: ScenarioDef = ScenarioDef {
    name: "fed_link_v12",
    expectation: Expectation::Pass,
    run: boxed_link,
};

fn boxed_invite(ctx: Arc<Ctx>) -> crate::scenario::ScenarioFuture {
    Box::pin(run_invite(ctx))
}

fn boxed_cold(ctx: Arc<Ctx>) -> crate::scenario::ScenarioFuture {
    Box::pin(run_cold(ctx))
}

fn boxed_link(ctx: Arc<Ctx>) -> crate::scenario::ScenarioFuture {
    Box::pin(run_link(ctx))
}

/// Idle no-owner tenant (same role as `invite_lifecycle`'s): an anchor for
/// `await_ready`, and legitimate `safe_rooms` for the sweep to protect.
const IDLE_TENANT: &str = "Fed Drill Anchor";

/// Compressed sweep tick (min the bridge accepts is 5).
const SWEEP_SECS: &str = "5";

/// Same expected-noise filter as every mgmt scenario.
fn unexpected_errors(bridge: &BridgeUnderTest) -> Vec<String> {
    bridge
        .scraper
        .error_events()
        .into_iter()
        .filter(|e| !e.message.contains("register the /bridge command"))
        .map(|e| e.message.clone())
        .collect()
}

// ─── #97 over real federation: remote DM invite → exactly one accept ────────

async fn run_invite(ctx: Arc<Ctx>) -> Result<()> {
    let cfg = &ctx.cfg;
    let fed = cfg.federation_or_skip()?.clone();

    // Bridge first — the incident shape is a REMOTE user DMing a RUNNING bot,
    // with the federated invite delivery + accept-join window live. Cooldown
    // armed exactly as prod (unset hides the #97 decline mode entirely).
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

    let ready = async {
        bridge.await_ready(&[IDLE_TENANT], Duration::from_secs(120)).await?;
        bridge
            .scraper
            .await_event(Duration::from_secs(120), |e| e.message.contains("management bot ready"))
            .await
            .context("management bot never became ready")?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    if let Err(e) = ready {
        bridge.kill().await;
        return Err(e);
    }

    let user = match MatrixManagementClient::register_on(
        &fed,
        &cfg.bot_user_id(),
        &format!("{}_remote", fed.user_prefix),
        "NVB Fed Remote",
    )
    .await
    .context("registering remote fixture user")
    {
        Ok(u) => u,
        Err(e) => {
            bridge.kill().await;
            return Err(e);
        }
    };

    let mut dm = None;
    let result = drive_invite(&user, &bridge, &mut dm).await;

    let teardown = match result {
        Ok(()) => bridge.sigterm_and_wait(Duration::from_secs(20)).await.context("SIGTERM drain"),
        Err(e) => {
            bridge.kill().await;
            Err(e)
        }
    };
    if let Some(dm) = &dm {
        if let Err(e) = user.leave_as(dm, user.mxid()).await {
            eprintln!("fed-invite user leave failed (non-fatal): {e}");
        }
    }
    teardown
}

async fn drive_invite(
    user: &MatrixManagementClient,
    bridge: &BridgeUnderTest,
    dm_out: &mut Option<String>,
) -> Result<()> {
    // The DM invite crosses federation here; the bot's accept-join is the
    // multi-second window the same-server twin can't open.
    let dm = user.create_dm().await.context("creating federated DM")?;
    *dm_out = Some(dm.clone());
    let marker = user.send(&dm, "drill: federated invite lifecycle").await?;

    // Generous: fixture→nether.im invite delivery + the bot's federated join
    // back into the fixture-hosted room both ride real federation.
    user.await_bot_joined(&dm, Duration::from_secs(120)).await?;
    user.wait_for_bot_message_containing(
        &dm,
        &marker,
        "I link Discord voice channels",
        Duration::from_secs(60),
    )
    .await
    .context("no greeting after the federated invite accept")?;

    // Settle window: federated invite redelivery / duplicate processing plays
    // out over the sync cycles after the accept (#97's trigger).
    tokio::time::sleep(Duration::from_secs(20)).await;

    // POST-FIX CONTRACT, room-scoped (same asserts as `mgmt_invite_offline`):
    // (a) exactly one accept…
    let accepts = bridge
        .scraper
        .events_matching(|e| {
            e.message.contains("accepting DM invite from self-serve user")
                && e.field("room_id") == Some(dm.as_str())
        })
        .len();
    if accepts != 1 {
        bail!("expected exactly ONE invite accept for {dm}, saw {accepts} (#97 double-processing)");
    }
    // (b) …zero cooldown declines of the just-joined room…
    let declines = bridge
        .scraper
        .events_matching(|e| {
            e.message.contains("declining self-serve invite") && e.field("room_id") == Some(dm.as_str())
        })
        .len();
    if declines != 0 {
        bail!(
            "the federated invite redelivery hit the just-stamped #89a cooldown and was \
             declined (join-then-leave, #97): {declines} decline(s)"
        );
    }
    // (c) …and the bot is still a member once the dust settles.
    let members = user.joined_members(&dm).await?;
    if !members.iter().any(|m| m != user.mxid()) {
        bail!("bot no longer joined to the federated DM after the settle window (#97)");
    }

    let errors = unexpected_errors(bridge);
    if !errors.is_empty() {
        bail!("bridge logged {} ERROR event(s); first: {}", errors.len(), errors[0]);
    }
    Ok(())
}

// ─── #104 over real federation: cold outreach to a remote crypto user ──────

async fn run_cold(ctx: Arc<Ctx>) -> Result<()> {
    let cfg = &ctx.cfg;
    let fed = cfg.federation_or_skip()?.clone();
    cfg.test_as_token()?; // the operator is a primary-side nvbtest user

    let op = MatrixManagementClient::register(cfg, "nvbtest_operator", "NVB Test Operator")
        .await
        .context("registering operator")?;
    // Remote crypto target, registered (device keys + OTKs uploaded on the
    // FIXTURE) before the operator's msg — the bot's megolm share must cross
    // federation to find them.
    let target = MatrixCryptoDmClient::register_on(
        &fed,
        &cfg.bot_user_id(),
        &format!("{}_cold", fed.user_prefix),
        "NVB Fed Cold Target",
    )
    .await
    .context("registering remote crypto target")?;
    // Stale pending invites from aborted earlier runs would confuse the
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
    let result = drive_cold(&op, &target, &bridge, &mut pending_dm, &mut op_dm).await;

    let teardown = match result {
        Ok(()) => bridge.sigterm_and_wait(Duration::from_secs(20)).await.context("SIGTERM drain"),
        Err(e) => {
            bridge.kill().await;
            Err(e)
        }
    };
    if let Err(e) = target.teardown(pending_dm.as_deref()).await {
        eprintln!("fed-cold target teardown failed (non-fatal): {e}");
    }
    if let Some(room) = &op_dm {
        if let Err(e) = op.leave_as(room, op.mxid()).await {
            eprintln!("fed-cold operator leave failed (non-fatal): {e}");
        }
    }
    teardown
}

async fn drive_cold(
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

    // Operator command DM (same-server) → the bot INITIATES a federated DM.
    let opdm = op.create_dm().await.context("opening operator DM")?;
    *op_dm = Some(opdm.clone());
    op.await_bot_joined(&opdm, Duration::from_secs(60)).await?;
    op.await_greeting(&opdm, Duration::from_secs(30)).await?;
    let ev = op
        .send(&opdm, &format!("msg {} nvb fed cold-outreach drill", target.mxid()))
        .await?;
    let reply = op.wait_for_reply(&opdm, &ev, Duration::from_secs(45)).await?;
    if !reply.contains("✅ Sent to") {
        bail!("operator msg was not delivered: {reply:?}");
    }

    // The invite must cross federation to the fixture — allow real latency.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    let room = loop {
        if let Some(room) = target.invited_rooms().into_iter().next() {
            break room;
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("remote target never saw the bot-initiated DM invite (federated delivery)");
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    *pending_dm = Some(room.clone());

    // Hold un-accepted across several sweep ticks: the #97-fixed exemption
    // must spare a pending federated invite too.
    tokio::time::sleep(Duration::from_secs(20)).await;
    let reaped = bridge.scraper.events_matching(|e| {
        e.message.contains("maintenance: left room")
            && e.field("room_id") == Some(room.as_str())
            && e.field("reason").is_some_and(|r| r.contains("#89b"))
    });
    if !reaped.is_empty() {
        bail!(
            "the #89b empty-room sweep reaped the bot-initiated federated DM {room} while \
             its invite was pending (#97 over federation)"
        );
    }

    // Accept, then read the pre-join 📣 — the #104 failure point: the megolm
    // session must have been shared to a device the bot could only learn of
    // via a FEDERATED device-list query at send time.
    target.join(&room).await.context("accepting the pending federated DM invite")?;
    target
        .wait_for_bot_message_containing(
            &room,
            "Message from the bridge operator",
            Duration::from_secs(45),
        )
        .await
        .context("operator message not decryptable by the remote target (#104 cold-outreach UTD)")?;
    let members = target.joined_members(&room).await?;
    if !members.iter().any(|m| m != target.mxid()) {
        bail!("bot not joined to the DM after the remote target accepted");
    }
    // If it decrypted at all, it must have crossed encrypted both ways.
    target.assert_transport_encrypted(&room).await?;

    let errors = unexpected_errors(bridge);
    if !errors.is_empty() {
        bail!("bridge logged {} ERROR event(s); first: {}", errors.len(), errors[0]);
    }
    Ok(())
}

// ─── #103 acceptance: link a remotely-minted v12 (domainless-id) room ───────

async fn run_link(ctx: Arc<Ctx>) -> Result<()> {
    let cfg = &ctx.cfg;
    let fed = cfg.federation_or_skip()?.clone();

    let tenants = tenant_entry(IDLE_TENANT, &cfg.room_a, &cfg.guild_id, &cfg.channel_a);
    let bridge = BridgeUnderTest::start_with_management(
        cfg,
        ctx.profile,
        &tenants,
        &ctx.artifacts.join("bridge"),
    )
    .await
    .context("starting bridge-under-test")?;

    let ready = async {
        bridge.await_ready(&[IDLE_TENANT], Duration::from_secs(120)).await?;
        bridge
            .scraper
            .await_event(Duration::from_secs(120), |e| e.message.contains("management bot ready"))
            .await
            .context("management bot never became ready")?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    if let Err(e) = ready {
        bridge.kill().await;
        return Err(e);
    }

    let linker = match MatrixManagementClient::register_on(
        &fed,
        &cfg.bot_user_id(),
        &format!("{}_linker", fed.user_prefix),
        "NVB Fed Linker",
    )
    .await
    .context("registering remote linker")
    {
        Ok(u) => u,
        Err(e) => {
            bridge.kill().await;
            return Err(e);
        }
    };

    let mut rooms = Vec::new();
    let result = drive_link(cfg, &linker, &bridge, &mut rooms).await;

    let teardown = match result {
        Ok(()) => bridge.sigterm_and_wait(Duration::from_secs(20)).await.context("SIGTERM drain"),
        Err(e) => {
            bridge.kill().await;
            Err(e)
        }
    };
    for room in &rooms {
        if let Err(e) = linker.leave_as(room, linker.mxid()).await {
            eprintln!("fed-link linker leave of {room} failed (non-fatal): {e}");
        }
    }
    teardown
}

async fn drive_link(
    cfg: &crate::config::E2eConfig,
    linker: &MatrixManagementClient,
    bridge: &BridgeUnderTest,
    rooms: &mut Vec<String>,
) -> Result<()> {
    // A v12 room minted on the FIXTURE: its room id is domainless (`!<hash>`,
    // no `:server` tail) and nether.im has never seen it — the #103 shape.
    // The bot is invited at creation so the federated invite carries the via
    // hint the probe join needs.
    let v12_room = linker
        .create_room_v12(&cfg.bot_user_id())
        .await
        .context("creating v12 room on the fixture")?;
    rooms.push(v12_room.clone());
    if v12_room.contains(':') {
        bail!("fixture minted a NON-v12 room id ({v12_room}) — check the fixture's room version support");
    }

    // Self-serve DM (federated, same machinery FED_DM_INVITE_LIFECYCLE proves).
    let dm = linker.create_dm().await.context("creating federated DM")?;
    rooms.push(dm.clone());
    linker.await_bot_joined(&dm, Duration::from_secs(120)).await?;
    linker.await_greeting(&dm, Duration::from_secs(60)).await?;

    // The #103 acceptance: `link` with the domainless v12 id must survive id
    // parsing, probe-join the remote room (via-hinted first contact), read
    // its state, and evaluate the linker's CREATOR authority (v12: creator
    // power is implicit — no users[] entry exists). All probes passing ⇒ the
    // pending approval code; the pending is never redeemed, so no tenant is
    // committed and no Discord-side cleanup is needed.
    let ev = linker
        .send(&dm, &format!("link FedV12 {v12_room} {} {}", cfg.guild_id, cfg.channel_a))
        .await?;
    // Generous: the probe join + state reads ride federation.
    let reply = linker.wait_for_reply(&dm, &ev, Duration::from_secs(90)).await?;
    if !reply.contains("Discord-side approval required") || !reply.contains("/bridge approve code:NVB-")
    {
        bail!(
            "link of the remotely-minted v12 room did not reach a pending approval code \
             (#103 regression — id parsing / via-hinted probe join / v12 creator \
             authority); got: {reply:?}"
        );
    }

    let errors = unexpected_errors(bridge);
    if !errors.is_empty() {
        bail!("bridge logged {} ERROR event(s); first: {}", errors.len(), errors[0]);
    }
    Ok(())
}
