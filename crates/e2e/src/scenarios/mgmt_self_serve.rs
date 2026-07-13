//! `mgmt_self_serve_link` — the open-registration GATE (#76).
//!
//! Live-validates the two-sided self-serve auth path with all three knobs on
//! (`require_room_authority` + `require_discord_approval` + `open_registration`,
//! emitted into every bridge-under-test by `bridge_proc::generate_config`).
//! This is the automated coverage the #71 DM-gate rewrite lacked: it drives a
//! non-allowlisted `@nvbtest_mgr` user DMing the real management bot and
//! asserts the whole self-serve surface:
//!
//! * self-serve reachability — `help` returns the SELF-SERVE tier;
//! * the #71 DM-gate — a command from a >2-member room is SILENTLY ignored
//!   (the exact regression this scenario exists to guard), while the DM still
//!   answers;
//! * the invite gate — a non-direct invite from a self-serve user is rejected;
//! * `require_room_authority` — a link from a non-moderator is refused
//!   (negative), and a link from a moderator passes every probe to a pending
//!   approval code (positive — needs the one-time PL grant, see below);
//! * `max_tenants_per_owner` — a pending link occupies the slot, so a second
//!   link is refused;
//! * `max_link_attempts` (#77) — a fresh user's probe-reaching `link` attempts
//!   are velocity-limited: the first few pass to the (failing) probe, the next
//!   is refused as over-budget;
//! * `max_tenants_per_guild` (#77) — once the seeded tenant plus one pending
//!   link fill the guild, a DIFFERENT owner's link to it is refused (before its
//!   probes, so no room authority is needed to observe the cap);
//! * the deny-list — a link to a banned room is refused;
//! * self-serve scoping — `list`/`unlink` see/touch only the user's own
//!   tenants, not a seeded foreign one.
//!
//! The Discord-side redemption (`/bridge approve`) is NOT driven here — a bot
//! cannot invoke a slash command. `handle_discord_approval` is covered by an
//! in-crate integration test (`nvb_matrix_rtc::mgmt` tests) plus one manual
//! staging `/bridge approve`.
//!
//! ONE-TIME SETUP (positive-authority path): `@nvbtest_mgr:<domain>` must be a
//! joined member of Auto Test 01 (`room_a`) with an EXPLICIT moderator grant —
//! a `users[]` power-level entry above `users_default` and ≥ 50 (grant PL 50 via
//! Element devtools → `m.room.power_levels`; #86). Verified 2026-07-11:
//! users_default 0, `@nvbtest_mgr` = 50 → passes. Without it the positive-link
//! assertion fails with a message that says so.
//! The other harness users (`nvbtest_mgr2`/`mgrx`/the #77 `nvbtest_mgr3`) need
//! NO setup — they only drive negative/pre-probe paths that never require power.

use crate::actors::mgmt_client::{MatrixManagementClient, register_and_join};
use crate::bridge_proc::{BridgeUnderTest, tenant_entry};
use crate::config::E2eConfig;
use crate::scenario::{Ctx, Expectation, ScenarioDef};
use anyhow::{Context as _, Result, bail};
use std::sync::Arc;
use std::time::Duration;

pub static MGMT_SELF_SERVE_LINK: ScenarioDef = ScenarioDef {
    name: "mgmt_self_serve_link",
    expectation: Expectation::Pass,
    run: boxed,
};

fn boxed(ctx: Arc<Ctx>) -> crate::scenario::ScenarioFuture {
    Box::pin(run(ctx))
}

/// A no-owner tenant seeded into tenants.toml so the self-serve scoping asserts
/// have a foreign bridge to be excluded from / refused. Points at Auto Test 01
/// so it starts cleanly and stays idle (no participants → bridge stays closed).
const SEEDED_TENANT: &str = "Seeded Other";

async fn run(ctx: Arc<Ctx>) -> Result<()> {
    let cfg = &ctx.cfg;
    // Fail fast + clearly if the test appservice isn't configured (every
    // assertion needs a masqueraded nvbtest user).
    cfg.test_as_token()?;

    // Seed a foreign (no-owner) tenant on room_a — idle, but present in
    // tenants.toml for the list/unlink scoping asserts.
    let tenants = tenant_entry(SEEDED_TENANT, &cfg.room_a, &cfg.guild_id, &cfg.channel_a);
    let bridge =
        BridgeUnderTest::start_with_management(cfg, ctx.profile, &tenants, &ctx.artifacts.join("bridge"))
            .await
            .context("starting bridge-under-test")?;

    let ready = async {
        bridge.await_ready(&[SEEDED_TENANT], Duration::from_secs(120)).await?;
        // The management bot is a second Client on the same appservice account;
        // its readiness is a distinct log line from the tenant's.
        bridge
            .scraper
            .await_event(Duration::from_secs(120), |e| e.message.contains("management bot ready"))
            .await
            .context("management bot never became ready")?;
        // Deny-list: ban a throwaway room id (checked before any room I/O, so
        // it need not exist). Written after start — the bot reads it per-command.
        bridge
            .seed_banlist(&format!("rooms = [\"!nvbbanned:{}\"]\n", cfg.server_domain()))
            .await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    if let Err(e) = ready {
        bridge.kill().await;
        return Err(e);
    }

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
    let banned_room = format!("!nvbbanned:{}", cfg.server_domain());
    let reply_to = Duration::from_secs(45);

    // ── Primary self-serve user + DM ─────────────────────────────────────────
    let mgr = MatrixManagementClient::register(cfg, "nvbtest_mgr", "NVB Test Manager").await?;
    let dm = mgr.create_dm().await.context("open primary DM")?;
    mgr.await_bot_joined(&dm, Duration::from_secs(60)).await?;
    // Consume the one-time join greeting so it never masquerades as a command
    // reply below (see `await_greeting`).
    mgr.await_greeting(&dm, Duration::from_secs(30)).await?;

    // 1. Self-serve reachability: `help` → the SELF-SERVE tier (not operator).
    let ev = mgr.send(&dm, "help").await?;
    let help = mgr.wait_for_reply(&dm, &ev, reply_to).await?;
    if !help.contains("remove a bridge you created") || help.to_lowercase().contains("ban ") {
        bail!("help did not return the self-serve command tier; got: {help:?}");
    }

    // 2. Invite gate: a NON-direct invite from a self-serve user must be
    //    rejected (bot leaves, never joins). Assert the bridge's own log.
    let _nondirect = mgr.create_room_nondirect().await.context("open non-direct room")?;
    bridge
        .scraper
        .await_event(Duration::from_secs(30), |e| {
            e.message.contains("rejecting non-direct invite from self-serve user")
        })
        .await
        .context("bot did not reject the non-direct self-serve invite (#43 invite gate)")?;

    // 3. #71 DM-gate: in a >2-member room the bot must stay SILENT. Use a
    //    SEPARATE DM (adding a third member to the primary DM would break the
    //    command tests below).
    let shared = mgr.create_dm().await.context("open gate-test DM")?;
    mgr.await_bot_joined(&shared, Duration::from_secs(60)).await?;
    mgr.await_greeting(&shared, Duration::from_secs(30)).await?;
    mgr.invite(&shared, &format!("@nvbtest_mgrx:{}", cfg.server_domain())).await?;
    register_and_join(cfg, "nvbtest_mgrx", &shared).await.context("third member join")?;
    // Now shared has 3 joined members (mgr + bot + mgrx) → commands ignored.
    let ev = mgr.send(&shared, "help").await?;
    mgr.expect_silence(&shared, &ev, Duration::from_secs(15))
        .await
        .context("#71 regression: bot answered a command in a >2-member room")?;

    // 4. require_room_authority NEGATIVE: a self-serve user with no power in
    //    room_a is refused. A distinct user (mgr2) so the primary user's
    //    positive path below is unaffected.
    let mgr2 = MatrixManagementClient::register(cfg, "nvbtest_mgr2", "NVB Test Manager 2").await?;
    let dm2 = mgr2.create_dm().await.context("open mgr2 DM")?;
    mgr2.await_bot_joined(&dm2, Duration::from_secs(60)).await?;
    mgr2.await_greeting(&dm2, Duration::from_secs(30)).await?;
    let ev = mgr2
        .send(&dm2, &format!("link NoAuth {} {} {}", cfg.room_a, cfg.guild_id, cfg.channel_a))
        .await?;
    let refusal = mgr2.wait_for_reply(&dm2, &ev, reply_to).await?;
    if !refusal.contains("individual moderator") {
        bail!("expected a room-authority refusal for a non-moderator; got: {refusal:?}");
    }

    // 4b. max_link_attempts (#77): a FRESH user's `link` attempts that reach the
    //     probe stage are velocity-limited. Run here, while the guild still holds
    //     only the seeded tenant (< the per-guild cap), so every attempt clears
    //     the cheaper caps and reaches the gate. Each targets room_a WITHOUT
    //     authority, so it fails at the probe (creating no tenant/pending) after
    //     spending one attempt — until the budget (3) is exhausted and the 4th is
    //     refused as over-budget. A distinct user so the earlier/later budgets
    //     are untouched.
    let mgr3 = MatrixManagementClient::register(cfg, "nvbtest_mgr3", "NVB Test Manager 3").await?;
    let dm3 = mgr3.create_dm().await.context("open mgr3 DM")?;
    mgr3.await_bot_joined(&dm3, Duration::from_secs(60)).await?;
    mgr3.await_greeting(&dm3, Duration::from_secs(30)).await?;
    for attempt in 1..=3 {
        let ev = mgr3
            .send(&dm3, &format!("link Burst{attempt} {} {} {}", cfg.room_a, cfg.guild_id, cfg.channel_a))
            .await?;
        let reply = mgr3.wait_for_reply(&dm3, &ev, reply_to).await?;
        // Within budget → reaches the probe, which refuses for lack of authority.
        if reply.contains("Too many link attempts") || !reply.contains("individual moderator") {
            bail!("attempt {attempt} should have reached the probe (not the rate gate); got: {reply:?}");
        }
    }
    // 4th attempt: budget spent → velocity-refused before any probe.
    let ev = mgr3
        .send(&dm3, &format!("link Burst4 {} {} {}", cfg.room_a, cfg.guild_id, cfg.channel_a))
        .await?;
    let gated = mgr3.wait_for_reply(&dm3, &ev, reply_to).await?;
    if !gated.contains("Too many link attempts") {
        bail!("expected a link-attempt rate-limit refusal on the 4th attempt; got: {gated:?}");
    }

    // 5. require_room_authority POSITIVE → pending approval code. Needs the
    //    one-time PL grant for @nvbtest_mgr in room_a (see the module doc).
    let ev = mgr
        .send(&dm, &format!("link GoodLink {} {} {}", cfg.room_a, cfg.guild_id, cfg.channel_a))
        .await?;
    let pending = mgr.wait_for_reply(&dm, &ev, Duration::from_secs(60)).await?;
    if !pending.contains("Discord-side approval required") || !pending.contains("/bridge approve code:NVB-") {
        bail!(
            "expected a pending approval code (all probes passed). Does @nvbtest_mgr have \
             an explicit moderator grant (users[] PL ≥ 50, above users_default) in {}? \
             Got: {pending:?}",
            cfg.room_a
        );
    }

    // 6. max_tenants_per_owner: the pending link from step 5 fills the (=1)
    //    slot, so a second link is refused BEFORE its probes.
    let ev = mgr
        .send(&dm, &format!("link Second {} {} {}", cfg.room_a, cfg.guild_id, cfg.channel_a))
        .await?;
    let cap = mgr.wait_for_reply(&dm, &ev, reply_to).await?;
    if !cap.contains("awaiting approval") || !cap.contains("the limit is 1") {
        bail!("expected a per-owner cap refusal; got: {cap:?}");
    }

    // 6b. Per-guild cap pending-scoping (#79 finding 4). The guild holds the
    //     seeded committed tenant (1) plus mgr's pending GoodLink (1). Before the
    //     fix, the per-guild cap summed BOTH — so a DIFFERENT owner's pending
    //     tipped the guild to the cap and blocked mgr2, letting anyone park
    //     pendings on a victim guild's public snowflake to freeze its onboarding.
    //     Now the pending count is scoped to the requester: for mgr2 only the
    //     seeded committed tenant counts (1 < 2), so the guild cap does NOT fire —
    //     mgr2 falls through to the room-authority probe and is refused THERE
    //     instead. A guild-cap refusal here would be a regression of the fix.
    let ev = mgr2
        .send(&dm2, &format!("link GuildFull {} {} {}", cfg.room_a, cfg.guild_id, cfg.channel_a))
        .await?;
    let refusal = mgr2.wait_for_reply(&dm2, &ev, reply_to).await?;
    if refusal.contains("per guild") {
        bail!("per-guild cap counted another owner's pending — regression of #79 finding 4; got: {refusal:?}");
    }
    if !refusal.contains("individual moderator") {
        bail!("expected mgr2 to fall through to a room-authority refusal (#79 finding 4 fix); got: {refusal:?}");
    }

    // 7. Deny-list: a link to the banned room is refused (checked before the
    //    cap, so the still-open pending slot is irrelevant here).
    let ev = mgr
        .send(&dm, &format!("link Banned {banned_room} {} {}", cfg.guild_id, cfg.channel_a))
        .await?;
    let banned = mgr.wait_for_reply(&dm, &ev, reply_to).await?;
    if !banned.contains("is banned") {
        bail!("expected a deny-list refusal for the banned room; got: {banned:?}");
    }

    // 8. Self-serve scoping: `list` shows mgr's own state — no committed
    //    tenants, but the GoodLink pending approval from the positive-link
    //    step (the #111 avoid-confusion status surface) — and NOT the seeded
    //    foreign tenant; `unlink` of the foreign tenant is refused.
    let ev = mgr.send(&dm, "list").await?;
    let list = mgr.wait_for_reply(&dm, &ev, reply_to).await?;
    if list.contains(SEEDED_TENANT) {
        bail!("self-serve list leaked a foreign tenant; got: {list:?}");
    }
    if !list.contains("GoodLink") || !list.contains("awaiting Discord approval") {
        bail!(
            "self-serve list does not surface the user's own pending link (#111 status); got: {list:?}"
        );
    }
    let ev = mgr.send(&dm, &format!("unlink \"{SEEDED_TENANT}\"")).await?;
    let unlink = mgr.wait_for_reply(&dm, &ev, reply_to).await?;
    if !unlink.contains("not yours") {
        bail!("expected an ownership refusal unlinking a foreign tenant; got: {unlink:?}");
    }

    // 9. Abuse report path (#78): a self-serve user may `report` an abusive
    //    bridge. It is advisory — recorded to the audit trail, never acted on —
    //    so the ack promises operator review, not removal. `guild <id>` passes
    //    the ban-shape check (numeric snowflake). Not rate-gated here:
    //    mgmt_cooldown_secs is unset in the harness config.
    let ev = mgr.send(&dm, &format!("report guild {} spamming voice", cfg.guild_id)).await?;
    let filed = mgr.wait_for_reply(&dm, &ev, reply_to).await?;
    if !filed.contains("has been recorded for an operator to review") {
        bail!("expected a report-filed ack; got: {filed:?}");
    }
    // The report reaches the audit trail (info-level marker on the write path).
    bridge
        .scraper
        .await_event(Duration::from_secs(30), |e| e.message.contains("abuse report filed"))
        .await
        .context("the report did not reach the audit trail (#78)")?;

    // 9b. `reports` is operator-only — a self-serve user is refused, not shown
    //     the trail.
    let ev = mgr.send(&dm, "reports").await?;
    let refused = mgr.wait_for_reply(&dm, &ev, reply_to).await?;
    if !refused.contains("operator-only") {
        bail!("expected `reports` to be operator-only for a self-serve user; got: {refused:?}");
    }

    // 10. Operator review (#78): an allowlisted operator DMs `reports` and sees
    //     the report mgr just filed, rendered with the exact `ban` to run —
    //     end-to-end proof the report landed in the trail and feeds the
    //     deny-list tooling. `nvbtest_operator` matches the harness allowlist.
    let op = MatrixManagementClient::register(cfg, "nvbtest_operator", "NVB Test Operator").await?;
    let opdm = op.create_dm().await.context("open operator DM")?;
    op.await_bot_joined(&opdm, Duration::from_secs(60)).await?;
    op.await_greeting(&opdm, Duration::from_secs(30)).await?;
    let ev = op.send(&opdm, "reports").await?;
    let reports = op.wait_for_reply(&opdm, &ev, reply_to).await?;
    if !reports.contains("oldest first") || !reports.contains(&format!("ban guild {}", cfg.guild_id)) {
        bail!("operator `reports` did not show the filed report with its ban suggestion; got: {reports:?}");
    }

    // 11. Operator observability (#94): `status` renders the pool's guild
    //     footprint (the harness pool has ≥1 bot, all in the test guild) and
    //     `stats` renders per-bridge uptime + process RSS. Both are
    //     operator-only (default-deny — not in `allowed_for_self_serve`).
    let ev = op.send(&opdm, "status").await?;
    let status = op.wait_for_reply(&opdm, &ev, reply_to).await?;
    if !status.contains("Puppet-pool guild footprint")
        || !status.contains("Distinct guilds served")
    {
        bail!("operator `status` did not render the pool footprint; got: {status:?}");
    }
    let ev = op.send(&opdm, "stats").await?;
    let stats = op.wait_for_reply(&opdm, &ev, reply_to).await?;
    if !stats.contains("Running bridges") || !stats.contains("Process RSS") {
        bail!("operator `stats` did not render bridge stats; got: {stats:?}");
    }
    // 11b. The same verbs are refused for the self-serve user.
    let ev = mgr.send(&dm, "status").await?;
    let refused = mgr.wait_for_reply(&dm, &ev, reply_to).await?;
    if !refused.contains("operator-only") {
        bail!("expected `status` to be operator-only for a self-serve user; got: {refused:?}");
    }

    // ── No unexpected ERROR events over the whole scenario ───────────────────
    // The `/bridge` slash-command registration depends on the puppet
    // application's `applications.commands` OAuth scope — orthogonal to the
    // Matrix self-serve path validated here (the approval CLICK is not driven),
    // so a registration error is filtered out rather than failing the run.
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
