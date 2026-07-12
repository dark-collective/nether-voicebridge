//! #99 — `mgmt_dm_e2ee`: the management-bot DM path over an **encrypted**
//! room, the shape real Element users hit by default.
//!
//! The plaintext `mgmt_self_serve_link` coverage structurally could not see
//! the E2EE direction the first real self-serve user exercised (#96/#97):
//! Element mints new DMs with `m.room.encryption` in `initial_state`, so the
//! bot must decrypt inbound commands and encrypt its replies (its matrix-sdk
//! Client uploads Olm keys + OTKs on its initial sync precisely for this).
//! This scenario drives that path end-to-end with a real crypto client
//! ([`MatrixCryptoDmClient`]) and asserts:
//!
//! 1. the bot auto-joins the encrypted DM invite and its greeting is
//!    **decryptable** by the user's device;
//! 2. `help` round-trips through encryption BOTH ways (encrypted command in,
//!    decryptable self-serve-tier reply out);
//! 3. the transport proof: every `m.room.message` in the DM crossed the wire
//!    as `m.room.encrypted` — a plaintext leak in either direction fails.

use anyhow::{Context as _, Result, bail};
use std::sync::Arc;
use std::time::Duration;

use crate::actors::crypto_dm::MatrixCryptoDmClient;
use crate::bridge_proc::{BridgeUnderTest, tenant_entry};
use crate::scenario::{Ctx, Expectation, ScenarioDef};

pub static MGMT_DM_E2EE: ScenarioDef = ScenarioDef {
    name: "mgmt_dm_e2ee",
    expectation: Expectation::Pass,
    run: boxed,
};

fn boxed(ctx: Arc<Ctx>) -> crate::scenario::ScenarioFuture {
    Box::pin(run(ctx))
}

/// An idle no-owner tenant so the bridge has a bridge to be "ready" about
/// (same anchor pattern as `mgmt_self_serve_link`; no participants → stays
/// closed).
const IDLE_TENANT: &str = "E2EE Idle Anchor";

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

    // The crypto client registers + syncs BEFORE creating the DM so its device
    // keys/OTKs are uploaded before the bot's first encrypted reply targets it.
    let user = MatrixCryptoDmClient::register(cfg, "nvbtest_encdm", "NVB Test E2EE DM")
        .await
        .context("registering crypto DM user")?;
    let dm = user.create_encrypted_dm().await.context("creating encrypted DM")?;

    let result = drive(&user, &dm, &bridge).await;

    // Teardown either way: the crypto actor leaves + logs out its ephemeral
    // device, then the bridge drains. The bot's own membership is left for its
    // #89b empty-room reap (prod appservice namespace — not ours to leave).
    if let Err(e) = user.teardown(&dm).await {
        eprintln!("crypto-dm teardown failed (non-fatal): {e}");
    }
    match result {
        Ok(()) => bridge.sigterm_and_wait(Duration::from_secs(20)).await.context("SIGTERM drain"),
        Err(e) => {
            bridge.kill().await;
            Err(e)
        }
    }
}

async fn drive(user: &MatrixCryptoDmClient, dm: &str, bridge: &BridgeUnderTest) -> Result<()> {
    // 1. Invite auto-join + a DECRYPTABLE greeting. The wait only accepts
    //    messages that decrypted successfully, so a bot that joined but
    //    replied unencrypted-to-us (missed device, key not shared) times out
    //    here rather than passing on ciphertext.
    user.wait_for_bot_message_containing(
        dm,
        "I link Discord voice channels",
        Duration::from_secs(60),
    )
    .await
    .context("bot greeting never arrived decryptable in the encrypted DM")?;

    // 2. `help` round-trips through encryption both ways: our command goes out
    //    encrypted (SDK auto-encrypts in this room), and the reply must come
    //    back decryptable with the SELF-SERVE tier text.
    user.send(dm, "help").await.context("sending encrypted help")?;
    let help = user
        .wait_for_bot_message_containing(dm, "remove a bridge you created", Duration::from_secs(45))
        .await
        .context("no decryptable self-serve help reply")?;
    if help.to_lowercase().contains("ban ") {
        bail!("encrypted-DM help returned the OPERATOR tier: {help:?}");
    }

    // 3. Transport proof: nothing in this DM crossed the wire as plaintext.
    user.assert_transport_encrypted(dm).await?;

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
