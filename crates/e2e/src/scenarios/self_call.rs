//! Your own call data (ADR 0005), live: a Matrix member of the encrypted test room enrolls a listen-only device of
//! their own account (a real OpenID token, verified by the bridge on the homeserver's federation API), then follows
//! `/self/events`: their own `self_call` and their own media key, which the bridge received as a call member. When
//! they leave the call the stream ends; unlinking revokes the device key.
//!
//! Cases the harness can't drive live (another member's key, a forged `sub`, a private address, the reaper with a
//! crashed client) are covered by the hermetic tests in `nvb-companion` and `nvb-matrix-rtc`.

use super::audio::{await_quiescent, sweep_test_memberships};
use crate::actors::matrix::MatrixSpeaker;
use crate::bridge_proc::{BridgeUnderTest, tenant_entry};
use crate::observers::RoomStatePoller;
use crate::scenario::{Ctx, Expectation, ScenarioDef};
use anyhow::{Context as _, Result, bail};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub static SELF_CALL_DATA: ScenarioDef = ScenarioDef {
    name: "self_call_data",
    expectation: Expectation::Pass,
    run: |ctx| Box::pin(run(ctx)),
};

const APP_DEVICE: &str = "NVBE2ELISTEN";

async fn run(ctx: Arc<Ctx>) -> Result<()> {
    let cfg = &ctx.cfg;
    let (room, channel) = cfg.encrypted_pair()?;
    let bridge_name = "Auto Test 02";
    let poller = RoomStatePoller::new(cfg)?;
    sweep_test_memberships(cfg, &poller, room).await?;

    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0")?;
        l.local_addr()?.port()
    };
    // The test homeserver may sit on a private network next to the runner; the address policy itself is unit-tested.
    let extra = format!(
        "reap_grace_ms = 300000\n\n[companions]\nenabled = true\nlisten = \"127.0.0.1:{port}\"\ntrusted_private_servers = [\"{}\"]\n",
        cfg.server_domain()
    );
    let tenants = tenant_entry(bridge_name, room, &cfg.guild_id, channel);
    let workdir = ctx.artifacts.join("bridge");
    let bridge = BridgeUnderTest::start_with_timing(cfg, ctx.profile, &tenants, &workdir, &extra)
        .await
        .context("starting bridge-under-test")?;
    if let Err(e) = bridge.await_ready(&[bridge_name], Duration::from_secs(120)).await {
        bridge.kill().await;
        return Err(e);
    }
    let result = drive(&ctx, room, port).await;
    match result {
        Ok(()) => {
            let _ = await_quiescent(&poller, room).await;
            bridge.sigterm_and_wait(Duration::from_secs(20)).await.context("SIGTERM drain")
        }
        Err(e) => {
            bridge.kill().await;
            Err(e)
        }
    }
}

fn path_escape(s: &str) -> String {
    s.replace('%', "%25").replace('@', "%40").replace(':', "%3A").replace('!', "%21")
}

/// An OpenID token for `mxid`, asked of its homeserver as that user (test appservice masquerade).
async fn openid_token(http: &reqwest::Client, cfg: &crate::config::E2eConfig, mxid: &str) -> Result<(String, String)> {
    let url = format!(
        "{}/_matrix/client/v3/user/{}/openid/request_token?user_id={}",
        cfg.homeserver_url.trim_end_matches('/'),
        path_escape(mxid),
        path_escape(mxid)
    );
    let r = http.post(url).bearer_auth(cfg.test_as_token()?).json(&serde_json::json!({})).send().await?;
    if !r.status().is_success() {
        bail!("openid request_token refused: {}", r.status());
    }
    let v: serde_json::Value = r.json().await?;
    Ok((
        v["access_token"].as_str().context("access_token")?.to_string(),
        v["matrix_server_name"].as_str().context("matrix_server_name")?.to_string(),
    ))
}

/// Read SSE frames until `want` matches one (`event`, `data`), or time out. Returns every frame read.
async fn read_events(
    resp: &mut reqwest::Response,
    buf: &mut String,
    timeout: Duration,
    mut want: impl FnMut(&str, &serde_json::Value) -> bool,
) -> Result<Vec<(String, serde_json::Value)>> {
    let deadline = Instant::now() + timeout;
    let mut seen = Vec::new();
    loop {
        while let Some(end) = buf.find("\n\n") {
            let frame: String = buf.drain(..end + 2).collect();
            let mut event = String::new();
            let mut data = serde_json::Value::Null;
            for line in frame.lines() {
                if let Some(e) = line.strip_prefix("event: ") {
                    event = e.to_string();
                } else if let Some(d) = line.strip_prefix("data: ") {
                    data = serde_json::from_str(d).unwrap_or(serde_json::Value::Null);
                }
            }
            if event.is_empty() {
                continue; // keepalive comment
            }
            let hit = want(&event, &data);
            seen.push((event, data));
            if hit {
                return Ok(seen);
            }
        }
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            bail!("timed out; events so far: {:?}", seen.iter().map(|(e, _)| e.as_str()).collect::<Vec<_>>());
        }
        match tokio::time::timeout(left, resp.chunk()).await {
            Ok(Ok(Some(c))) => buf.push_str(&String::from_utf8_lossy(&c)),
            Ok(Ok(None)) => bail!("event stream closed; events so far: {:?}", seen.iter().map(|(e, _)| e.as_str()).collect::<Vec<_>>()),
            Ok(Err(e)) => bail!("event stream error: {e}"),
            Err(_) => {}
        }
    }
}

async fn drive(ctx: &Ctx, room: &str, port: u16) -> Result<()> {
    let cfg = &ctx.cfg;
    let http = reqwest::Client::new();
    let api = format!("http://127.0.0.1:{port}/_nvb/v1");

    let speaker = MatrixSpeaker::join(cfg, room, true, "nvbtest_alice", "NVB Test Alice").await?;
    let filler = speaker.start_filler();
    let mxid = speaker.mxid().to_string();
    let outcome = async {
        // The discovery document says the feature is on.
        let d: serde_json::Value = http.get(format!("{api}/")).send().await?.json().await.unwrap_or_default();
        eprintln!("   discovery: {d}");

        // A rejected token goes all the way to the homeserver and back.
        let (_, server) = openid_token(&http, cfg, &mxid).await?;
        let body = serde_json::json!({ "room_id": room, "device_id": APP_DEVICE, "openid_token": "not-a-token", "matrix_server_name": server });
        let r = http.post(format!("{api}/self/enroll")).json(&body).send().await?;
        let status = r.status().as_u16();
        let v: serde_json::Value = r.json().await.unwrap_or_default();
        if status != 401 || v["error"] != "invalid_token" {
            bail!("a bad OpenID token should be refused as invalid_token, got {status} {v}");
        }

        let (token, server) = openid_token(&http, cfg, &mxid).await?;
        let body = serde_json::json!({ "room_id": room, "device_id": APP_DEVICE, "openid_token": token, "matrix_server_name": server });
        let r = http.post(format!("{api}/self/enroll")).json(&body).send().await?;
        let status = r.status().as_u16();
        let v: serde_json::Value = r.json().await.unwrap_or_default();
        if status != 201 {
            bail!("enrollment refused: {status} {v}");
        }
        if v["user_id"] != mxid.as_str() || v["device_id"] != APP_DEVICE {
            bail!("enrollment answered for the wrong device: {v}");
        }
        let key = v["key"].as_str().context("key")?.to_string();
        eprintln!("   enrolled {} as key {}", v["user_id"], v["key_id"]);

        // The stream opens once the bridge sees the membership.
        let mut resp = None;
        for _ in 0..30 {
            let r = http.get(format!("{api}/self/events")).query(&[("room_id", room)]).bearer_auth(&key).send().await?;
            if r.status().is_success() {
                resp = Some(r);
                break;
            }
            let s = r.status();
            let v: serde_json::Value = r.json().await.unwrap_or_default();
            if v["error"] != "not_in_call" {
                bail!("self events refused: {s} {v}");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        let mut resp = resp.context("self events stayed not_in_call")?;
        let mut buf = String::new();

        // Own call state, then own key (non-null once the speaker's key reached the bridge).
        let seen = read_events(&mut resp, &mut buf, Duration::from_secs(30), |e, d| e == "self_call" && d["in_call"] == true).await?;
        let call = &seen.last().context("self_call")?.1;
        eprintln!("   self_call: {call}");
        let ids: Vec<String> = call["transports"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|t| t["identities"].as_array().cloned().unwrap_or_default())
            .filter_map(|i| i.as_str().map(str::to_string))
            .collect();
        if !ids.iter().any(|i| i.starts_with(&format!("{mxid}:"))) {
            bail!("self_call did not name the speaker's own device: {call}");
        }
        if ids.iter().any(|i| i.ends_with(APP_DEVICE)) {
            bail!("self_call listed the listen-only device itself: {call}");
        }
        let seen = read_events(&mut resp, &mut buf, Duration::from_secs(60), |e, d| e == "key" && d["key"].is_string()).await?;
        let k = &seen.last().context("key")?.1;
        if k["params"]["key_derivation"] != "HKDF" || k["params"]["ratchet_salt"] != "LKFrameEncryptionKey" {
            bail!("key params unexpected: {}", k["params"]);
        }
        if seen.iter().any(|(e, d)| e == "key" && d["identity"].as_str().is_some_and(|i| !i.starts_with(&format!("{mxid}:")) && i.starts_with('@'))) {
            bail!("a key event named someone else's identity");
        }
        eprintln!("   own key received (index {}, device {})", k["index"], k["device_id"]);
        Ok::<_, anyhow::Error>((key, resp, buf))
    }
    .await;
    filler.stop().await;
    let (key, mut resp, mut buf) = match outcome {
        Ok(v) => v,
        Err(e) => {
            speaker.leave().await;
            return Err(e);
        }
    };

    // Leaving the call ends the stream (after its short not-in-call grace).
    speaker.leave().await;
    let seen = read_events(&mut resp, &mut buf, Duration::from_secs(60), |e, _| e == "end" || e == "revoked").await?;
    let last = seen.last().context("end")?;
    if last.0 != "end" || last.1["reason"] != "not_in_call" {
        bail!("expected end/not_in_call after leaving, got {} {}", last.0, last.1);
    }
    drop(resp);

    // Unlinking revokes the device key.
    let r = http.delete(format!("{api}/self/enroll")).bearer_auth(&key).send().await?;
    if !r.status().is_success() {
        bail!("unlink refused: {}", r.status());
    }
    let r = http.get(format!("{api}/self/events")).query(&[("room_id", room)]).bearer_auth(&key).send().await?;
    if r.status().as_u16() != 401 {
        bail!("a revoked device key should be refused, got {}", r.status());
    }
    Ok(())
}
