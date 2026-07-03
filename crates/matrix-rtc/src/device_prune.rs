//! Device hygiene (#53): history-scoped pruning of retired device ids, plus the
//! server-side presence check behind deletion self-heal.
//!
//! Motivated by the 2026-07-03 outage: a manual cleanup deleted LIVE devices; on
//! restart the bridge resurrected the same ids with identical keys, the homeserver
//! did not re-announce them, and clients permanently refused to Olm-encrypt media
//! keys to them (silent encrypted audio, zero errors). Two invariants defend
//! against the whole class:
//!
//! - **History-scoped prune:** a store only ever deletes device ids **it itself
//!   retired** (recorded when an id is re-minted). Never age or "looks stale"
//!   heuristics — the bridge runs alternately on several machines (dev box + VPS)
//!   against the same appservice, and any heuristic eventually eats another
//!   deployment's idle-but-live ids, recreating the wedge.
//! - **Deletion self-heal:** a persisted id whose device keys have vanished
//!   server-side is never resurrected — the caller re-mints a fresh id instead
//!   (`matrix::resolve_store_device_id` for the bot, ghost bootstrap for ghosts),
//!   so a wrongful deletion from ANY source degrades to a one-time benign re-key.

use crate::rtc::ghost::{cs_url, err_body};
use std::path::PathBuf;

/// The bot's retired-id history: one device id per line under the bridge store.
/// (Ghost histories live inside each ghost's identity blob instead.)
pub(crate) fn retired_ids_path(store_path: &str) -> PathBuf {
    std::path::Path::new(store_path).join("retired_device_ids")
}

/// Read the bot's retired ids. Missing file or empty `store_path` ⇒ empty.
pub(crate) fn read_retired(store_path: &str) -> Vec<String> {
    if store_path.is_empty() {
        return Vec::new();
    }
    match std::fs::read_to_string(retired_ids_path(store_path)) {
        Ok(s) => s
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(str::to_owned)
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// Best-effort append one retired id to the bot's history (deduplicated).
pub(crate) fn append_retired(store_path: &str, id: &str) {
    if store_path.is_empty() || id.is_empty() {
        return;
    }
    let mut ids = read_retired(store_path);
    if ids.iter().any(|x| x == id) {
        return;
    }
    ids.push(id.to_owned());
    write_retired(store_path, &ids);
}

/// Best-effort rewrite of the bot's history (empty ⇒ remove the file).
pub(crate) fn write_retired(store_path: &str, ids: &[String]) {
    if store_path.is_empty() {
        return;
    }
    let path = retired_ids_path(store_path);
    let res = if ids.is_empty() {
        match std::fs::remove_file(&path) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    } else {
        std::fs::write(&path, ids.join("\n") + "\n")
    };
    if let Err(e) = res {
        tracing::warn!(error = %e, path = %path.display(), "failed to persist retired-device history");
    }
}

/// Pure self-heal decision over a `/keys/query` response body: does `(user, device)`
/// currently have uploaded device keys?
///
/// - `Some(true)` / `Some(false)` when the response is well-formed (the `device_keys`
///   map is present; a user absent from it demonstrably has no keys).
/// - `None` when the shape is unrecognizable — callers MUST fail open (reuse the id):
///   re-minting on a transient/malformed response would churn identities for nothing.
pub(crate) fn device_keys_present(
    resp: &serde_json::Value,
    user_mxid: &str,
    device_id: &str,
) -> Option<bool> {
    let map = resp.get("device_keys")?.as_object()?;
    match map.get(user_mxid) {
        Some(devices) => Some(devices.get(device_id).is_some()),
        None => Some(false),
    }
}

/// Ask the homeserver whether `(user, device)` has device keys. `None` = couldn't
/// determine (network/HS error) — fail open.
pub(crate) async fn query_device_keys_present(
    http: &reqwest::Client,
    homeserver_url: &str,
    as_token: &str,
    user_mxid: &str,
    device_id: &str,
) -> Option<bool> {
    let url = match cs_url(homeserver_url, &["keys", "query"], None) {
        Ok(u) => u,
        Err(e) => {
            tracing::warn!(error = %e, "device presence check: bad homeserver URL");
            return None;
        }
    };
    let body = serde_json::json!({ "device_keys": { user_mxid: [] } });
    let resp = match http.post(url).bearer_auth(as_token).json(&body).send().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "device presence check: /keys/query failed (failing open)");
            return None;
        }
    };
    if !resp.status().is_success() {
        let status = resp.status();
        let body = err_body(resp).await;
        tracing::warn!(%status, %body, "device presence check: /keys/query error (failing open)");
        return None;
    }
    let json: serde_json::Value = match resp.json().await {
        Ok(j) => j,
        Err(e) => {
            tracing::warn!(error = %e, "device presence check: unparseable /keys/query body (failing open)");
            return None;
        }
    };
    device_keys_present(&json, user_mxid, device_id)
}

/// Delete the given retired device ids of `user_mxid`. Returns the ids that could NOT
/// be deleted (kept in history for a later pass); an id already absent server-side
/// counts as pruned.
///
/// Server behaviors probed live on Tuwunel 2026-07-03 (they drive the shape of
/// [`delete_device`]):
/// - A body-less `DELETE /devices/{id}` is `400 M_NOT_JSON` — always send `{}`.
/// - The appservice-masqueraded delete is `403 "user must be authenticated and device
///   identified"` (NOT a 401 UIA challenge). MSC4190 servers accept exactly this
///   request UIA-free, so it stays attempt 1.
/// - Even a self-delete by the device's own session gets `401` with EMPTY UIA flows —
///   Tuwunel today offers no completable client-API path; the admin room is the only
///   way. Policy refusals therefore abandon the pass with one WARN, and the retained
///   history flushes automatically once the homeserver gains MSC4190.
pub(crate) async fn prune_retired(
    http: &reqwest::Client,
    homeserver_url: &str,
    as_token: &str,
    user_mxid: &str,
    retired: Vec<String>,
) -> Vec<String> {
    // One masqueraded device listing up front: ids already gone (deleted manually, or
    // by a prior pass that failed to persist) are dropped without any DELETE churn —
    // and without the login-as-dead-device fallback, which would otherwise RE-CREATE
    // an already-gone device just to fail deleting it.
    let existing = match list_device_ids(http, homeserver_url, as_token, user_mxid).await {
        Some(ids) => ids,
        None => return retired, // listing failed — retry everything next pass
    };

    let mut pending: Vec<String> = Vec::new();
    let mut iter = retired.into_iter();
    while let Some(id) = iter.next() {
        if !existing.contains(&id) {
            tracing::info!(user = %user_mxid, device_id = %id, "retired device already absent server-side");
            continue;
        }
        match delete_device(http, homeserver_url, as_token, user_mxid, &id).await {
            DeleteOutcome::Gone => {
                tracing::info!(user = %user_mxid, device_id = %id, "pruned retired device");
            }
            DeleteOutcome::Retry => pending.push(id),
            DeleteOutcome::Refused => {
                // Server policy — the remaining ids would be refused identically.
                tracing::warn!(
                    user = %user_mxid,
                    "homeserver refuses appservice device deletion (no MSC4190 / empty UIA flows) — retired ids kept; they will flush when the homeserver allows it, or delete them via the admin interface"
                );
                pending.push(id);
                pending.extend(iter);
                break;
            }
        }
    }
    pending
}

/// Masqueraded `GET /devices` → the user's current device ids; `None` on any failure.
async fn list_device_ids(
    http: &reqwest::Client,
    homeserver_url: &str,
    as_token: &str,
    user_mxid: &str,
) -> Option<std::collections::HashSet<String>> {
    let url = cs_url(homeserver_url, &["devices"], Some(user_mxid)).ok()?;
    let resp = http.get(url).bearer_auth(as_token).send().await.ok()?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = err_body(resp).await;
        tracing::warn!(%status, %body, user = %user_mxid, "device prune: GET /devices failed");
        return None;
    }
    let json: serde_json::Value = resp.json().await.ok()?;
    Some(
        json.get("devices")?
            .as_array()?
            .iter()
            .filter_map(|d| d.get("device_id")?.as_str().map(str::to_owned))
            .collect(),
    )
}

enum DeleteOutcome {
    /// Deleted, or was already gone (404).
    Gone,
    /// Transient/unexpected failure — keep the id and try again next start.
    Retry,
    /// Server policy refuses appservice device deletion — abandon this pass.
    Refused,
}

async fn delete_device(
    http: &reqwest::Client,
    homeserver_url: &str,
    as_token: &str,
    user_mxid: &str,
    device_id: &str,
) -> DeleteOutcome {
    // Attempt 1: appservice-masqueraded DELETE with `{}` body — the MSC4190 shape,
    // UIA-free on servers that implement it.
    let url = match cs_url(homeserver_url, &["devices", device_id], Some(user_mxid)) {
        Ok(u) => u,
        Err(e) => {
            tracing::warn!(error = %e, "device prune: bad homeserver URL");
            return DeleteOutcome::Retry;
        }
    };
    match send_delete(http, url, as_token).await {
        DeleteStatus::Gone => return DeleteOutcome::Gone,
        DeleteStatus::AuthRejected => {} // fall through to attempt 2
        DeleteStatus::Other => return DeleteOutcome::Retry,
    }

    // Attempt 2: log in AS the dead device (the appservice may assert any device id in
    // its namespace — this only mints a token for the EXISTING device, it cannot
    // create one because the listing above proved it exists) and self-delete.
    let localpart = user_mxid
        .strip_prefix('@')
        .and_then(|s| s.split(':').next())
        .unwrap_or_default();
    let own_token =
        match crate::matrix::appservice_login_as(homeserver_url, as_token, localpart, device_id)
            .await
        {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, device_id, "device prune: login-as-retired-device failed");
                return DeleteOutcome::Retry;
            }
        };
    let url = match cs_url(homeserver_url, &["devices", device_id], None) {
        Ok(u) => u,
        Err(_) => return DeleteOutcome::Retry,
    };
    match send_delete(http, url, &own_token).await {
        DeleteStatus::Gone => DeleteOutcome::Gone,
        DeleteStatus::AuthRejected => DeleteOutcome::Refused,
        DeleteStatus::Other => DeleteOutcome::Retry,
    }
}

enum DeleteStatus {
    Gone,
    /// 401 (UIA challenge, possibly with empty flows) or 403 (policy).
    AuthRejected,
    Other,
}

async fn send_delete(http: &reqwest::Client, url: reqwest::Url, token: &str) -> DeleteStatus {
    let resp = match http
        .delete(url)
        .bearer_auth(token)
        .json(&serde_json::json!({}))
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "device prune: DELETE failed (will retry next start)");
            return DeleteStatus::Other;
        }
    };
    match resp.status().as_u16() {
        200 | 404 => DeleteStatus::Gone,
        401 | 403 => DeleteStatus::AuthRejected,
        _ => {
            let status = resp.status();
            let body = err_body(resp).await;
            tracing::warn!(%status, %body, "device prune: DELETE refused (kept in history)");
            DeleteStatus::Other
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn presence_decision_over_keys_query_shapes() {
        let present = serde_json::json!({
            "device_keys": { "@bot:hs": { "dark-voice-abc": {"keys": {}} } }
        });
        assert_eq!(device_keys_present(&present, "@bot:hs", "dark-voice-abc"), Some(true));
        // Device absent but user known → definitively gone.
        assert_eq!(device_keys_present(&present, "@bot:hs", "dark-voice-xyz"), Some(false));
        // User entirely absent from a well-formed map → demonstrably no keys.
        let no_user = serde_json::json!({ "device_keys": {} });
        assert_eq!(device_keys_present(&no_user, "@bot:hs", "dark-voice-abc"), Some(false));
        // Unrecognizable shapes → indeterminate → callers fail open.
        assert_eq!(device_keys_present(&serde_json::json!({}), "@bot:hs", "d"), None);
        assert_eq!(
            device_keys_present(&serde_json::json!({"device_keys": 3}), "@bot:hs", "d"),
            None
        );
    }

    #[test]
    fn retired_history_round_trip() {
        let n: u64 = rand::random();
        let dir = std::env::temp_dir().join(format!("nvb-retired-test-{n:016x}"));
        std::fs::create_dir_all(&dir).unwrap();
        let store = dir.to_str().unwrap();

        assert!(read_retired(store).is_empty(), "missing file reads as empty");
        append_retired(store, "dark-voice-old1");
        append_retired(store, "dark-voice-old2");
        append_retired(store, "dark-voice-old1"); // dedup
        assert_eq!(read_retired(store), vec!["dark-voice-old1", "dark-voice-old2"]);

        write_retired(store, &["dark-voice-old2".to_owned()]);
        assert_eq!(read_retired(store), vec!["dark-voice-old2"]);

        // Emptying removes the file entirely.
        write_retired(store, &[]);
        assert!(!retired_ids_path(store).exists());
        assert!(read_retired(store).is_empty());

        // Empty store path is a no-op sentinel everywhere.
        append_retired("", "x");
        assert!(read_retired("").is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
