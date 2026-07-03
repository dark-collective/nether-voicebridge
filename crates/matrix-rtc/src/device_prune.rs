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

/// Delete the given retired device ids of `user_mxid` (appservice-asserted
/// masquerade). Returns the ids that could NOT be deleted (to keep in history for a
/// later pass); a 404 counts as deleted.
///
/// UIA handling: the CS-API may gate `DELETE /devices/{id}` behind interactive auth.
/// On a 401 the delete is retried once with `m.login.application_service` as the
/// auth stage; if the server still refuses, pruning is abandoned for this pass with
/// one WARN (delete manually via the homeserver admin interface in that case).
pub(crate) async fn prune_retired(
    http: &reqwest::Client,
    homeserver_url: &str,
    as_token: &str,
    user_mxid: &str,
    retired: Vec<String>,
) -> Vec<String> {
    let mut pending: Vec<String> = Vec::new();
    let mut iter = retired.into_iter();
    while let Some(id) = iter.next() {
        match delete_device(http, homeserver_url, as_token, user_mxid, &id).await {
            DeleteOutcome::Gone => {
                tracing::info!(user = %user_mxid, device_id = %id, "pruned retired device");
            }
            DeleteOutcome::Retry => pending.push(id),
            DeleteOutcome::UiaRefused => {
                // No point hammering the remaining ids — same policy will refuse them.
                tracing::warn!(
                    user = %user_mxid,
                    "homeserver requires interactive auth for device deletion — prune skipped (ids kept for a later pass; delete manually via the admin interface if the policy never changes)"
                );
                pending.push(id);
                pending.extend(iter);
                break;
            }
        }
    }
    pending
}

enum DeleteOutcome {
    /// Deleted, or was already gone (404).
    Gone,
    /// Transient/unexpected failure — keep the id and try again next start.
    Retry,
    /// The server demands UIA we cannot satisfy — abandon this pass.
    UiaRefused,
}

async fn delete_device(
    http: &reqwest::Client,
    homeserver_url: &str,
    as_token: &str,
    user_mxid: &str,
    device_id: &str,
) -> DeleteOutcome {
    let url = match cs_url(homeserver_url, &["devices", device_id], Some(user_mxid)) {
        Ok(u) => u,
        Err(e) => {
            tracing::warn!(error = %e, "device prune: bad homeserver URL");
            return DeleteOutcome::Retry;
        }
    };
    // First attempt without auth; on a UIA challenge retry once asserting the
    // appservice login type (accepted by homeservers that exempt appservices).
    for auth in [None, Some(serde_json::json!({"auth": {"type": "m.login.application_service"}}))] {
        let mut req = http.delete(url.clone()).bearer_auth(as_token);
        if let Some(body) = &auth {
            req = req.json(body);
        }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, device_id, "device prune: DELETE failed (will retry next start)");
                return DeleteOutcome::Retry;
            }
        };
        match resp.status().as_u16() {
            200 | 404 => return DeleteOutcome::Gone,
            401 => continue, // UIA challenge — fall through to the auth-asserting attempt.
            _ => {
                let status = resp.status();
                let body = err_body(resp).await;
                tracing::warn!(%status, device_id, %body, "device prune: DELETE refused (kept in history)");
                return DeleteOutcome::Retry;
            }
        }
    }
    DeleteOutcome::UiaRefused
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
