//! Lease and key events in the instance's `<data_dir>/audit.jsonl`, in the same
//! line format the management bot writes (`{"ts","actor","event","detail"}`).
//! Best-effort: a failed write WARNs and never blocks the request.

use std::io::Write;

/// Same bound the management bot rotates at (one generation kept).
const AUDIT_MAX_BYTES: u64 = 64 * 1024 * 1024;

pub fn audit_path(data_dir: &str) -> String {
    format!("{}/audit.jsonl", data_dir.trim_end_matches('/'))
}

pub fn append(path: &str, actor: &str, event: &str, detail: serde_json::Value) {
    let ts = crate::unix_now();
    let mut line = serde_json::json!({ "ts": ts, "actor": actor, "event": event, "detail": detail }).to_string();
    line.push('\n');
    if std::fs::metadata(path).is_ok_and(|m| m.len() >= AUDIT_MAX_BYTES) {
        if let Err(e) = std::fs::rename(path, format!("{path}.1")) {
            tracing::warn!(error = %e, path, "audit log rotation failed");
        }
    }
    // One write per line, so lines from the management bot (same file, O_APPEND)
    // never interleave mid-line.
    let res = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .and_then(|mut f| f.write_all(line.as_bytes()));
    if let Err(e) = res {
        tracing::warn!(error = %e, path, "audit log write failed");
    }
}
