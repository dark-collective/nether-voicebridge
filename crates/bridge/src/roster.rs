//! A machine-readable roster of the bridges this process is running, written to
//! `<data_dir>/bridges.json` for the ops monitor (#131).
//!
//! Why this file exists: on 2026-08-11 three bridges died and stayed dead for
//! three days while `nvb-watch` reported the instance healthy every day. Its two
//! health signals were an *event* scan of the last 15 minutes of journal (a dead
//! bridge is silent, so `err=0` forever) and `systemctl is-active` (the process
//! was alive — only 3 of its 9 bridges weren't). Neither can answer "is every
//! bridge actually up?", so the supervisor now states it outright.
//!
//! The `updated` stamp is a dead-man's switch: if the file stops being
//! refreshed, the writer itself is wedged and the monitor should say so rather
//! than trust stale contents.

use std::fmt::Write as _;

/// What a bridge is doing right now, from the supervisor's point of view.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BridgeState {
    /// Running normally.
    Alive,
    /// Failed and waiting out its restart backoff (#130).
    Restarting,
    /// Configured but not running at all — the state that used to be invisible.
    Down,
    /// Stopped for good because its Matrix room is unavailable (banned); not
    /// restarted until its config changes or the process restarts.
    Parked,
}

impl BridgeState {
    fn as_str(self) -> &'static str {
        match self {
            BridgeState::Alive => "alive",
            BridgeState::Restarting => "restarting",
            BridgeState::Down => "down",
            BridgeState::Parked => "parked",
        }
    }
}

/// One line of the roster.
#[derive(Clone, Debug)]
pub struct RosterEntry {
    pub name: String,
    /// `"static"` (config.toml) or `"tenant"` (tenants.toml).
    pub origin: &'static str,
    pub state: BridgeState,
    /// Unix seconds since this bridge entered its current state.
    pub since: u64,
    /// Consecutive failures backing the restart delay; 0 when healthy.
    pub consecutive_failures: u32,
}

/// Render the roster as JSON.
///
/// Hand-rolled rather than via `serde_json` so this crate keeps its dependency
/// footprint and, more importantly, so the exact shape the monitor parses is
/// visible in one place. Bridge names come from operator config and can contain
/// quotes or backslashes, so they are escaped.
pub fn render(updated: u64, pid: u32, entries: &[RosterEntry]) -> String {
    let mut s = String::with_capacity(128 + entries.len() * 96);
    let _ = write!(s, "{{\"updated\":{updated},\"pid\":{pid},\"bridges\":[");
    for (i, e) in entries.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        let _ = write!(
            s,
            "{{\"name\":\"{}\",\"origin\":\"{}\",\"state\":\"{}\",\"since\":{},\"consecutive_failures\":{}}}",
            escape(&e.name),
            e.origin,
            e.state.as_str(),
            e.since,
            e.consecutive_failures
        );
    }
    s.push_str("]}\n");
    s
}

/// Minimal JSON string escaping for the fields we emit.
fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out
}

/// Write the roster to `<data_dir>/bridges.json`, atomically.
///
/// Best-effort, exactly like the management bot's audit log: a monitoring file
/// must never be able to take a bridge down, so a failure WARNs and the process
/// carries on. Written to a temp file and renamed so the monitor can never read
/// a half-written roster and conclude a bridge is missing.
pub fn write(data_dir: &str, contents: &str) {
    let dir = data_dir.trim_end_matches('/');
    let path = format!("{dir}/bridges.json");
    let tmp = format!("{path}.tmp");
    let res = std::fs::write(&tmp, contents).and_then(|()| std::fs::rename(&tmp, &path));
    if let Err(e) = res {
        tracing::warn!(error = %e, path, "bridge roster write failed");
        let _ = std::fs::remove_file(&tmp);
    }
}

/// Unix-seconds wall clock, saturating to 0 before the epoch.
pub fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(name: &str, state: BridgeState, fails: u32) -> RosterEntry {
        RosterEntry {
            name: name.to_string(),
            origin: "static",
            state,
            since: 1_755_000_000,
            consecutive_failures: fails,
        }
    }

    #[test]
    fn renders_the_shape_the_monitor_parses() {
        let out = render(
            1_755_000_100,
            4242,
            &[
                entry("Light Voice", BridgeState::Alive, 0),
                entry("Dark Voice", BridgeState::Restarting, 3),
            ],
        );
        assert!(out.starts_with(r#"{"updated":1755000100,"pid":4242,"bridges":["#));
        assert!(out.contains(r#"{"name":"Light Voice","origin":"static","state":"alive","since":1755000000,"consecutive_failures":0}"#));
        assert!(out.contains(r#""state":"restarting","since":1755000000,"consecutive_failures":3"#));
        assert!(out.ends_with("]}\n"));
    }

    #[test]
    fn empty_roster_is_still_valid_json() {
        assert_eq!(render(7, 1, &[]), "{\"updated\":7,\"pid\":1,\"bridges\":[]}\n");
    }

    #[test]
    fn a_down_bridge_is_reported_not_omitted() {
        // The #131 failure mode: a bridge that is configured but not running
        // must be visible, because "absent" is what looked healthy for 3 days.
        let out = render(1, 1, &[entry("Light Gaming", BridgeState::Down, 5)]);
        assert!(out.contains(r#""name":"Light Gaming""#));
        assert!(out.contains(r#""state":"down""#));
    }

    #[test]
    fn operator_supplied_names_cannot_break_the_json() {
        let out = render(1, 1, &[entry(r#"we"ird\name"#, BridgeState::Alive, 0)]);
        assert!(out.contains(r#""name":"we\"ird\\name""#));
    }
}
