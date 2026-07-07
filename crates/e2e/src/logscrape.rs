//! LogScraper: turns the bridge-under-test's `NVB_LOG_JSON=1` stdout into
//! queryable, awaitable events.
//!
//! Each stdout line is one `tracing_subscriber` JSON event:
//! `{"timestamp","level","target","fields":{"message",…},"span"?,"spans"?}`.
//! Non-JSON lines (panic output on stderr routed here, stray prints) are kept
//! as raw events with `level: "RAW"` so nothing the process says is lost.
//!
//! Two access patterns:
//! * post-hoc: [`LogScraper::events_matching`] over the ring buffer, and
//! * live: [`LogScraper::await_event`] — subscribes FIRST, then scans the
//!   buffer, so an event landing between the scan and the await is never
//!   missed.

use serde_json::Value;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use tokio::sync::broadcast;

/// Ring-buffer capacity. A compressed-profile scenario produces a few thousand
/// lines; 50k covers a long real-timing scenario with headroom (the full raw
/// log is separately persisted to the artifacts dir by `BridgeUnderTest`).
const RING_CAP: usize = 50_000;

/// One parsed log event. Carries the full event shape; individual scenarios
/// read only what they assert on (dead-code allowance covers the rest).
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct LogEvent {
    pub timestamp: String,
    /// `TRACE`..`ERROR`, or `RAW` for an unparseable line.
    pub level: String,
    /// e.g. `nvb_matrix_rtc::matrix`.
    pub target: String,
    /// The `message` field.
    pub message: String,
    /// All non-message event fields (e.g. `mxid`, `idx`, `bridge`).
    pub fields: serde_json::Map<String, Value>,
    /// Flattened field maps of the active span stack (each includes the span's
    /// own fields; the bridge span carries `name = <bridge label>`).
    pub spans: Vec<serde_json::Map<String, Value>>,
    /// The verbatim line. Load-bearing for bridge attribution: the bridge
    /// span's FIELD `name = <label>` collides with tracing-json's span-name
    /// key (`"name":"bridge"`) — duplicate JSON keys that serde_json
    /// collapses to the last one, losing the label from `spans`. Only the
    /// raw text reliably retains it (live-confirmed 2026-07-07).
    pub raw: String,
}

impl LogEvent {
    /// Does this event belong to bridge `label`? Matched against the raw
    /// line (see `raw`); the quoted-JSON form keeps it from matching a
    /// substring of a longer label or a message body by accident.
    pub fn in_bridge(&self, label: &str) -> bool {
        self.raw.contains(&format!("\"name\":\"{label}\""))
    }

    /// A string event field (e.g. `mxid`), if present.
    #[allow(dead_code)] // D2 scenarios assert per-field (mxid/idx) values
    pub fn field(&self, key: &str) -> Option<&str> {
        self.fields.get(key).and_then(Value::as_str)
    }

    pub fn is_error(&self) -> bool {
        self.level == "ERROR"
    }
}

/// Parse one `tracing_subscriber` JSON line. `None` = not JSON.
fn parse_line(line: &str) -> Option<LogEvent> {
    let v: Value = serde_json::from_str(line).ok()?;
    let obj = v.as_object()?;
    let fields_obj = obj.get("fields").and_then(Value::as_object).cloned().unwrap_or_default();
    let message = fields_obj.get("message").and_then(Value::as_str).unwrap_or_default().to_owned();
    let mut fields = fields_obj;
    fields.remove("message");

    let mut spans: Vec<serde_json::Map<String, Value>> = Vec::new();
    if let Some(arr) = obj.get("spans").and_then(Value::as_array) {
        spans.extend(arr.iter().filter_map(Value::as_object).cloned());
    } else if let Some(s) = obj.get("span").and_then(Value::as_object) {
        spans.push(s.clone());
    }

    Some(LogEvent {
        timestamp: obj.get("timestamp").and_then(Value::as_str).unwrap_or_default().to_owned(),
        level: obj.get("level").and_then(Value::as_str).unwrap_or_default().to_owned(),
        target: obj.get("target").and_then(Value::as_str).unwrap_or_default().to_owned(),
        message,
        fields,
        spans,
        raw: line.to_owned(),
    })
}

/// Shared scraper: `BridgeUnderTest` ingests lines; scenarios query/await.
pub struct LogScraper {
    ring: Mutex<VecDeque<Arc<LogEvent>>>,
    tx: broadcast::Sender<Arc<LogEvent>>,
}

impl Default for LogScraper {
    fn default() -> Self {
        Self::new()
    }
}

impl LogScraper {
    pub fn new() -> Self {
        // Capacity bounds how far a slow awaiter may lag before it misses
        // events (it would then rely on the ring scan); generous.
        let (tx, _) = broadcast::channel(4096);
        Self { ring: Mutex::new(VecDeque::with_capacity(1024)), tx }
    }

    /// Feed one raw stdout/stderr line.
    pub fn ingest_line(&self, line: &str) {
        let ev = Arc::new(parse_line(line).unwrap_or_else(|| LogEvent {
            timestamp: String::new(),
            level: "RAW".into(),
            target: String::new(),
            message: line.to_owned(),
            fields: Default::default(),
            spans: Vec::new(),
            raw: line.to_owned(),
        }));
        {
            let mut ring = self.ring.lock().unwrap_or_else(PoisonError::into_inner);
            if ring.len() >= RING_CAP {
                ring.pop_front();
            }
            ring.push_back(ev.clone());
        }
        let _ = self.tx.send(ev);
    }

    /// All buffered events satisfying `pred`.
    pub fn events_matching(&self, pred: impl Fn(&LogEvent) -> bool) -> Vec<Arc<LogEvent>> {
        self.ring
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|e| pred(e))
            .cloned()
            .collect()
    }

    /// All buffered `ERROR` events — the "no errors" postcondition.
    pub fn error_events(&self) -> Vec<Arc<LogEvent>> {
        self.events_matching(LogEvent::is_error)
    }

    /// Wait until an event satisfying `pred` exists (past or future), up to
    /// `timeout`. Subscribe-then-scan ordering makes this race-free.
    pub async fn await_event(
        &self,
        timeout: Duration,
        pred: impl Fn(&LogEvent) -> bool,
    ) -> anyhow::Result<Arc<LogEvent>> {
        let mut rx = self.tx.subscribe();
        if let Some(hit) = self.events_matching(&pred).into_iter().next() {
            return Ok(hit);
        }
        let fut = async {
            loop {
                match rx.recv().await {
                    Ok(ev) => {
                        if pred(&ev) {
                            return Ok(ev);
                        }
                    }
                    // Lagged: events were dropped from the broadcast queue —
                    // they are still in the ring, so re-scan and continue.
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        if let Some(hit) = self.events_matching(&pred).into_iter().next() {
                            return Ok(hit);
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        anyhow::bail!("log stream closed before a matching event")
                    }
                }
            }
        };
        tokio::time::timeout(timeout, fut)
            .await
            .map_err(|_| anyhow::anyhow!("timed out waiting for log event"))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The REAL wire shape (live-captured 2026-07-07): the span object carries
    // DUPLICATE "name" keys — the span field (bridge label) and the span name.
    const SAMPLE: &str = r#"{"timestamp":"2026-07-07T01:35:51.489202Z","level":"INFO","target":"nvb_matrix_rtc::matrix","fields":{"message":"member joined — assigning puppet","mxid":"@x:nether.im","display_name":"X"},"span":{"name":"Auto Test 01","name":"bridge"},"spans":[{"name":"Auto Test 01","name":"bridge"}]}"#;

    #[test]
    fn parses_tracing_json_shape() {
        let ev = parse_line(SAMPLE).expect("parses");
        assert_eq!(ev.level, "INFO");
        assert_eq!(ev.target, "nvb_matrix_rtc::matrix");
        assert_eq!(ev.message, "member joined — assigning puppet");
        assert_eq!(ev.field("mxid"), Some("@x:nether.im"));
        assert!(!ev.fields.contains_key("message"), "message not duplicated into fields");
        // Duplicate-key collapse makes `spans` unreliable for the label; the
        // raw-line matcher is the supported accessor.
        assert!(ev.in_bridge("Auto Test 01"));
        assert!(!ev.in_bridge("Auto Test 02"));
    }

    #[test]
    fn non_json_becomes_raw_event() {
        let s = LogScraper::new();
        s.ingest_line("thread 'main' panicked at src/main.rs:1:1:");
        let raws = s.events_matching(|e| e.level == "RAW");
        assert_eq!(raws.len(), 1);
        assert!(raws[0].message.contains("panicked"));
    }

    #[tokio::test]
    async fn await_event_sees_past_and_future() {
        let s = Arc::new(LogScraper::new());
        s.ingest_line(SAMPLE);
        // Past event: found via the ring scan.
        let hit = s
            .await_event(Duration::from_millis(100), |e| e.message.contains("member joined"))
            .await
            .expect("past event found");
        assert_eq!(hit.field("mxid"), Some("@x:nether.im"));

        // Future event: delivered via broadcast.
        let s2 = s.clone();
        let waiter = tokio::spawn(async move {
            s2.await_event(Duration::from_secs(2), |e| e.message == "late").await
        });
        tokio::task::yield_now().await;
        s.ingest_line(r#"{"timestamp":"t","level":"WARN","target":"x","fields":{"message":"late"}}"#);
        let ev = waiter.await.unwrap().expect("future event found");
        assert_eq!(ev.level, "WARN");

        // Timeout path.
        let miss = s.await_event(Duration::from_millis(50), |e| e.message == "never").await;
        assert!(miss.is_err());
    }

    #[test]
    fn error_events_filter() {
        let s = LogScraper::new();
        s.ingest_line(r#"{"timestamp":"t","level":"ERROR","target":"x","fields":{"message":"boom"}}"#);
        s.ingest_line(SAMPLE);
        assert_eq!(s.error_events().len(), 1);
    }
}
