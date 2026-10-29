//! The `/_nvb/v1` listener (ADR 0004 §7): plain HTTP/1.1 meant to sit behind a
//! TLS reverse proxy. Bearer companion keys; one server-sent-events stream per
//! lease, which is also the lease's liveness. ADR 0005 adds `/self/enroll` and
//! `/self/events`: a member's own call data for a listen-only device of theirs.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http::{HeaderValue, Method, Request, Response, StatusCode, header};
use http_body::{Body, Frame, SizeHint};
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use nvb_config::companions::CompanionKey;
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

use crate::hub::{CompanionHub, EnrollRequest, LeaseShared, Refusal, SERVED_KINDS, SelfStream, StreamGuard};
use crate::observe::Snapshot;

pub const API_PREFIX: &str = "/_nvb/v1";

/// Largest request body accepted (lease requests are a few dozen bytes).
const MAX_BODY: usize = 16 * 1024;
/// Concurrent connections; beyond it new ones are closed at once.
const MAX_CONNECTIONS: usize = 512;
/// How often an `observe` stream checks the room for changes.
const OBSERVE_POLL: Duration = Duration::from_millis(500);
/// How often a `/self/events` stream re-checks pairing presence and its key.
const SELF_POLL: Duration = Duration::from_millis(500);
/// How long a `/self/events` stream stays open after its owner's last call membership is gone (a client re-posting
/// its membership is not a departure).
const SELF_NOT_IN_CALL_GRACE: Duration = Duration::from_secs(15);
/// How often a `/self/events` stream re-checks that its owner is still a member of the room.
const SELF_MEMBER_RECHECK: Duration = Duration::from_secs(60);

// ── response bodies ─────────────────────────────────────────────────────────────

/// A fixed body, or an event stream fed by the lease's task. The stream body
/// holds the lease's [`StreamGuard`], so a client hanging up (hyper drops the
/// body) starts the lease's grace.
pub enum NvbBody {
    Full(Full<Bytes>),
    Sse { rx: mpsc::Receiver<Bytes>, _guard: Option<StreamGuard> },
}

impl Body for NvbBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        match self.get_mut() {
            NvbBody::Full(f) => Pin::new(f).poll_frame(cx),
            NvbBody::Sse { rx, .. } => rx.poll_recv(cx).map(|b| b.map(|b| Ok(Frame::data(b)))),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            NvbBody::Full(f) => f.is_end_stream(),
            NvbBody::Sse { .. } => false,
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            NvbBody::Full(f) => f.size_hint(),
            NvbBody::Sse { .. } => SizeHint::default(),
        }
    }
}

fn json(status: StatusCode, v: serde_json::Value) -> Response<NvbBody> {
    let mut r = Response::new(NvbBody::Full(Full::new(Bytes::from(v.to_string()))));
    *r.status_mut() = status;
    r.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    r.headers_mut().insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

fn refusal(r: Refusal) -> Response<NvbBody> {
    let status = StatusCode::from_u16(r.status).unwrap_or(StatusCode::BAD_REQUEST);
    json(status, serde_json::json!({ "error": r.reason, "message": r.message }))
}

fn error(status: StatusCode, reason: &'static str, message: &str) -> Response<NvbBody> {
    json(status, serde_json::json!({ "error": reason, "message": message }))
}

// ── server ──────────────────────────────────────────────────────────────────────

/// Bind the configured address and serve until `shutdown`. Also runs the hub's
/// lease housekeeping.
pub async fn serve(hub: Arc<CompanionHub>, shutdown: CancellationToken) -> std::io::Result<()> {
    let addr: SocketAddr = hub
        .config()
        .listen
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let Some(listener) = bind_with_retry(addr, &shutdown).await? else { return Ok(()) };
    serve_on(listener, hub, shutdown).await;
    Ok(())
}

/// Bind `addr`, retrying while the address isn't there yet or is still taken.
/// At boot the bridge can start before the interface carrying `listen` (a
/// VPN/tailnet address) is up: the first bind then fails with
/// `AddrNotAvailable`, and giving up there left companions off until the next
/// restart. Other errors (permissions, a malformed address) fail at once.
/// `None` = shut down while waiting.
async fn bind_with_retry(addr: SocketAddr, shutdown: &CancellationToken) -> std::io::Result<Option<TcpListener>> {
    use std::io::ErrorKind::{AddrInUse, AddrNotAvailable};
    let mut delay = std::time::Duration::from_secs(1);
    let mut attempts: u32 = 0;
    loop {
        match TcpListener::bind(addr).await {
            Ok(l) => {
                if attempts > 0 {
                    tracing::info!(%addr, attempts, "companion listener bound after retrying");
                }
                return Ok(Some(l));
            }
            Err(e) if matches!(e.kind(), AddrNotAvailable | AddrInUse) => {
                attempts += 1;
                // Loud once, then about every 5 minutes (the delay caps at 30 s).
                if attempts == 1 || attempts.is_multiple_of(10) {
                    tracing::warn!(%addr, error = %e, attempts, "companion listener can't bind yet — retrying");
                }
                tokio::select! {
                    _ = shutdown.cancelled() => return Ok(None),
                    _ = tokio::time::sleep(delay) => {}
                }
                delay = (delay * 2).min(std::time::Duration::from_secs(30));
            }
            Err(e) => return Err(e),
        }
    }
}

/// Serve on an already-bound listener (tests bind port 0).
pub async fn serve_on(listener: TcpListener, hub: Arc<CompanionHub>, shutdown: CancellationToken) {
    if let Ok(addr) = listener.local_addr() {
        tracing::info!(%addr, "companion listener up ({API_PREFIX})");
    }
    tokio::spawn(hub.clone().run_sweeper(shutdown.clone()));
    let slots = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    loop {
        let (stream, peer) = tokio::select! {
            _ = shutdown.cancelled() => break,
            r = listener.accept() => match r {
                Ok(x) => x,
                Err(e) => {
                    tracing::warn!(error = %e, "companion listener accept failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
        };
        let Ok(permit) = slots.clone().try_acquire_owned() else {
            tracing::warn!(%peer, "companion listener at its connection limit — closing a new connection");
            continue;
        };
        let hub = hub.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let svc = service_fn(move |req| {
                let hub = hub.clone();
                let shutdown = shutdown.clone();
                async move { Ok::<_, Infallible>(handle(hub, req, peer, shutdown).await) }
            });
            let conn = http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(Duration::from_secs(10))
                .serve_connection(TokioIo::new(stream), svc);
            if let Err(e) = conn.await {
                tracing::debug!(%peer, error = %e, "companion connection ended with an error");
            }
        });
    }
    tracing::info!("companion listener stopped");
}

#[derive(Deserialize)]
struct LeaseRequest {
    kind: String,
    #[serde(default)]
    room_id: Option<String>,
    /// `stream:attach`: whose ghost the stream is attached to.
    #[serde(default)]
    participant: Option<ParticipantRef>,
}

#[derive(Deserialize)]
struct ParticipantRef {
    discord_user_id: String,
}

async fn handle(
    hub: Arc<CompanionHub>,
    req: Request<Incoming>,
    peer: SocketAddr,
    shutdown: CancellationToken,
) -> Response<NvbBody> {
    let path = req.uri().path().to_string();
    let Some(rest) = path.strip_prefix(API_PREFIX) else {
        return error(StatusCode::NOT_FOUND, "not_found", "unknown path");
    };
    let segs: Vec<&str> = rest.split('/').filter(|s| !s.is_empty()).collect();

    if segs.is_empty() {
        return if req.method() == Method::GET { discovery(&hub) } else { method_not_allowed() };
    }
    if segs[0] != "leases" && segs[0] != "self" {
        return error(StatusCode::NOT_FOUND, "not_found", "unknown path");
    }

    let client = client_address(&hub, &req, peer);
    if segs[0] == "self" && !hub.config().self_enroll {
        return error(StatusCode::NOT_FOUND, "not_found", "unknown path");
    }
    // Enrollment is the one unauthenticated write: it carries an OpenID token instead of a key.
    if segs.as_slice() == ["self", "enroll"] && req.method() == Method::POST {
        return enroll(&hub, req, &client).await;
    }
    let bearer = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim);
    let key = match hub.authenticate(bearer, &client) {
        Ok(k) => k,
        Err(r) => return refusal(r),
    };

    match (req.method().clone(), segs.as_slice()) {
        (Method::POST, ["leases"]) => open_lease(&hub, &key, req).await,
        (Method::GET, ["leases", id, "events"]) => events(&hub, &key, id, shutdown),
        (Method::POST, ["leases", id, "live"]) => match hub.stream_live(&key, id) {
            Ok(()) => json(StatusCode::OK, serde_json::json!({ "live": true })),
            Err(r) => refusal(r),
        },
        (Method::POST, ["leases", id, "renew"]) => match hub.renew(&key, id) {
            Ok(at) => json(StatusCode::OK, serde_json::json!({ "expires_at": at })),
            Err(r) => refusal(r),
        },
        (Method::DELETE, ["leases", id]) => match hub.release(&key, id) {
            Ok(()) => json(StatusCode::OK, serde_json::json!({ "released": true })),
            Err(r) => refusal(r),
        },
        (Method::DELETE, ["self", "enroll"]) => match hub.unenroll(&key) {
            Ok(()) => json(StatusCode::OK, serde_json::json!({ "unlinked": true })),
            Err(r) => refusal(r),
        },
        (Method::GET, ["self", "events"]) => {
            let Some(room_id) = query_param(req.uri().query(), "room_id") else {
                return error(StatusCode::BAD_REQUEST, "bad_request", "room_id is required");
            };
            self_events(&hub, &key, &room_id, shutdown).await
        }
        (_, ["leases"]) | (_, ["leases", _]) | (_, ["leases", _, "events" | "renew" | "live"]) => method_not_allowed(),
        (_, ["self", "enroll" | "events"]) => method_not_allowed(),
        _ => error(StatusCode::NOT_FOUND, "not_found", "unknown path"),
    }
}

fn method_not_allowed() -> Response<NvbBody> {
    error(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed", "method not allowed here")
}

/// The configured proxy header (first address of a list), else the TCP peer.
fn client_address(hub: &CompanionHub, req: &Request<Incoming>, peer: SocketAddr) -> String {
    hub.config()
        .client_ip_header
        .as_deref()
        .and_then(|h| req.headers().get(h))
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| peer.ip().to_string())
}

fn discovery(hub: &CompanionHub) -> Response<NvbBody> {
    let c = hub.config();
    json(StatusCode::OK, serde_json::json!({
        "version": 1,
        "kinds": SERVED_KINDS,
        "limits": {
            "max_lease_hours": c.max_lease_hours,
            "max_leases_per_key": c.max_leases_per_key,
            "lease_requests_per_minute": c.lease_requests_per_minute,
            "sse_grace_secs": c.sse_grace_secs,
            "sse_keepalive_secs": c.sse_keepalive_secs,
        },
        "self": c.self_enroll,
    }))
}

/// One query parameter, percent-decoded.
fn query_param(query: Option<&str>, name: &str) -> Option<String> {
    query?.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        (k == name).then(|| percent_decode(v)).flatten()
    })
}

fn percent_decode(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' => {
                let hex = std::str::from_utf8(b.get(i + 1..i + 3)?).ok()?;
                out.push(u8::from_str_radix(hex, 16).ok()?);
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

#[derive(Deserialize)]
struct EnrollBody {
    room_id: String,
    device_id: String,
    openid_token: String,
    matrix_server_name: String,
}

/// `POST /self/enroll` (ADR 0005). The body (and so the OpenID token) is never logged.
async fn enroll(hub: &CompanionHub, req: Request<Incoming>, client: &str) -> Response<NvbBody> {
    let body = match Limited::new(req.into_body(), MAX_BODY).collect().await {
        Ok(b) => b.to_bytes(),
        Err(_) => return error(StatusCode::PAYLOAD_TOO_LARGE, "bad_request", "request body too large or unreadable"),
    };
    // serde's message could quote the input; answer a fixed sentence instead.
    let Ok(b) = serde_json::from_slice::<EnrollBody>(&body) else {
        return error(StatusCode::BAD_REQUEST, "bad_request", "expected { room_id, device_id, openid_token, matrix_server_name }");
    };
    let req = EnrollRequest { room_id: b.room_id, device_id: b.device_id, openid_token: b.openid_token, matrix_server_name: b.matrix_server_name };
    match hub.enroll(&req, client).await {
        Ok((key, token)) => json(StatusCode::CREATED, serde_json::json!({
            "key": token,
            "key_id": key.id,
            "user_id": key.issuer,
            "device_id": key.device,
            "events_url": format!("{API_PREFIX}/self/events?room_id={{room_id}}"),
        })),
        Err(r) => refusal(r),
    }
}

async fn self_events(hub: &Arc<CompanionHub>, key: &CompanionKey, room_id: &str, shutdown: CancellationToken) -> Response<NvbBody> {
    let stream = match hub.open_self_events(key, room_id).await {
        Ok(s) => s,
        Err(r) => return refusal(r),
    };
    let (tx, rx) = mpsc::channel(64);
    tokio::spawn(self_event_loop(stream, tx, shutdown));
    sse_response(rx, None)
}

/// Feed one `/self/events` stream: the whole current state first, then changes, until the owner has been out of the
/// call for [`SELF_NOT_IN_CALL_GRACE`], the key is revoked, the bridge stops, or the client leaves.
async fn self_event_loop(stream: SelfStream, tx: mpsc::Sender<Bytes>, shutdown: CancellationToken) {
    let hub = stream.hub().clone();
    let keepalive = Duration::from_secs(hub.config().sse_keepalive_secs);
    let mut ka = tokio::time::interval_at(tokio::time::Instant::now() + keepalive, keepalive);
    let mut poll = tokio::time::interval(SELF_POLL);
    let mut member_check = tokio::time::interval_at(tokio::time::Instant::now() + SELF_MEMBER_RECHECK, SELF_MEMBER_RECHECK);
    let mut view_rx = stream.facts.view();
    let mut last = stream.state();
    let mut out_since: Option<tokio::time::Instant> = None;
    for (name, data) in last.events_since(None) {
        if tx.send(sse(name, &data)).await.is_err() {
            return;
        }
    }
    let end = |reason: &str| sse("end", &serde_json::json!({ "room_id": stream.room_id, "reason": reason }));
    loop {
        let refresh = tokio::select! {
            _ = tx.closed() => return,
            _ = shutdown.cancelled() => {
                let _ = tx.send(end("shutdown")).await;
                return;
            }
            r = view_rx.changed() => {
                if r.is_err() {
                    // The room's bridge stopped or restarted: the client reconnects and gets fresh state.
                    let _ = tx.send(end("bridge_restarting")).await;
                    return;
                }
                true
            }
            _ = poll.tick() => {
                if !hub.key_valid(&stream.key_id) {
                    let _ = tx.send(sse("revoked", &serde_json::json!({ "reason": "key_revoked" }))).await;
                    return;
                }
                true
            }
            _ = member_check.tick() => {
                if !matches!(tokio::time::timeout(Duration::from_secs(5), stream.facts.is_joined(&stream.issuer)).await, Ok(Ok(true))) {
                    let _ = tx.send(end("not_a_member")).await;
                    return;
                }
                false
            }
            _ = ka.tick() => {
                if tx.send(Bytes::from_static(b": keep-alive\n\n")).await.is_err() {
                    return;
                }
                false
            }
        };
        if !refresh {
            continue;
        }
        let now = stream.state();
        for (name, data) in now.events_since(Some(&last)) {
            if tx.send(sse(name, &data)).await.is_err() {
                return;
            }
        }
        if now.in_call {
            out_since = None;
        } else if out_since.get_or_insert_with(tokio::time::Instant::now).elapsed() >= SELF_NOT_IN_CALL_GRACE {
            let _ = tx.send(end("not_in_call")).await;
            return;
        }
        last = now;
    }
}

async fn open_lease(hub: &CompanionHub, key: &CompanionKey, req: Request<Incoming>) -> Response<NvbBody> {
    let body = match Limited::new(req.into_body(), MAX_BODY).collect().await {
        Ok(b) => b.to_bytes(),
        Err(_) => return error(StatusCode::PAYLOAD_TOO_LARGE, "bad_request", "request body too large or unreadable"),
    };
    let lr: LeaseRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return error(StatusCode::BAD_REQUEST, "bad_request", &format!("invalid JSON body: {e}")),
    };
    if lr.kind == "stream:attach" {
        let Some(uid) = lr.participant.as_ref().and_then(|p| p.discord_user_id.parse::<u64>().ok()) else {
            return error(StatusCode::BAD_REQUEST, "bad_request", "stream:attach needs participant.discord_user_id");
        };
        return match hub.open_stream_lease(key, lr.room_id.as_deref(), uid).await {
            Ok(g) => json(StatusCode::CREATED, stream_grant_json(&g.shared)),
            Err(r) => refusal(r),
        };
    }
    match hub.open_lease(key, &lr.kind, lr.room_id.as_deref()) {
        Ok(g) => {
            let l = &g.shared;
            json(StatusCode::CREATED, serde_json::json!({
                "lease_id": l.id,
                "kind": l.kind,
                "room_id": l.room_id,
                "expires_at": l.expires_at(),
                "events_url": format!("{API_PREFIX}/leases/{}/events", l.id),
            }))
        }
        Err(r) => refusal(r),
    }
}

/// The 201 body of a `stream:attach` lease: where to connect, with which key, and what may be published.
fn stream_grant_json(l: &LeaseShared) -> serde_json::Value {
    use base64::Engine;
    let Some(s) = l.stream.as_ref() else { return serde_json::json!({}) };
    let t = s.grant.transport.borrow().clone();
    serde_json::json!({
        "lease_id": l.id,
        "kind": l.kind,
        "room_id": l.room_id,
        "expires_at": l.expires_at(),
        "events_url": format!("{API_PREFIX}/leases/{}/events", l.id),
        "live_url": format!("{API_PREFIX}/leases/{}/live", l.id),
        "consent": s.basis,
        "participant": { "discord_user_id": s.discord_user_id.to_string(), "ghost_mxid": s.grant.ghost_mxid, "device_id": s.grant.device_id },
        "transport": t.map(|t| transport_json(&t)),
        "media": {
            "e2ee": s.grant.e2ee,
            "key_b64": s.grant.key.map(|k| base64::engine::general_purpose::STANDARD.encode(k)),
            "key_index": 0,
            "key_provider": { "key_derivation": "HKDF", "ratchet_salt": "LKFrameEncryptionKey", "ratchet_window_size": 16, "encryption": "AES-GCM" },
        },
        "allowed_sources": ["screen_share", "screen_share_audio"],
    })
}

fn transport_json(t: &crate::stream::StreamTransport) -> serde_json::Value {
    serde_json::json!({ "sfu_url": t.sfu_url, "jwt": t.jwt, "identity": t.identity, "fetched_at": t.fetched_at })
}

fn events(hub: &Arc<CompanionHub>, key: &CompanionKey, lease_id: &str, shutdown: CancellationToken) -> Response<NvbBody> {
    let guard = match hub.attach_stream(key, lease_id) {
        Ok(g) => g,
        Err(r) => return refusal(r),
    };
    let (tx, rx) = mpsc::channel(32);
    tokio::spawn(event_loop(hub.clone(), guard.shared.clone(), tx, shutdown));
    sse_response(rx, Some(guard))
}

fn sse_response(rx: mpsc::Receiver<Bytes>, guard: Option<StreamGuard>) -> Response<NvbBody> {
    let mut r = Response::new(NvbBody::Sse { rx, _guard: guard });
    let h = r.headers_mut();
    h.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    // Ask nginx-style proxies not to buffer the stream.
    h.insert("x-accel-buffering", HeaderValue::from_static("no"));
    r
}

fn sse(event: &str, data: &serde_json::Value) -> Bytes {
    Bytes::from(format!("event: {event}\ndata: {data}\n\n"))
}

/// Feed one lease's event stream until the lease ends or the client leaves.
async fn event_loop(hub: Arc<CompanionHub>, lease: Arc<LeaseShared>, tx: mpsc::Sender<Bytes>, shutdown: CancellationToken) {
    let keepalive = Duration::from_secs(hub.config().sse_keepalive_secs);
    let mut ka = tokio::time::interval_at(tokio::time::Instant::now() + keepalive, keepalive);
    let mut poll = tokio::time::interval(OBSERVE_POLL);
    let mut expires = lease.expires_at();
    let mut seen: Option<(usize, u64, Vec<u64>)> = None;
    let mut last: Option<Snapshot> = None;
    let observing = lease.kind == "observe";
    let mut transport_rx = lease.stream.as_ref().map(|s| s.grant.transport.clone());

    let opening = serde_json::json!({
        "lease_id": lease.id, "kind": lease.kind, "room_id": lease.room_id, "expires_at": expires,
    });
    if tx.send(sse("lease", &opening)).await.is_err() {
        return;
    }
    loop {
        if let Some(reason) = lease.ended() {
            let _ = tx.send(sse("revoked", &serde_json::json!({ "reason": reason }))).await;
            return;
        }
        tokio::select! {
            _ = tx.closed() => return,
            _ = shutdown.cancelled() => {
                let _ = tx.send(sse("revoked", &serde_json::json!({ "reason": "shutdown" }))).await;
                return;
            }
            _ = lease.notify.notified() => {
                let now = lease.expires_at();
                if now != expires && lease.ended().is_none() {
                    expires = now;
                    if tx.send(sse("renewed", &serde_json::json!({ "expires_at": now }))).await.is_err() {
                        return;
                    }
                }
            }
            r = async { match transport_rx.as_mut() { Some(rx) => rx.changed().await.is_ok(), None => std::future::pending().await } } => {
                if !r { transport_rx = None; continue; }
                let t = transport_rx.as_mut().and_then(|rx| rx.borrow_and_update().clone());
                if let Some(t) = t {
                    if tx.send(sse("transport", &transport_json(&t))).await.is_err() {
                        return;
                    }
                }
            }
            _ = ka.tick() => {
                if tx.send(Bytes::from_static(b": keep-alive\n\n")).await.is_err() {
                    return;
                }
            }
            _ = poll.tick(), if observing => {
                let registry = &hub.links().registry;
                let board = registry.board_for_room(&lease.room_id);
                let attached = hub.attached_in(&lease.room_id);
                let mut att: Vec<u64> = attached.iter().copied().collect();
                att.sort_unstable();
                let (ptr, rev) = board.as_ref().map(|b| (Arc::as_ptr(b) as usize, b.revision())).unwrap_or((0, 0));
                let stamp = (ptr, rev, att);
                if last.is_some() && seen.as_ref() == Some(&stamp) {
                    continue;
                }
                seen = Some(stamp);
                let snap = Snapshot::take(registry, &lease.room_id, &lease.issuer, &attached);
                for (name, data) in snap.events_since(last.as_ref()) {
                    if tx.send(sse(name, &data)).await.is_err() {
                        return;
                    }
                }
                last = Some(snap);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nvb_config::CompanionsConfig;
    use nvb_config::companions::{CompanionKeys, KeyScope};
    use nvb_config::links::LinkShared;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    const ROOM: &str = "!room:hs";

    async fn start() -> (SocketAddr, Arc<CompanionHub>, LinkShared, CancellationToken) {
        let links = LinkShared::in_memory();
        let dir = std::env::temp_dir().join(format!("nvb-http-{}-{}", std::process::id(), crate::token::lease_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let hub = CompanionHub::with_store(
            CompanionsConfig { enabled: true, ..Default::default() },
            Arc::new(CompanionKeys::in_memory()),
            links.clone(),
            dir.join("audit.jsonl").to_string_lossy().into_owned(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let sd = CancellationToken::new();
        tokio::spawn(serve_on(listener, hub.clone(), sd.clone()));
        (addr, hub, links, sd)
    }

    /// One HTTP/1.1 request on a fresh connection; returns (status, body).
    async fn request(addr: SocketAddr, method: &str, path: &str, token: Option<&str>, body: &str) -> (u16, String) {
        let mut s = TcpStream::connect(addr).await.unwrap();
        let auth = token.map(|t| format!("Authorization: Bearer {t}\r\n")).unwrap_or_default();
        let req = format!(
            "{method} {path} HTTP/1.1\r\nHost: x\r\n{auth}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        s.write_all(req.as_bytes()).await.unwrap();
        let mut out = String::new();
        s.read_to_string(&mut out).await.unwrap();
        let status = out.split(' ').nth(1).unwrap().parse().unwrap();
        let body = out.split_once("\r\n\r\n").map(|(_, b)| b.to_string()).unwrap_or_default();
        (status, body)
    }

    #[tokio::test]
    async fn discovery_auth_and_lease_lifecycle() {
        let (addr, hub, _links, sd) = start().await;
        let (st, body) = request(addr, "GET", "/_nvb/v1", None, "").await;
        assert_eq!(st, 200);
        assert!(body.contains("\"observe\""));

        assert_eq!(request(addr, "POST", "/_nvb/v1/leases", None, "{}").await.0, 401);
        let (_, tok) = hub.create_key("@mod:hs", KeyScope::Room(ROOM.into()), &["observe".into()], "").unwrap();
        assert_eq!(request(addr, "POST", "/_nvb/v1/leases", Some(&tok), "nope").await.0, 400);
        let (st, body) = request(addr, "POST", "/_nvb/v1/leases", Some(&tok), r#"{"kind":"observe"}"#).await;
        assert_eq!(st, 201, "{body}");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        let id = v["lease_id"].as_str().unwrap().to_string();
        assert_eq!(v["room_id"], ROOM);

        let (st, body) = request(addr, "POST", &format!("/_nvb/v1/leases/{id}/renew"), Some(&tok), "").await;
        assert_eq!(st, 200, "{body}");
        assert_eq!(request(addr, "DELETE", &format!("/_nvb/v1/leases/{id}"), Some(&tok), "").await.0, 200);
        assert_eq!(request(addr, "DELETE", &format!("/_nvb/v1/leases/{id}"), Some(&tok), "").await.0, 404);
        assert_eq!(request(addr, "GET", "/_nvb/v1/leases", Some(&tok), "").await.0, 405);
        assert_eq!(request(addr, "GET", "/elsewhere", None, "").await.0, 404);
        sd.cancel();
    }

    /// Read from `s` into `got` until it contains `needle` (5 s budget).
    async fn read_until(s: &mut TcpStream, got: &mut String, needle: &str) {
        let mut buf = [0u8; 4096];
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !got.contains(needle) {
            let n = tokio::time::timeout_at(deadline, s.read(&mut buf)).await.expect("timed out").unwrap();
            assert!(n > 0, "stream closed early: {got}");
            got.push_str(&String::from_utf8_lossy(&buf[..n]));
        }
    }

    #[tokio::test]
    async fn observe_stream_reports_the_room_and_its_end() {
        let (addr, hub, links, sd) = start().await;
        let board = links.board_for(ROOM, &[], &[]);
        links.registry.register("b", board.clone());
        board.set_ghost_naming("nvb_", "hs");
        let (key, tok) = hub.create_key("@mod:hs", KeyScope::Room(ROOM.into()), &["observe".into()], "").unwrap();
        let (_, body) = request(addr, "POST", "/_nvb/v1/leases", Some(&tok), r#"{"kind":"observe"}"#).await;
        let id = serde_json::from_str::<serde_json::Value>(&body).unwrap()["lease_id"].as_str().unwrap().to_string();

        let mut s = TcpStream::connect(addr).await.unwrap();
        let req = format!("GET /_nvb/v1/leases/{id}/events HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {tok}\r\n\r\n");
        s.write_all(req.as_bytes()).await.unwrap();

        let mut got = String::new();
        read_until(&mut s, &mut got, "event: listeners").await;
        assert!(got.contains("text/event-stream"));
        assert!(got.contains("event: lease"));
        assert!(got.contains("event: participants\ndata: {\"participants\":[]}"));

        // A second stream on the same lease is refused while this one is open.
        let (st, _) = request(addr, "GET", &format!("/_nvb/v1/leases/{id}/events"), Some(&tok), "").await;
        assert_eq!(st, 409);

        board.set_discord_present(7, true);
        board.note_ghost(7, Some("Seven"));
        read_until(&mut s, &mut got, "@nvb_7:hs").await;

        hub.revoke_key(&key.id, "@mod:hs").unwrap();
        read_until(&mut s, &mut got, "event: revoked\ndata: {\"reason\":\"key_revoked\"}").await;
        sd.cancel();
    }

    #[tokio::test]
    async fn bind_retries_until_the_address_is_free() {
        let taken = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = taken.local_addr().unwrap();
        let sd = CancellationToken::new();
        let waiter = tokio::spawn({
            let sd = sd.clone();
            async move { bind_with_retry(addr, &sd).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert!(!waiter.is_finished(), "must keep retrying while the port is taken");
        drop(taken);
        let got = tokio::time::timeout(std::time::Duration::from_secs(10), waiter).await.unwrap().unwrap().unwrap();
        assert_eq!(got.unwrap().local_addr().unwrap(), addr);
    }

    #[tokio::test]
    async fn bind_retry_stops_on_shutdown() {
        let taken = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = taken.local_addr().unwrap();
        let sd = CancellationToken::new();
        let waiter = tokio::spawn({
            let sd = sd.clone();
            async move { bind_with_retry(addr, &sd).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        sd.cancel();
        let got = tokio::time::timeout(std::time::Duration::from_secs(5), waiter).await.unwrap().unwrap().unwrap();
        assert!(got.is_none());
    }
}
