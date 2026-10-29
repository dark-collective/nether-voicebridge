//! Just enough Matrix server-server discovery to verify an OpenID token once (ADR 0005 enrollment):
//! server-name parsing, `.well-known/matrix/server` delegation, SRV (`_matrix-fed._tcp`, then the deprecated
//! `_matrix._tcp`), port 8448, TLS validated against the (delegated) server name — and an address policy that refuses
//! private, loopback, link-local and this host's own addresses before any connection is made.
//!
//! Every connection is pinned to an address the policy has checked (no second DNS lookup between check and connect),
//! redirects are not followed, and response bodies are capped. The OpenID token travels only in the one userinfo
//! request and never into a log line (`reqwest` errors are logged without their URL).

use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::time::Duration;

use tokio::net::UdpSocket;

const DEFAULT_FEDERATION_PORT: u16 = 8448;
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const DNS_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_BODY: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyError {
    /// The server name is not a valid Matrix server name.
    BadServerName,
    /// Every address the server resolved to is refused by the address policy.
    AddressRefused,
    /// Discovery or the request failed (DNS, TLS, timeout, non-JSON).
    Unreachable(String),
    /// The homeserver did not accept the token.
    TokenRejected,
}

impl std::fmt::Display for VerifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VerifyError::BadServerName => f.write_str("not a valid Matrix server name"),
            VerifyError::AddressRefused => f.write_str("the server resolves only to refused addresses"),
            VerifyError::Unreachable(why) => write!(f, "the server could not be reached: {why}"),
            VerifyError::TokenRejected => f.write_str("the homeserver rejected the OpenID token"),
        }
    }
}

impl std::error::Error for VerifyError {}

pub type VerifyFuture<'a> = Pin<Box<dyn Future<Output = Result<String, VerifyError>> + Send + 'a>>;

/// Resolves an OpenID token to the user it was issued for, asking the token's own homeserver.
pub trait OpenIdVerifier: Send + Sync {
    /// `GET /_matrix/federation/v1/openid/userinfo` on `server_name`; the `sub` it answers.
    fn userinfo<'a>(&'a self, server_name: &'a str, token: &'a str) -> VerifyFuture<'a>;
}

// ── server names ──────────────────────────────────────────────────────────────

/// A parsed Matrix server name: host (a DNS name, or an IP literal) and an optional explicit port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerName {
    pub host: Host,
    pub port: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Host {
    Name(String),
    Ip(IpAddr),
}

impl Host {
    /// As it goes in a URL authority.
    fn authority(&self) -> String {
        match self {
            Host::Name(n) => n.clone(),
            Host::Ip(IpAddr::V4(a)) => a.to_string(),
            Host::Ip(IpAddr::V6(a)) => format!("[{a}]"),
        }
    }
}

fn valid_dns_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 253
        && s.split('.').all(|l| {
            !l.is_empty() && l.len() <= 63 && l.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') && !l.starts_with('-') && !l.ends_with('-')
        })
}

/// Parse a server name per the spec grammar (`host[:port]`, IPv6 literals in brackets). Lowercases DNS names.
pub fn parse_server_name(s: &str) -> Option<ServerName> {
    if s.is_empty() || s.len() > 255 {
        return None;
    }
    let (host, port) = if let Some(rest) = s.strip_prefix('[') {
        let (ip, after) = rest.split_once(']')?;
        let ip: Ipv6Addr = ip.parse().ok()?;
        let port = match after {
            "" => None,
            p => Some(p.strip_prefix(':')?.parse::<u16>().ok()?),
        };
        (Host::Ip(IpAddr::V6(ip)), port)
    } else {
        let (h, port) = match s.rsplit_once(':') {
            Some((h, p)) => (h, Some(p.parse::<u16>().ok()?)),
            None => (s, None),
        };
        if let Ok(ip) = h.parse::<Ipv4Addr>() {
            (Host::Ip(IpAddr::V4(ip)), port)
        } else if valid_dns_name(h) {
            (Host::Name(h.to_ascii_lowercase()), port)
        } else {
            return None;
        }
    };
    if port == Some(0) {
        return None;
    }
    Some(ServerName { host, port })
}

// ── address policy ────────────────────────────────────────────────────────────

/// Addresses no enrollment may make the bridge contact: loopback, private, shared (CGNAT / tailnets), link-local,
/// unique-local, multicast, unspecified, broadcast, documentation and benchmarking ranges.
pub fn is_refused_range(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(a) => {
            let o = a.octets();
            a.is_loopback()
                || a.is_private()
                || a.is_link_local()
                || a.is_multicast()
                || a.is_unspecified()
                || a.is_broadcast()
                || a.is_documentation()
                || o[0] == 0
                || (o[0] == 100 && (o[1] & 0xc0) == 64) // 100.64.0.0/10 shared address space
                || (o[0] == 192 && o[1] == 0 && o[2] == 0) // 192.0.0.0/24 protocol assignments
                || (o[0] == 198 && (o[1] & 0xfe) == 18) // 198.18.0.0/15 benchmarking
                || o[0] >= 240 // reserved
        }
        IpAddr::V6(a) => {
            if let Some(v4) = a.to_ipv4_mapped() {
                return is_refused_range(IpAddr::V4(v4));
            }
            let s = a.segments();
            a.is_loopback()
                || a.is_unspecified()
                || a.is_multicast()
                || (s[0] & 0xfe00) == 0xfc00 // fc00::/7 unique local
                || (s[0] & 0xffc0) == 0xfe80 // fe80::/10 link local
                || (s[0] & 0xffc0) == 0xfec0 // fec0::/10 site local (deprecated)
                || (s[0] == 0x2001 && s[1] == 0x0db8) // documentation
                || (s[0] == 0x0064 && s[1] == 0xff9b) // NAT64 (maps onto IPv4, possibly private)
                || (s[0] == 0x2002) // 6to4 (embeds an IPv4 address)
                || (s[0] == 0 && s[1] == 0 && s[2] == 0 && s[3] == 0 && s[4] == 0 && s[5] == 0) // IPv4-compatible
        }
    }
}

/// Is `ip` an address of this host? Binding a socket to it succeeds only if it is.
pub fn is_own_address(ip: IpAddr) -> bool {
    std::net::UdpSocket::bind(SocketAddr::new(ip, 0)).is_ok()
}

/// What the resolver may connect to.
#[derive(Debug, Clone, Default)]
pub struct AddressPolicy {
    /// Server names (lowercase, as written in the request) exempt from the policy: the operator's own homeserver on a
    /// private network or on this host.
    pub trusted: Vec<String>,
}

impl AddressPolicy {
    fn allows(&self, server_name: &str, ip: IpAddr) -> bool {
        if self.trusted.iter().any(|t| t.eq_ignore_ascii_case(server_name)) {
            return true;
        }
        !is_refused_range(ip) && !is_own_address(ip)
    }
}

// ── DNS: A/AAAA via the system resolver, SRV via a minimal UDP/TCP query ─────

/// One SRV answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Srv {
    pub priority: u16,
    pub weight: u16,
    pub port: u16,
    pub target: String,
}

fn build_query(id: u16, name: &str, qtype: u16) -> Option<Vec<u8>> {
    let mut q = Vec::with_capacity(32 + name.len());
    q.extend_from_slice(&id.to_be_bytes());
    q.extend_from_slice(&[0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0]); // RD; 1 question
    for label in name.trim_end_matches('.').split('.') {
        if label.is_empty() || label.len() > 63 {
            return None;
        }
        q.push(label.len() as u8);
        q.extend_from_slice(label.as_bytes());
    }
    q.push(0);
    q.extend_from_slice(&qtype.to_be_bytes());
    q.extend_from_slice(&1u16.to_be_bytes()); // IN
    Some(q)
}

/// Read a (possibly compressed) name at `pos`; returns it and the offset just past it in the original position.
fn read_name(msg: &[u8], mut pos: usize) -> Option<(String, usize)> {
    let mut labels: Vec<String> = Vec::new();
    let mut end: Option<usize> = None;
    for _ in 0..64 {
        let len = *msg.get(pos)? as usize;
        if len & 0xc0 == 0xc0 {
            let ptr = ((len & 0x3f) << 8) | *msg.get(pos + 1)? as usize;
            end.get_or_insert(pos + 2);
            if ptr >= pos {
                return None; // forward pointers make loops possible
            }
            pos = ptr;
            continue;
        }
        if len == 0 {
            return Some((labels.join("."), end.unwrap_or(pos + 1)));
        }
        let label = msg.get(pos + 1..pos + 1 + len)?;
        labels.push(String::from_utf8_lossy(label).to_ascii_lowercase());
        pos += 1 + len;
    }
    None
}

/// Parse the SRV answers of a response to query `id`. `None` = malformed or not our answer.
pub fn parse_srv_response(msg: &[u8], id: u16) -> Option<Vec<Srv>> {
    if msg.len() < 12 || u16::from_be_bytes([msg[0], msg[1]]) != id || msg[2] & 0x80 == 0 {
        return None;
    }
    let rcode = msg[3] & 0x0f;
    if rcode == 3 {
        return Some(Vec::new()); // NXDOMAIN
    }
    if rcode != 0 {
        return None;
    }
    let qd = u16::from_be_bytes([msg[4], msg[5]]) as usize;
    let an = u16::from_be_bytes([msg[6], msg[7]]) as usize;
    let mut pos = 12;
    for _ in 0..qd {
        let (_, p) = read_name(msg, pos)?;
        pos = p + 4;
    }
    let mut out = Vec::new();
    for _ in 0..an {
        let (_, p) = read_name(msg, pos)?;
        let rtype = u16::from_be_bytes([*msg.get(p)?, *msg.get(p + 1)?]);
        let rdlen = u16::from_be_bytes([*msg.get(p + 8)?, *msg.get(p + 9)?]) as usize;
        let rdata = p + 10;
        msg.get(rdata..rdata + rdlen)?;
        if rtype == 33 && rdlen >= 7 {
            let r = &msg[rdata..];
            let (target, _) = read_name(msg, rdata + 6)?;
            out.push(Srv {
                priority: u16::from_be_bytes([r[0], r[1]]),
                weight: u16::from_be_bytes([r[2], r[3]]),
                port: u16::from_be_bytes([r[4], r[5]]),
                target,
            });
        }
        pos = rdata + rdlen;
    }
    Some(out)
}

fn nameservers() -> Vec<SocketAddr> {
    let conf = std::fs::read_to_string("/etc/resolv.conf").unwrap_or_default();
    conf.lines()
        .filter_map(|l| l.trim().strip_prefix("nameserver"))
        .filter_map(|ns| ns.trim().split('%').next().and_then(|a| a.parse::<IpAddr>().ok()))
        .map(|ip| SocketAddr::new(ip, 53))
        .take(3)
        .collect()
}

async fn query_srv_at(ns: SocketAddr, name: &str) -> Option<Vec<Srv>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let id: u16 = rand::random();
    let q = build_query(id, name, 33)?;
    let bind: SocketAddr = if ns.is_ipv4() { (Ipv4Addr::UNSPECIFIED, 0).into() } else { (Ipv6Addr::UNSPECIFIED, 0).into() };
    let sock = UdpSocket::bind(bind).await.ok()?;
    sock.send_to(&q, ns).await.ok()?;
    let mut buf = vec![0u8; 4096];
    let n = loop {
        let (n, from) = tokio::time::timeout(DNS_TIMEOUT, sock.recv_from(&mut buf)).await.ok()?.ok()?;
        if from == ns {
            break n;
        }
    };
    let msg = &buf[..n];
    if msg.len() >= 3 && msg[2] & 0x02 != 0 {
        // Truncated: ask again over TCP.
        let mut tcp = tokio::time::timeout(DNS_TIMEOUT, tokio::net::TcpStream::connect(ns)).await.ok()?.ok()?;
        let mut framed = (q.len() as u16).to_be_bytes().to_vec();
        framed.extend_from_slice(&q);
        tcp.write_all(&framed).await.ok()?;
        let mut len = [0u8; 2];
        tokio::time::timeout(DNS_TIMEOUT, tcp.read_exact(&mut len)).await.ok()?.ok()?;
        let mut body = vec![0u8; u16::from_be_bytes(len) as usize];
        tokio::time::timeout(DNS_TIMEOUT, tcp.read_exact(&mut body)).await.ok()?.ok()?;
        return parse_srv_response(&body, id);
    }
    parse_srv_response(msg, id)
}

/// SRV records for `name`, best first (lowest priority, then highest weight). Empty when there are none or DNS fails.
async fn lookup_srv(name: &str) -> Vec<Srv> {
    for ns in nameservers() {
        if let Some(mut v) = query_srv_at(ns, name).await {
            v.retain(|s| !s.target.is_empty() && s.port != 0);
            v.sort_by(|a, b| a.priority.cmp(&b.priority).then(b.weight.cmp(&a.weight)));
            return v;
        }
    }
    Vec::new()
}

// ── resolution ────────────────────────────────────────────────────────────────

/// Where to send federation requests: the name TLS is validated against (and the URL authority) plus the checked
/// addresses the connection is pinned to.
#[derive(Debug, Clone)]
struct Target {
    host: Host,
    port: u16,
    addrs: Vec<SocketAddr>,
}

impl Target {
    fn base_url(&self) -> String {
        format!("https://{}:{}", self.host.authority(), self.port)
    }
}

pub struct FederationVerifier {
    policy: AddressPolicy,
}

impl FederationVerifier {
    pub fn new(policy: AddressPolicy) -> Self {
        Self { policy }
    }

    /// Resolve `host` (DNS A/AAAA, or the literal) and keep the addresses the policy allows.
    async fn checked_addrs(&self, server_name: &str, host: &Host, port: u16) -> Result<Vec<SocketAddr>, VerifyError> {
        let all: Vec<SocketAddr> = match host {
            Host::Ip(ip) => vec![SocketAddr::new(*ip, port)],
            Host::Name(n) => match tokio::time::timeout(DNS_TIMEOUT, tokio::net::lookup_host((n.as_str(), port))).await {
                Ok(Ok(it)) => it.collect(),
                _ => return Err(VerifyError::Unreachable(format!("cannot resolve {n}"))),
            },
        };
        let ok: Vec<SocketAddr> = all.iter().copied().filter(|a| self.policy.allows(server_name, a.ip())).collect();
        if ok.is_empty() {
            if !all.is_empty() {
                tracing::warn!(server_name, "enrollment: server resolves only to refused addresses — not contacting it");
            }
            return Err(if all.is_empty() { VerifyError::Unreachable("no addresses".into()) } else { VerifyError::AddressRefused });
        }
        Ok(ok)
    }

    fn client(&self, t: &Target) -> Result<reqwest::Client, VerifyError> {
        let mut b = reqwest::Client::builder()
            .use_rustls_tls()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(HTTP_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .no_proxy();
        if let Host::Name(n) = &t.host {
            b = b.resolve_to_addrs(n, &t.addrs);
        }
        b.build().map_err(|e| VerifyError::Unreachable(e.without_url().to_string()))
    }

    async fn get_json(&self, t: &Target, path_and_query: &str) -> Result<(u16, Option<serde_json::Value>), VerifyError> {
        let client = self.client(t)?;
        let url = format!("{}{}", t.base_url(), path_and_query);
        let mut resp = client.get(url).send().await.map_err(|e| VerifyError::Unreachable(e.without_url().to_string()))?;
        let status = resp.status().as_u16();
        let mut body = Vec::new();
        while let Some(chunk) = resp.chunk().await.map_err(|e| VerifyError::Unreachable(e.without_url().to_string()))? {
            if body.len() + chunk.len() > MAX_BODY {
                return Err(VerifyError::Unreachable("response too large".into()));
            }
            body.extend_from_slice(&chunk);
        }
        Ok((status, serde_json::from_slice(&body).ok()))
    }

    /// `host` with SRV, else the default port.
    async fn via_srv(&self, server_name: &str, tls_host: &str) -> Result<Target, VerifyError> {
        for prefix in ["_matrix-fed._tcp", "_matrix._tcp"] {
            if let Some(srv) = lookup_srv(&format!("{prefix}.{tls_host}")).await.into_iter().next() {
                let addrs = self.checked_addrs(server_name, &Host::Name(srv.target.clone()), srv.port).await?;
                return Ok(Target { host: Host::Name(tls_host.to_string()), port: srv.port, addrs });
            }
        }
        let host = Host::Name(tls_host.to_string());
        let addrs = self.checked_addrs(server_name, &host, DEFAULT_FEDERATION_PORT).await?;
        Ok(Target { host, port: DEFAULT_FEDERATION_PORT, addrs })
    }

    /// Server discovery (Matrix server-server API, "Resolving server names").
    async fn resolve(&self, server_name: &str) -> Result<Target, VerifyError> {
        let sn = parse_server_name(server_name).ok_or(VerifyError::BadServerName)?;
        // 1. IP literal; 2. explicit port.
        if let Host::Ip(_) = sn.host {
            let port = sn.port.unwrap_or(DEFAULT_FEDERATION_PORT);
            let addrs = self.checked_addrs(server_name, &sn.host, port).await?;
            return Ok(Target { host: sn.host, port, addrs });
        }
        let Host::Name(name) = &sn.host else { return Err(VerifyError::BadServerName) };
        if let Some(port) = sn.port {
            let addrs = self.checked_addrs(server_name, &sn.host, port).await?;
            return Ok(Target { host: sn.host.clone(), port, addrs });
        }
        // 3. Delegation through `.well-known/matrix/server` (any failure: no delegation).
        if let Ok(addrs) = self.checked_addrs(server_name, &sn.host, 443).await {
            let wk = Target { host: sn.host.clone(), port: 443, addrs };
            if let Ok((200, Some(v))) = self.get_json(&wk, "/.well-known/matrix/server").await {
                if let Some(delegated) = v.get("m.server").and_then(|s| s.as_str()).and_then(parse_server_name) {
                    // 3.1/3.2: an IP literal or an explicit port is used as is; otherwise SRV on the delegated name.
                    if matches!(delegated.host, Host::Ip(_)) || delegated.port.is_some() {
                        let port = delegated.port.unwrap_or(DEFAULT_FEDERATION_PORT);
                        let addrs = self.checked_addrs(server_name, &delegated.host, port).await?;
                        return Ok(Target { host: delegated.host, port, addrs });
                    }
                    return self.via_srv(server_name, &delegated.host.authority()).await;
                }
            }
        }
        // 4./5. SRV on the server name, else port 8448.
        self.via_srv(server_name, name).await
    }
}

fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

impl OpenIdVerifier for FederationVerifier {
    fn userinfo<'a>(&'a self, server_name: &'a str, token: &'a str) -> VerifyFuture<'a> {
        Box::pin(async move {
            let target = self.resolve(server_name).await?;
            let path = format!("/_matrix/federation/v1/openid/userinfo?access_token={}", urlencode(token));
            match self.get_json(&target, &path).await? {
                (200, Some(v)) => v
                    .get("sub")
                    .and_then(|s| s.as_str())
                    .map(str::to_string)
                    .ok_or_else(|| VerifyError::Unreachable("userinfo without sub".into())),
                (401 | 403 | 404, _) => Err(VerifyError::TokenRejected),
                (status, _) => Err(VerifyError::Unreachable(format!("userinfo answered {status}"))),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_names_parse_per_the_grammar() {
        let n = |h: &str| Host::Name(h.into());
        assert_eq!(parse_server_name("Example.org"), Some(ServerName { host: n("example.org"), port: None }));
        assert_eq!(parse_server_name("example.org:8448"), Some(ServerName { host: n("example.org"), port: Some(8448) }));
        assert_eq!(parse_server_name("1.2.3.4:80").unwrap().host, Host::Ip("1.2.3.4".parse().unwrap()));
        assert_eq!(parse_server_name("[::1]:8448").unwrap().port, Some(8448));
        assert!(parse_server_name("[::1]").is_some());
        for bad in ["", "exa mple.org", "example.org:", "example.org:0", "example.org:99999", "-bad.org", "a..b", "[::1", "user@host", "host/path", "::1"] {
            assert!(parse_server_name(bad).is_none(), "{bad} must be rejected");
        }
    }

    #[test]
    fn private_and_special_ranges_are_refused() {
        for ip in [
            "127.0.0.1", "10.1.2.3", "172.16.0.1", "192.168.1.1", "169.254.169.254", "100.64.0.37", "0.0.0.0", "255.255.255.255",
            "224.0.0.1", "198.18.0.1", "192.0.0.8", "::1", "::", "fd00::1", "fe80::1", "::ffff:10.0.0.1", "::ffff:127.0.0.1",
            "64:ff9b::a00:1", "2002:a00:1::", "2001:db8::1",
        ] {
            assert!(is_refused_range(ip.parse().unwrap()), "{ip} must be refused");
        }
        for ip in ["1.1.1.1", "93.184.216.34", "2606:4700::1111", "100.128.0.1"] {
            assert!(!is_refused_range(ip.parse().unwrap()), "{ip} is public");
        }
    }

    #[test]
    fn own_addresses_are_recognised_and_trusted_names_exempt() {
        assert!(is_own_address("127.0.0.1".parse().unwrap()));
        assert!(!is_own_address("192.0.2.1".parse().unwrap()), "TEST-NET-1 is never configured on a host");
        let p = AddressPolicy { trusted: vec!["hs.internal".into()] };
        assert!(p.allows("HS.internal", "10.0.0.5".parse().unwrap()));
        assert!(!p.allows("other.org", "10.0.0.5".parse().unwrap()));
        assert!(!p.allows("other.org", "127.0.0.1".parse().unwrap()));
    }

    /// A captured-shape SRV answer with name compression: `_matrix-fed._tcp.example.org` → 10 5 8448 matrix.example.org.
    fn srv_answer(id: u16) -> Vec<u8> {
        let mut m = Vec::new();
        m.extend_from_slice(&id.to_be_bytes());
        m.extend_from_slice(&[0x81, 0x80, 0, 1, 0, 1, 0, 0, 0, 0]);
        let qname_at = m.len();
        for l in ["_matrix-fed", "_tcp", "example", "org"] {
            m.push(l.len() as u8);
            m.extend_from_slice(l.as_bytes());
        }
        m.push(0);
        m.extend_from_slice(&[0, 33, 0, 1]);
        // Answer: name = pointer to the question.
        m.extend_from_slice(&[0xc0, qname_at as u8, 0, 33, 0, 1, 0, 0, 0x0e, 0x10]);
        let mut rdata = vec![0, 10, 0, 5, 0x21, 0x00]; // priority 10, weight 5, port 8448
        rdata.push(6);
        rdata.extend_from_slice(b"matrix");
        // "example.org" by pointer into the question (offset of "example").
        rdata.extend_from_slice(&[0xc0, (qname_at + 1 + 11 + 1 + 4) as u8]);
        m.extend_from_slice(&(rdata.len() as u16).to_be_bytes());
        m.extend_from_slice(&rdata);
        m
    }

    #[test]
    fn srv_answers_parse_with_compression_and_reject_strangers() {
        let msg = srv_answer(0x1234);
        assert_eq!(
            parse_srv_response(&msg, 0x1234),
            Some(vec![Srv { priority: 10, weight: 5, port: 8448, target: "matrix.example.org".into() }])
        );
        assert_eq!(parse_srv_response(&msg, 0x9999), None, "an answer to another query is ignored");
        assert_eq!(parse_srv_response(&msg[..msg.len() - 3], 0x1234), None, "truncated garbage is rejected");
        let mut nx = msg.clone();
        nx[3] = 0x83;
        assert_eq!(parse_srv_response(&nx, 0x1234), Some(vec![]));
        let q = build_query(7, "_matrix-fed._tcp.example.org", 33).unwrap();
        assert_eq!(&q[..2], &[0, 7]);
        assert!(build_query(7, "a..b", 33).is_none());
    }

    #[test]
    fn a_pointer_loop_does_not_hang() {
        let mut m = vec![0u8; 12];
        m.extend_from_slice(&[0xc0, 12]);
        assert!(read_name(&m, 12).is_none());
    }

    #[test]
    fn tokens_are_url_encoded() {
        assert_eq!(urlencode("abc-_.~"), "abc-_.~");
        assert_eq!(urlencode("a b/+="), "a%20b%2F%2B%3D");
    }

    #[tokio::test]
    async fn literal_private_addresses_are_refused_before_any_connection() {
        let v = FederationVerifier::new(AddressPolicy::default());
        for sn in ["127.0.0.1:8448", "[::1]", "10.0.0.1", "169.254.169.254:80"] {
            assert_eq!(v.userinfo(sn, "tok").await, Err(VerifyError::AddressRefused), "{sn}");
        }
        assert_eq!(v.userinfo("not a name", "tok").await, Err(VerifyError::BadServerName));
    }
}
