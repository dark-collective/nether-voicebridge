# Upstream bug report draft — Tuwunel↔Tuwunel federated invite-joins failing

_Internal memo + paste-ready issue body for the Tuwunel tracker
(https://github.com/matrix-construct/tuwunel/issues). Written 2026-07-12 from
the nether.im operator's logs; file it as yourself (same personal-submission
etiquette as other upstream contributions). Possibly related existing issue:
matrix-construct/tuwunel#224 ("Can't message people or join rooms on other
homeservers") — skim before filing to avoid a duplicate._

---

## Suggested title

Federated invite-joins between two Tuwunel 1.8.1 servers fail persistently:
peer's `200` responses rejected with `Deserialization … missing field "event"`;
invite handshake broken in both directions

## Body

**Environment.**
- Server A: `nether.im` — Tuwunel `1.8.1-1` (deb), Ubuntu 24.04, reached via a
  Cloudflare tunnel (inbound federation demonstrably works: multiple other
  servers, including B, participate in A's rooms).
- Server B: `tinier.town` — Tuwunel `1.8.1` per its federation version
  endpoint (`{"name":"Tuwunel","version":"1.8.1","compiler":"rustc 1.95.0"}`),
  delegated via `.well-known` to `matrix.tinier.town:443`. Not operated by us.

**Summary.** A user on B repeatedly invites an appservice user on A to fresh
DM rooms (created on B). A accepts and attempts the join. For a period this
worked (multiple successful A→B invite-joins). Then it began failing on every
attempt and has stayed broken for 10+ hours, across a full restart of A:

```
ERROR failed to join invited room room_id=!…:tinier.town
error=500 Internal Server Error: {"errcode":"M_UNKNOWN","error":"Server
returned bad 200 response: Deserialization(Json(Error(\"missing field
`event`\", line: 1, column: 2)))"}
```

i.e. A performs the federated join against B, B answers **HTTP 200**, and A's
parser rejects the body as JSON missing the `event` field at line 1, column 2
(suggesting a near-empty or truncated object rather than a malformed large
body).

**The reverse direction fails identically.** A `createRoom` on A with an
`invite` for the B user initially produced **no invite membership event at
all** (silent drop — the room was created, the invitee's `m.room.member`
state simply never appeared). After A was restarted, retrying that invite
fails loudly with the *same* error shape, presumably from the
`/_matrix/federation/v2/invite` exchange:

```
{"errcode":"M_UNKNOWN","error":"Server returned bad 200 response:
Deserialization(Json(Error(\"missing field `event`\", line: 1, column: 2)))"}
```

**Timeline (UTC, 2026-07-12).**
- 02:25 — two A→B invite-joins to B-hosted private rooms succeed normally.
- 04:19, 04:30 — further A→B invite-joins succeed (join completes in ~1–3 s).
- 04:00 — one join rejected `403 … "Auth check failed: cannot join a room
  that is not `public`"` from B for a room A had a pending invite to
  (possibly the invite was retracted first; noting for completeness).
- **04:37 onward — every A→B join fails**: one 30 s timeout, then the
  `missing field \`event\`` deserialization error on every attempt:
  04:37, 04:46, 05:05, 05:31, and 14:45 (10 hours later). Fresh room each
  time (the user kept recreating DMs).
- ~05:20 — A fully restarted: no change. (A's application layer was also
  upgraded twice during the window — unrelated to federation code — no
  change.)

**Reachability is healthy throughout**, which is what makes this interesting:
- `GET B/_matrix/federation/v1/version` → 200, correct JSON.
- `GET B/_matrix/key/v2/server` → 200 in ~340 ms.
- Unauthenticated `PUT B/_matrix/federation/v2/invite/…` → proper
  `M_FORBIDDEN: Missing Authorization header` Matrix JSON.

So B answers unauthenticated probes correctly; only the authenticated
event-exchange endpoints (`make_join`/`send_join`/`invite`) return bodies A
cannot parse — consistently, in both directions of the relationship.

**Root-cause update (live capture).** `missing field \`event\` at line 1,
column 2` means the parser exhausted the object right after `{` — **B is
returning HTTP 200 with a literal empty body `{}`** on authenticated
federation writes, alongside intermittent transport-level failures
(`Request error: error sending request`), while GETs stay healthy.

**Control experiments (same day, ~19:30–20:30 UTC).** These narrow the fault
surface further:

1. **B federates fine with Synapse.** A user on `4d2.org` (Synapse 1.153.0)
   established a DM with the same B user while A↔B stayed broken — B is not
   globally broken; the failure is specific to this pair.
2. **Tuwunel 1.8.1 ↔ Tuwunel 1.8.1 works in an otherwise-identical
   topology.** A freshly-provisioned Tuwunel 1.8.1 test server
   (`nether.autos`), fronted by a Cloudflare tunnel exactly like A, completed
   scripted invite/join round trips with A **in both directions** — including
   a cross-server join of a v12 (domainless-id) room — hours after A↔B broke.
   Same software, same version, same edge topology as the failing pair. This
   eliminates "Tuwunel↔Tuwunel invite-join is broken in 1.8.1" as the
   explanation.
3. **B's edge and auth layers answer correctly to everything except
   validly-signed A requests.** `matrix.tinier.town` fronts Tuwunel with
   Caddy (`via: 1.1 Caddy` — no Cloudflare/WAF in the path). From outside:
   an unauthenticated bodied `PUT /_matrix/federation/v2/invite/…` returns a
   proper Matrix `M_FORBIDDEN: Missing Authorization header` under
   `Tuwunel/1.8.1`, `Synapse/1.153.0`, and browser user-agents alike (no UA
   discrimination); the same request with a **forged** `X-Matrix` header
   claiming `origin="nether.im"` returns a proper
   `403 M_FORBIDDEN: Failed to verify X-Matrix signatures`. So requests on
   the failing endpoints demonstrably REACH B's Tuwunel, which resolves A's
   keys and runs signature verification correctly — only requests that PASS
   verification get the empty-`200` treatment. A middleware cannot
   discriminate valid from invalid signatures, which weakens the
   pure-middleware theory: the empty `200` is produced by (or triggered
   inside) Tuwunel after verification — e.g. a server ACL / ignore-list
   path — or by per-pair state.
4. **It is every authenticated endpoint, reads included.** A same-day
   capture of A querying B's public rooms directory over federation fails
   identically: `Server returned bad 200 response: Deserialization(Json(
   Error("missing field `chunk`", line: 1, column: 2)))` — the same
   empty-object `200`, on a read-only endpoint unrelated to invite-join.

We're pursuing the B-side half with B's operator (config/ACL check + B-side
logs). Filing anyway because two requester-side improvements would have saved
hours, plus one behavioral question:

**Asks.**
1. **Name the endpoint in the error.** `Server returned bad 200 response:
   Deserialization(…)` doesn't say whether `make_join`, `send_join`, or
   `v2/invite` failed — with a fleet of identical errors we couldn't tell
   which exchange was breaking without packet-level work.
2. **`createRoom` should surface a failed federated invite, not WARN and
   continue.** When the `v2/invite` send fails, the room is created and the
   response is a clean 200 — the invitee's membership event simply never
   exists. From the client's perspective the invite silently vanished
   (`WARN tuwunel_api::client::room::create: Failed to send invite`). At
   minimum the createRoom response could carry a warning; ideally a
   per-invite failure surface.
3. **Does `forbidden_remote_server_names` (or any other server ACL /
   ignore-list) answer a blocked origin's authenticated requests with
   `200 {}`?** If so, an explicit `M_FORBIDDEN` would make this class
   diagnosable from the remote side — an empty 200 is not.
4. What `RUST_LOG`/tracing filter on the requesting side logs the raw
   response body for federation exchanges, for the next time someone needs
   to see the `{}` with their own eyes?

Happy to provide precise timestamps/room ids privately and to coordinate with
B's operator (who is reachable to us). The visible user impact is that DMs
from B users to A's appservice bots can never be established.

---

_Internal notes (not part of the upstream body): first failure window
coincided with nothing we changed — A's restarts were 03:58 and 04:34 UTC,
joins still succeeded at 04:19/04:30 after the first one. B's operator
(@dau:tinier.town) should be asked whether anything on their side changed
~04:35. Full evidence trail: prod journal 2026-07-12 02:25–14:46, and
voice-bridge issues #96/#97/#103._

_Internal notes 2 (control-experiment session, same day evening): **do not
file upstream until B's operator answers two things** — (a) does
`tinier.town`'s `tuwunel.toml` carry `forbidden_remote_server_names` (or any
ACL/ignore list) matching `nether.im`, and did anything change there (config
edit, Caddy config, server update/restart) around 04:35 UTC; (b) what does
B's own Tuwunel journal log for `nether.im` while A retries a join — i.e.
does B knowingly serve the `200 {}` (blocklist path → B-side config, file
only the "empty 200 is undiagnosable" polish issue) or does it error
internally (→ file the full report with both-side logs). The dau-side
`help`-silence report is fully explained by the outage: his events never
reached A at all (A-side journal has no trace of his join/messages in the
public fallback room). Control-experiment details live in the section above;
fixture provisioning in `docs/e2e-homeserver.md` (#109)._
