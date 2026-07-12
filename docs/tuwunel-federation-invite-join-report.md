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

**Questions.**
1. Which response struct produces `missing field \`event\`` at line 1 col 1–2
   here — `make_join`, `send_join` (v1/v2), or the `v2/invite` response? The
   error string doesn't name the endpoint; that would help a lot.
2. Is there any code path where Tuwunel commits a `200` for these endpoints
   but writes an empty/partial JSON body (error after status committed,
   response body dropped by a streaming layer, etc.)?
3. Known 1.8.1 regression in this area? (Both ends are 1.8.1.)
4. What `RUST_LOG`/tracing filter on the *requesting* side would log the raw
   response body so we can capture exactly what B returns?

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
