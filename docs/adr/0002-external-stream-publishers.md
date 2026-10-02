# ADR 0002 — External stream publishers: per-stream identities for media that does not come from the bridge

**Status:** Proposed (2026-10-02). **Deciders:** operator + Claude. **Supersedes:** none.
**Amends:** ADR 0001 §7 item 2 ("presenter slots": publish under the streamer's *ghost*) — that
design is rejected below and replaced by this one.

## Context

People want to show a video stream in a bridged call that the bridge itself does not produce: a game
captured with OBS, a camera rig, a phone, or any other external source. The bridge is
audio-only (#8 is the long-term video relay) and must stay that way for this purpose: it should not
receive, transcode or host anyone's video.

What an external publisher needs, in MatrixRTC terms: a **call participant of its own** — a Matrix
account with a call membership, a LiveKit connection on the right SFU, and (in encrypted rooms) its
own media key exchanged with everyone in the call. Element Call does all of that for any logged-in
account; what is missing is *an account that represents the stream*, created per stream, labelled
honestly, and cleaned up afterwards.

ADR 0001 §7 planned "presenter slots": hand an external tool a **ghost's** membership, SFU token and
media key so a stream appears on that person's ghost tile. Working through it (2026-10-02) showed:

- a ghost's media key also authenticates the ghost's **voice** — whoever holds it can speak as that
  person;
- a paired person whose Matrix half is in the call has **no ghost** (ADR 0001 §2), so there is
  nothing to publish under exactly when the streamer is also watching;
- the publisher would have to hand-roll MatrixRTC (membership, multi-SFU election, re-keying) instead
  of running Element Call — the interop surface that broke Commet twice (#135, #139);
- it ties the bridge to whatever produces the stream.

Constraints:

1. **The bridge never handles Discord user accounts or Discord media** (ADR 0001 constraint 1), and it
   does not verify, inspect or depend on where an external stream comes from.
2. **Generic.** The feature is "let an authorised external publisher appear in a bridged call as a
   labelled stream". Any tool that can run a MatrixRTC client may use it.
3. **Authorisation is Matrix-side.** The room's bridge authority (ADR 0001 §3: the tenant who linked
   the room, or the operator for static bridges) decides who may publish; nothing consults Discord.
4. **Honest attribution.** A stream tile must read as an external stream, never as a person's own
   participation.
5. **Echo/loop safety** (CLAUDE.md constraint 5). A stream identity is bridge-owned: never puppeted
   on Discord, its audio never forwarded to Discord.
6. **Reference-implementation interop.** The publisher should be able to run stock (self-hosted,
   standalone) Element Call — no hand-rolled MatrixRTC required.

## Decision

### 1. Publisher keys (issued by the room authority)

The room authority creates keys in its management DM: `publisher key create [label]` →
`nvbpk_<random>` (shown once; stored hashed), `publisher key list`, `publisher key revoke <id>`.
A key is bound to **one bridged room**. The operator can create keys for static bridges by config.
The room's disclosure text gains "authorised external streams may appear in this call".

### 2. Stream slots (per-stream identities)

A key holder opens a **slot** for one stream: `POST /_nvb/v1/streams` with the key as bearer token
and a requested **label** (e.g. `Alex — gameplay`). The bridge:

1. mints a fresh user in its appservice namespace, `@<stem>_stream_<random>` (never collides with
   ghosts' `@<stem>_<digits>`), display name `<label> 📺`, optional avatar from the request (an mxc
   URI the bridge copies, or none);
2. joins it to the key's room (appservice-asserted join, PL 0) and marks it **bridge-owned**: no
   puppet, not a human, audio never forwarded to Discord;
3. logs in as it with `m.login.application_service` on a fresh device and returns
   `{ user_id, access_token, device_id, homeserver_url, room_id, expires_at }`.

The publisher runs Element Call (or anything MatrixRTC-compatible) with that login and joins the
room's call. Element Call does membership, SFU selection and key exchange itself; the bridge's ghosts
deliver their keys to the new device like to any participant.

`DELETE /_nvb/v1/streams/<id>` (or expiry) closes the slot: log out the device, leave the room,
deactivate the stream user. Slots expire after `max_slot_hours` (default 6) unless renewed, and are
reaped if their user holds no call membership for `idle_minutes` (default 10).

### 3. Containment

The access token is an ordinary Matrix login for a throwaway user; the homeserver has no per-token
scopes, so the bridge enforces them after the fact through its appservice event stream:

- a stream user's `m.room.message`/state events (other than its own call membership) are redacted
  and the slot is revoked;
- a join to any other room is undone and the slot is revoked;
- display-name/avatar changes are reverted to the label;
- per-room cap on concurrent slots (default 4), per-key rate limit on slot creation;
- an audit line per slot open/close/revoke (key id, room, label, user id) in the existing audit trail.

### 4. Where it is reachable

The `/_nvb/v1` API is served by the bridge on its own listener and exposed through the homeserver's
reverse proxy only if the operator enables it (`[publishers] enabled = true`); off by default.

## Alternatives considered

- **Presenter slots under the ghost (ADR 0001 §7, option "A").** Rejected: voice-key exposure, no
  ghost for paired streamers, hand-rolled MatrixRTC, Discord-specific.
- **Bring-your-own Matrix account only.** Works today with no bridge changes (an external publisher ran this
  way in testing) and stays the fallback for unbridged rooms, but one account = one concurrent
  stream with one label, and nothing ties a tile to who is streaming. Not enough for several streams
  at once.
- **Transport over the management bot DM instead of HTTP.** No new listener, but every publisher
  would need an E2EE Matrix client just to ask for a slot, and an access token would travel in chat
  history. HTTP with a revocable bearer key is simpler for any tool. The DM keeps key *issuance*.
- **The bridge ingests and publishes the stream itself (WHIP / RTMP ingest).** The only way plain
  OBS (which speaks WHIP, not MatrixRTC) can reach an **encrypted** call — an SFU-side ingress cannot
  apply Element Call's end-to-end media encryption. Needs video in the bridge (#8). Deferred: a
  later *ingest* kind of slot on the same keys, rate limits and identities.

## Consequences

**Easier:** any number of concurrent streams, each on its own honestly labelled tile; every kind of
publisher uses the same documented API; publishing tools stay separate projects; encrypted rooms work
because the publisher is a real Element Call participant.

**Harder / costs:** a new authenticated HTTP surface (off by default, needs the reverse-proxy route,
key hashing, rate limits, tests); containment by event-stream policing rather than token scopes; one
device per slot (cleaned up on close; device hygiene alarms must not treat stream devices as leaks);
a privacy-policy paragraph (labels and stream-user ids are stored in the audit trail).

**Does not solve:** plain OBS into encrypted rooms (needs the ingest kind, #8); bridging stream
video to Discord (the bridge stays audio-only; stream audio is deliberately not puppeted); proving
what a publisher shows — authorisation is "the room authority trusts this key", exactly as with any
participant they let in.

**Platform terms:** the bridge issues Matrix credentials and nothing else; it never touches Discord
sessions or Discord media. Whatever a publisher streams, and from where, is that publisher's
responsibility, as with any participant a room lets in.

**Follow-ups:** implement behind `[publishers]`; mgmt commands; containment policing; scenario tests
(slot open → EC joins → ghosts key it → close → device gone; misbehaving token revoked); ingest
slots with #8.
