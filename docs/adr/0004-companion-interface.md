# ADR 0004 — The companion interface: leases for media, representation and observation

**Status:** Accepted (2026-10-12; phase 1 — keys, leases, `observe` — implemented). **Deciders:** operator + Claude. **Supersedes:** ADR 0002 (its stream slots become
one lease kind of this interface; its rejection of publishing under a ghost is revised in §9).
**Amends:** ADR 0001 §7 (the contract between the bridge and any client gains item 3), ADR 0003 (the "future" paragraph
is answered by the `listen:self` lease; its rejected per-pairing listening setting returns in a stale-proof form).
**Amended by:** [ADR 0005](0005-your-own-call-data.md) §7 — constraint 1, §5.3, §10 and the "companion generates the
media key" alternative (a member's own keys to that member's own device).

## Context

The bridge produces exactly two kinds of media: Discord voice into the Matrix call (ghosts) and Matrix voice into
Discord (puppets). People keep asking for things next to that which the bridge should not do itself:

- show a **video stream** in the bridged call that belongs to a Discord member the bridge already represents as a ghost
  (their game capture, a camera rig, a phone — produced by a tool of their own), so it reads as *that person's* stream;
- show a labelled stream that belongs to nobody in particular (an event feed, a slide deck);
- let a paired person speak on Discord through their own Discord account (ADR 0001) and, later, hear the Discord side
  there too, so the bridge needs no bot in the channel while they are the only Matrix participant
  (ADR 0003, "future");
- run a room-owner's helper that reacts to bridge events (a soundboard, a recorder with consent, a music bot that pauses
  when the call empties).

ADR 0002 answered the first two with per-stream throwaway Matrix logins and rejected publishing under a ghost. Working
through the ghost case again with the bridge's own key handling in hand (§9) shows that the objections were about a
*particular* design (handing out the ghost's voice device), not about attaching a stream to a ghost as such. Meanwhile
every "next to the bridge" feature keeps re-deriving the same four questions: who may do it, how the bridge knows it is
happening, what the bridge hands over, and when it ends. This ADR answers them once.

Constraints (unchanged from ADR 0001/0002, restated because they shape every choice below):

1. **The bridge is a Discord bot application and nothing else.** It observes what bots see (voice states, members),
   speaks through its puppet pool, and never holds, drives or depends on a Discord user session. Companions are a
   Matrix-side concern: the bridge never asks a companion where its media comes from, never verifies it, and the
   interface must work identically for a tool that captures a local game window and for anything else. The bridge
   never receives, inspects, transcodes or forwards a companion's media. *(Amended by ADR 0005 §7: a member's own
   device may get that member's own media key and call state.)*
2. **Only the bridge is an appservice.** Ghosts are the bridge's. A companion never gets appservice powers, a Matrix
   login, a ghost's device, or any key that authenticates a ghost's voice.
3. **Interop lives in one place.** MatrixRTC membership, SFU election (multi-SFU, CLAUDE.md #8), key distribution (Olm
   or plaintext, per room), device identity rules (#30) are already implemented and live-tested for ghosts. Companions
   must not re-implement them; the bridge does the Matrix half, the companion only the LiveKit half.
4. **No stale state.** Everything a companion is allowed to do is a *lease* tied to live facts the bridge observes
   itself (the represented member is in the channel, the call is active, the key is valid). A companion that vanishes
   leaves nothing behind.
5. **Authorisation is explicit and revocable at every level**: room authority (ADR 0001 §3), the represented person,
   the operator.
6. **Echo/loop safety** (CLAUDE.md constraint 5): everything a companion publishes is bridge-owned on the Matrix side
   and never reaches Discord; nothing a companion publishes can be mistaken for a human's voice.
7. **Generic, not speculative.** The interface serves the examples above and nothing more; new lease kinds are added by
   a later ADR, not by making this one abstract.

## Decision

### 1. Vocabulary

- **Companion** — any program a room's authority or a person trusts to do one of the things below. The bridge knows a
  companion only by its key.
- **Key** — a bearer credential the bridge issues; it has an **issuer**, a **scope** (one bridged room, or *self* — the
  issuer's own pairings) and a set of **kinds** it may request. Keys are shown once and stored hashed.
- **Lease** — a time-bounded, revocable grant of one kind, bound to live state. A lease is the only thing a companion
  ever holds besides its key.
- **Represented participant** — a Discord member the bridge currently represents on Matrix as a ghost (ADR 0001 §2: in
  the channel, not opted out, not excluded, not a present pairing).
- **Stream device** — a second Matrix device of a ghost, run by the bridge, on which a companion's stream is published.

### 2. Lease kinds (v1)

| Kind | What the companion may do | Scope of the key | Who else must agree |
|---|---|---|---|
| `stream:attach` | publish one screen-share stream (video, optional stream audio) on a **stream device of a represented participant's ghost** | room | the represented person (§4) |
| `stream:standalone` | publish one labelled stream on a **bridge-minted stream identity** (ADR 0002's slot, re-based on leases) | room | nobody beyond the room authority |
| `observe` | receive the room's bridge events (§6) | room | — |
| `listen:self` | declare that the key's issuer hears the Discord side through their own paired Discord account, so the bridge stops counting them as a Matrix listener while both halves are present | self | — (affects only the issuer) |

A **paired person speaking on Discord** through their own Discord account (ADR 0001 §2, ADR 0003) needs **nothing**
from this interface: their Discord account being in the channel while their Matrix account is in the call *is* the
grant, re-evaluated from presence. That stays as it is; this ADR only adds the listening half.

### 3. Keys

Issued in the management DM, mirroring `relay allow` / `publisher key` from ADR 0001/0002:

```
companion key create <room-id> <kinds…> [label]     — room authority (ADR 0001 §3) or operator
companion key create self <kinds…> [label]          — any paired person, for their own pairings
companion key list [room-id|self]
companion key revoke <key-id>
```

- Format `nvbck_<key-id>.<secret>`; the bridge stores `(key-id, sha256(secret), issuer, scope, kinds, label,
  created)` in `<data_dir>/companions.toml` (same atomic-write contract as `pairings.toml`). Revoking a key ends its
  leases immediately.
- Kinds are fixed at creation; a key for `stream:attach` cannot ask for `observe` unless it was created with both.
- Operators create keys for static bridges by config (`[[companion_key]]` with a hashed secret), as ADR 0002 planned.
- Room-scoped keys die with the room's bridge (unlink deletes them); self-scoped keys die with the last pairing.
- The room's disclosure text (link notice on Discord, `status` on Matrix) gains: *"authorised companion tools may show
  members' streams in this call and may receive this room's bridge events."*

### 4. Consent for `stream:attach` — enforced on the bridge's own observations

A stream may be attached to the ghost of Discord member **D** in room **R** only while **all** of these hold, each
checked by the bridge against what it sees itself, never against anything the companion claims:

1. the key is valid, room-scoped to R, and includes `stream:attach`;
2. D is a **represented participant** in R right now (in the channel; has a ghost — not opted out via
   `/bridge optout`, not config-excluded, not a present pairing — see §9 for the paired case);
3. the call is **active** on the Matrix side (at least one Matrix listener, ADR 0003) — there is nobody to show the
   stream to otherwise, and no reason to spend an SFU connection;
4. **the represented person agrees**, by one of:
   a. **Discord's own streaming flag.** D's voice state reports that D is streaming to the channel (`self_stream`,
      visible to every bot in the guild, already part of the voice state the bridge mirrors for mute). The common
      case: someone already showing their game or screen to the Discord channel wants the Matrix side to see it too,
      and runs a capture tool of their own (OBS, a phone or console capture app) that publishes the same view. The
      flag says D is choosing to show their screen to this channel right now, in a channel whose link notice says it
      is bridged and that companion tools may show members' streams on the Matrix side; a stream under D's name while
      the flag is set stays within that choice. The lease starts only
      while the flag is set and ends (after a short grace for reconnects, default 60 s) when it clears.
   b. **A personal grant.** D ran `/bridge companion allow <key-id>` on Discord (ephemeral reply; stored per
      `(D, key-id)`; `/bridge companion deny <key-id>` or `/bridge companion list` to manage; `/bridge optout` wipes
      all). This covers a person who streams from their own tool without using Discord's streaming at all.
   c. **The person's own pairing.** D is paired with Matrix user M (ADR 0001 §1) and the key is M's self-scoped key —
      the person authorising their own companion.

   Any one suffices; the audit line records which.
5. no other lease is attached to D's ghost (one stream per represented participant; first come, first served — a Discord
   member shows one stream at a time, and a second tile under the same person would be confusing).
6. per-room cap on concurrent attached + standalone streams (default 4) and per-key rate limit on lease creation.

Condition 4a gates only *when* a stream may appear under D's name. It is a signal the person controls with one click
and the bridge reads first-hand; the bridge does not inspect what a companion publishes (constraint 1, §10).

### 5. The stream device — what the bridge does for an attached stream

For ghost `@<prefix><D>:<server>` with voice device `D<key>-<salt>`, the bridge provisions a **stream device**
`S<key>-<salt>` (same salt mechanism as #30, so the device id is unique per crypto store; its own crypto store under
`<store>/ghosts/`, keyed by the device id like a ghost's). Then, mirroring `run_ghost` step for step:

1. **Membership.** Posts `org.matrix.msc3401.call.member` with state key `_<ghost>_S<key>-<salt>_m.call`, content
   `device_id = S…`, `focus_active = oldest_membership` (so Commet sees it, CLAUDE.md #8), `foci_preferred[0]` = the
   call's **elected** SFU (the same one the voice ghost publishes on), fixed `created_ts`, hourly refresh of `expires`.
   Marked bridge-owned: never reaped by the stale-membership reaper, swept by the orphan sweep at startup (the sweep
   learns the `S` device-id shape).
2. **Transport.** Fetches an OpenID token as the ghost (appservice masquerade, as ghosts do) and exchanges it at the
   elected SFU's lk-jwt service with the legacy body for the stream device → LiveKit identity `@<ghost>:S…`, 1 h token
   (lk-jwt-service `with_ttl(60*60)`, verified in its `handler.rs`). The bridge refreshes the token before expiry and
   re-issues it when the election moves (a `transport` event, §7); the companion reconnects.
3. **Media key.** In a room where `media_e2ee` resolves on: the bridge generates the 16-byte key, installs nothing
   itself (it never publishes on this device), and distributes it to the call's human member devices exactly as a
   ghost does (`distribute_key_loop`, Olm or plaintext per room, with the #51/#52 refresh and session-rotation rules),
   under `member.claimed_device_id = S…`. The key is stable for the lease (ghost media keys are already stable per
   identity; only Olm sessions rotate). Peers map it to `@<ghost>:S…` (matrix-js-sdk `RTCEncryptionManager`: keys are
   held per `userId:deviceId` and installed once a matching membership exists; the bridge's own `install_peer_key`
   does the same by `sender:claimed_device_id`). **The voice device's key is a different key on a different identity
   and is never sent to the companion.** In an unencrypted room there is no media key. *(ADR 0005 §7: the only keys
   ever handed on are a member's own, to that member's own enrolled device.)*
4. **Hands the companion** (§7) the SFU WebSocket URL, the JWT, the LiveKit identity, the key (base64) and its index,
   the key-provider parameters Element Call expects (HKDF over the raw key, ratchet salt `LKFrameEncryptionKey`,
   AES-GCM — CLAUDE.md constraint 3, already proven by the ghosts), and the allowed track sources.
5. **Polices what it can.** lk-jwt tokens carry `can_publish` + `can_subscribe` with no per-source restriction
   (verified; `can_publish_sources` is never set), so a token cannot be scoped to screen share. The bridge's E2EE
   subscriber already sits in the LiveKit room and sees every `TrackPublished` event; if a stream-device identity
   publishes a `microphone` or `camera` source, the lease is **revoked and audited**. This is detection, not
   prevention: the track is live before the bridge sees it, so a companion can be heard or seen under the person's
   name for the seconds until revocation lands (the subscriber reacts on the `TrackPublished` event; target < 2 s).
   It cannot do so twice with the same key without the room authority re-issuing it.
6. **Keeps it out of Discord.** The identity starts with a ghost MXID, so `is_bridge_excluded_identity` already
   excludes its tracks from the subscriber's forwarding (Discord members see the stream natively or not at all —
   never via the bridge). Its stream audio is never puppeted; it is never a human for lifecycle; it does not count as
   a Matrix listener.
7. **Tears down**: retracts the membership, drops the LiveKit token (it expires within the hour regardless), ends key
   distribution, logs the audit line. The ghost's voice device is untouched.

`stream:standalone` is the same machinery on a bridge-minted user `@<prefix>stream_<random>:<server>` with display name
`<label> 📺` and a single device — ADR 0002's slot, minus the Matrix login: the companion gets a transport and a key, not
an account, so ADR 0002 §3's event-stream policing of a throwaway login is no longer needed (it could not post
messages, join rooms or change its name because it never holds a Matrix token).

### 6. `observe` and `listen:self`

`observe` streams, per room, what the companion would otherwise have to scrape from Matrix state and guess from
Discord: `participants` (represented members: ghost MXID, Discord user id, display name, `streaming` flag, which
ghost has an attached lease), `call_active`, `listeners` (count only), `pairing_present` for the key issuer's own
pairings. It carries no audio, no message content, no member list of the Discord guild beyond the bridged channel,
and never a Discord token or session detail. It is what the bridge already discloses through ghosts and puppets,
delivered as events instead of as room state.

`listen:self` is a lease on a self-scoped key: *"while this lease lives and my Discord half is in the channel, do not
count my Matrix account as a listener."* `LinkBoard::matrix_listeners()` (ADR 0003) subtracts such members; the
capture anchor is released when nobody else listens. The moment the lease ends or the Discord half leaves, the person
is a listener again — ADR 0003's rule, with the "per-pairing setting" it rejected replaced by something that cannot go
stale. It is the person's own hearing that is affected, so only they can grant it, and only for themselves.

### 7. Transport and authentication

**HTTP + bearer key on the bridge's own listener (`/_nvb/v1`), off by default**, exposed through the operator's
reverse proxy (TLS there), as ADR 0002 chose — kept because it needs nothing from the companion but an HTTP client,
and the key is revocable without touching any Matrix account. Added: a **server-sent-events channel per lease**, which
is also the lease's liveness.

```
GET  /_nvb/v1                                → { version, kinds, limits }      (unauthenticated discovery)
POST /_nvb/v1/leases   { kind, room_id, participant?: {discord_user_id}, label?, sources? }
      → 201 { lease_id, kind, expires_at, events_url, participant: {ghost_mxid, display_name},
              transport: { sfu_url, jwt, identity, jwt_expires_at },
              media: { e2ee, key_b64?, key_index?, key_provider: {…} },
              allowed_sources: ["screen_share","screen_share_audio"] }
      → 409 { reason: "not_represented" | "not_consented" | "call_inactive" | "already_attached" | "cap" | … }
GET  /_nvb/v1/leases/{id}/events           (SSE; the companion keeps it open)
      events: transport (new sfu_url/jwt — election moved or token refreshed), key (future), renewed,
              revoked { reason }, and for observe: participants, call_active, listeners, pairing_present
POST /_nvb/v1/leases/{id}/renew             → { expires_at }
DELETE /_nvb/v1/leases/{id}
```

- The SSE channel sends a comment line every ~25 s so CDN/reverse-proxy idle timeouts (often ~100 s) never cut a
  quiet lease.
- Leases expire after `max_lease_hours` (default 6) unless renewed; the SSE connection dropping starts a grace
  (default 30 s), after which the lease ends and the membership is retracted. A bridge restart drops every lease (they
  are in-memory); companions re-request, and the startup orphan sweep retracts anything left in room state.
- Keys: constant-time hash compare; per-key and per-IP rate limits; every lease open/renew/end/revoke is one line in the
  existing `audit.jsonl` (key id, kind, room, ghost or stream identity, consent basis, reason).
- Versioning: `/_nvb/v1` is additive; new fields and event types may appear, companions ignore what they do not know;
  a breaking change is `/_nvb/v2`. `GET /_nvb/v1` lists the kinds this instance offers so a companion can fail early.

### 8. Lifecycle, failure modes

| Event | Bridge action |
|---|---|
| represented member leaves the channel / stops being represented (pairing's other half arrives, opt-out) | lease ends after `leave_grace` (the existing Discord leave-grace, so a reconnect does not flap) |
| streaming flag clears (consent 4a) | lease ends after 60 s grace |
| call goes inactive (last Matrix listener leaves) | lease ends after the ghost's own idle grace |
| election moves to another SFU | new token on the new SFU, `transport` event; membership re-posted with `created_ts` kept |
| JWT about to expire | new token, `transport` event (same SFU; the companion may hot-swap or reconnect) |
| companion publishes a disallowed source | lease revoked, `revoked{reason:"source"}`, audit |
| key revoked, room unlinked, room banned | leases revoked at once |
| SSE drops | 30 s grace, then end |
| bridge restarts | leases gone; orphan sweep clears memberships; companions retry `POST` |
| homeserver refuses membership post (PL regression) | lease fails to open with a clear reason (the same 403 the ghosts hit) |

Two memberships of one ghost user (voice + stream) are ordinary MatrixRTC: Element Call shows the stream as that
participant's presentation; clients that render per device show a second tile of the same person. The stream device
posts no membership before the lease and keeps none after it.

### 9. Re-evaluating ADR 0002's rejection of "presenter slots under the ghost"

ADR 0002 rejected attaching to ghosts on four grounds. What changed:

1. *"A ghost's media key also authenticates its voice."* True of the design ADR 0001 sketched (hand out the voice
   device's membership, token and key). Not true of a **separate device**: MatrixRTC keys are per device
   (`io.element.call.encryption_keys` carries `member.claimed_device_id`; Element Call keys the provider by
   `userId:deviceId` and needs a membership for that device; the bridge does the same). The companion receives only the
   stream device's key and token; it cannot decrypt, publish or masquerade on the voice identity. ADR 0002's own
   footnote ("a separate stream device yields a second tile anyway") is exactly what we want: the stream shows as
   *the person's presentation*, which is the attribution people asked for.
2. *"A paired person in the call has no ghost."* Still true, and still right: while D's own Matrix account M is in the
   call, D is represented by *M*, and the bridge cannot (constraint 2) and should not publish under M. A stream for a
   paired-and-present person belongs on their own Matrix account, from their own Matrix client or a companion they run with
   their own credentials — the bridge is not a party to that. `POST /leases` answers `represented_by_self` so a
   companion can act accordingly. (A "stream-only ghost" for this case is a possible later kind; not needed now.)
3. *"The publisher would hand-roll MatrixRTC."* Only if the publisher does the Matrix half. Here the **bridge** does
   membership, election, migration, token refresh and key distribution — code it already runs for every ghost — and the
   companion does exactly what a LiveKit client does: connect with a URL and a JWT, install one key, publish a track.
   The interop surface stays where it is tested.
4. *"Discord-specific."* "Attach to a represented participant" is defined on the bridge's own concept (a ghost), not on
   how the stream is produced; the same lease serves a person's own capture tool, a camera appliance, or anything else
   that can publish to LiveKit. `stream:standalone` remains for streams that represent nobody.

What ADR 0002 got right and this ADR keeps: Matrix-side authorisation by the room authority, bearer keys over HTTP,
caps and rate limits, honest labelling of standalone streams, audit lines, "off by default".

### 10. Privacy and policy

The privacy policy gains a paragraph: companion keys (issuer, scope, hashed secret, label), lease audit lines (key id,
room, represented member's Discord id or stream label, consent basis, timestamps), the personal grants from
`/bridge companion allow`, and the fact that **whether you are streaming in a bridged channel** (Discord's streaming
flag) and your ghost's presence may be delivered to the room's authorised companions. The bridge never carries a
companion's media; what a companion shows is the companion's responsibility, as with any participant a room admits.

**Unencrypted rooms:** a lease's LiveKit token always allows subscribing (lk-jwt grants it unconditionally), so in a room
without media encryption a lease-holder can receive **everyone's audio in the call** for as long as the lease lives —
the same as any participant the room admits, but a companion is not a person others can see listening. The disclosure
text must say so for unencrypted bridged rooms, and a room authority issuing a key there is granting that. In an
encrypted room a lease-holder holds no peer key and receives only undecryptable media.
The Discord-side link notice and the Matrix `status` text are extended as in §3. *(ADR 0005 §8 adds the paragraph
for enrolled listen-only devices.)*

## Alternatives considered

- **ADR 0002 as is (throwaway logins, standalone tiles only).** Gives no attribution to the person streaming, needs
  after-the-fact policing of a Matrix login, and gives a companion more (an account) than it needs. Kept only as the
  idea behind `stream:standalone`, re-based on leases.
- **Hand the companion the ghost's voice device** (ADR 0001 §7's original sketch). Rejected for the reasons ADR 0002
  gave; the stream device removes them.
- **Companion generates the media key and the bridge only distributes it.** Symmetric, but the bridge would then
  forward key material it did not make and could not vouch for; and the bridge must own rotation if it is ever added.
  Bridge-generated, bridge-distributed, handed over once. *(ADR 0005 §7 forwards keys the bridge did not make in one
  case only — a requester's own keys, to the requester — and states what is and isn't authenticated.)*
- **Give the companion a scoped LiveKit token instead of policing sources.** Not possible: tokens come from lk-jwt
  services the bridge does not control (often a foreign homeserver's), and they never set `can_publish_sources`. The
  bridge could mint its own tokens only on an SFU whose API secret it holds — the opposite of the "SFU is derived,
  never configured" rule and useless on foreign elected SFUs.
- **Transport over the management DM or Matrix to-device messages.** Rejected as in ADR 0002: every companion would
  need an (E2EE) Matrix client just to ask, and a self-scoped `listen:self` companion often has no Matrix account at
  all.
- **A Unix socket / local-only API.** Companions run on their users' machines, the bridge on a host; a local socket
  serves only co-located tools. Could be added as a second listener; not instead.
- **WebSocket instead of SSE.** Equivalent; SSE is one-directional, which is all the bridge→companion channel needs,
  and trivially consumable from any HTTP client. Commands stay as plain requests.
- **Consent by Matrix-side allow-list only (no streaming-flag check).** Leaves the represented person out of the
  decision entirely. The flag is the one consent signal the person already controls with one click and the bridge
  observes first-hand; the personal grant covers everyone else.
- **Let the bridge decide *when* a stream should be attached (allocation).** The bridge only grants; whether, when and
  with what a companion publishes is the companion's scheduling. `observe` exists so it need not scrape state.
- **A "stream-only ghost" for paired-and-present people.** Deferred (§9 item 2).

## Consequences

**Easier:** a stream in the call reads as the right person's; every companion-shaped feature (stream, helper, listener
self-service) uses one key model, one lease model, one audit trail and one listener; companions carry no Matrix
credentials and no MatrixRTC logic; ADR 0002's login policing disappears; the bridge's lifecycle gets honest about
listeners who hear Discord themselves.

**Harder / costs:** a new authenticated HTTP listener with SSE (the bridge has none today), key storage and hashing,
a second device per ghost on demand (crypto store, orphan sweep, device hygiene must treat `S…` devices as expected),
token refresh and election migration pushed to a third party, source policing via the subscriber's track events, the
`observe` fan-out, three new DM commands and two slash commands, a privacy-policy and disclosure update, and scenario
tests for every row of §8. Consent 4a gives a lease to anyone the room authority trusts with a key, for any member who
is streaming — the person's recourse is `/bridge optout` or `/bridge companion deny`, which must be documented next to
the flag.

**Does not solve (by design):** streams for paired-and-present people (§9.2); bridging any stream *to* Discord (the
bridge stays audio-only there, #8); limiting what a companion can *subscribe* to at the SFU in an **unencrypted** room
(a lease-holder is a LiveKit participant like any other there; in an encrypted room it holds no peer key and cannot
decrypt anything); proving what a companion shows.

**Follow-ups:** implement behind `[companions] enabled = false`; `companion key` and `/bridge companion` commands;
stream device in `rtc::ghost`; listener and subscriber changes; e2e scenarios (attach → tile under the ghost in an
encrypted room; flag clears → lease ends; disallowed source → revoked; election moves → transport event; restart →
clean state; `listen:self` releases the anchor and restores on lease end); privacy policy, disclosure texts, and the
self-serve guide; mark ADR 0002 superseded and add pointers in ADR 0001 §7 and ADR 0003.
