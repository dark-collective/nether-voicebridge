# ADR 0005 — Your own call data for your own tools

**Status:** Proposed (2026-10-29; implemented on a branch, not yet released). **Deciders:** operator + Claude.
**Amends:** ADR 0004 — constraint 1, §5.3, §10 and the alternative "companion generates the media key" (§7 below).

## Context

In an encrypted MatrixRTC call a client sends its media key only to the call's members. That is the right default, but
it locks out a whole class of tools a person runs **for themselves, on their own account**: a recorder of their own
voice, a broadcast-software source showing their own camera, a second client of theirs that only listens. Such a tool is
a device of the person's account, it is in the same room, and the media it wants is the person's own — yet it cannot
decrypt it without becoming a call member, i.e. without posting an `m.call.member` and appearing in everyone's roster
as an extra participant.

The bridge is a call member in every bridged call. It already receives each member's media key, because it needs it to
carry their voice to Discord. It also already knows things the person's tool would otherwise have to re-derive: which
SFU(s) the call is on (MSC4195 multi-SFU, CLAUDE.md #8), the person's LiveKit identities in both forms, and whether the
person's paired Discord account (ADR 0001) is in the bridged channel.

So the bridge can offer one narrow thing: **a member's own call data, to a device of that member's own account.** This
ADR decides how that device proves who it is, what exactly it receives, how it finds the bridge, how it stays out of
the way of other features (the stale-membership reaper, paired-person puppets), and what it cannot be used for.

Constraints carried over: ADR 0004's constraint 1 (the bridge never holds a user credential and never carries a
companion's media), its §7 transport and key machinery, CLAUDE.md constraints 3 (HKDF frame keys), 5 (echo exclusion),
6 (lifecycle) and 8 (multi-SFU).

## Decision

### 1. The `self` scope and device keys

ADR 0004 §3 already has a *self* key scope ("the issuer's own pairings"). This ADR adds a **device key**: a self-scoped
companion key that also names one device of the issuer's account, `(mxid, device_id)`, and carries the single kind
`self:call`.

- A device key is minted only by enrollment (§2) — never through the management DM, and `self:call` cannot be
  requested for any other key (it is not a mintable kind).
- It is usable in every bridged room the issuer is joined to, for that issuer's data only.
- It is stored like every companion key (hashed secret, label `listen-only device <id>`), shows up in the issuer's
  `companion key list self`, and is revoked like any key. The device can also unlink itself (`DELETE
  /_nvb/v1/self/enroll`).
- Enrolling the same `(mxid, device_id)` again **replaces** the previous key once the new enrollment is confirmed
  (§2a); the old one stops working at that moment. An unconfirmed re-enrollment leaves the working key alone, so a
  replayed token cannot knock out a device.
- At most **5 active device keys per account**: confirming a sixth device evicts the account's oldest. Pending
  enrollments are capped the same way among themselves and never evict an active key.

### 2. Enrollment: OpenID once, a pending device key back

`POST /_nvb/v1/self/enroll` with `{ room_id, device_id, openid_token, matrix_server_name }`. The OpenID token is one
the device obtained from its own homeserver (`/_matrix/client/v3/user/{userId}/openid/request_token`). The bridge
checks, in this order, and stops at the first failure:

1. **Rate limit per client address**, before any other work (`[companions] enrollments_per_minute`, default 5). An
   IPv6 client is counted per /64. Behind a proxy (`client_ip_header`), a single-value header the proxy sets itself
   (`CF-Connecting-IP`) is preferred; for a list (`X-Forwarded-For`) the **last** entry — the one the operator's proxy
   appended — is used.
2. **Input shape:** a room id, a device id of `[A-Za-z0-9_.=-]{1,64}`, a bounded token, a valid server name.
3. **Capacity:** the management bot must be running (it confirms enrollments, §2a; else 503
   `approval_unavailable`), and at most 16 verifications run at once across all clients (503 `busy` beyond).
4. **Membership pre-check:** the room is bridged by this process and has a **joined member on
   `matrix_server_name`** in the bridge's own synced state. Otherwise 401 — before any network call. This also bounds
   the set of hosts the bridge will ever contact to servers already in its rooms.
5. **Server discovery per the server-server spec:** IP literal, explicit port, `/.well-known/matrix/server` delegation,
   `_matrix-fed._tcp` / `_matrix._tcp` SRV, else port 8448. TLS is validated against the (delegated) server name; no
   redirects, no proxy, bounded bodies and timeouts.
6. **Address policy:** every resolved address is checked before connecting. Loopback, private, link-local, unique
   local, carrier-grade NAT, documentation, multicast, translation prefixes and the bridge host's own addresses are
   refused (403 `address_refused`). An operator whose homeserver really is on a private network lists it in
   `[companions] trusted_private_servers`.
7. **`GET /_matrix/federation/v1/openid/userinfo`** on that server. The returned `sub` must be a user **of that same
   server** (a server vouches only for its own users) and must be **joined** to `room_id`.
8. The token is used for that one request and dropped. It never appears in logs (the request type redacts it from
   debug output; error messages never echo the request body) and is never stored.

The answer is `201 { key, key_id, user_id, device_id, events_url, status: "pending_approval", approval_expires_in }`;
the key is shown once. It opens nothing yet.

There is no standing OpenID bearer: after enrollment the device authenticates with its device key only, and the bridge
holds no credential of the user.

### 2a. Confirmation from the account itself

An OpenID token is **not audience-bound**. Element Call hands one to lk-jwt-service — under multi-SFU also to foreign
homeservers' — and widgets and integration managers receive them too. A token proves only that someone held a token of
the account, so it alone must not unlock the account's media keys (that would defeat E2EE against a foreign SFU).

So enrollment creates the device key **pending**, with a random 8-character confirmation code (stored hashed) that
expires after 10 minutes. The management bot DMs the account, in its existing DM with the bot:

> 🔑 A device asked to receive your own call data in `<room>`: listen-only device `<device>` (key `<key-id>`). If that
> was you, reply `companion key approve <code>` within 10 minutes. Otherwise ignore this (it expires), or
> `companion key revoke <key-id>`.

Only a message **from the enrolling MXID, in that DM room, with the right code** activates the key. A wrong code is
refused; five wrong codes drop the account's pending enrollments. Reading the DM and answering it needs the account's
own access token, which a token-only attacker does not have — but the person's own client has, so it can post the
reply automatically. The code is never in the HTTP answer. Expired pending keys are deleted by the hub's sweep. Any
member with a pending enrollment may send `companion key approve`, whatever their self-serve tier. Re-enrolling
replaces an earlier pending enrollment of the same device.

### 3. What the device receives: `GET /_nvb/v1/self/events`

A server-sent event stream for one room, `?room_id=…`, authenticated with the device key. A pending key is refused
(403 `pending_approval`). It opens **only while the key's owner is joined to the room and has a call membership
there** (409 `not_in_call` otherwise), connects rate-limited. One stream per device: a new connection **replaces** the
old one (which ends with `end { reason: "replaced" }`), so a client that lost its connection silently can reconnect
at once. A client that stops reading for 10 s is dropped, so a stalled client never holds up the revoke and end
checks. Consequence: the bridge learns the device's address only at enrollment and while its owner is in a call.

On every connect the bridge sends the full current state first, then changes:

- `self_call { room_id, in_call, encrypted, transports: [{ livekit_service_url, livekit_alias, identities }] }` — the
  owner's **real** devices in the call (one entry per membership: a legacy `m.call.member` with state key = MXID
  lists several devices, each with its own expiry), grouped by the SFU each publishes on as the bridge resolves it (its election,
  plus each member's preferred SFU), and every LiveKit identity each device may appear under: legacy
  `@mxid:DEVICE`, and the MSC4195 hashed forms once the device's `member.id` is known. Member ids arrive with the
  device's key events, so `self_call` is **sent again** when they first become known. The enrolled device itself is
  never listed.
- `key { room_id, device_id, identity, identities, index, key, params }` — encrypted rooms only: the owner's current
  media key per own device and every rotation. `key: null, index: null` means "none yet, wait" (bridge just restarted,
  owner alone in the call, client slow to re-key). `params` are the frame-crypto parameters the bridge itself decrypts
  with (HKDF, ratchet salt `LKFrameEncryptionKey`, window 16, key ring 256, AES-GCM — CLAUDE.md constraint 3), so the
  device never hard-codes a client's constants.
- `paired { room_id, present }` — whether the owner's paired Discord account (ADR 0001) is in the bridged channel;
  `false` without a pairing. No pairing is required for anything else.
- `revoked` when the key is revoked or replaced; `end { reason }` when the owner leaves the room (`not_a_member`), has
  had no call membership for 15 s (`not_in_call`), a newer connection of the same device took over (`replaced`), the
  bridge restarts (`bridge_restarting`) or shuts down.

**The key claim, exactly.** The bridge forwards only keys that the requester's own account sent to the bridge, each
labelled with the device identity its sender claimed. Over Olm key transport the sending account is authenticated by
the encryption; over plaintext key transport it is not. Either way the recipient of such a key is that same account, so
a key forged under someone's name could only ever mislead its own sender's device — it can never reach anyone else. The
filter is one function (`OwnState::for_owner`): keys whose sender is not the requester are dropped, whatever device
they claim.

Unencrypted rooms get `self_call` and `paired` and never a key (there is none). The bridge never sends another
member's key, a ghost's key, or anything about other members.

### 4. Discovery from bridge-owned room state

A device must find the bridge's API from the room alone. When `[companions] public_url` is set (unset by default:
nothing is advertised), each bridge keeps its room advertising
`{ "api_base": "<public_url>/_nvb/v1", "version": 1 }`:

- as the state event **`im.nether.voicebridge.api`** with state key = the bridge bot's MXID, when the bot's power
  level allows sending it;
- otherwise as the field `im.nether.voicebridge.api` of the bot's own `m.room.member` content.

Clients read the state event first and fall back to the member field. A homeserver may drop unknown keys from member
content, so the state event is the preferred form. **Room admins:** to enable it, allow the bridge bot to send
`im.nether.voicebridge.api` (in `m.room.power_levels`, set `events["im.nether.voicebridge.api"]` to the bot's level or
lower, or raise the bot's level to `state_default`). The bridge re-checks every 10 minutes and writes only when the
room's copy differs. If the member field it wrote is missing again (the homeserver stripped it), it logs that once and
waits 6 hours before rewriting its membership, instead of rewriting it every check. When `public_url` is unset, an
advertisement left by an earlier configuration is withdrawn once (empty state content, member field removed). Because
the state key is the bot's MXID, the room has one authority for it (ADR 0004).

### 5. The stale-membership reaper ignores enrolled listen-only devices

A listen-only device connects to the SFU under its owner's account and publishes nothing. If the owner's real client
crashes, the device may still be connected — and the reaper (#25), which treats any live participant of an MXID as
proof that MXID's membership is alive, would never clear the dead membership.

So the reaper's live roster **excludes enrolled listen-only identities**: for each **confirmed** device key (a pending
enrollment hides nobody), the legacy identity
`@mxid:DEVICE` and both MSC4195 hashes with `member.id = device_id` (the device names itself as the member when it asks
lk-jwt for a token, so its hashes are known in advance). Nothing else about the reaper changes: presence is still
matched on the bare MXID, and hashed identities of real devices are resolved to their MXID through the member ids
learnt from key events (every device of a legacy multi-device membership, not only the first). An owner who enrolls a
device id that is also one of their real call devices only hides their own client from the reaper. In an unencrypted
room there are no key events, so MSC4195 hashed identities of real members are still not resolved there — a hashed
member is matched only through a legacy identity, as before this ADR.

### 6. Departure grace for paired people

When the Discord half of a paired person vanishes, the Matrix half is usually leaving too, a moment later. Assigning a
puppet the instant the Discord half goes would flash a bot under the person's name on the way out. So the bridge keeps
the Matrix half listen-only for `[timing] paired_departure_grace_ms` (default 5 s), and for as long as a leave of that
member is being debounced. The member's first voiced frame still assigns the puppet at once (the arrival-grace rule of
ADR 0003). When the grace ends, a member still in the call and still unrepresented gets the puppet and the usual
"your Discord session dropped" notice.

### 7. Amendments to ADR 0004

- **Constraint 1** gains: *"The bridge may give a device of a member's own account that member's own media key and
  call state (ADR 0005). It still never holds a user credential beyond one enrollment request, and never receives or
  forwards media."*
- **§5.3** (the stream device's media key): unchanged for stream devices; add *"Keys the bridge receives as a call
  member are never handed to a companion, with one exception: a member's own keys, to that member's own enrolled
  device (ADR 0005 §3)."*
- **§10** (privacy) gains the paragraph in §8 below.
- **Alternative "companion generates the media key"**: its reason "the bridge would forward key material it did not
  make and could not vouch for" stays true for stream devices. ADR 0005 does forward keys it did not make, and says
  exactly which: only a requester's own keys, to the requester, with the authenticity statement of §3 — no vouching
  is implied.

### 8. Privacy and threat model

Privacy policy addition (`docs/privacy-policy.md`, "Companion tools and your own devices"): *a listen-only device you
enroll gives the bridge your account and the device id you chose, kept as a companion key until you unlink, revoke or
re-enroll it (an unconfirmed enrollment is deleted after 10 minutes), plus audit lines of enrollment, confirmation and
each connection; the sign-in token is checked once and never stored or logged; at enrollment and while that device's
event stream is open the bridge sees its network address; the bridge sends it only your own media key, your own call
state and whether your paired Discord account is in the channel.*

| Threat | Answer |
|---|---|
| A replayed OpenID token (lk-jwt-service, a foreign SFU's token service, a widget or integration manager saw it) | Enrollment yields only a pending key (§2a); it activates only on a reply from the account itself, in its DM with the bot, with the code sent there. A pending re-enrollment never displaces a working key. |
| Someone gets another member's key | Keys are filtered by sender = requester (§3); identity comes from the requester's own homeserver via OpenID, and `sub` must be a joined member on that server. |
| A forged key labelled with someone's device | It can only be sent by its sender's account, and is delivered only back to that account (§3). |
| A malicious server vouches for a foreign user | `sub` must be on the server that answered (§2.6). |
| Enrollment as an SSRF probe | Only servers with a joined member in a bridged room are contacted (§2.4); resolved addresses are policy-checked (§2.6); rate limit first (§2.1, IPv6 per /64), at most 16 verifications in flight (§2.3). |
| Token leakage | Used once, never stored, redacted from logs (§2.7). |
| A stolen device key | Usable only for its owner's own data, only while the owner is in a call; revocable from the DM or by the device; replaced on a confirmed re-enroll; the owner's own reconnect evicts a second holder's stream. |
| Long-lived tracking of the device | The stream opens only during a call membership (§3). |
| The device keeps a dead membership alive | Excluded from the reaper's roster (§5). |
| Attribution under MSC4195 | A hashed listen-only participant cannot be mapped back to its owner by others (it posts no membership) and publishes nothing; this is stated here openly. On legacy lk-jwt the connection is visibly the owner's. |

## Alternatives considered

- **A standing OpenID bearer on every request.** Every connect would make the bridge call out to a homeserver and
  hold a fresh user credential; enrollment once with a device-bound key is cheaper and holds nothing.
- **Let the device post its own call membership.** Then it is a call member and gets keys from every client — but it
  appears in every roster as an extra participant, is counted as a human for lifecycle, and receives *everyone's* key.
- **Mint keys through the management DM.** Works for room authorities, but it would need a human to carry a token from
  the DM into the device. Instead the device enrolls itself and the DM is used only for the confirmation (§2a), which
  the person's own client can answer automatically.
- **OpenID alone, no confirmation.** Rejected: OpenID tokens are not audience-bound (§2a), so anyone a client gave one
  to — an SFU's token service, a widget — could enroll as the user and receive their media keys.
- **Advertise the API in `.well-known`.** Ties a room-level feature to the homeserver operator; room state keeps it
  with the room's bridge. A deployment can still document its URL elsewhere.

## Consequences

- New config: `[companions] public_url`, `self_enroll` (**default off**, only with `enabled`, and needs the
  `[management]` bot — an instance that runs companions gains no new endpoint on upgrade), `enrollments_per_minute`,
  `trusted_private_servers`; `[timing] paired_departure_grace_ms` (a behavior change for existing configs: see §6).
- The bridge contacts homeservers' federation APIs (only those already in its rooms) — new outbound traffic.
- The reaper resolves MSC4195 hashed identities of real members once their key arrived (previously a hashed member
  was matched only by a legacy identity).
- Follow-ups: the room-state form needs room admins to grant the bot the event's power level; a client library is
  out of scope here.
