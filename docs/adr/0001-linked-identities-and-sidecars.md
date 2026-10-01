# ADR 0001 — Linked identities, presence-based exclusion, and the sidecar boundary

**Status:** Accepted (2026-10-01). **Deciders:** operator + Claude. **Supersedes:** none.

## Context

The bridge represents every Matrix call participant on Discord as a puppet bot and every Discord
voice member on Matrix as a ghost. That is right for people who exist on only one side. It is wrong
for a person who is present on **both** sides at once:

- a user who is in the Discord voice channel with their own account (from a desk, a phone, or a
  thin client they run for exactly this purpose) *and* in the Matrix call with their own account
  gets a ghost of themselves on Matrix and a puppet of themselves on Discord, and hears themselves
  twice;
- a **sidecar** — a user-run helper that holds a Discord user session and does what bots cannot
  (watch and relay a Go Live stream, carry a Matrix user's voice into Discord *as their own
  Discord account*) — has exactly this shape, plus one extra account of its own in the Matrix call.

Live test 2026-10-01 (Light Gaming): a sidecar's Discord account was ghosted, its Matrix account
was puppeted, and before #142 the puppet replayed the share audio into Discord. The bridge needs a
first-class notion of "these two accounts are one person".

Constraints that shape the decision:

1. **The hosted bridge never holds Discord user tokens.** Running user accounts from the bridge's
   host correlates them with the bot pool; a ban there takes the service down for everyone.
   Anything that needs a user account runs on the user's own machine (the sidecar), under the
   user's own ToS risk.
2. **Only the bridge is an appservice.** Ghosts and puppets are created by the bridge alone. A
   sidecar must never need appservice powers; it is an ordinary Matrix account plus Discord
   sessions.
3. **No stale state.** A persistent per-account flag (e.g. "opted out") goes wrong the moment the
   account is used normally again. Whatever decides "step aside" must be re-evaluated from live
   facts.
4. **Self-service.** A tenant of the public instance must be able to do all of this without the
   instance operator; the operator keeps an override.
5. **Sidecar-agnostic.** The bridge must not assume how a sidecar is built (today: a Chrome puppet
   driven over DevTools; later possibly a Vencord/Vesktop plugin + a Matrix companion). The
   contract between the two projects must stay small.
6. **Discord-side actions by user accounts are a fingerprint risk.** One ephemeral slash command
   per account is acceptable; messages, repeated logins or polling are not.

## Decision

### 1. Pairing = a proven link between one Matrix account and one Discord account

A *pairing* records that `@alice:hs` and Discord user `1234…` are the same person. It is created by
the person themself, proving control of both sides, mirroring the existing link/approve flow:

1. `@alice:hs` DMs the management bot: `pair` → the bot replies with a one-time code
   (`NVB-PAIR-XXXXXX`, single-use, expires in 10 minutes, rate-limited per account).
2. Discord user `1234…` runs `/bridge pair code:NVB-PAIR-XXXXXX` in any guild the instance's bots
   serve. No guild permission is needed — it only affects the person themself. The bridge checks
   the invoking user id server-side and stores the pairing.
3. Both sides get a confirmation. `unpair` (DM) or `/bridge unpair` deletes it.

Cardinality: a Matrix account may pair **several** Discord accounts (primary, burner, designated
relay alt). A Discord account pairs with **at most one** Matrix account. Pairings are per bridge
*instance* and apply in every room that instance bridges — they describe a person, not a room.

Optional **delegate**: a pairing may name a sidecar Matrix account that carries this person
(`pair delegate @host-sidecar:hs`). The delegate receives operational notices on the person's behalf
and, in a later ADR, presenter slots. It grants nothing else.

### 2. Exclusion is a function of presence, never a stored flag

In a bridged (channel, room) pair, for each pairing `(M, D)`:

> if `D` is in the voice channel **and** `M` is in the room call, the bridge creates **no ghost for
> `D`**, **no puppet for `M`**, forwards neither one's audio, and counts **neither** as a human for
> lifecycle.

Otherwise both are ordinary participants. The rule is re-evaluated on every presence change on
either side. Consequences by construction:

- `M` alone in the call → puppet as today. `D` alone in the channel → ghost as today.
- A sidecar carrying `M` dies → `D` leaves the channel within seconds → `M` gets a puppet again,
  automatically. Nothing to clean up.
- A person can never be a hidden listener: exclusion requires them to be visible on *both* sides as
  themselves.
- Giving an account away = `unpair`; forgetting to is harmless unless both halves show up together.

**Transitions are events.** Today the puppet/ghost lifecycle reacts to joins and leaves. Exclusion
starting (the second half arrives) must *retire* an existing ghost/puppet; exclusion ending must
*create* one. The implementation adds "exclusion changed" alongside join/leave in both the Discord
presence set (`puppet.rs`) and the Matrix membership handling (`matrix-rtc`).

### 3. Service accounts (the sidecar's own Matrix account)

A sidecar's own Matrix account sits in the call (it must, to receive media and keys) but represents
nobody on Discord. It is declared a **service account per room** by the room's bridge authority
(the tenant who linked the room, or the operator by config for static bridges):
`relay allow @host-sidecar:hs` in the authority's management DM, `relay deny` to revoke. A service
account gets no puppet and does not count as a human. Because this hides a Matrix participant from
Discord users, it is **authority-gated**, and the room's disclosure text mentions that approved
relays may be present.

### 4. Operator override and the no-management-bot path

Per-bridge config lists `excluded_discord_user_ids` and `excluded_matrix_user_ids` (#141) have the
same effect unconditionally. They are the admin override (an account that will not unpair) and the
only mechanism on instances that run without a management bot.

### 5. Notices and kill switches

- When a person's exclusion ends because their Discord half vanished while they are still in the
  call, the management bot DMs them (and their delegate): "your Discord session dropped; you are
  bridged through a bot until it returns". Rate-limited (at most one per person per 10 minutes).
- Kill switches, in order of reach: the person (`unpair`), the room authority (`relay deny`),
  room moderators (kick either account as usual), the operator (config lists / instance ban).

### 6. Data

The bridge stores pairings `(matrix_user_id, discord_user_id, delegate?, created_at)` in its data
directory, and per-room service-account approvals with the tenant data. Nothing else — no tokens,
no sessions. `unpair` deletes the pairing. The privacy policy gains one paragraph.

### 7. The contract between the bridge and any sidecar

The bridge offers exactly two things to a sidecar, both over Matrix (management DM / slash
command), and never touches media:

1. **identity**: pairing + service-account approval (this ADR);
2. **presenter slots** (ADR 0002, later): a handout of a ghost's call membership, SFU token and
   media key so a sidecar can publish a Discord streamer's Go Live *under that streamer's ghost*.

Everything else — holding Discord sessions, joining channels, watching streams, carrying a paired
person's Matrix voice into Discord as their own account — is the sidecar's business and may be
implemented any way (browser puppet, client plugin, headless library).

## Alternatives considered

- **Operator config lists only (#141 as the whole feature).** Simple, but not self-service: every
  tenant who runs a sidecar would need the instance operator. Kept as the override, not the feature.
- **Extend `/bridge optout` to "excluded".** Smallest change, but it is a stored per-account flag
  and breaks constraint 3: switch the relay alt to another account, or stop relaying, and the stale
  opt-out silently unbridges a real user. Rejected.
- **Relay-declared leases with heartbeats.** The sidecar announces "relaying in room X with alt Y"
  and renews it. Works, but it is redundant with presence (the alt being in the channel *is* the
  lease), adds a protocol, and still needs pairing for anti-griefing. The lease concept survives
  only where it adds information: presenter slots (ADR 0002).
- **Hosted double-puppeting (the bridge runs user accounts, mautrix-style).** Best UX, but violates
  constraint 1: puts the bot pool at risk, and voice/video from user accounts is far more
  detectable than mautrix's text puppeting. Rejected for the hosted instance; a self-hosted
  instance owner may still choose to run a sidecar on the same box.
- **Merging the sidecar into the bridge.** Would couple a ToS-clean appservice to ToS-violating
  user-account automation in one codebase and one deployment story. The two-item contract above
  keeps them separate at no functional cost.

## Consequences

**Easier:** a person can be themselves on both sides with no duplication; a sidecar becomes an
ordinary user that happens to run on a server; failover to a bot is automatic and visible; the
later presenter-slot work has its trust anchor (pairing + service approval); a Vencord-based
sidecar is a sidecar-only change.

**Harder / costs:** exclusion transitions touch both crates' lifecycle code (the riskiest part —
needs scenario tests: late sidecar, dropped sidecar, both halves leaving in each order, two paired
accounts of one person present at once); one more identity mapping to store, disclose and delete;
two new DM commands + one slash command; the slash command is the single Discord-side action a
user account performs.

**Does not solve (by design):** two sidecars relaying the same stream (relay #5, later via
presenter slots); the sidecar's own tile in the call; streams appearing under the relay's name
instead of the streamer's (ADR 0002).

**Follow-ups:** ADR 0002 presenter slots; sidecar "voice as self" mode (needs nothing new from the
bridge once pairing exists); privacy-policy paragraph; docs/self-serve-bridging section; a
Vencord/Vesktop sidecar variant as a design issue in the sidecar repo.
