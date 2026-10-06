# ADR 0001 — Linked identities, presence-based exclusion, and companion clients

**Status:** Accepted (2026-10-01; terminology revised 2026-10-02 — decision unchanged).
**Deciders:** operator + Claude. **Supersedes:** none. **Amended by:** ADR 0002 (§7 item 2), ADR 0003 (§2 lifecycle).

## Context

The bridge represents every Matrix call participant on Discord as a puppet bot and every Discord
voice member on Matrix as a ghost. That is right for people who exist on only one side. It is wrong
for a person who is present on **both** sides at once:

- a user who is in the Discord voice channel with their own account (from a desk, a phone, or a
  thin client) *and* in the Matrix call with their own account gets a ghost of themselves on Matrix
  and a puppet of themselves on Discord, and hears themselves twice;
- a **companion client** — any user-run program signed in as a person on Discord and/or Matrix
  (a second device, a thin client, a desktop helper) — has exactly this shape, and may bring one
  extra Matrix account of its own into the call.

Live test 2026-10-01: a companion client's Discord account was ghosted and its Matrix account was
puppeted, so its audio was replayed back into Discord (fixed separately in #142). The bridge needs a
first-class notion of "these two accounts are one person".

Constraints that shape the decision:

1. **The hosted bridge never holds Discord user tokens.** Running user accounts from the bridge's
   host correlates them with the bot pool; a ban there takes the service down for everyone.
   Anything signed in as a user runs on that user's own device, under their own responsibility.
2. **Only the bridge is an appservice.** Ghosts and puppets are created by the bridge alone. A
   companion client never needs appservice powers; it is an ordinary client of each platform.
3. **No stale state.** A persistent per-account flag (e.g. "opted out") goes wrong the moment the
   account is used normally again. Whatever decides "step aside" must be re-evaluated from live
   facts.
4. **Self-service.** A tenant of the public instance must be able to do all of this without the
   instance operator; the operator keeps an override.
5. **Client-agnostic.** The bridge must not assume how a companion client is built. The contract
   between the bridge and any client must stay small.
6. **Minimal Discord-side actions.** One ephemeral slash command per account is acceptable;
   anything more is not needed.

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

Cardinality: a Matrix account may pair **several** Discord accounts (e.g. a desktop and a secondary
account). A Discord account pairs with **at most one** Matrix account. Pairings are per bridge
*instance* and apply in every room that instance bridges — they describe a person, not a room.

Optional **delegate**: a pairing may name another Matrix account that acts for this person
(`pair delegate @helper:hs`). The delegate receives operational notices on the person's behalf. It
grants nothing else.

### 2. Exclusion is a function of presence, never a stored flag

In a bridged (channel, room) pair, for each pairing `(M, D)`:

> if `D` is in the voice channel **and** `M` is in the room call, the bridge creates **no ghost for
> `D`**, **no puppet for `M`**, forwards neither one's audio, and counts **neither** as a human for
> lifecycle.

Otherwise both are ordinary participants. The rule is re-evaluated on every presence change on
either side. Consequences by construction:

- `M` alone in the call → puppet as today. `D` alone in the channel → ghost as today.
- A client carrying `D` drops → `D` leaves the channel within seconds → `M` gets a puppet again,
  automatically. Nothing to clean up.
- A person can never be a hidden listener: exclusion requires them to be visible on *both* sides as
  themselves.
- Giving an account away = `unpair`; forgetting to is harmless unless both halves show up together.

**Transitions are events.** Today the puppet/ghost lifecycle reacts to joins and leaves. Exclusion
starting (the second half arrives) must *retire* an existing ghost/puppet; exclusion ending must
*create* one. The implementation adds "exclusion changed" alongside join/leave in both the Discord
presence set (`puppet.rs`) and the Matrix membership handling (`matrix-rtc`).

### 3. Service accounts (a client's own Matrix account)

A companion client may sit in the call with a Matrix account of its own that represents nobody on
Discord. It is declared a **service account per room** by the room's bridge authority (the tenant
who linked the room, or the operator by config for static bridges): `relay allow @helper:hs` in the
authority's management DM, `relay deny` to revoke. A service account gets no puppet and does not
count as a human. Because this hides a Matrix participant from Discord users, it is
**authority-gated**, and the room's disclosure text mentions that approved service accounts may be
present.

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

### 7. The contract between the bridge and any client

The bridge offers clients only Matrix-side things (management DM / slash command) and never touches
their media:

1. **identity**: pairing + service-account approval (this ADR);
2. **external stream publishing**: see ADR 0002, which replaces the "presenter slot" idea first
   sketched here.

Everything a client does on either platform is the client's own business, under its user's
responsibility.

## Alternatives considered

- **Operator config lists only (#141 as the whole feature).** Simple, but not self-service: every
  tenant with a companion client would need the instance operator. Kept as the override, not the
  feature.
- **Extend `/bridge optout` to "excluded".** Smallest change, but it is a stored per-account flag
  and breaks constraint 3: switch to another account, or stop using the client, and the stale
  opt-out silently unbridges a real user. Rejected.
- **Client-declared leases with heartbeats.** The client announces "present in room X as account Y"
  and renews it. Works, but it is redundant with presence (the account being in the channel *is* the
  lease), adds a protocol, and still needs pairing for anti-griefing. Rejected.
- **Hosted double-puppeting (the bridge runs user accounts, mautrix-style).** Best UX, but violates
  constraint 1: puts the bot pool at risk. Rejected for the hosted instance.

## Consequences

**Easier:** a person can be themselves on both sides with no duplication; failover to a bot is
automatic and visible; any client design works without bridge changes.

**Harder / costs:** exclusion transitions touch both crates' lifecycle code (the riskiest part —
needs scenario tests: late client, dropped client, both halves leaving in each order, two paired
accounts of one person present at once); one more identity mapping to store, disclose and delete;
two new DM commands + one slash command.

**Does not solve (by design):** a service account's own tile in the call; per-stream attribution
(ADR 0002).

**Follow-ups:** ADR 0002; privacy-policy paragraph; docs/self-serve-bridging section.

**Amended by ADR 0003** (2026-10-07): the Matrix half of a present pairing still counts as a *listener* — the
bridge keeps carrying the Discord side to them — though not as a represented participant.
