# ADR 0003 — A paired person in the call is still a Matrix listener

**Status:** Accepted (2026-10-07; operator chose the rule change over a per-pairing setting). **Deciders:** operator + Claude. **Amends:** ADR 0001 §2 (the lifecycle half of
the presence rule).

## Context

ADR 0001 §2: while a pairing's two halves are both present (Discord account `D` in the voice channel, Matrix user `M`
in the room call), the bridge creates no ghost for `D`, no puppet for `M`, forwards neither one's audio — and
**counts neither as a human for lifecycle**.

The last clause turned out wrong in practice. Live test 2026-10-06 (Light Voice): `M` joined the call from a Matrix
client; `D` was a companion client that speaks for `M` on Discord. Everything ADR 0001 promises worked — `M`'s puppet
was released, `D` got no ghost, nobody heard `M` twice. But a minute later `M` stopped hearing the Discord side:

1. **The capture anchor lives only while a Matrix participant has a puppet.** `M`'s puppet was released, so the
   anchor became capture-only, waited out its idle grace and left Discord voice ("no Matrix user present").
2. **The Matrix side's "call active" counts only non-bridge-owned members** (`LinkBoard::matrix_humans`), so it saw 0.

With nobody capturing, the Discord speakers' ghosts went silent, although `M` was in the call listening.

The clause was meant to stop the bridge from running *just* for a person who already hears Discord through their own
Discord client. But it does not achieve that consistently: as soon as any other Matrix participant is in the call, the
ghosts play, and `M`'s Matrix client plays them too. All it really does is switch Discord audio off when `M` happens
to be the only Matrix participant — the one case where `M` most needs it if their Discord half does not play audio to
them (a companion client that only speaks, a muted desk client, a headless helper).

## Decision

Split the two meanings of "human" that ADR 0001 merged:

- **Represented** (who gets a puppet or a ghost, whose audio is forwarded) — unchanged: a present pairing's halves are
  bridge-owned; no puppet for `M`, no ghost for `D`, neither's audio forwarded.
- **Listening** (who the bridge carries the *other* side's audio to) — new rule: **the Matrix half of a pairing is a
  Matrix listener like any other call member.** Its Discord half is not a Discord listener: the person hears Matrix
  through their Matrix client already, and Matrix speakers do not need puppets *for them*.

Concretely:

1. `LinkBoard` gets `matrix_listeners()` = call members present that are not service accounts and not config-excluded
   (paired members **included**). `matrix_humans()` keeps its meaning (represented members) for puppet decisions.
2. **Call active** (Matrix side) follows `matrix_listeners() > 0` instead of `matrix_humans() > 0`.
3. **The capture anchor** stays (or is acquired) while the board has a Matrix listener and the channel has a Discord
   speaker to capture — not only while some participant holds a puppet. When a paired person is the only Matrix
   listener, the anchor is capture-only (standby nickname), as it already is between puppets.
4. Echo safety is untouched: `D` stays excluded from capture (its audio is `M`'s voice), `M` from puppet playback.

Lifecycle consequence: a paired person alone in the call keeps the bridge open, exactly like any other lone Matrix
participant does today. "Close when the last human leaves both sides" now reads "the last listener".

## Alternatives considered

- **A per-pairing listening side** (`pair listen matrix|discord`, default = today's behaviour). Keeps the old
  behaviour available, but adds a setting most people would not know they need, and the "discord" value still only
  matters in the lone-Matrix-participant case described above. Rejected unless a real companion setup needs it; it can
  be added later without changing this rule's default.
- **Count the Discord half too** (symmetric). Would assign puppets for Matrix speakers solely for `D`, whose person
  already hears them on Matrix — wasted bots and the double-hearing ADR 0001 wanted to avoid. Rejected.
- **Keep ADR 0001 and require another Matrix participant.** Not a fix.

## Consequences

**Easier:** a paired person hears the Discord side whenever they are in the call, whoever else is there; the rule is
easier to state (represented ≠ listening).

**Harder / costs:** the anchor's lifetime no longer follows puppet assignments alone — the Discord pool needs the
board's listener count (it already holds the board for the ghost gate). New transitions to test: paired person alone on
Matrix with Discord speakers present; their Discord half arriving/leaving while they stay; last listener leaving.

**Tests:** `links.rs` unit tests for `matrix_listeners`; pool tests (anchor kept / acquired for a paired listener,
released when the last listener leaves); e2e `linked_identity_exclusion` extended: paired person alone on Matrix + a
Discord speaker → the speaker's ghost audio reaches the call.

**Docs:** ADR 0001 gets a pointer to this ADR; the self-serve pairing section says the bridge keeps carrying the Discord
side to you while you are paired.

**Implementation (0.4.2):** `LinkBoard::matrix_listeners` / `matrix_listener_only`; the Matrix side drives call-active
from listeners and calls `PlaybackRouter::listen` / `unlisten` on join, leave and pairing transitions (listen *before*
the puppet's release, unlisten *after* a re-assign, so the anchor never lapses); the Discord pool keeps a per-channel
listener set, acquires a capture-only anchor for a lone listener (`ensure_capture_anchor`, standby nickname) and keeps
it through every release path while a listener is present.

**Future (operator, 2026-10-07):** a companion client that speaks for its person may later also *capture* the Discord
side for them, so no bridge bot is needed in the channel while they are the only Matrix participant. That needs a new,
explicit contract between the bridge and such clients — a separate ADR.

**Follow-up (ADR 0004, 2026-10-12):** the "future" case is answered by the companion interface's `listen:self` lease.
