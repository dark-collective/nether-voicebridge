# Feasibility Assessment — Issue #38 "Automatic Testing"

## Context

Issue #38 (enhancement) asks for a **dedicated Discord server + bots that run
automatic regression tests** every time we make major changes: simulate many
speakers (playing known audio files), verify overflow works, ensure no dangling
ghosts, with paired **encrypted + unencrypted** Matrix voice rooms mirroring the
Discord channels. It should be flexible enough for eventual self-bots
(livestream) and other platforms (Signal/Telegram), and should help catch audio
corruption (#32).

This document is a **feasibility analysis + design** for issue #38, captured for a
later implementation session — no testing code has been built yet. Build phases
are sketched as a "shape it would take" pointer, not committed work. Two design
decisions are locked at the end (gap handling, timer strategy).

## Verdict (short version)

**Feasible, and the codebase is unusually well-positioned for it — but the issue
describes three very different tiers of testing fused into one ask, with very
different cost/payoff.** The right move is to split them:

1. **Hermetic audio-pipeline regression tests (cheap, huge payoff, no live infra).**
   The audio core is deliberately SDK-decoupled and already has a wav-in/wav-out
   harness. This is where #32-class corruption actually lives, and it can be
   caught with zero Discord/Matrix involvement.
2. **Hermetic allocation/lifecycle simulation (cheap, in-process).** Overflow,
   steal, promotion, and stale-ghost classification are already exposed as pure
   functions + `test_*` helpers — assertable without any network.
3. **Live end-to-end orchestration (expensive, real Discord + real LiveKit).**
   This is the literal "dedicated Discord server with bots" the issue describes.
   Genuinely useful but **cannot be hermetic CI** and is gated by Discord
   bot-account supply and abuse heuristics.

My recommendation: **build 1 and 2 first** (they deliver most of the regression
safety and run on every commit), then scope 3 as a separate on-demand harness.

## What already exists (reuse, don't rebuild)

- **wav harness** — `crates/audio/src/wav.rs`: `read/write_wav_mono_48k`,
  `WavMonoWriter`, plus jitter-path roundtrip + mixer tests. Pure PCM, no SDKs.
- **Audio core is decoupled by design** (CLAUDE.md "Audio core"): downmix, jitter
  buffer, `PlaybackMixer`, `rms_dbfs` are all unit-testable in isolation.
- **Debug taps already built (#32 work):** Tap A (`crates/discord/src/capture.rs`,
  per-SSRC stereo WAV) + Tap B (`WavMonoWriter`, per-ghost publish WAV), gated on
  `capture_wav`. These give input-vs-output artifacts for similarity assertions.
- **Allocation is already test-instrumented** — `crates/discord/src/puppet.rs`
  exposes `test_slot_of`, `test_anchor`, `test_is_overflow`, `test_free_count`,
  `test_is_unhealthy`, plus an existing allocation/steal/promote test suite
  (~lines 2236–2621).
- **Stale-ghost logic is already a pure function** —
  `matrix.rs::classify_stale_memberships` (decision) + `reap_stale_memberships`
  (effect) + `ghost.rs::sweep_stale_ghosts` (startup). The "no dangling ghost"
  assertion is `classify_*` over mock membership sets.
- **Synthetic speaker playback path is free** — a test bot loads a WAV into a
  `PlaybackMixer`, and the existing `PlaybackSource → RawAdapter → call.play_input`
  pipeline (`crates/discord/src/lib.rs:57+`) plays it. No new audio code needed.
- **Matrix RTC join is reusable** — the `matrix-rtc` crate already joins LiveKit
  calls; a synthetic Matrix speaker can reuse that path rather than driving a
  real Element client.
- **Rich structured logs** — every assign/steal/promote/failover/ghost-retire is
  a `tracing` event with bridge/participant/idx fields (a live harness scrapes
  these).

## The real feasibility constraints (the honest part)

- **No mock for Discord voice or MatrixRTC.** Discord is DAVE-only (constraint 1)
  and matrix-rust-sdk can't join calls (constraint 2). So *live* tests need
  **real** Discord bot applications in a real guild and a real LiveKit/EC call.
  There is no in-process fake for the transports.
- **CI today doesn't even run `cargo test`/clippy** (`.forgejo/workflows/docker.yml`
  only builds). Tier 1/2 require adding a normal test job first — trivial but
  currently absent.
- **Live E2E can't run in hermetic CI** — it needs network egress to Discord, a
  homeserver (nether.im), LiveKit, and live secrets. It's an **on-demand /
  nightly self-hosted-runner** job, not a per-PR gate.
- **Discord bot supply is the hard limiter.** CLAUDE.md explicitly forbids
  mass-creating bot applications in a loop (abuse heuristics). Each speaker bot =
  a deliberately-created app invited to the test guild. This caps "large number
  of participants" to however many bots we hand-provision (plus they double as
  the puppet pool's own bots only if not under test simultaneously).
- **Audio assertions must be tolerance-based, not bit-exact.** Opus, resampling,
  jitter, and 20ms↔10ms reframing mean output ≠ input byte-for-byte. Need a
  similarity metric (RMS-envelope correlation / cross-correlation / spectral
  distance with a threshold), which is the one genuinely new algorithm to write.
- **Self-bots / Signal / Telegram extensibility is real but forward-looking** —
  design the harness around a platform-agnostic `ParticipantDriver` +
  `Scenario` abstraction so those slot in later; do not build them now.

## Shape it would take, if/when we build it (for a later session)

Three tiers, in payoff order — sketch only, not committed work:

- **Tier 1 — hermetic audio regression (cheap, per-commit).** Add `cargo test` +
  clippy to CI (currently absent), then a golden/similarity harness extending
  `wav.rs`: known WAV → full in-process chain (downmix → jitter → mix → 10ms
  drain) → assert similarity to a committed golden via a tolerance metric. Direct,
  automatable answer to #32. The one new algorithm = a tolerance-based audio
  similarity metric (bit-exact won't work).
- **Tier 2 — hermetic allocation/lifecycle (cheap, per-commit).** Extend the
  existing `puppet.rs` allocation tests into named scenarios (overflow → steal →
  promote) via the `test_*` helpers, plus no-dangling-ghost tests over
  `classify_stale_memberships` and the open/close transitions.
- **Tier 3 — live E2E orchestrator (expensive, on-demand only).** The literal
  issue: a separate `nvb-e2e` crate driving real Discord speaker bots + synthetic
  Matrix-RTC speakers against paired enc/unenc rooms, asserting via logs +
  `m.call.member` state + Tap A/B WAV comparison. Build around a platform-agnostic
  `Scenario` + `ParticipantDriver` trait so self-bots / Signal / Telegram slot in
  later. Runs nightly on a self-hosted runner with secrets — never a hermetic
  per-PR gate. Pre-req: a dedicated test guild, hand-provisioned speaker-bot apps,
  and paired enc/unenc rooms (`m.call.member` PL 0 per CLAUDE.md).

If revisited, start with Tier 1+2 (most regression safety per unit of effort) and
scope Tier 3 separately once test bots/rooms are provisioned.

---

# Design discussion — the "algorithm" and what we'd simulate (Tier 3)

## Two distinct "algorithms"

1. **The audio-integrity oracle** — the assertion primitive that decides whether
   audio crossed *intact* (the #32 question). Tolerance-based, not bit-exact.
2. **The scenario director** — the orchestration that drives both sides through a
   scripted sequence of actions and runs assertions between them. The user's
   "mod moves a bot to the wrong channel" example lives here.

### The audio-integrity oracle
Bit-exact comparison is impossible (Opus + resample + 20↔10ms reframe + jitter).
Feed a **known, distinctive** source WAV (speech sample, or a chirp/sweep with
known spectral content), capture the far-side output, then:
- **Align** source↔captured via cross-correlation peak (absorbs latency offset).
- **Normalize** gain (absorbs level differences).
- Score with a metric robust to codec artifacts — candidates: segmental SNR,
  normalized cross-correlation coefficient, or mel/MFCC spectral distance. Pick
  one, threshold for pass/fail, **and keep the continuous score** to trend across
  nightly runs (a regression = the score drifts down even if still "passing").
- Add two targeted detectors for #32's actual symptom ("intermittent unpleasant
  audio"): a **dropout detector** (silence where the source had energy) and a
  **glitch/transient detector** (broadband energy bursts / clipping /
  discontinuities where the source was smooth). Note #32 is probabilistic — a
  short clip may not catch a once-a-minute glitch, so the corruption scenario
  wants a *long* sample (minutes) or repeated runs.
Capture points: reuse Tap A / Tap B WAVs for in-process bisection; for true
end-to-end, a synthetic **listener** on the far side records the track to WAV
(Matrix listener subscribes to the ghost's LiveKit track; Discord listener bot
records the puppet's output).

### The scenario director (orchestration model)
A scenario is an ordered list of **steps**; the runner executes them against a
set of **actors** and checks **assertions** via **observers**.

```
Scenario  = [ Step ]
Step      = Action(actor, verb, args) | Assert(predicate, within=timeout) | Wait(dur|cond)
Actors    = discord_speaker[i]   (our bot apps; verbs: join, leave, move, speak(wav), mute, unmute)
            discord_mod          (privileged bot w/ Move/Disconnect Members; verbs: force_move, disconnect, kick)
            matrix_speaker[j]    (reuse matrix-rtc join; verbs: join_call, speak(wav), mute, leave_clean, leave_unclean)
            matrix_admin         (verbs: set_power_level, set_encryption, invite, post_phantom_member)
Observers = LogScraper (structured tracing events + fields)
            RoomStatePoller (CS-API m.call.member: count active vs Empty, by sender class)
            ConnCountProbe (puppets actually in each VC + active ghosts; baseline = 0)
            WavComparator (the audio oracle above)

run(scenario):
  setup    -> assert BASELINE (channels empty, no active m.call.member, conn_count == 0)
  steps    -> fire actions; assertions POLL until pass or timeout (never instant — grace timers)
  teardown -> drive all actors out; assert RETURN-TO-BASELINE (the no-dangling-ghost / no-zombie check)
  report   -> pass/fail per step + artifacts (logs, WAVs, state snapshots)
```

Scenarios are **independent and idempotent**: each starts from baseline and must
restore it. The baseline assertion is both precondition and postcondition — it
*is* the "no dangling ghosts / connection count back to zero" check, run after
every scenario for free.

The two adversarial actors are what make the interesting edge cases reachable:
- a **Discord mod bot** (Move Members + Disconnect Members) to force-move /
  disconnect / kick puppets — the user's example and its cousins;
- a **Matrix admin** + the ability to leave *uncleanly* (join LiveKit then die
  without retracting → stale `m.call.member`) and to post a **phantom** member
  (m.call.member without ever joining LiveKit → exercises the 45s reaper).

## Behavior / edge-case catalogue (what we'd systematically simulate)

Grouped by subsystem. "D" = Discord-side director action, "M" = Matrix-side.
Each row = trigger → expected behavior → how asserted. ✗ marks ones that (per
code reading) are **likely gaps today** — tests would surface them.

**Puppet allocation (Matrix speakers → Discord puppets)**
- 1 speaker → 1 named puppet, audio crosses. (M: join+speak; assert log
  `assigned participant to puppet` + WavComparator far-side)
- N ≤ pool → N puppets, one is the capture anchor. (assert `test_anchor`-class
  log + N nicknames)
- Overflow: speakers > pool → extras mixed into anchor's overflow track, named.
  (assert `overflow to anchor mix`)
- Promotion (FCFS): a dedicated speaker leaves → oldest overflow promoted into
  the freed bot. (assert `promoted overflow speaker into freed pool bot`)
- Steal-for-activation: fill channel A, then open channel B → steal a bot from A,
  A's victim demoted to overflow. (assert `stole bot from donor`)
- Re-promotion after steal-release (regression for #24 bug): B frees → A regains.
- Guild-aware: a bot not invited to guild X is never assigned to X. (negative)
- Mute mirroring: Matrix speaker mute/unmute → puppet mutes/unmutes.
- One user, multiple devices → exactly ONE puppet (keying on bare MXID).
- Membership-device ≠ track-device → still one puppet (regression for 8bf4a90).

**Puppet voice-connection faults**
- Channel-hop join timeout → quarantine stuck bot + failover + backoff
  15→30→60→120 (regression for #23; can fault-inject a join delay).
- ✗ **Mod force-moves a puppet to wrong channel → should rejoin correct channel.**
  NOT IMPLEMENTED today (`puppet.rs:1655` ignores the bot's own
  `voice_state_update`). Test would red.
- ✗ **Mod disconnects a puppet from voice → should rejoin.** NOT IMPLEMENTED
  (`DriverDisconnect` is logged only, `lib.rs:374`). Test would red.
- Bot dies / token revoked mid-call → failover; degrade honestly if no spare.

**Ghost set (Discord speakers → Matrix ghosts)**
- Discord speaker → named+avatared ghost in EC.
- Discord speaker with NO Matrix human present → ghost still shown (presence
  phase, membership-only) and eager-connects on call_active.
- Ghost retracts (Empty event) on Discord speaker leave.
- Membership flap ~90s → debounce keeps ghost visible (no silent rejoin).
- Self-heal retry: a failed membership post retries. (fault-inject one 5xx)
- Startup stale-ghost sweep: pre-seed a leftover ghost `m.call.member`, start
  bridge → swept/retracted. (`sweep_stale_ghosts`)
- Reaper: M posts a **phantom** member (no LiveKit join) → emptied after 45s
  grace. (`classify_stale_memberships`)
- Third-party Discord bot speaking → bridged as a ghost.
- Anchor consolidation + 90s grace-release → no nameless idle `[Matrix] ` bot.

**Lifecycle**
- Open on first human (either side); close after last human leaves both (12s
  leave grace).
- Baseline connection count == 0 after close (no zombie voice, no dangling
  ghost) — the universal postcondition.
- Bridge-owned bots/ghosts never count as humans (don't keep a call open).
- Rapid join/leave race → ghost rejoin awaits old teardown (regression).
- Graceful shutdown (SIGINT + SIGTERM both drain to baseline).

**E2EE / media (run the whole matrix in BOTH an encrypted and an unencrypted room)**
- Encrypted: Discord→Matrix audible (ghost Olm key delivery) AND Matrix→Discord
  audible (subscriber gets the human's media key).
- Unencrypted: same, plaintext transport.
- Auto-detect encryption from room state.
- Fresh-store device identity (regression for #30): wipe a store, restart →
  peers still accept the new device → audio still flows.
- ✗/negative: room at default PL 50 for `m.call.member` → ghost direction breaks;
  assert the loud diagnostic log fires (guards against silent misconfig).

**Audio integrity (#32)** — the oracle, applied across the above
- Per-speaker similarity in↔out, both directions; clean under overflow mixing;
  long-sample glitch/dropout sweep to catch intermittent corruption.

**Multi-bridge**
- Two bridges, two rooms → strict isolation, no cross-talk (regression).

## Runtime reality (answering "how long would a run take")

The bridge is full of **real wall-clock grace timers** that dominate test
duration, not CPU:
- leave grace **12s**, reaper grace **45s**, membership flap/heartbeat **~90s**,
  anchor grace-release **~90s**, quarantine backoff **15→30→60→120s**.

A faithful run that *waits these out* makes a full matrix on the order of
**tens of minutes** (a single reaper or flap scenario alone burns 45–90s of pure
waiting, times ~30 scenarios × 2 rooms). That's fine for a **nightly** job.

To make it CI-friendly, the highest-leverage lever is to make these grace
durations **config-overridable** (a "test timing profile" that compresses them to
e.g. 1–3s). Tradeoff: compressed timers lose fidelity to production timing (and a
few flap/debounce behaviors are *about* the timing), so keep a **two-speed**
design — compressed profile for a fast suite, a small **real-timing** subset for
the nightly. This single change (thread the grace constants through config) is
probably the biggest enabler for the whole effort.

## CI integration (answering "could it be part of CI")

- **Hermetic Tier 1+2** (audio oracle on in-process pipeline + allocation/
  lifecycle over `test_*` and `classify_*`): yes — ordinary `cargo test`, runs
  **per-PR**, no live infra. Build this first.
- **Live Tier 3**: needs real Discord + homeserver + LiveKit + secrets, so it's a
  **self-hosted-runner** job — either **nightly (scheduled)** or **manual
  dispatch** ("run before a release"). It should **not** block PRs: external
  services (Discord rate limits, EC's ~90s membership flap, LiveKit) make it
  inherently flaky, so assertions must poll-with-timeout, classify hard-fail vs
  warn, and emit artifacts for triage. A green nightly is a strong release
  signal; a red one is a triage ticket, not a merge block.

## Provisioning the user owns (manual, one-time)
- A dedicated **test guild** with multiple voice channels.
- A set of hand-created **speaker bot apps** (count = how wide we can simulate;
  abuse-heuristic limited, create deliberately) + **one privileged mod bot**.
- Paired **encrypted + unencrypted** Matrix rooms per Discord channel, each with
  `org.matrix.msc3401.call.member` at **PL 0**.
- Test Matrix user accounts (for synthetic speakers) + appservice namespace
  matching the bridge's `sender_localpart`.

## Decisions (locked)

**Gaps → file as known-fail (DECIDED).** When the suite finds a gap (e.g. the
force-move / external-disconnect → no-rejoin gaps already surfaced), the scenario
documents the *expected* behavior and is marked **known-fail / xfail**, and the
fix is a **separate follow-up issue** — not folded into this work. Upshot: the
test framework stays a **pure observability artifact** that changes **no bridge
behavior**, which keeps it cleanly reviewable and avoids smuggling behavior
changes through test work (consistent with the gate-merge-on-user-testing rule).
First two issues to file from this design: external force-move rejoin, and
external-disconnect rejoin (both = watch the puppet's own `voice_state_update`,
compare actual vs assigned channel, re-issue join).

**Timers → two-speed, config-overridable (RECOMMENDED, user deferred to me).**
Thread the grace constants (leave 12s, reaper 45s, flap/heartbeat ~90s, anchor
release ~90s, quarantine 15→120s) through config. Run the suite under a
**compressed profile** (1–3s) for speed; run a **small real-timing subset**
nightly so the timing-dependent behaviors (flap debounce, grace-release) are
still exercised at true durations. Why this over real-timing-only: a faithful
full matrix is tens of minutes of pure waiting, which kills iteration; compressed
timers keep the *logic* under test while the nightly subset preserves *timing*
fidelity. Caveat to honor: a handful of behaviors are fundamentally *about* the
timing (debounce windows, flap absorption) — those stay in the real-timing subset
and are NOT trusted under compression. This is the single biggest enabler and is
a modest, also-operationally-useful change.
