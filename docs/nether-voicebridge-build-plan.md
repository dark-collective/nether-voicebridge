# Nether Voice Bridge — Build & Test Plan

Incremental phases. **Every phase compiles, runs, and has a concrete
acceptance test.** The two cryptographic boundaries (DAVE receive, Element Call
E2EE) are proven first, each with an artifact you can literally listen to,
before any routing logic exists. Rust difficulty ramps with the phases.

Assumes intermediate Rust (you know ownership/traits/async; you haven't shipped
a real project). Notes flag the idioms that matter, not syntax.

---

## Phase 0 — Scaffold & config
**Build:** Cargo project, `tokio` main, TOML config via `serde`, `tracing`
logging, `anyhow`/`thiserror` error scaffolding.
**Rust concepts:** modules, `serde` derive, `Result`/`?`, `tracing` spans.
**Acceptance:** `cargo run` loads config and logs a structured summary of the
configured channel-pair, homeserver, lk-jwt URL, bot tokens (redacted).
**Artifact:** running binary that does nothing but prove the skeleton.

## Phase 1 — Matrix presence (no media)
**Build:** `matrix-sdk` login as the bridge bot, sync, join the room, register
an event handler for `m.call` membership state events. Log call start/stop and
the current human participant list.
**Rust concepts:** async event handlers, shared state behind `Arc`, closures
capturing state.
**Acceptance:** start/stop a call in Element Call; logs show
"call active, participants: [...]" updating live.
**Artifact:** logs tracking real Matrix call membership.

## Phase 2 — Discord presence (no media)
**Build:** `serenity` client, gateway connect, watch Voice State Updates for
the target channel; `songbird` join/leave the channel.
**Rust concepts:** trait-based event handlers (`EventHandler`), `async_trait`,
intents/config.
**Acceptance:** the bot appears in the Discord voice channel; users
joining/leaving are logged with their IDs.
**Artifact:** logs tracking real Discord voice membership.

## Phase 3 — Discord audio capture  ⚠ proves DAVE receive
**Build:** `songbird` receive: per-SSRC Opus from Discord speakers, DAVE
decrypt, decode to PCM, write each speaker's audio to a `.wav`. Log RMS levels.
**Rust concepts:** songbird `VoiceEvent`/receive callbacks, byte/PCM buffers,
per-key maps (SSRC → writer).
**Acceptance:** talk in Discord → the bridge writes a playable `.wav` of your
voice. **This is the proof that DAVE receive works with the pinned songbird.**
**Artifact:** `capture_<ssrc>.wav` you can open and hear.

## Phase 4 — Matrix media: join LiveKit + play a test tone  ⚠⚠ highest risk
**Build:** Adapt the `matrix-jukebox` RTC path. Bridge participates in the
MatrixRTC session: parse `.well-known` foci, fetch the LiveKit JWT from
lk-jwt-service, receive the to-device key material, derive keys with **HKDF**
(not livekit's PBKDF2 default), connect to the LiveKit room via `livekit`,
publish a generated sine/pink-noise track.
**Rust concepts:** integrating a heavy SDK, `hkdf`/`sha2`, feeding raw keys to
livekit frame crypto, audio source plumbing.
**Acceptance:** start a call in Element Call → bridge joins and you **hear the
tone**. This proves the entire MatrixRTC + EC-E2EE path — the riskiest part.
**Artifact:** audible tone in a real Element Call.
**Note:** budget the most time here. matrix-jukebox's pink-noise milestone is
your proof it's achievable; lean on its key-derivation code.

**STATUS: ✅ DONE** (2026-06-14, commit `24ba809`). `@dark` hears a clean 440 Hz
tone in Element Call; teardown returns to baseline. **Key finding:** nether.im's
call room is *unencrypted* and Element Call runs media frame E2EE **off** for it,
so the bridge connects with `media_e2ee = false` and publishes **plaintext**
frames. Encrypting into this non-E2EE call made peers hear our ciphertext as
static — that was the whole blocker. The HKDF/to-device key-exchange machinery
(`src/rtc/{keys,session}.rs`) is implemented but **dormant behind `media_e2ee`**;
it is only needed for an *encrypted*-room deployment and, if ever enabled, would
need Olm-encrypted to-device to interop with current EC (it currently sends keys
plaintext). Stages: Stage 1 = participant appears (`153d056`/`52f7683`);
Stage 2 = audible tone (`df62d00` E2EE attempt, superseded by `24ba809`).

## Phase 5 — One-way: Discord → Matrix, single speaker
**Build:** Wire Phase 3 capture PCM into Phase 4 publish. One Discord speaker
published into the Matrix call **as the bridge's own identity** (ghosts come
later). Add the `audio` core: reframe (20 ms stereo → 10 ms mono), jitter
buffer. Build the **file-in/out test harness** here: feed a `.wav` to the
publish path without Discord, and vice versa, so the core is unit-testable.
**Rust concepts:** channels (`tokio::sync::mpsc`) between capture and publish
tasks, ring buffers, backpressure, splitting a crate.
**Acceptance:** talk in Discord → heard in Element Call.
**Artifact:** first real one-way bridged audio.

**STATUS: ✅ DONE** (2026-06-14, commit `2077c89`). Talking in Discord is heard
in Element Call (bridge publishes one mono track as its own identity) and it
survives Matrix leave/rejoin. Built the SDK-free `audio` core (downmix,
saturating mix, `JitterBuffer`, wav harness) + a long-lived audio-consumer fed
via a `SourceControl` mpsc. The workspace split was deferred to Phase 6.
**Capture-death RESOLVED** (commit `794821b`). Reproduced under instrumentation
(`1662330`): it was OUR bug, not DAVE/songbird — a `debug_assert` in `mix_mono`
panicked the songbird event thread whenever **two Discord speakers overlapped in
one tick** with mismatched frame lengths, stopping `VoiceTick` until restart.
Fix: `mix_mono` handles unequal lengths (pad short with silence); no panics on the
audio hot path. The DAVE-epoch hypothesis was wrong (confirmed live: 4
simultaneous speakers now fine). Lesson logged in memory.

## Phase 6 — Reverse: Matrix → Discord, single speaker
**Build:** Subscribe to a Matrix participant's track (`livekit` subscribe),
decode to PCM, play through the **already-connected listener bot** (songbird does
RX+TX on one connection — no extra voice-channel member needed). Reuse the audio
core (reframe the other way) with a **per-participant** jitter buffer (LiveKit
tracks arrive independently — the Phase 5 tick-mixing shortcut does NOT carry
over). **Split the crate into the workspace here**
(`config`/`audio`/`discord`/`matrix-rtc`/`bridge`), now that the audio core is
exercised in both directions.
**Rust concepts:** livekit subscribe/track callbacks, symmetric routing,
workspace split.
**Acceptance:** talk in Element Call → heard in Discord. Both directions now
exist (single-speaker, single-identity).
**Artifact:** bidirectional audio, one speaker each way.

## Phase 7 — Puppet pool (Matrix → Discord attribution)
**Build:** Multiple Discord bot tokens; a pool allocator (checkout/return keyed
on Matrix membership); per-user puppet assignment; per-guild nickname = Matrix
display name; join/leave driven by membership events. (A **persistent** pool —
puppets joined at bridge-open and kept connected — is worth considering to
minimise voice-channel churn; revisit once the capture-death investigation
lands.)
**Rust concepts:** managing N concurrent connections, an allocator state
machine, `HashMap<MatrixUserId, Puppet>` behind a mutex, task-per-connection.
**Acceptance:** two Matrix users in the call show as two **named** Discord
participants you can individually volume/mute.
**Artifact:** per-user attribution on the Discord side.
**Reminder:** add bridge-owned puppet SSRCs to the echo-exclusion set.

**STATUS: 🟡 IMPLEMENTED — builds clean, 30 tests pass, NOT yet verified live.**
Decisions (confirmed with user): **on-demand per participant** (a puppet joins
voice + sets its nickname when a Matrix participant's LiveKit track subscribes,
and leaves when it ends) and **overflow → listener** (more simultaneous Matrix
speakers than puppets fall back to the existing Phase-6 listener mix — no audio
lost, just un-attributed). Cross-crate routing lives in a new SDK-free
`nvb_audio::routing` (`PlaybackRouter`/`ParticipantSink`/`MixerSink`) so
`matrix-rtc` and `discord` stay decoupled. `nvb_discord::puppet::PuppetPool`
implements `PlaybackRouter`: each puppet is its own serenity `Client` + songbird
(TX-only `Config::default()`) with its own `PlaybackMixer`, driven by an
Assign/Release command loop that captures the gateway `Context` from `ready`.
Echo-exclusion: the exclude set became a shared `Arc<RwLock<HashSet<u32>>>` —
each puppet inserts its own SSRC on `DriverConnect`, removes it on release; the
listener's `mix_tick` read-locks it per tick. **Open risk for the live test:**
the puppet nickname comes from `participant.name()` (fallback: identity) — it is
unverified what Element Call actually populates there, so identity/name/metadata
are logged at every `TrackSubscribed`. Contingency = matrix-sdk profile lookup.
**Live acceptance still to run** (per the build plan above).

## Phase 8 — Ghost set (Discord → Matrix attribution)
**Build:** Register the appservice namespace on Tuwunel; mint a ghost per
Discord speaker; each ghost posts its own `m.call` membership, gets its own
lk-jwt token, connects as its own LiveKit participant, publishes that speaker's
audio.
**Rust concepts:** appservice (matrix-sdk), N livekit participants, masquerading
as namespaced users.
**Acceptance:** two Discord users show as two distinct ghosts (correct names +
avatars) in Element Call.
**Artifact:** per-user attribution on the Matrix side.
**Reminder:** add ghost identities to the echo-exclusion set; verify lk-jwt
accepts ghost OpenID tokens.

## Phase 9 — Lifecycle coordinator & teardown
**Build:** The on-demand open/close state machine; first-human-opens /
last-human-closes; reconnection; tombstone following; call-end handling;
guaranteed cleanup (no leaked connections, puppets returned, ghosts retired).
**Rust concepts:** a real state machine (enum states + transitions), `Drop`
/ explicit teardown, cancellation (`CancellationToken`), graceful shutdown.
**Acceptance:** open/close repeatedly from both sides; connection count returns
to baseline every time; no orphaned bots/ghosts.
**Artifact:** robust lifecycle.

## Phase 10 — Hardening
**Build:** reconnection with backoff, error recovery, drift/clock-skew
compensation over long calls, resource caps, observability (per-bridge metrics,
structured tracing), and a generic **capture-health watchdog** — recover from a
genuine voice-connection drop (`DriverDisconnect` with no reconnect) by rejoining
the channel. (Nice-to-have robustness; the Phase 5 "capture-death" turned out to
be an unrelated `mix_mono` panic, since fixed — not a connection issue.)
**Acceptance:** a multi-hour call with users churning on both sides stays
stable; no drift-induced desync; clean recovery from a dropped connection.
**Artifact:** production-usable bridge.

---

## Testing strategy

- **Unit-test the `audio` core in isolation** with the wav file-in/out harness
  (built in Phase 5): correctness of reframe, mix, resample, jitter buffer —
  no live SDKs needed.
- **Config parsing**: unit tests for the TOML schema.
- **Integration milestones are manual** (live Discord + Matrix) but each has a
  trivially-verifiable artifact: a wav you can hear (P3), a tone in Element
  (P4), audio crossing (P5/6), named participants (P7/8), baseline connection
  count after close (P9).
- **Decouple at module boundaries** so you can replace either live SDK with a
  file/source stub when iterating on the router.

## Suggested session cadence

Phases 0–2 are one warm-up session. Phase 3 and Phase 4 each deserve their own
session (the de-risking milestones). 5 done; 6 on its own (adds the workspace
split). 7 and 8 each their own. 9 and 10 ongoing. Write a short handoff note at
the end of each.
