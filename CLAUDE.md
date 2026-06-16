# CLAUDE.md — Nether Voice Bridge

Guidance for Claude Code working on this repo. Read alongside
`docs/nether-voicebridge-architecture.md` and `docs/nether-voicebridge-build-plan.md`.

## What this is

A bidirectional voice bridge between Discord voice channels and Matrix
(MatrixRTC / Element Call) room calls. **Single Rust binary, tokio runtime.**
Each `[[bridge]]` pairs one Discord channel ↔ one Matrix room; multiple bridges
run in one process. Matrix speakers appear on Discord as individually-controllable
puppet bots, drawn from a **single shared pool** of bot applications that is
allocated dynamically across all bridged channels (one bot per active channel is
its capture "anchor"; the rest are dedicated puppets, with steal-for-activation
and FCFS overflow — see `crates/discord/src/puppet.rs`). Discord speakers appear
in Element Call as appservice ghosts. See the architecture doc for the full
design; build the phases in order from the build plan, each with its acceptance
test.

## Environment (do not re-architect)

- Homeserver: **Tuwunel** at `nether.im`. MatrixRTC / Element Call **already
  works** here — lk-jwt-service + a LiveKit container are deployed on nether-red
  and calls function. Do not redesign the homeserver or the RTC infra; this bot
  is just another participant.
- **Room requirement (per bridged room):** the Matrix room must allow **power
  level 0** for the `org.matrix.msc3401.call.member` state event — the standard
  Element Call setup. Otherwise ghosts (and the E2EE subscriber) can't post their
  `m.call.member` and won't appear in the call → Discord→Matrix fails with a 403
  *"not enough power for org.matrix.msc3401.call.member"*. Starting a proper
  Element Call in the room configures this (or set it via /devtools →
  `m.room.power_levels`, `events["org.matrix.msc3401.call.member"] = 0`). Verified
  2026-06-16: a room left at the default state PL (50) silently breaks the ghost
  direction. **Gotcha:** Element's friendly "Roles & Permissions → Join Element
  calls" shows *"Default"* both when unset (→ `state_default` 50) and when an
  explicit `0` override is present — so it's unreliable; check the RAW
  `m.room.power_levels`.
- **The call room is *unencrypted* and Element Call runs media frame E2EE
  *off*** (verified 2026-06-14). The bridge runs `media_e2ee = false` and
  publishes **plaintext** LiveKit frames; encrypting into this non-E2EE call
  makes peers hear static. Keep it off unless the room becomes encrypted — see
  constraint #3, which now applies only to that deferred encrypted-room branch.
- Discord: one or more guilds/voice channels; a single shared pool of N puppet
  bot applications (config `[puppets]`), dynamically allocated across all bridged
  channels — no dedicated listener, one bot per active channel is the floating
  capture **anchor**. Allocation is **guild-aware**: a bot only serves channels
  in guilds it's been invited to (learned from `ready`/`guild_create`), so the
  pool can be shared elastically (all bots in all guilds) or partitioned (some
  bots per guild). Invite each bot — with Connect, Speak, Use Voice Activity,
  Change Nickname — to every guild you want it usable in.

## Hard constraints (these cause silent failure if ignored)

1. **Discord voice is DAVE-only (mandatory since March 2026).** Use a
   DAVE-capable `songbird` release and **pin it**. The bots are legitimate MLS
   participants; that's why decryption works. Do not attempt non-E2EE fallback.
2. **`matrix-rust-sdk` cannot join MatrixRTC sessions.** It is used here only
   for: client login, sync, room state events, and the appservice/ghosts. The
   actual RTC session-join (membership, lk-jwt fetch, to-device key exchange,
   LiveKit participation) lives in the `matrix-rtc` crate, adapted from the
   `matrix-jukebox` reference. Do not try to join calls via matrix-rust-sdk.
3. **Element Call media E2EE uses HKDF; livekit-rust's built-in frame E2EE uses
   PBKDF2.** Derive keys manually (`hkdf` + `sha2`, mirroring matrix-jukebox)
   and feed raw keys to LiveKit's frame crypto. Do **not** use livekit-rust's
   default E2EE key provider — it will silently fail to interop with Element
   Call.
4. **Puppet bot avatars cannot be set per-user at runtime.** Bot avatars are
   global and rate-limited (~1 change / 10 min); per-guild bot avatars don't
   exist. Set the per-guild **nickname** per user; avatars are static per
   puppet (or pre-baked per known regular). Do not attempt runtime avatar
   swapping.
5. **Echo/loop prevention is correctness-critical.** The capture anchor puppet
   hears the other puppets' playback; the LiveKit subscriber sees the ghosts'
   tracks. Maintain
   a set of bridge-owned identities (puppet user IDs on Discord, ghost
   identities on Matrix) and exclude them from capture/forward on both sides.
   Build this into the AudioRouter from the start.
6. **Lifecycle is on-demand.** Open when the first human appears on either side;
   close when the last human leaves both. Bridge-owned bots/ghosts never count
   as humans. Guarantee teardown returns the connection count to baseline.
7. **Pin `songbird` and `livekit` to specific releases.** Both ends are
   version-sensitive (DAVE, E2EE). Never float these.

## Conventions

- **Errors:** `thiserror` for library/crate errors, `anyhow` at the binary
  boundary. No `unwrap`/`expect` in audio or runtime hot paths.
- **Logging:** `tracing` only (never `println!`). Use spans per bridge and per
  participant.
- **Config:** TOML via `serde`. Secrets (bot tokens, Matrix access/appservice
  tokens) come from the config file or env, **never committed**. The LiveKit
  endpoint/SFU URLs are auto-derived from the homeserver's `.well-known`
  `rtc_foci` and the lk-jwt response — not configured.
- **Async:** one tokio task per long-lived connection (each puppet, subscriber,
  each ghost). Wire tasks with `tokio::sync::mpsc`; use
  `CancellationToken` for shutdown.
- **Audio core:** keep it in PCM and **decoupled from the live SDKs** so it can
  be unit-tested with the wav file-in/out harness. The two clocks (Discord vs
  LiveKit) need per-stream jitter buffers; frame size (20 ms vs 10 ms) and
  channel layout (stereo vs mono) differ — reframe/mix in the core.
- **Crate structure:** start single-crate with modules; split into the
  workspace (`config`, `audio`, `discord`, `matrix-rtc`, `bridge`) around
  Phase 5 once `matrix-rtc` and `audio` stabilize.

## Licensing

`matrix-jukebox` is the reference for the `matrix-rtc` crate. **Verify its
license before copying any code.** If incompatible, study it and reimplement.

## Build / run / test

- `cargo build`, `cargo run`.
- Per-phase acceptance tests are in the build plan; most integration milestones
  are manual audio checks (a wav you can hear, a tone in Element Call, audio
  crossing, named participants, baseline connection count after close).
- Unit tests target the `audio` core via the wav harness and the config schema.

## Do NOT

- Suggest moving the Discord side to Node/discord.js (voice receive is broken
  there for DAVE).
- Use livekit-rust's default E2EE key provider (KDF mismatch — see constraint 3).
- Attempt per-user runtime avatar changes on puppet bots (constraint 4).
- Forget echo-exclusion when adding any new capture or forward path
  (constraint 5).
- Float songbird/livekit versions (constraint 7).
- Mass-create Discord bot applications in a scripted loop (abuse heuristics);
  create them deliberately.
