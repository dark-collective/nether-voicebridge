# CLAUDE.md — Nether Voice Bridge

Guidance for Claude Code working on this repo. Read alongside
`docs/nether-voicebridge-architecture.md` and `docs/nether-voicebridge-build-plan.md`.

## What this is

A bidirectional voice bridge between Discord voice channels and Matrix
(MatrixRTC / Element Call) room calls. **Single Rust binary, tokio runtime.**
Each `[[bridge]]` pairs one Discord channel ↔ one Matrix room; multiple bridges
run in one process. Matrix speakers appear on Discord as individually-controllable
puppet bots, drawn from a **single shared pool** of bot applications that is
allocated dynamically across all bridged channels, **per (bot, guild)** — one bot
holds an independent voice connection per guild, so N bots serve up to N puppets
per guild across unlimited guilds simultaneously (#46); one bot per active channel
is its capture "anchor", the rest are dedicated puppets, with steal-for-activation
and FCFS overflow — see `crates/discord/src/puppet/` (the sync allocator core is
`puppet/alloc.rs`). Discord speakers appear
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
- **Bot joins as a full appservice member — no `guest_access` needed.** The bridge
  always authenticates via `m.login.application_service`. Some homeservers (Tuwunel)
  **guest-classify that login session** — so a client `/join` with it 403s *"Guests
  are not allowed to join this room"* on any room lacking
  `m.room.guest_access=can_join`, even when invited. Fixed
  (`matrix.rs::appservice_join_room`, verified 2026-06-17): the bot joins via an
  appservice-**asserted** `POST /join` with the `as_token` (the same path ghosts use
  → a full, non-guest member), so **no room needs `guest_access`**. Join rules are
  still enforced: a **restricted/invite-only** room still needs the bot **invited**
  (or in the allow-space) first.
- **Per-room media E2EE is auto-detected (opt-out override).** By default the
  bridge reads the room's `m.room.encryption` state at startup and resolves
  `media_e2ee` from it (encrypted ⇒ on, unencrypted ⇒ off) — `livekit.media_e2ee`
  is now an `Option<bool>` override, set only to force a value. The matching matters
  (verified 2026-06-14): EC runs frame E2EE off in an unencrypted room, so
  encrypting into it makes peers hear static; not encrypting into an E2EE call
  means peers can't decrypt — hence auto-detect. Resolution lives in
  `nvb_config::LiveKitConfig::resolved_media_e2ee/_key_transport` +
  `matrix::detect_room_encryption`, threaded from `matrix-rtc/src/lib.rs::run`.
  Because the persistent store is now opened unconditionally (detection can only run
  *after* the first sync, but the store must be ready *before* it), an unencrypted
  bridge also keeps a store. The media key then reaches the call's humans by one of
  two transports (`livekit.key_transport`, also an `Option` — auto-selects `olm`
  for an encrypted room, else `plaintext`):
  - `"plaintext"` — homeserver-readable to-device key send (Path 2).
  - `"olm"` — **true E2EE (Path 3)**: each ghost becomes its own crypto client and
    Olm-encrypts the key per recipient device via
    `Encryption::encrypt_and_send_raw_to_device`. **Implemented + live-verified
    2026-06-16** on the encrypted "Light Voice" bridge. Expected log noise: a
    recipient's **stale/offline** devices (no one-time keys) show as
    `unreachable` — delivery to their *live* device still succeeds; only a
    `reached NO recipient devices` WARN means an actual failure.
  A room can only go unencrypted→encrypted (one-way latch); if that happens at
  runtime the bridge logs loudly and tears down (restart re-detects E2EE-on).
  See constraint #3 (livekit's KDF mismatch) — it applies whenever `media_e2ee`
  resolves on, both transports.
- Discord: one or more guilds/voice channels; a single shared pool of N puppet
  bot applications (config `[puppets]`), dynamically allocated across all bridged
  channels — no dedicated listener, one bot per active channel is the floating
  capture **anchor**. The allocation unit is **(bot, guild)** (#46): a bot holds
  one independent voice connection per guild and multiplexes them, so one guild
  claiming bots never starves another — but two bridged channels in the SAME
  guild still need distinct bots (one voice Call per guild per bot). A bot only
  serves channels in guilds it's been invited to (learned from
  `ready`/`guild_create`), so the pool can be shared elastically (all bots in
  all guilds) or partitioned (some bots per guild). Invite each bot — with
  Connect, Speak, Use Voice Activity, Change Nickname — to every guild you want
  it usable in.

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
  `rtc_foci` and the lk-jwt response — not configured. Each bridge's
  `matrix.store_path` is **optional**: when omitted it auto-derives to
  `<data_dir>/<bridge>` (`data_dir` defaults to `./nether-store`, overridden by
  the `NVB_DATA_DIR` env — the Docker image sets it to `/app/data`). Resolution
  happens in `Config::resolve_store_paths` (`crates/config/src/resolve.rs`); an
  explicit `store_path` always wins. **Server/appservice profiles:** the
  homeserver/appservice fields shared across bridges (`homeserver_url`,
  `matrix_domain`, `bot_user_id`, `appservice_id`, `appservice_token`)
  live once in a named `[server.<name>]` profile; a bridge sets `server =
  "<name>"` + its own `room_id` (bridge-level) + `[bridge.discord]` and inherits
  the rest (`Config::resolve_servers` — an explicit `[bridge.matrix]` value still
  wins). `matrix.device_id` is also **optional**: when omitted it auto-derives
  from the bridge `name` (same slug as `store_path`, so device + store stay
  consistent and unique-per-bridge) via `Config::resolve_device_ids`.
  `bot_user_id` stays required (its appservice localpart can't be derived from
  `appservice_id`). `matrix_domain` is **optional**: when omitted, the Matrix
  domain is extracted from `homeserver_url`; set it explicitly when those differ
  (e.g. `homeserver_url` is an internal proxy address). `validate()` rejects
  unknown `server` references, any required matrix field still empty after merge,
  and duplicate `(homeserver_url, bot_user_id, device_id)` across bridges.
- **Device ids are minted unique-per-crypto-store, not just per-bridge (#30).** At
  runtime the configured/derived device id is only a *base*: the bot mints
  `<base>-<rand>` paired with its crypto store (`matrix.rs::resolve_store_device_id`,
  marker `<store_path>/device_id`) and each ghost folds in a per-store salt →
  `D<key>-<salt>` (`resolve_ghost_device_salt`; ghost crypto store dir keyed by the
  resulting device id). This is mandatory: the *same* device id presenting *different*
  Olm signing keys across two stores (e.g. a dev box and a VPS, or a wiped store)
  makes peers reject it as `SigningKeyChanged` → media keys never delivered → silent
  encrypted audio. A wiped/copied/second store therefore **re-provisions a brand-new
  device** (and resets that store's crypto); the one-time re-key is expected and
  peer-safe (peers see a new device, not a changed key). Don't "stabilize" device ids
  back to the bare base.
- **Async:** one tokio task per long-lived connection (each puppet, subscriber,
  each ghost). Wire tasks with `tokio::sync::mpsc`; use
  `CancellationToken` for shutdown.
- **Audio core:** keep it in PCM and **decoupled from the live SDKs** so it can
  be unit-tested with the wav file-in/out harness. The two clocks (Discord vs
  LiveKit) need per-stream jitter buffers; frame size (20 ms vs 10 ms) and
  channel layout (stereo vs mono) differ — reframe/mix in the core.
- **Crate structure:** start single-crate with modules; split into the
  workspace (`config`, `audio`, `discord`, `matrix-rtc`, `bridge`) around
  Phase 5 once `matrix-rtc` and `audio` stabilize. The largest files are now
  module directories rather than single files, each split along its natural
  seams (a `mod.rs` root holds the shared type defs + public surface, siblings
  hold the impl blocks): `discord/src/puppet/` (`alloc` = the std-Mutex
  allocator core, `voice`, `events`, `commands`, `ghosts`, `router`, `sinks`),
  `matrix-rtc/src/rtc/ghost/` (`identity`, `pool`, `lifecycle`, `keydist`,
  `appservice` = raw CS-API primitives), `matrix-rtc/src/mgmt/` (`parse`,
  `authority`, `handlers`, `pending`), and `config/src/` (`schema`, `resolve`,
  `validate`). External paths (`nvb_discord::SharedPool`, `nvb_config::Config`,
  `nvb_matrix_rtc::mgmt::*`, …) are unchanged — the splits were pure moves.

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
