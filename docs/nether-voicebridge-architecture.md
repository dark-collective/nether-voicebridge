# Nether Voice Bridge — Architecture

A bidirectional voice bridge between a Discord voice channel and a Matrix
(MatrixRTC / Element Call) room call, written in Rust as a single binary.

## Goal

- A Matrix user in the room's call is heard on Discord **as a distinct,
  individually mute/volume-able participant** (puppet bot per user).
- A Discord user in the voice channel is heard in the Matrix call **as a
  distinct participant** (appservice ghost per user) with correct name + avatar.
- Connect on join, disconnect on leave, on both sides.

## Why all-Rust (the short version)

- Discord voice now **mandates DAVE** (E2EE, MLS-based). `songbird` supports
  DAVE send/receive; `discord.js` receive is currently broken, ruling out Node.
- LiveKit's canonical client is `livekit` (Rust); the Node/Python SDKs are
  bindings over it. Headless publish/subscribe is first-class in Rust.
- MatrixRTC session-join is **not** in any maintained SDK. `matrix-rust-sdk`
  explicitly cannot join RTC sessions yet (it only hosts Element Call as a
  widget). The one headless reference implementation, `matrix-jukebox`, is
  100% Rust and already does membership, the lk-jwt token fetch, the to-device
  key exchange, and an Element-Call-compatible E2EE KDF.

So Rust is the only language where both ends are either maintained-strong
(Discord, LiveKit) or have a working in-language reference (MatrixRTC).

## Runtime shape

Single binary, `tokio` multi-threaded runtime. No second process, no
PCM-over-socket glue. A central **Coordinator** owns one **Bridge** per
configured (Discord channel ↔ Matrix room) pair. Everything else is tokio
tasks and channels.

```
                         ┌──────────────────────────────────────┐
                         │              Coordinator              │
                         │  (one Bridge per channel-pair)        │
                         └──────────────────────────────────────┘
                                          │
                    ┌─────────────────────┴─────────────────────┐
                    │                  Bridge                    │
                    │  lifecycle state machine + AudioRouter     │
                    └────────────────────────────────────────────┘
        Discord side  │                                      │  Matrix side
                      ▼                                      ▼
   ┌───────────────────────────────┐        ┌──────────────────────────────────┐
   │ Listener bot (1)              │        │ Subscriber participant (1)        │
   │  songbird RX, all SSRCs       │        │  livekit-rust, subscribes to all  │
   │  DAVE decrypt → Opus → PCM    │        │  remote tracks → Opus → PCM       │
   ├───────────────────────────────┤        ├──────────────────────────────────┤
   │ Puppet pool (N bots)          │        │ Ghost set (N appservice users)    │
   │  one per Matrix speaker       │        │  one per Discord speaker          │
   │  PCM → Opus → DAVE → play     │        │  PCM → publish own LiveKit track  │
   │  nickname = Matrix display    │        │  + post m.call membership event   │
   └───────────────────────────────┘        └──────────────────────────────────┘
```

### Direction: Discord → Matrix
1. Listener bot (songbird, receive on) captures per-SSRC Opus from every
   Discord speaker; DAVE-decrypts; decodes to PCM.
2. For each *human* Discord speaker, the Coordinator has minted a **ghost**
   (`@discord_<id>:nether.im`) which: posts its `m.call` membership state
   event, fetches a LiveKit JWT from lk-jwt-service for its identity, connects
   to the LiveKit room as a participant, and **publishes that speaker's PCM**
   as its own audio track.
3. Element Call therefore shows each Discord user as a separate participant
   with correct name + avatar (Matrix ghosts have arbitrary avatars, no rate
   limit).

### Direction: Matrix → Discord
1. A single **subscriber participant** (the bridge's own LiveKit identity)
   subscribes to every real Matrix participant's published track; decodes to
   PCM, demuxed by participant identity.
2. For each *human* Matrix participant, the Coordinator checks out a **puppet
   bot** from the pool, joins it to the Discord voice channel, sets its
   per-guild nickname to the Matrix display name, and **plays that
   participant's PCM** through it.
3. Discord shows each Matrix user as a separate participant a listener can
   right-click → User Volume / mute. (Avatars are static-per-puppet — see
   constraints.)

## Echo / loop prevention (correctness-critical)

The listener bot hears **everything** in the Discord channel, including the
puppets' own playback. The LiveKit subscriber sees **all** tracks, including
the ghosts the bridge itself publishes. Without filtering you get audio loops
and users hearing themselves.

Rule: the AudioRouter maintains the set of **bridge-owned identities** —
puppet bot user IDs (Discord) and ghost user IDs / LiveKit identities
(Matrix) — and excludes them from capture/forward on both sides:
- Discord RX: drop SSRCs belonging to our puppet bots.
- LiveKit RX: drop tracks whose participant identity is one of our ghosts (or
  the subscriber's own identity).

This must be in place before Phase 5; build it into the router from the start.

## Lifecycle (on-demand)

The Bridge opens when the **first human** appears on **either** side and closes
when the **last human** leaves **both** sides. Bridge-owned bots/ghosts never
count as humans.

- Discord membership comes from gateway Voice State Updates.
- Matrix membership comes from `m.call` membership state events in the room.
- Open: join listener bot to channel; establish subscriber participant +
  bridge's own MatrixRTC membership.
- Per participant: allocate puppet (Matrix human) / mint ghost (Discord human).
- Close: tear down all connections, return puppets to pool, retire ghosts,
  drop memberships. No leaked connections (test this explicitly).

Handle room tombstones (follow upgrades) and call-end events.

## Audio core

- Both sides are 48 kHz Opus, so resampling is near-trivial — but **frame size
  and channel layout differ**: Discord is 20 ms stereo; livekit-rust's
  `AudioSource` consumes 10 ms mono i16 frames (verify against the pinned
  version). Reframe and down/up-mix accordingly.
- Two independent 48 kHz clocks (Discord gateway vs LiveKit) → **per-stream
  jitter buffer** and drift handling. A small ring buffer per participant.
- Keep the core in PCM (`i16` or `f32`) and **decouple it from the live SDKs**
  so it can be unit-tested with file in/out (see build plan: wav harness).
- libopus encode/decode: songbird can hand you decoded PCM on RX and take PCM
  on TX; livekit-rust encodes/decodes Opus internally via libwebrtc. You mostly
  live in PCM in the middle.

## E2EE specifics

- **Discord (DAVE):** mandatory since March 2026. Each puppet and the listener
  are legitimate MLS group members and get ratcheted sender keys — decryption
  works because they're real participants. `songbird` handles the MLS dance;
  pin a DAVE-capable release.
- **Matrix (Element Call media E2EE):** per-sender keys distributed via Matrix
  to-device events. **Element Call uses HKDF; livekit-rust's built-in frame
  E2EE uses PBKDF2** — a real incompatibility. Do the key derivation yourself
  (HKDF, mirroring `matrix-jukebox`) and feed raw keys to LiveKit's frame
  crypto; do **not** rely on livekit-rust's default key provider. This is the
  highest-risk module in the project.

## Proposed crate layout

Start single-crate with modules through ~Phase 4; split into a workspace once
`matrix-rtc` and `audio` stabilize (Phase 5). Target workspace:

```
nether-voicebridge/
├── Cargo.toml                # workspace
├── crates/
│   ├── config/               # TOML config (serde)
│   ├── audio/                # PCM types, reframe, mix, resample, jitter buffer
│   ├── discord/              # serenity + songbird: listener + puppet pool
│   ├── matrix-rtc/           # vendored/adapted from matrix-jukebox:
│   │                         #   membership, lk-jwt, to-device keys, EC HKDF,
│   │                         #   livekit-rust participant
│   └── bridge/               # Coordinator, lifecycle state machine, AudioRouter
└── src/main.rs               # load config, start Coordinator
```

## Dependencies (verify latest on crates.io; PIN songbird + livekit)

- `tokio` — async runtime
- `serde`, `toml` — config
- `tracing`, `tracing-subscriber` — logging
- `anyhow` (binary boundary), `thiserror` (library errors)
- `serenity` + `songbird` — Discord gateway/voice + DAVE  *(pin songbird)*
- `livekit`, `livekit-api` — LiveKit participant + token  *(pin livekit)*
- `matrix-sdk` (+ its appservice support — verify current crate/feature name)
- `audiopus` or `opus` — Opus where manual decode/encode is needed
- `hkdf`, `sha2` — Element Call key derivation
- `dasp` / `rubato` — sample manip / resample; `ringbuf` — jitter buffers
- `dashmap` or `tokio::sync` maps — shared state

## Open questions / risks

1. **EC E2EE KDF** — highest risk. matrix-jukebox proves it's solvable; budget
   real time. Track the MSC4195 spec discussion (HKDF vs PBKDF2 may still move).
2. **matrix-jukebox license** — confirm before vendoring any code; otherwise
   study and reimplement.
3. **lk-jwt for appservice ghosts** — ghosts are local nether.im users so they
   should be "full-access"; verify lk-jwt-service accepts appservice-minted
   ghost OpenID tokens.
4. **Discord bot pool sizing & abuse heuristics** — N tokens, created
   deliberately, each invited with Connect/Speak/Use Voice Activity/Change
   Nickname.
5. **Drift/clock skew** over long calls — validate jitter/drift handling in
   Phase 10.
