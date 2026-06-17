# Nether Voice Bridge

A bidirectional voice bridge between **Discord** voice channels and **Matrix**
([MatrixRTC](https://github.com/matrix-org/matrix-spec-proposals/pull/4143) /
[Element Call](https://github.com/element-hq/element-call)) room calls. Talk in
Discord and be heard in a Matrix call — and vice-versa — with every speaker
showing up as their own named participant on the far side.

A single Rust binary on a tokio runtime; one process can bridge many
channel↔room pairs at once.

<!-- TODO: add screenshots — e.g. a Matrix call roster showing Discord users as
     ghosts, and a Discord voice channel showing Matrix users as named puppet
     bots. -->

## What it does

- **Both directions, fully attributed.** Matrix speakers appear in Discord as
  individually-controllable **puppet bots** (named after the Matrix user, mutable
  per-user). Discord speakers appear in the Matrix call as **appservice ghosts**
  (named + avatared after the Discord user).
- **See who's waiting.** Discord users show up in the Matrix call as ghosts **even
  when no Matrix user is in the call** (membership only, no media) — so Matrix
  users can tell at a glance whether a room is worth joining. The moment a Matrix
  user joins, every present ghost goes live (audio + E2EE keys) before anyone even
  speaks; no bot sits idle in a Discord voice channel just to show presence.
- **Multiple bridges, one process.** Each `[[bridge]]` pairs one Discord channel
  with one Matrix room; add more blocks to bridge more pairs. The Discord puppet
  bots are a single shared pool, allocated across channels on demand.
- **Real Discord E2EE.** The bots are legitimate
  [DAVE](https://discord.com/blog/meet-dave-e2ee-for-audio-video) (MLS)
  participants, so audio decrypts correctly — no non-E2EE fallback.
- **Optional Matrix media E2EE.** For end-to-end-encrypted Matrix rooms, the
  bridge derives Element Call's HKDF frame keys and (with `key_transport = "olm"`)
  delivers per-ghost keys via Olm — true E2EE the homeserver can't read.
- **On-demand lifecycle.** A bridge opens when the first human appears on either
  side and tears down to baseline when the last human leaves both.

## How it works

Matrix speakers → Discord runs through a shared **puppet pool** (one floating
"anchor" bot captures the channel; extra bots become dedicated per-speaker
puppets). Discord speakers → Matrix runs through an appservice **ghost** per
speaker, each its own LiveKit participant. A shared, SDK-decoupled **audio core**
handles the two clocks (Discord 20 ms / LiveKit 10 ms), reframing, mixing, and
per-stream jitter buffering. Echo/loop prevention excludes all bridge-owned
identities from capture on both sides.

The full design and the phased build history live in:

- [`docs/nether-voicebridge-architecture.md`](docs/nether-voicebridge-architecture.md) — architecture & rationale
- [`docs/nether-voicebridge-build-plan.md`](docs/nether-voicebridge-build-plan.md) — phase-by-phase build plan
- [`docs/docker-usage.md`](docs/docker-usage.md) — running under Docker + CI notes

## Requirements

- A **Matrix homeserver with MatrixRTC / Element Call working** (lk-jwt-service +
  a LiveKit SFU). The bridge is just another call participant — it does not deploy
  or manage the RTC infrastructure.
- One or more **Discord bot applications** (the puppet pool), each invited to your
  guild(s) with *Connect, Speak, Use Voice Activity, Change Nickname*.
- An **appservice registration** on the homeserver for the Matrix-side ghosts.
- **Per bridged room:** the Matrix room must allow power level **0** for the
  `org.matrix.msc3401.call.member` state event (the standard Element Call setup),
  or Discord→Matrix ghosts can't post their membership. See the architecture doc.

## Quickstart (from source)

```bash
cp config.example.toml config.toml   # then fill in your tokens/IDs
cargo run                            # reads ./config.toml (override with CONFIG_PATH)
```

`config.example.toml` documents every field. **Never commit `config.toml`** — it
holds bot tokens and Matrix access/appservice tokens.

## Docker

```bash
cp config.example.toml config.toml   # fill in
docker compose up -d
docker compose logs -f
```

The matrix-sdk store persists on a named volume; store paths are auto-derived
under `/app/data` so no `store_path` config is needed. See
[`docs/docker-usage.md`](docs/docker-usage.md) for the full guide, multi-arch
builds, and CI/runner notes.

## Configuration notes

- **`store_path` is optional** — omitted, it auto-derives to
  `<data_dir>/<bridge>` (`data_dir` defaults to `./nether-store`; the Docker image
  sets `NVB_DATA_DIR=/app/data`). Each bridge gets a distinct store.
- **LiveKit endpoints are auto-derived** from the homeserver's `.well-known`
  `rtc_foci` and the lk-jwt response — not configured by hand.
- **`media_e2ee` is auto-detected** per bridge from the room's
  `m.room.encryption` state (encrypted ⇒ on with per-ghost Olm, unencrypted ⇒
  off); it's an optional override now — set it only to force a value.
  `key_transport` likewise auto-selects (`olm` for an encrypted room, else
  plaintext).

## Community & contributing

- Issues and roadmap: the project's Forgejo repo at
  [nether.codes/dark/nether-voicebridge](https://nether.codes/dark/nether-voicebridge).
- <!-- TODO: add the Matrix room/space link for the project community. -->

## License

<!-- TODO: choose and add a license (add a LICENSE file and state it here). -->
