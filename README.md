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
cp config.example.toml config.toml   # fill in - or set environment variables
docker compose up -d
docker compose logs -f
```

### Environment Variables
| Variable | Required | Default | Notes |
|---|---|---|---|
| `NETHER_PUPPET_TOKENS` | ✓ | | Comma-separated Discord bot tokens |
| `NETHER_DISCORD_GUILD_ID` | ✓ | | |
| `NETHER_DISCORD_CHANNEL_ID` | ✓ | | |
| `NETHER_MATRIX_HOMESERVER_URL` | ✓ | | |
| `NETHER_MATRIX_ROOM_ID` | ✓ | | Internal room ID (`!abc:nether.im`) |
| `NETHER_MATRIX_BOT_USER_ID` | one of¹ | | Full bot MXID |
| `NETHER_MATRIX_SENDER_LOCALPART` | one of¹ | | Bot localpart; derives MXID from homeserver domain |
| `NETHER_MATRIX_DEVICE_ID` | | *(auto-derived)* | Derived from the bridge name when unset |
| `NETHER_MATRIX_APPSERVICE_ID` | ✓ | | |
| `NETHER_MATRIX_APPSERVICE_TOKEN` | ✓ | | |
| `NETHER_PUPPET_NICKNAME_PREFIX` | | `[Matrix] ` | Set to empty string to disable |
| `NETHER_BRIDGE_NAME` | | `bridge-0` | Label used in logs; also the store-path leaf |
| `NETHER_DISCORD_CAPTURE_WAV` | | `false` | Debug only — writes unbounded WAV files |
| `NETHER_MATRIX_STORE_PATH` | | *(auto-derived)* | Derived as `/app/data/<bridge-name>` on the volume; rarely set |
| `NETHER_MATRIX_BOT_DISPLAY_NAME` | | | |
| `NETHER_MATRIX_BOT_AVATAR_PATH` | | | Path inside the container |
| `NETHER_LIVEKIT_MEDIA_E2EE` | | *(auto-detected)* | Detected from the room's `m.room.encryption`; set `true`/`false` only to force |
| `NETHER_LIVEKIT_KEY_TRANSPORT` | | *(auto-selected)* | `olm` for encrypted rooms, `plaintext` otherwise; set only to force |
| `NETHER_LIVEKIT_LK_JWT_URL` | | *(auto-derived)* | Override lk-jwt-service endpoint |
| `NETHER_LIVEKIT_URL` | | *(auto-derived)* | Override LiveKit SFU WebSocket URL |

¹ At least one of `BOT_USER_ID` or `SENDER_LOCALPART` is required. When only
`SENDER_LOCALPART` is set, the full MXID is derived as
`@{localpart}:{homeserver}`. Both can be set independently when the appservice
namespace anchor and the bot account differ.

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
- **`media_e2ee`** is per-bridge: `false` for unencrypted rooms, `true` for
  encrypted rooms (with `key_transport = "olm"` for true per-ghost E2EE).

## Community & contributing

- Issues and roadmap: the project's Forgejo repo at
  [nether.codes/dark/nether-voicebridge](https://nether.codes/dark/nether-voicebridge).
- <!-- TODO: add the Matrix room/space link for the project community. -->

## License

<!-- TODO: choose and add a license (add a LICENSE file and state it here). -->
