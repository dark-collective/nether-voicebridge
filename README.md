# Nether Voice Bridge

A bidirectional voice bridge between **Discord** voice channels and **Matrix**
([MatrixRTC](https://github.com/matrix-org/matrix-spec-proposals/pull/4143) /
[Element Call](https://github.com/element-hq/element-call)) room calls. Talk in
Discord and be heard in a Matrix call — and vice-versa — with every speaker
showing up as their own named participant on the far side.

A single Rust binary on a tokio runtime; one process can bridge many
channel↔room pairs at once.

> [!WARNING]
> **Heavy active development — pre-1.0.** This project is **vibe-coded** and has
> **not yet had a human code review**. It works in day-to-day use on the author's
> homeserver, but expect rough edges, breaking changes, and bugs. Run it at your
> own risk, and please report what breaks. Come say hi / get help in the Matrix
> room: [`#nether-voicebridge:nether.im`](https://matrix.to/#/%23nether-voicebridge:nether.im).

## Screenshots

![A Matrix Element Call (left) and the bridged Discord voice channel (right) side
by side: Discord speakers appear as named ghosts in the Element call, and Matrix
speakers appear as named puppet bots in Discord.](docs/images/nether-voicebridge.png)

*Left: an Element Call with the bridged participants. Right: the same conversation
in the Discord voice channel — each side shows the other's speakers as their own
named participants.*

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
- [`docs/packaging.md`](docs/packaging.md) — `.deb`/`.rpm` packages + systemd service

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
- **Per bridged room (non-public only):** on the appservice-auth path the bot
  joins as a full member via the appservice, so the room needs **no
  `guest_access`** — but a **restricted or invite-only** room still requires the
  bot user to be **invited** (or a member of the room's allow-space) first.

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

## Install (Debian/Ubuntu & Fedora/RHEL)

Pre-built `.deb` and `.rpm` packages are attached to each
[release](https://nether.codes/dark/nether-voicebridge/releases). They install the
binary to `/usr/bin`, a systemd unit, and an example config, and create a dedicated
`nether-voicebridge` system user.

```bash
# Debian / Ubuntu
sudo apt install ./nether-voicebridge_<version>_amd64.deb
# Fedora / RHEL
sudo dnf install ./nether-voicebridge-<version>-1.x86_64.rpm
```

Then configure and start it:

```bash
# Create the live config from the shipped example and fill in your tokens/IDs.
sudo cp /etc/nether-voicebridge/config.example.toml /etc/nether-voicebridge/config.toml
sudo chown root:nether-voicebridge /etc/nether-voicebridge/config.toml
sudo chmod 640 /etc/nether-voicebridge/config.toml   # holds bot/appservice secrets
sudoedit /etc/nether-voicebridge/config.toml

sudo systemctl enable --now nether-voicebridge
journalctl -u nether-voicebridge -f
```

The service reads `/etc/nether-voicebridge/config.toml` and stores matrix-sdk data
(Olm crypto + state) under `/var/lib/nether-voicebridge`. See
[`docs/packaging.md`](docs/packaging.md) for the package layout and how to build
the packages yourself (`packaging/build-packages.sh`).

### Standalone binary (other distros)

For distros without `.deb`/`.rpm` (Arch, Gentoo, NixOS, …), each
[release](https://nether.codes/dark/nether-voicebridge/releases) also ships a
self-contained tarball — the binary plus `config.example.toml`, the systemd unit,
`LICENSE`, and this README — alongside a `SHA256SUMS` file:

```bash
ver=<version>   # e.g. 0.2.0
base=https://nether.codes/dark/nether-voicebridge/releases/download/v$ver
curl -LO $base/nether-voicebridge-$ver-x86_64-linux.tar.gz
curl -LO $base/SHA256SUMS
sha256sum -c SHA256SUMS --ignore-missing   # verify
tar xzf nether-voicebridge-$ver-x86_64-linux.tar.gz
```

> The binary is **amd64-only** and built against **glibc 2.36** (dynamically linked
> to **libopus**), so it needs your distro's `libopus` + `ca-certificates` and a
> glibc host — it won't run on Alpine/musl or much older glibc. There, prefer the
> Docker image, which carries its own runtime.

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

## Future directions

Ideas being considered — **none are committed, scheduled, or guaranteed.** They're
recorded here to sketch where the project *could* go, not to promise it will. Some
may turn out to be impractical or not worth the complexity.

- **An independent human code review / security audit.** The bridge handles DAVE
  (MLS) and Matrix media E2EE; a careful review by human eyes — especially of the
  key-handling and echo-exclusion paths — would raise confidence beyond automated
  checks. Worth doing before any "stable" claim.
- **Video relay (webcam / screen share).** Carrying video — and screen/app share
  *with* its audio — between Discord and Element Call, not just voice. This requires
  driving Discord with **self-bots** (ToS violation) to relay video streams.
- **Soundboard support.** Bridging Discord's soundboard clips (and/or a Matrix-side
  equivalent) through the audio core.
- **Interop with mautrix-discord ghosts.** Where a room already runs the
  [mautrix-discord](https://github.com/mautrix/discord) text/chat bridge, exploring
  whether voice ghosts could align with or reuse its existing Discord-user ghosts,
  rather than presenting a second, separate set of puppets/ghosts.

If any of these matters to you, say so in the community room or on the issue tracker
(below) — interest is what moves an idea up the list.

## Community & contributing

- **Development & support room:**
  [`#nether-voicebridge:nether.im`](https://matrix.to/#/%23nether-voicebridge:nether.im)
  — questions, help, and where the work happens.
- Issues and roadmap: the project's Forgejo repo at
  [nether.codes/dark/nether-voicebridge](https://nether.codes/dark/nether-voicebridge).

## License

Licensed under the **GNU Affero General Public License v3.0 or later**
(AGPL-3.0-or-later). See [`LICENSE`](LICENSE) for the full text.
