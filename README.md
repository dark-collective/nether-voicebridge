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
> space [`#nether-voicebridge:nether.im`](https://matrix.to/#/%23nether-voicebridge:nether.im)
> or on the [Discord server](https://discord.gg/BQYxEdu7s2).

## Two ways to get a bridge

**Use the hosted instance (recommended).** We run a public instance on
`nether.im`, currently in **open beta** — free, self-serve, nothing to install:
DM `@nether-voicebridge_bot:nether.im` and link your own room in a few minutes.
Start with the wiki guide at
**[nether.wiki/matrix/voice-bridge](https://nether.wiki/matrix/voice-bridge/)**,
or the in-repo [hosted-instance notes](docs/hosted-instance.md) and
[self-serve walkthrough](docs/self-serve-bridging.md). It *is* a beta —
you may hit rough edges, and we're grateful to everyone testing it with us;
tell us what breaks in
[`#nether-voicebridge-help:nether.im`](https://matrix.to/#/%23nether-voicebridge-help%3Anether.im).

**Or self-host it.** Everything below — requirements,
[quickstart](#quickstart-from-source), [Docker](#docker),
[packages](#install-debianubuntu--fedorarhel), configuration — is for running
your own instance on your own homeserver. The same self-serve DM flow works on
your instance too, once you enable [open registration](#open-registration).

## Community

Development happens in the open on both platforms — fittingly, kept in sync by a
bridge (the [mautrix-discord](https://github.com/mautrix/discord) text bridge; this
project handles the voice side). The [Matrix development-space rooms](https://matrix.to/#/%23nether-voicebridge:nether.im) are bridged to
the matching channels on the [Discord server](https://discord.gg/BQYxEdu7s2). Same conversation on both sides; join from whichever platform you prefer.

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
- **Mute carries across.** Mute state is mirrored both ways — mute yourself in the
  Matrix call and your Discord puppet goes muted too, and a muted Discord speaker
  shows muted in Element Call.
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
- **Self-healing call roster.** On homeservers without
  [MSC4140](https://github.com/matrix-org/matrix-spec-proposals/pull/4140) delayed
  events, a client that crashes or fails to connect can leave a "phantom"
  `m.call.member` that never expires. The bridge spots call members with no live
  media after a grace period and clears them, so the roster doesn't fill with
  participants who aren't really there.

## Future directions

The north star is **hosted, mautrix-style bridging** — so you don't have to run the
appservice yourself. A server owner would invite the shared pool of "slot" bots to
their Discord and a management bot to their Matrix room, then **configure everything
over DM commands** (the way the public mautrix bridges let you `!login` and go),
bridging voice channels without installing anything. Getting there is a sequence:

**1. Lighten the cost of encrypted calls first.** Today each Discord speaker's
Matrix-side **ghost** runs a full crypto client (its own store + a perpetual `/sync`)
in encrypted rooms, which doesn't scale to many concurrent calls. Slimming the ghost
to a lean, send-only crypto identity is the precondition for hosting anyone.
([#44](https://nether.codes/dark/nether-voicebridge/issues/44))

**2. Build toward configure-without-install hosting, step by step.**

- **Runtime provisioning.** The bridge currently spawns its bridges once at startup
  from a config file. Hosting needs to add and remove a bridge *while running*, with
  tenant state that survives a restart.
- **Per-guild puppet sharing.** Let the one shared bot pool serve many Discord
  servers at once, instead of being claimed by a single channel at a time.
- **A management bot + `!` commands.** Bind a Matrix room to a Discord voice channel,
  check status, and unbridge — all over chat, with an auth model so only the right
  people can bridge a given room/server.
  ([#4](https://nether.codes/dark/nether-voicebridge/issues/4))
- **Multi-tenant hardening.** Per-tenant resource isolation, automatic room-prereq
  checks, and a careful read of Discord's terms for a public voice relay.
  ([#43](https://nether.codes/dark/nether-voicebridge/issues/43))

**3. Clean up and review.** An independent **human code review / security audit** —
especially of the key-handling and echo-exclusion paths — plus a simplification pass,
before picking the next big thing.
([#41](https://nether.codes/dark/nether-voicebridge/issues/41))

### Further out

- **Video relay (webcam / screen share).** Carrying video — and screen/app share
  *with* its audio — between Discord and Element Call, not just voice. This requires
  driving Discord with **self-bots** (ToS violation) to relay video streams.
  ([#8](https://nether.codes/dark/nether-voicebridge/issues/8))
- **Soundboard support.** Bridging Discord's soundboard clips (and/or a Matrix-side
  equivalent) through the audio core.
- **Interop with mautrix-discord ghosts.** Where a room already runs the
  [mautrix-discord](https://github.com/mautrix/discord) text/chat bridge, exploring
  whether voice ghosts could align with or reuse its existing Discord-user ghosts,
  rather than presenting a second, separate set of puppets/ghosts.
  ([#12](https://nether.codes/dark/nether-voicebridge/issues/12))

The live roadmap board is at
[the project's Projects tab](https://nether.codes/dark/nether-voicebridge/projects).
If any of this matters to you, say so in the community room, on
[Discord](https://discord.gg/BQYxEdu7s2), or on the issue tracker
(below) — interest is what moves an idea up the list.

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

Don't want to self-host? See [Use the hosted instance](docs/hosted-instance.md)
to bridge your channel on the project's shared deployment instead.

- A **Matrix homeserver with MatrixRTC / Element Call working** (lk-jwt-service +
  a LiveKit SFU). The bridge is just another call participant — it does not deploy
  or manage the RTC infrastructure.
- One or more **Discord bot applications** (the puppet pool), each invited to your
  guild(s) with *Connect, Speak, Use Voice Activity, Change Nickname*. See
  [`docs/discord-bot-setup.md`](docs/discord-bot-setup.md) for a full walkthrough.
- An **appservice registration** on the homeserver for the bridge bot and the
  Matrix-side ghosts (see `appservice-registration.yaml`). Its users-namespace
  regex stem must equal the bridge's `sender_localpart`, covering both the
  `@{stem}_bot` bot and the `@{stem}_<id>` ghosts.
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
holds bot tokens and Matrix appservice tokens.

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
`SENDER_LOCALPART` is set, the bot MXID is derived as
`@{localpart}_bot:{homeserver}` (the `_bot` suffix avoids the appservice's bare
sender user, which Synapse blocks from sync). `SENDER_LOCALPART` is also the stem
of the ghost MXIDs (`@{localpart}_<id>`), so it must equal the stem of your
appservice users-namespace regex — set the regex to `@{localpart}_.*` so it covers
both the `_bot` user and the ghosts. Set `BOT_USER_ID` explicitly only to override
the derived bot (it must still be in that namespace, or be the registration's own
sender user).

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
- **`sender_localpart` is the appservice namespace stem** — it derives the bot
  (`@{stem}_bot`) and the ghosts (`@{stem}_<id>`), so it must equal the stem of your
  registration's `namespaces.users` regex (default `nether-voicebridge`). Set an
  explicit `bot_user_id` only to override the derived bot. Upgrading an existing
  deployment needs no appservice re-registration unless you change this stem — see
  the CHANGELOG "Upgrading" note.
- **LiveKit endpoints are auto-derived** from the homeserver's `.well-known`
  `rtc_foci` and the lk-jwt response — not configured by hand.
- **`media_e2ee` is auto-detected** per bridge from the room's
  `m.room.encryption` state (encrypted ⇒ on with per-ghost Olm, unencrypted ⇒
  off); it's an optional override now — set it only to force a value.
  `key_transport` likewise auto-selects (`olm` for an encrypted room, else
  plaintext).

## Runtime bridges & the management bot

Bridges don't have to be static config. Two ways to add/remove them at runtime:

- **`tenants.toml`** (under `data_dir`): the same `[[bridge]]` schema (explicit
  `name` required, `server = "…"` references the static config's profiles).
  Reload with SIGHUP (`systemctl reload nether-voicebridge`); the running set is
  reconciled — additions start, removals stop cleanly, changed entries restart.
  Pre-flight edits with `nether-voicebridge --check-tenants`.
- **The management bot** (`[management]` in config.toml): allowlisted operators
  DM the bridge's bot user on Matrix and drive the same mechanism with commands
  — `link <name> <room-id> <guild-id> <channel-id>`, `unlink <name>`, `list`,
  `ban`/`unban`/`bans`, `report`/`reports` (see [Abuse controls](#abuse-controls)),
  `help`. The bot writes `tenants.toml` and triggers the reconcile in-process,
  rolling the file back if validation rejects it, so bot edits and hand edits
  never conflict. It only accepts room invites from allowlisted MXIDs. With
  `[management]` set, a config with zero `[[bridge]]` blocks is valid — bridges
  can be created entirely over DM. Requires at least one `[server.<name>]`
  profile; see `config.example.toml`.

The usual room prerequisites apply to every bridge however it's created: the
Matrix room must allow PL 0 for `org.matrix.msc3401.call.member`, restricted
rooms need the bot invited, and the puppet bots must be invited to the Discord
guild. The management bot's `link` verifies all of this up front — it joins the
room, reads the raw power levels, and confirms MatrixRTC discovery before
writing anything, replying with a specific fix if a prerequisite is missing
(puppet-bot guild membership is a warning only, since bots learn their guilds
as they connect). Note the bot must also be able to *speak* in the room you DM
it from: in a room whose power levels block PL-0 message sends it stays mute.

### Two-sided link auth

By default an allowlisted operator may `link` any joinable room to any channel
the puppet bots can reach. For a hosted/multi-tenant deployment, two optional
`[management]` knobs make a link require consent from **both** sides:

- `require_room_authority = true` — the person running `link` must be a joined
  member of the target Matrix room with an **explicit individual moderator
  grant**: a `users[them]` power-level entry above the room's `users_default`
  and at least 50 (the Element "Moderator" level), not just an allowlisted
  operator. Keying on an explicit above-default grant (rather than
  `state_default`) means a room can't authorize every member by lowering
  `state_default` or raising `users_default` (#86).
- `require_discord_approval = true` — a `link` that passes every probe is held
  **pending** (nothing written) and answered with a one-time code. Someone with
  the **Manage Server** permission in the target Discord guild must run
  `/bridge approve code:<code>` within `approval_timeout_secs` (default 600 s);
  only then is the bridge created, and both sides are notified. The slash
  command is registered by the puppet bot at `approval_bot_index` (default 0,
  the first `[puppets]` token) — (re-)invite that bot's application with the
  `applications.commands` OAuth scope (in addition to `bot`) or the command
  will not appear. Separate the scopes with `%20`, not `+`
  (`&scope=bot%20applications.commands` — the `+` form can silently drop the
  second scope). Discord hides the command from members without Manage
  Server, and the bridge re-checks the invoker's permission server-side.

### Open registration

With both two-sided knobs on, `open_registration = true` turns the management
bot into a self-serve service: any Matrix user (not just the `allow` list) can
DM the bot — direct invites only, so no one can pull it into arbitrary rooms —
and use a scoped command set: `link` (room authority + Discord approval are
mandatory), `unlink` and `list` limited to bridges they created, `report` (flag
an abusive bridge — see [Abuse controls](#abuse-controls)), and `help`.
Operator commands (`ban`/`unban`/`bans`, `reports`, unscoped unlink/list) remain
allowlist-only, deny-listed users are refused at the door, and
`[limits] max_tenants_per_owner` caps how many bridges one user may own
(pending approvals count). Config validation refuses `open_registration`
without both `require_room_authority` and `require_discord_approval` — without
those, any Matrix user could bridge rooms and guilds they don't control.

**Invite-only middle tier (#82).** To run a closed beta rather than a fully
public instance, set `[management] self_serve_allow` — then only those MXIDs
(plus operators) get the self-serve tier; everyone else is ignored. The config
value seeds `<data_dir>/self_serve_allow.toml` once; thereafter operators edit
it live over DM with `serve-allow <@mxid>` / `serve-remove <@mxid>` (and
`serve-open` to reopen to everyone, `serve-list` to show it). Unset ⇒ open to
every Matrix user.

For a hosted instance, set `privacy_policy_url` — it is appended to the link
and approval disclosure texts, and the `/bridge approve` outcome is posted
publicly in the channel where it was run, so every bridged server has a
visible record of the link and its approver. A hostable policy template lives
at `docs/privacy-policy.md`, and `docs/nether-voicebridge-verification-runbook.md`
covers the 100-server App Verification gate. Point end users at
`docs/self-serve-bridging.md` — the step-by-step walkthrough of the DM `link`
flow and the approval code, written for the people doing the linking.

### Abuse controls

Voice content is unmoderatable by design (never recorded or transcribed), so
the bridge's abuse tooling is **reactive**: a durable audit trail plus a report
path that feeds the deny-list.

- **Audit trail** — every lifecycle-security event (`link`, `approve`, `unlink`,
  `ban`/`unban`, `report`, and invite accept/reject decisions) is appended as one
  JSON line to `<data_dir>/audit.jsonl`, recording who, which guild/room, when,
  and — for approvals — the approver. An operator can review it over DM without
  host access with `reports [count]`, which renders the most recent reports each
  next to the exact `ban` command to run.
- **`report <owner|guild|room> <value> [reason]`** — available to self-serve
  users too (the people most likely to be harmed), this files an abuse report to
  the audit trail. It is deliberately advisory: a report **never** removes a
  bridge on its own (auto-acting on untrusted input would itself be an abuse
  vector) — an operator reviews it and decides whether to `ban`. Reports are
  rate-limited (shared with the `mgmt_cooldown_secs` budget) so they can't flood
  the trail, and free-text reasons are length-capped.
- **`ban <owner|guild|room> <value>`** — operator-only; deny-lists the target and
  tears down every matching tenant through the normal reconcile path. Banned
  owners lose self-serve access entirely; the deny-list is re-checked at approval
  time so a ban that lands while a link is pending still blocks it.

## Community & contributing

- **Development & support space:**
  [`#nether-voicebridge:nether.im`](https://matrix.to/#/%23nether-voicebridge:nether.im)
  — questions, help, and where the work happens.
- Issues and roadmap: the project's Forgejo repo at
  [nether.codes/dark/nether-voicebridge](https://nether.codes/dark/nether-voicebridge)
  — see the [roadmap board](https://nether.codes/dark/nether-voicebridge/projects).

## License

Licensed under the **GNU Affero General Public License v3.0 or later**
(AGPL-3.0-or-later). See [`LICENSE`](LICENSE) for the full text.
