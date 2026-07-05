# Changelog

All notable changes to Nether Voice Bridge are documented here. This project
adheres to [Semantic Versioning](https://semver.org/).

## [0.3.0-alpha.6] - 2026-07-05

### Fixed
- **A reaped phantom call membership now releases its Discord puppet (#54).** When
  a Matrix user's client died without leaving the call, the #25 reaper correctly
  emptied their stale `m.call.member` after the 45s no-LiveKit grace — but the
  retraction is a state event *sent by the bridge bot* on the phantom's state_key,
  and the membership handler attributed it to its sender: the bot, which the echo-
  exclusion then discarded. The phantom's participant entry survived, so the puppet
  sat in the Discord voice channel until a bridge restart (voice recovery would
  even re-seat it as the capture anchor). Retractions are now attributed to the
  membership's *owner* (looked up by state_key), so a reap flows through the normal
  12s leave grace and releases the puppet — verified live: force-killed client,
  puppet gone in ~75s. Also covers any third-party retraction (e.g. a moderator
  emptying someone's membership).

## [0.3.0-alpha.5] - 2026-07-05

### Fixed
- **"Robotic"/distorted Discord speakers heard through the bridge (#36) — root-caused
  and fixed.** songbird 0.6.0's DAVE receive path hands every decrypted voice packet
  to the Opus decoder with the DAVE supplemental section (truncated AES128-GCM tag,
  ULEB128 nonce, size byte, `0xFAFA` marker — typically 12–16 trailing non-audio
  bytes) still appended. Opus never errors on the trailing garbage, but CELT/
  hybrid-mode frames read bits from the packet end, so affected frames decode into
  metallic warble — transiently for desktop speakers (as their encoder switches
  modes) and near-constantly for one PS5 sender, while Discord's own clients heard
  the same speakers clean. Fixed by vendoring songbird 0.6.0 (`vendor/songbird`,
  wired via `[patch.crates-io]`) with the stored packet carrying its receive-time
  corrected payload tail so the decode slice excludes the trailer. Confirmed live:
  the permanently-affected PS5 speaker was immediately clean. Being reported
  upstream. Also bumps davey 0.1.3 → 0.1.4 (libdave-parity receive hardening).

### Changed
- **Per-guild puppet allocation & multiplexing: one guild's calls no longer starve
  every other guild of bots (#46).** A Discord bot maintains an independent voice
  connection per guild (how public music bots scale), but the allocator previously
  treated each bot as a single indivisible resource — if one guild's channel claimed
  all N pool bots, every other guild waited. The allocation unit is now
  **(bot, guild)**: each bot multiplexes engagements across guilds (its own songbird
  Call, playback mixer, SSRC, and speaking gates per guild), so N bots serve up to N
  simultaneous puppets *per guild* across unlimited guilds. Two bridged channels in
  the same guild still need distinct bots (one voice Call per guild per bot), and
  bot-stealing now effectively only occurs between channels of one guild — the
  scarce resource. Join-timeout quarantine and disconnect recovery are per
  (bot, guild) too, so a broken voice server in one guild never sidelines a bot's
  engagements elsewhere. Groundwork for multi-tenant hosting (#43/#45/#47).

### Added
- `multiguild_spike` example (`cargo run -p nvb-discord --example multiguild_spike`):
  live GO/NO-GO probe that one bot can hold two concurrent DAVE voice sessions —
  per-guild capture WAVs, distinct playback tones, soak logging.

### Verified
- Ghost Olm session rotation (#52, shipped in alpha.4) is now fully live-verified:
  membership- and max-age-triggered rotations fire on schedule, are non-disruptive
  mid-call, re-deliver keys immediately, and hold file descriptors flat.

## [0.3.0-alpha.4] - 2026-07-04

### Added
- **Ghost Olm session rotation: a listener whose client loses the ability to decrypt
  one ghost's key messages now self-heals instead of staying silent until a bridge
  restart (#52).** A listener-side dead inbound Olm session is invisible to the bridge
  (`delivered=1` — the send succeeds, the decrypt fails) and unrecoverable by the
  protocol: the client's `m.dummy` recovery is a to-device message our send-only
  ghosts never receive. Each ghost therefore opportunistically rebuilds its Olm
  machine — same device identity, fresh sessions from fresh one-time keys, exactly
  what a restart does — on call-membership changes (rate-limited to one per 90 s) and
  whenever its sessions age past 15 minutes. Worst-case one-speaker silence drops
  from "until the next bridge restart" to ~90 s in an active call (a rejoin now
  actually heals the rejoiner) and ~16 min in a fully static one. Configurable via
  `rotate_ghost_sessions` (bridge or `[server.<name>]` level; default on; Olm
  transport only).

## [0.3.0-alpha.3] - 2026-07-04

### Added
- **Device hygiene: the bridge now cleans up after its own device identities and
  survives having them deleted out from under it (#53).** Two mechanisms, born from a
  production outage where a manual homeserver device cleanup deleted the bridges' LIVE
  devices — on restart the bridge resurrected the same ids with identical keys, the
  homeserver never re-announced them, and clients silently refused to send call media
  keys forever (encrypted Matrix→Discord went mute with zero errors anywhere):
  - *Deletion self-heal:* before reusing a persisted identity (bot or ghost), the
    bridge checks whether its device keys still exist server-side; if they were
    deleted while offline it re-provisions a fresh device instead of resurrecting the
    old id — a one-time, peer-safe re-key instead of permanent silence. Fails open on
    transient errors (an unreachable homeserver never churns the identity).
  - *History-scoped pruning:* every re-mint records the abandoned id (bot:
    `<store>/retired_device_ids`; ghost: a new `retired` field in its identity blob),
    and at startup the bridge deletes exactly those ids from the homeserver. A store
    can only ever delete ids **it itself retired** — never age-based guesses — so
    parallel deployments (a dev machine and a VPS sharing one appservice) can never
    prune each other's live devices. Configurable via `prune_stale_devices`
    (bridge or `[server.<name>]` level; default on; gates pruning only — the
    self-heal is unconditional).
  - Caveat: Tuwunel currently offers no client-API device deletion for appservice
    users (no MSC4190; even a self-delete gets an unsatisfiable UIA challenge), so
    pruning there logs one WARN and retains the history — it flushes automatically
    if the homeserver ever allows it. Verified against live Tuwunel behavior.

## [0.3.0-alpha.2] - 2026-07-03

### Fixed
- **Rejoining Matrix listeners are re-keyed in ~250 ms instead of up to ~35 s** (#51):
  ghost media-key sends force a device-list re-fetch on the first send and on every
  membership change, so a listener who leaves and rejoins an encrypted call hears all
  Discord speakers near-instantly.
- **Ghost media keys survive ghost respawns** (respawn media-key cache), and each ghost
  keeps ONE stable crypto identity persisted across process restarts (#35: vodozemac
  account pickle per ghost) — ending the per-restart device churn that piled stale
  devices onto peers' device lists and wedged their Olm sessions.

### Added
- E2EE decrypt-state observability and media-key fingerprint logging: `generated`,
  `frame-encrypt`, and `sent` fingerprints can be correlated per ghost to prove
  key-path integrity without ever logging key material.

## [0.3.0-alpha.1] - 2026-06-28

Opens the **0.3.x development line** toward hosted, configure-without-install
bridging (issue #43). Unstable/alpha — expect rough edges.

### Changed
- **Encrypted-call ghosts no longer each run a full matrix-sdk crypto client,
  retiring the file-descriptor exhaustion behind "certain users don't come through
  under load."** Each Discord speaker's ghost delivered its media key via a complete
  `matrix_sdk::Client` — its own SQLite crypto store (a deadpool pool of ~32 file
  descriptors), an initial `sync_once`, and a *perpetual* `/sync` loop — so a roomful
  of encrypted speakers blew past the open-file limit, crypto sync died, and those
  speakers went inaudible (previously only worked around by raising `LimitNOFILE` to
  65536). A ghost is send-only (it never receives or decrypts), so each now uses a
  lean in-memory `matrix-sdk-crypto` `OlmMachine`, driven by an on-demand `/keys/query`
  right before a key send instead of a persistent store and a standing sync. Per-ghost
  crypto file-descriptor cost is now **zero** (independent of speaker count) and the
  per-ghost homeserver `/sync` fan-out is gone. A ghost's crypto identity is reused
  across rejoins for the process lifetime, so a speaker who leaves and rejoins (or
  returns from AFK) is re-keyed seamlessly and stays audible. No configuration change;
  encrypted calls behave the same (#44).

## [0.2.4] - 2026-06-24

### Fixed
- **A Discord voice connection that drops mid-call no longer silently blacks out the
  bridge.** A songbird `DriverDisconnect` on an active puppet was logged and otherwise
  ignored, so the bot never rejoined — and because one bot can be both a channel's
  capture anchor (Discord→Matrix) *and* a Matrix user's playback puppet
  (Matrix→Discord), a single dropped voice connection silently killed BOTH directions
  while the bookkeeping still read healthy (the real *"connection dies after a while"*).
  An unexpected `DriverDisconnect` now rejoins the same bot — keeping its anchor role and
  audio route — and falls back to the existing failover/quarantine only if the rejoin
  keeps failing; a periodic reconcile sweep also rejoins any live bot found out of voice.
  A deliberate leave is correctly *not* treated as a fault.
- **Discord speakers become audible in Matrix in ~1-3s instead of ~20-37s, and no longer
  drop out on every membership change** (#42). Three changes to encrypted ghost
  media-key delivery:
  - *Device-scoped targeting* — a ghost now keys only the device(s) actually in the call
    (from `m.call.member`), not every device its Matrix listeners own. On accounts with
    many sessions this cuts per-recipient fan-out (~12→1), eliminating doomed one-time-key
    claims against offline/stale devices and the "nobody can decrypt this ghost" windows
    — matching Element Call's own behavior.
  - *Lean ghost sync* — each ghost's crypto bootstrap uses a filtered `/sync` (timeline,
    ephemeral, presence and account data dropped; room membership + encryption kept),
    roughly halving the bootstrap and lightening homeserver load, device tracking intact.
  - *Crypto warmed during presence* — a ghost bootstraps its crypto while idling in
    presence (before a Matrix human joins) instead of every present ghost stampeding the
    homeserver the instant someone joins, so its key is ready immediately on join.

## [0.2.3] - 2026-06-23

### Fixed
- **Encrypted bridges with many simultaneous speakers: some speakers no longer go
  silently inaudible under a crowd.** Each Discord speaker's ghost is its own
  matrix-sdk client with a SQLite crypto store (db + `-wal` + `-shm`) plus sockets,
  so a roomful of speakers could exhaust the default 1024 open-file limit →
  `unable to open database file` → that ghost's crypto sync died and the speaker
  was inaudible in the encrypted call. The binary now raises its own `nofile` soft
  limit to 65536 at startup (covers Docker and manual runs, not just systemd), and
  the systemd unit sets `LimitNOFILE=65536` as a backstop. This was the root cause
  behind "certain users don't come through" on encrypted calls under load
  (complements the 0.2.2 key-retry fix).

## [0.2.2] - 2026-06-23

### Fixed
- **Encrypted bridges: a speaker no longer goes silent for up to a minute when a
  ghost's crypto client is still catching up.** Each Discord speaker's ghost is its
  own crypto client; if it sent its media key before its device-list sync had tracked
  the call's members, it reached nobody — and the key-distribution loop then drained
  its burst and dropped to the 60s heartbeat, so that speaker was inaudible in the
  encrypted call for up to a minute (*"certain users don't come through"*). The loop
  now keeps fast-retrying (~3s) until the ghost's sync catches up, bounding the
  worst-case silent window to a few seconds (#39).

## [0.2.1] - 2026-06-23

### Changed
- **Matrix auth is now appservice-login only.** The regular-account `access_token`
  config field (and `NETHER_MATRIX_ACCESS_TOKEN`) is removed; every bridge
  self-provisions its device via `m.login.application_service`. **Breaking** for any
  config that set `access_token` — drop the field.
- **Bot MXID derives with a `_bot` suffix.** When `bot_user_id` is omitted, it is
  derived as `@{sender_localpart}_bot:{homeserver}` (previously `@{sender_localpart}`).
  The bare `sender_localpart` user is the appservice's sender, which Synapse blocks
  from sync; the `_bot` namespace user syncs normally. Set `bot_user_id` explicitly
  to keep a different in-namespace bot.

### Added
- **`sender_localpart` now drives the ghost prefix** (`@{sender_localpart}_<id>`)
  instead of a hard-coded `nether-voicebridge_`, so a custom localpart works
  (closes #37). `CONFIG_PATH` pointing at a missing file now falls back to
  `NETHER_*` environment variables (Docker default-path convenience).

### Fixed
- The bridge bot's MXID (`@{sender_localpart}_bot`) shares the ghost prefix; the
  startup ghost sweep no longer misclassifies it as a ghost and retracts its own
  `m.call.member` (`is_ghost_identity` now requires a numeric suffix).
- Switching `sender_localpart` no longer orphans old `@nether-voicebridge_<id>`
  ghost memberships — they're recognized as legacy and cleaned up at startup.

### Upgrading
- Set the bridge config `sender_localpart` to the **stem of your appservice
  `namespaces.users` regex** (default `nether-voicebridge` for the standard
  registration). Keep an explicit `bot_user_id` if you were using one.
- **No appservice re-registration is required** for the upgrade itself: the bot and
  ghosts stay within your existing `@{stem}_.*` namespace (and the bot may be the
  registration's own sender user). Re-register **only** if you adopt a *custom*
  `sender_localpart` whose stem differs from the namespace regex — then update the
  regex to `@{new-stem}_.*` (reuse the same `as_token`/`hs_token` so the bridge
  config's `appservice_token` stays valid).
- Remove any `access_token` / `NETHER_MATRIX_ACCESS_TOKEN` from your config.

## [0.2.0] - 2026-06-20

First packaged release: a single Rust binary that bridges Discord voice channels
and Matrix (MatrixRTC / Element Call) room calls bidirectionally.

### Added
- **Bidirectional voice bridging.** Matrix speakers appear on Discord as
  individually-controllable puppet bots drawn from a single shared pool, allocated
  dynamically across all bridged channels (anchor capture + steal-for-activation +
  FCFS overflow). Discord speakers appear in Element Call as appservice ghosts
  (one LiveKit participant per speaker, named + avatared).
- **Per-room media E2EE, auto-detected.** The bridge reads `m.room.encryption` at
  startup and resolves `media_e2ee`/`key_transport` from it (encrypted ⇒ Olm,
  unencrypted ⇒ plaintext); both can be overridden. True per-recipient Olm key
  delivery for encrypted rooms.
- **Multi-bridge.** One process runs N independent `[[bridge]]` pipelines; shared
  `[server.<name>]` profiles; auto-derived `store_path`/`device_id` per bridge.
- **Packaging.** `.deb`/`.rpm` (nfpm) + standalone x86_64 tarball + SHA256SUMS,
  a systemd unit, and a Docker image. SIGINT/SIGTERM graceful drain.
- **Appservice self-provisioning** (no static `access_token` required) and
  appservice-asserted room joins (no `guest_access` needed).
- Discord→Matrix audio-corrosion diagnostics (#32): opt-in capture/publish WAV
  taps + always-on jitter/frame counters.

### Fixed
- **Encrypted audio breaking when the same bridge runs against more than one
  crypto store** (#30 and follow-ups). Matrix device ids are now minted
  **unique-per-crypto-store** rather than derived solely from the bridge name —
  the bot subscriber via `resolve_store_device_id` and each ghost via
  `resolve_ghost_device_salt`. A device id presenting different Olm signing keys
  across stores (e.g. a dev box and a VPS, or a wiped store) was rejected by peers
  as `SigningKeyChanged`, so media keys never arrived and encrypted calls went
  silent in one or both directions. A wiped/copied/second store now re-provisions
  a fresh device (one-time, peer-safe re-key).
- Automatic reaping of phantom `m.call.member` entries left by clients that
  disconnect uncleanly (#25), and startup retraction of the bridge's own stale
  call memberships.

### Known issues
- Re-provisioned device ids leave empty `m.call.member` tombstones and stale
  homeserver device entries (harmless `Rejecting device` log noise); cleanup is a
  follow-up.
- Intermittent Discord→Matrix audio corrosion under investigation (#32);
  diagnostics are shipped but the root cause is not yet fixed.

[0.2.1]: https://nether.codes/dark/nether-voicebridge/releases/tag/v0.2.1
[0.2.0]: https://nether.codes/dark/nether-voicebridge/releases/tag/v0.2.0
