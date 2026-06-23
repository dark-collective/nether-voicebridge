# Changelog

All notable changes to Nether Voice Bridge are documented here. This project
adheres to [Semantic Versioning](https://semver.org/).

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
