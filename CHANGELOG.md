# Changelog

All notable changes to Nether Voice Bridge are documented here. This project
adheres to [Semantic Versioning](https://semver.org/).

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

[0.2.0]: https://nether.codes/dark/nether-voicebridge/releases/tag/v0.2.0
