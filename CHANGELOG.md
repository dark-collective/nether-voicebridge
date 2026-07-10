# Changelog

All notable changes to Nether Voice Bridge are documented here. This project
adheres to [Semantic Versioning](https://semver.org/).

## [0.3.0-alpha.15] - 2026-07-10

### Fixed
- **Ghost tasks no longer leak on a bridge's error exit (#67).** A mid-call
  fatal error on the Matrix side (e.g. a sync error carrying an invalidated
  token) used to drop the sibling futures without cancelling shutdown,
  detaching every live ghost task — their `m.call.member` was never retracted
  (phantom participants lingered in Element Call) and their key-distribution
  loops kept heartbeating to a dead call. The three top-level futures now
  cancel the shared shutdown token the moment any of them errors, so the
  others observe it and run their bounded teardown drains (ghosts retract and
  retire) before the error propagates. Also: the ghost HTTP client gained a
  request timeout so teardown can't hang on a wedged homeserver connection,
  and a dropped watch sender no longer busy-spins a ghost's mute/presence
  select. The supervisor cancels a completed bridge's root token as a
  belt-and-suspenders backstop.
- **Stale ghost membership from abort-without-await teardown (#68).** The
  presence-phase and pre-publish ghost retire paths aborted the membership
  task and immediately sent the leave, so an in-flight join PUT could be
  serialized by the server *after* the leave and strand a phantom participant
  tile. Those paths now cancel-then-await the membership task (mirroring the
  active-phase teardown), so any in-flight join settles before the leave is
  sent — the leave is always the last writer.
- **Management/supervisor robustness (#71).** Management commands are now
  accepted only in direct (≤2-member) rooms — the shared bot account is joined
  to every bridged voice room and to rooms a link probe joined it into, so
  usage text, operator-only refusals, ban notices, and approval codes can no
  longer be triggered into those shared rooms (operators keep full function
  via DM). The shared puppet pool now degrades instead of aborting: one
  bad/revoked bot token is logged and the remaining puppets keep serving,
  rather than dropping the whole pool and leaving a Discord-less zombie. And
  tenant reconcile defers a replacement that reuses a removed bridge's
  store/device/channel until that bridge has fully torn down, so the two
  sessions can't race on one crypto store.

## [0.3.0-alpha.14] - 2026-07-10

### Fixed
- **Receive-side E2EE decryptor wedge on fresh joins (#73).** In an encrypted
  room, a publisher whose frames reached the subscriber before their media key
  arrived was permanently silent on Discord — libwebrtc's inbound frame
  decryptor never recovers from decrypting-before-keyed (its E2EE state even
  reports `Ok`), and later key installs/rotations don't heal it. The bridge now
  tracks whether a peer's key was present when its track was subscribed and,
  when the key lands for a keyless-subscribed track, re-subscribes it once —
  rebuilding the receiver/decryptor against the now-keyed provider (the proven
  "key before frames" order). The key provider is also built, shared, and
  re-seeded from the peer-key cache *before* the LiveKit connect, closing the
  same frames-before-key window on disconnect-recovery rejoins. Unencrypted
  bridges are unaffected.
- **LiveKit disconnect recovery for the subscriber and ghosts (#66).** A
  terminal room death (SFU restart/outage beyond the SDK's own retries) now
  tears the subscriber session down and rejoins — keeping puppet routes and
  posted membership, replaying every delivered peer key into the fresh
  provider — instead of leaving a dead session the stale-membership reaper
  would then misread (it no longer reaps real humans during the dead-room
  window). Ghosts get the same treatment: a per-attempt event drain plus a
  reconnect loop with backoff replaces the silent ghost death, ending the
  one-way-audio aftermath of an SFU blip.

### Added
- **e2e testkit, Matrix actors, and audio-crossing scenarios (#38 Phase D2).**
  `nvb-matrix-rtc` gains a test-only `testkit` feature wrapping the private
  ghost join/publish machinery; the `nvb-e2e` harness gains MatrixSpeaker /
  MatrixListener / MatrixPhantom actors and five live scenarios (`smoke`,
  `audio_{m2d,d2m}_{unenc,enc}`) scored by the Phase-B audio oracle, all
  runnable against a live homeserver with production untouched.
- **`audio_m2d_enc_freshjoin` scenario (#73 regression).** Deterministically
  reproduces the frames-before-key ordering (the speaker streams before the
  bridge process starts) and asserts the keyless subscribe, the one-shot heal
  re-subscribe, and intact post-heal audio.

### Changed
- **Ghost key-recipient enumeration decoupled from the bot Client (#50).**
  Ghost media-key distribution enumerates recipients from raw room state
  instead of the bridge bot's matrix-sdk Client internals.

## [0.3.0-alpha.13] - 2026-07-07

### Added
- **Unlink-side disclosure notice (#64, closing the issue).** When a bridge
  link is removed — by `unlink` or by a ban's teardown sweep — the bot posts
  "no longer bridged" into the voice channel's text chat, completing the
  disclosure story started in alpha.12. Neutral wording either way (a ban is
  not broadcast in-channel). Best-effort: a bot without Send Messages on the
  channel degrades to a log warning. Removals made by hand-editing
  `tenants.toml` + SIGHUP bypass the management bot and carry no notice.

## [0.3.0-alpha.12] - 2026-07-07

### Added
- **Per-user opt-out: `/bridge optout` / `/bridge optin` (#63).** Any Discord
  user can exclude themselves from bridging, instance-wide: their voice is
  never relayed to Matrix (dropped at the capture demux) and no ghost
  represents them (existing ghosts are retired on the spot; minting — including
  the anchor's member seeding — is gated). Persisted at `<data_dir>/optout.toml`
  across restarts. The `/bridge` command is no longer restricted to Manage
  Server as a whole — the approve subcommand keeps its server-side
  Manage Server check, while optout/optin are open to everyone (Discord has no
  per-subcommand permissions). Note: clients cache command definitions —
  reload Discord (Ctrl+R) if `/bridge` shows as outdated after upgrading.
- **In-channel disclosure notice (#64, link side).** When a link is approved,
  the bot posts into the bridged voice channel's text chat: bridged to Matrix,
  voice relayed off-platform (live only, never recorded), who approved it, and
  the privacy-policy link — visible to people who join long after the
  approval. Requires Send Messages on the voice channel (degrades to a log
  warning without it). The unlink-side notice remains open on #64.
- **ToS follow-up docs and disclosure plumbing (#43).** A hostable privacy
  policy (`docs/privacy-policy.md`), the App-Verification runbook with a
  ready-to-send developer-support inquiry
  (`docs/nether-voicebridge-verification-runbook.md`), a `[management]
  privacy_policy_url` knob appended to the link/approve disclosure texts, and
  `/bridge approve` outcomes now post publicly (the in-server record of the
  manager's consent).
- **`[timing]` grace-timer profile (#38).** The lifecycle grace timers
  (quarantine backoff, voice-health sweep, anchor idle grace) are now
  configurable for the test harness — with a loud startup warning when a
  non-default profile is active. Production configs need no change.

### Internal
- Testing framework (#38): Tier-2 named lifecycle scenarios.

## [0.3.0-alpha.11] - 2026-07-07

### Added
- **Open registration (#43).** With `[management] open_registration = true`
  (default off; config validation requires BOTH two-sided-auth knobs), Matrix
  users not on the operator allowlist can DM the bot — direct invites only,
  deny-listed users refused — and self-serve a scoped command set: `link`
  (room authority + Discord approval mandatory), `unlink`/`list` limited to
  bridges they created (via the owner records), and `help`.
  `ban`/`unban`/`bans` stay operator-only. New `[limits]
  max_tenants_per_owner` caps bridges per self-serve owner (pending approval
  codes count toward it; operators exempt).
- **Standby label for idling bridge bots.** After the last Matrix user leaves
  a call while Discord users remain, the bot that lingers through the
  reconnect grace window now wears `[puppets] standby_nickname` (default
  `"[Matrix: Standby]"`, a whole nickname — the prefix is not applied) instead
  of a bare prefix that read like a participant's name.

### Fixed
- **The bridge no longer goes dormant with Discord users still in the channel
  (#59).** The Matrix call-end teardown used to release the capture anchor
  ~0.5 s after the pool had decided to keep it for the reconnect grace,
  leaving the bridge dead until a Matrix user happened to return (observed 36
  minutes on prod). The call-end path now honors the keep decision: the anchor
  idles as capture-only for the 90 s grace — a returning Matrix user gets
  instant audio with no reconnect churn — then releases to baseline.
  Process/tenant teardown still drains unconditionally.
- **A force-moved puppet bot now returns to its assigned channel (#62).** A
  moderator moving a bridge bot to another voice channel keeps the voice
  connection alive, so no disconnect fires and the health sweep saw a
  "connected" (but misplaced) bot. The pool now watches its own bots'
  voice-state events and verifies actual-vs-assigned channel (the periodic
  sweep checks it too), recovering the same bot back in ~3 seconds via the
  existing epoch-checked rejoin — including a settle pause that stops the
  in-flight move from cancelling the rejoin and needlessly quarantining the
  bot.

## [0.3.0-alpha.10] - 2026-07-06

### Added
- **Two-sided link auth (#47).** Two optional `[management]` knobs (both default
  off) make a `link` require consent from both sides of the bridge.
  `require_room_authority = true`: the sender must be a joined member of the
  target Matrix room with power level ≥ its `state_default` (a room moderator),
  not just an allowlisted operator. `require_discord_approval = true`: a link
  that passes every probe is held pending — nothing written — under a one-time
  code until someone with the Manage Server permission in the target guild runs
  `/bridge approve code:<code>` (a slash command registered by the puppet bot at
  `approval_bot_index`, default 0; that bot's application must be invited with
  the `applications.commands` OAuth scope — join scopes with `%20`, not `+`).
  Pending links expire after `approval_timeout_secs` (default 600), the ban
  list is re-checked at approval time, and both sides are notified of the
  outcome. The audit log gains `link_pending`/`approve_rejected`, and `link_ok`
  records who approved.
- **Admission budgets: the `[limits]` block (#47).** Optional hard caps for a
  hosted deployment — `max_tenants` (boot, SIGHUP reload, and `link` all
  validate against it; the whole set is rejected beyond it),
  `max_ghosts_per_bridge` (at the cap a new Discord speaker is not bridged
  until a slot frees), and `mgmt_cooldown_secs` (per-operator cooldown between
  state-changing management commands). Absent fields mean unlimited — the
  previous behavior.
- **Abuse controls (#47).** Tenants now record their `owner` (the MXID that
  linked them) and creation time. New management commands `ban <owner|guild|room>
  <value>` / `unban` / `bans` maintain deny-lists in `<data_dir>/banlist.toml`;
  a ban also unlinks every matching running tenant through the same declarative
  reconcile path everything else uses. `link` refuses banned owners/guilds/rooms
  and fails closed if the ban list is unreadable. Every state-changing command
  is recorded in an append-only `<data_dir>/audit.jsonl`.

### Fixed
- **One tenant's panic can no longer take down the whole process (#47).** Each
  bridge future is wrapped in a panic boundary at the supervisor, so a panicking
  bridge tears down like a normal exit — its neighbours and the puppet pool keep
  running.

### Internal
- Discord ToS posture documented for a hosted instance
  (`docs/nether-voicebridge-discord-tos.md`, #47/#61).
- Testing framework (#38): committed design doc, a per-PR hermetic test+clippy
  CI job, and the Tier-1 audio-integrity oracle with deterministic probes.

## [0.3.0-alpha.9] - 2026-07-05

### Added
- **`link` now verifies room prerequisites before writing anything (#47).** The
  management bot's `link` command probes the room up front instead of replying
  with an advisory checklist: it joins the room (restricted/invite-only rooms
  get a friendly "invite the bot first"), reads the raw `m.room.power_levels`
  to confirm ghosts may post `org.matrix.msc3401.call.member` (the classic
  silent-failure misconfiguration), and confirms the homeserver publishes a
  MatrixRTC LiveKit focus. Any of these failing replies with a specific,
  actionable error and leaves `tenants.toml` untouched. Discord guild coverage
  (is any puppet bot in the target guild?) is checked too but only warns —
  bots learn their guilds as they connect, so a fresh process can't tell
  "not invited" from "still connecting".

## [0.3.0-alpha.8] - 2026-07-05

### Added
- **Matrix management bot (#4).** Tenant bridges can now be managed over Matrix
  DM instead of editing `tenants.toml` by hand. A new optional `[management]`
  config block (requires a `[server.<name>]` profile; `allow` lists the
  operator MXIDs) starts a bot on the appservice bot account with its own
  device + store. It auto-joins room invites from allowlisted operators only,
  and serves: `link <name> <matrix-room-id> <discord-guild-id>
  <discord-channel-id> [server]`, `unlink <name>`, `list`, and `help`. Commands
  atomically rewrite `tenants.toml` (comments and hand-edits preserved) and
  trigger the same in-process reconcile SIGHUP uses — a rejected change rolls
  the file back, so the file never lies about the running set, and bot edits
  and hand edits never conflict. With `[management]` configured, a config with
  zero `[[bridge]]` blocks (and no tenants yet) is a valid boot — a hosted
  deployment can start empty and be provisioned entirely over DM.

### Fixed
- **Ghosts now publish an Olm fallback key at bootstrap and on session rotation
  (#55).** Previously a ghost only uploaded a finite pool of one-time keys at
  bootstrap, never replenished in-run and with no fallback — once drained, a
  client attempting to (re-)establish an Olm session with the ghost (e.g. an
  unwedge attempt) would fail its `/keys/claim` silently. With a fallback key
  published, claims keep succeeding indefinitely (`fallback: true`), keeping
  future client-side session recovery viable.

## [0.3.0-alpha.7] - 2026-07-05

### Added
- **Runtime bridge provisioning (#45).** Bridges can now be added, removed, and
  reconfigured while the process runs — the structural precondition for hosted
  bridging (#43) and the management bot (#4). A new supervisor owns the running
  bridge set; `<data_dir>/tenants.toml` (same `[[bridge]]` schema as the static
  config, explicit `name` required, `server = "…"` refs resolve against the
  static `[server.*]` profiles) declares the desired runtime set; **SIGHUP**
  (`systemctl reload`) reconciles: additions start, removals tear down cleanly
  to baseline, changed entries stop and respawn with the new config (only after
  the old instance fully unwound — store handles closed, puppet-pool key freed).
  A reload that fails validation is rejected whole and loudly; the running set
  is never partially applied. Static `[[bridge]]`s are untouched by reloads, and
  a deployment may run with zero static bridges (tenants-only). New pre-flight:
  `nether-voicebridge --check-tenants [PATH]` prints per-entry verdicts and
  exits 0/1. Runtime-added bridges are validated against the live set with the
  same invariants as startup (unique store, unique (homeserver, bot, device),
  duplicate room/channel warnings).

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
