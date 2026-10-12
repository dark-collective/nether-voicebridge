# Changelog

All notable changes to Nether Voice Bridge are documented here. This project
adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- **Companion interface, phase 1 (off by default):** `[companions]` starts an HTTP listener at `/_nvb/v1` for
  companion tools. Keys are issued, listed and revoked in the management DM (`companion key create|list|revoke`;
  room keys need authority over the room, self keys act for your own pairings; only a hash of each key is stored, in
  `companions.toml`). A key holder can open an `observe` lease: a server-sent-events stream of the room's represented
  Discord members (ghost, display name, whether Discord reports them streaming), whether the call is active, and its
  listener count. Leases are time-bounded, end when their event stream stays closed for `sse_grace_secs`, end at once
  when their key is revoked or the room is unlinked, and every open/renew/end is recorded in `audit.jsonl`. Other
  lease kinds can already be issued on keys and answer `kind_unavailable` until they are served.

## [0.4.8] - 2026-10-11

**Hanging up both halves of a paired account no longer flashes a ghost into the call.**

### Changed
- **Paired accounts (ADR 0001):** when a paired person leaves the Matrix call while their own Discord account is still
  in the channel, the bridge now waits `[timing] paired_leave_grace_ms` (30 s) before giving that account a ghost. A
  person who hangs up both no longer flashes into the call as their own ghost for a few seconds.
- **Logging:** audio from a Discord speaker the bridge deliberately doesn't ghost (a paired person speaking through
  their own account, a configured exclusion) is no longer reported every second as a warning ("speaker frames
  dropped — no ghost sink yet"). Frames from a speaker not yet matched to a Discord user still warn.

## [0.4.7] - 2026-10-11

**A paired account's Discord session can drop and come back without a failover.**

### Changed
- **Paired accounts (ADR 0001):** when a paired person's own Discord session leaves the voice channel while they
  are still in the Matrix call, the bridge now waits 30 s (`[timing] paired_leave_grace_ms`) before bridging them
  through a puppet again and sending the "your Discord session left" DM. A reconnect within that time changes
  nothing and sends no DM.
- **Failover DM** names the room (its name when the management bot knows it) and links it, instead of showing the
  bare room id.

## [0.4.6] - 2026-10-11

**No more Discord-side "RTCP decryption failed" warnings.**

### Fixed
- **Logging:** Discord voice control packets of a type the RTCP parser doesn't read (feedback, goodbye and
  similar) arrived with no contents, so decrypting them always failed and logged a warning about once a second
  per call. They're now skipped quietly; audio was never affected.

## [0.4.5] - 2026-10-10

**Quieter, more useful Discord voice logs.**

### Changed
- **Logging:** the Discord-side "RTCP decryption failed" warning now names the sending participant
  (SSRC and Discord user), the RTCP packet type and per-sender success/failure counts, and is
  rate-limited instead of logging every packet (it was flooding the journal at ~1/s per call).

### Fixed
- **Release packaging:** the SHA256SUMS step fetches the static tarballs by attachment URL (the
  v0.4.4 release had to be finished by hand).

## [0.4.4] - 2026-10-07

Re-release of 0.4.3 (whose release packaging failed before publishing — no 0.4.3 downloads exist).

### Fixed
- **Release packaging:** the AppImage build resolved its tool paths after changing directory and failed;
  they are now resolved up front.

## [0.4.3] - 2026-10-07 [not published]

**A Discord user who left no longer lingers in the Matrix call.**

### Fixed
- **Phantom Discord users in the call.** When a Discord user left, their ghost's `m.call.member` was retracted
  with a single request; if the homeserver was slow or erroring, that request failed and the ghost stayed
  listed in the call until its membership expired (up to 4 h — longer in clients that ignore expiry). Each
  bridge now sweeps its room every minute and retracts call memberships of ghosts it no longer runs: its own
  (matched by this store's ghost device salt, so a second bridge on the same room is never touched) and any
  expired ghost membership. Leave failures now log the full error chain.

### Added
- **More release downloads:** per arch (`x86_64` / `aarch64`), a **fully static** binary tarball
  (`…-linux-static.tar.gz`; no runtime dependencies, runs on Alpine/musl and old glibc) and an **AppImage**,
  alongside the existing `.deb`, `.rpm`, and tarball. `SHA256SUMS` now covers every asset. See `docs/packaging.md`.
- **AppImage self-update (zsync):** release AppImages embed update information, so AppImageUpdate, Gear Lever,
  AppImageLauncher, etc. can update them in place from the stable channel.

### Changed
- **Faster releases:** release builds keep a BuildKit layer cache between runs (dependencies are rebuilt only
  when `Cargo.lock` changes), the Rust base image is pinned (`rust:1.96.0-bookworm`), and a release stays a
  draft until every asset is attached.

## [0.4.2] - 2026-10-07

**A paired person in the call keeps hearing the Discord side (ADR 0003).**

### Fixed
- **Discord audio stopped reaching a paired person.** While both halves of a pairing were present, the bridge
  counted the person as nobody: with no other Matrix participant, the Discord capture bot left after its idle grace
  and the Discord speakers' ghosts went silent. The Matrix half of a present pairing now counts as a **listener**:
  "call active" and the capture anchor follow listeners, so the anchor stays — capture-only, standby nickname —
  while the person is in the call. Puppets and ghosts are unchanged: still none for a present pair, and neither
  half's audio is forwarded.
- A capture-only anchor whose voice join failed stayed recorded as the channel's anchor; it is now cleared and,
  if a listener needs capture, replaced.

### Added
- `PlaybackRouter::listen` / `unlisten` (default no-ops) and `LinkBoard::matrix_listeners` /
  `matrix_listener_only`.

## [0.4.1] - 2026-10-01

**Hotfix: a bridge-owned member's audio track no longer mints a puppet.**

### Fixed
- fix(subscriber): the LiveKit track consumer now honours linked-identity
  exclusion (ADR 0001) like the membership path does. A subscribed audio track
  from a paired/service/excluded member assigns a puppet only while the member
  is ordinary, re-checks about once a second, releases when they become
  bridge-owned and assigns again when they stop being. Seen live after 0.4.0:
  a relay account's muted microphone track created a silent puppet although
  its membership was correctly skipped.

## [0.4.0] - 2026-10-01

**Linked identities: a person present on both sides is one person to the
bridge (ADR 0001).**

### Added
- **Pairing.** A Matrix user links their own Discord account(s) with a proof on
  each side: `pair` (management DM) mints a one-time code, `/bridge pair
  code:…` (run as that Discord account) redeems it. While a paired Discord
  account is in a bridged voice channel *and* its Matrix account is in the
  room's call, the bridge creates no ghost for the one and no puppet for the
  other, forwards neither, and counts neither as a human. Re-evaluated from
  live presence on every change — nothing to switch off later. `unpair`,
  `pairings`, `pair delegate <@mxid>`, `/bridge unpair`.
- **Service accounts per room.** `relay allow|deny|list <room-id> [@mxid]`
  (room authority or operator): a Matrix account in the call that gets no
  puppet and is not counted — for user-run helpers such as stream relays.
- **Config overrides** `excluded_discord_user_ids` / `excluded_matrix_user_ids`
  per bridge: unconditional exclusion, the operator's override and the path for
  instances without a management bot.
- **Failover notice.** When a paired person's Discord session drops while they
  stay in the call, they (and their delegate) are told once per 10 minutes that
  they are bridged through a bot until it returns.
- Exclusion transitions are first-class lifecycle events: a companion client
  arriving late retires an already-minted ghost/puppet; one dropping re-creates them.
- New data files `<data_dir>/pairings.toml` and `relays.toml`; privacy policy
  and self-serve docs updated; `docs/adr/` introduced.

### Changed
- Config schema gains two optional per-bridge lists (see above); the
  management bot and `/bridge` command gain the verbs listed above.

## [0.3.12] - 2026-10-01

**A Matrix screen share no longer plays its audio into Discord voice.**

### Fixed
- fix(subscriber): screen-share audio/video from Matrix is never bridged into
  Discord voice (#142). The subscriber drops publications whose source is
  screen share or screen-share audio at publish and subscribe time and
  unsubscribes from them; a screen share has no Discord voice counterpart, and
  forwarding the share's audio through a puppet could double it or feed it back. Supersedes 0.3.11, which was tagged but never
  deployed.

## [0.3.11] - 2026-10-01

**Harness-only release: the nightly audio oracle no longer fails an intact
capture because its first second was rough, and the build stays lint-clean
on Rust 1.99.**

### Fixed
- **e2e audio oracle: global alignment could lock onto the silent tail of a
  capture.** Nightly #418 scored `audio_d2m_unenc` at mean NCC 0.009 although
  cycles 2–7 were intact at ≥ 0.98: a slip inside the first cycle zeroed the
  true lag's refinement score (computed over the first second only) and a lag
  overlapping ~120 ms of tail dither won. Alignment now requires candidates to
  overlap at least half the shorter signal, refines over 3 s, and ranks
  candidates by correlation over their whole overlap. Replaying the failed
  night's WAVs through the fixed oracle gives mean NCC 0.860, 5/8 → PASS.

### Changed
- CI: `clippy::double_must_use` is allowed at the root of the crates that use
  `async_trait` — Rust 1.99's clippy flags the `#[must_use]` the macro puts on
  its generated methods, which broke every `-D warnings` run.

### Added
- `crates/audio/examples/oracle_replay.rs`: replay the e2e audio oracle
  offline on a probe/captured WAV pair from a run artifact.

## [0.3.10] - 2026-09-30

### Fixed
- **The bridge bot's own E2EE roster membership now refreshes too.** In an
  encrypted room the bot posts an `m.call.member` so peers send it their media
  keys; it was posted once per call with the default 4-hour expiry, so a single
  encrypted call longer than 4 hours silently dropped the bot from the roster
  and stopped key delivery. It now uses the same keeper as the ghosts: fixed
  `created_ts` per activation (kept across a disconnect-recovery rejoin),
  `expires` recomputed on every post, hourly refresh until the call ends.

## [0.3.9] - 2026-09-30

### Fixed
- **Long-present Discord users vanished from the Matrix call after 4 hours.** A
  ghost's `m.call.member` carries `expires` relative to `created_ts`, and 0.3.8
  pinned `created_ts` across re-posts (so the call-focus election stays stable)
  — which meant a re-post inherited the *original* expiry and the server
  deduplicated it. A Discord user who had sat in the channel for more than
  4 hours before a call started was invisible to Commet and Element even
  though their audio flowed (live: Light Voice, 2026-09-30). Ghosts now do
  what Element Call does: `created_ts` stays fixed, every post computes
  `expires` so the membership is valid for 4 hours *from now*, and a keeper
  task re-posts hourly for as long as the ghost lives (presence and active
  phases, and after a migration).

## [0.3.8] - 2026-09-30

**Multi-SFU MatrixRTC (MSC4195 `multi_sfu`): users on other homeservers can be
bridged again.** Element Call 0.21 (July 2026) made multi-SFU the default:
every participant publishes on its *own* homeserver's LiveKit SFU instead of
the whole call electing one. The bridge only ever joined the SFU from its own
`.well-known`, so a matrix.org user on a current Element Call was never heard
(their audio went to matrix.org's SFU) — and the stale-membership reaper read
their absence from *our* SFU as "phantom" and kept emptying their live
membership (84 times in one evening on the `Limited-VC` tenant, 2026-09-28;
the `bsch` tenant never worked for the same reason).

### Added
- **Per-SFU subscriber fan-in.** Every human member's publishing SFU is
  resolved from their `m.call.member` exactly as matrix-js-sdk does
  (`multi_sfu` ⇒ own `foci_preferred[0]`; `oldest_membership` ⇒ the oldest
  membership's transport). For each such SFU that isn't ours the RTC loop runs
  one extra subscribe-only LiveKit session — JWT minted at *their* lk-jwt
  service with our OpenID token, the standard federated path — feeding the
  same puppet router. Reconciled on join, on membership change, when a foreign
  session dies (backoff 15 s → 5 min), and on every reaper tick; all dropped
  at call end. E2EE shares the primary session's key provider (keys are per
  publisher, SFU-agnostic) and the #73 heal gate.
- **Ghosts publish on the call's *elected* SFU and follow it.** Legacy
  `oldest_membership` clients (Commet, Element Call < 0.21) listen only on the
  SFU elected by the oldest active membership; when that member is on another
  homeserver, a ghost publishing on our SFU is inaudible to them (live-observed
  with Commet on matrix.org, 2026-09-29: Discord heard Matrix, Matrix heard
  nothing). Each ghost now reads the election from room state when it connects,
  publishes there (JWT from that SFU's lk-jwt via its own OpenID token), and
  migrates mid-call when the election moves — the old SFU connection is dropped,
  the track republished, and the membership re-posted with the new
  `foci_preferred` and its original `created_ts`, so re-posting never changes
  who is oldest. Ghost and subscriber memberships keep the spec's legacy
  `focus_selection: oldest_membership` with `foci_preferred` naming exactly
  that SFU: Commet's election skips any `focus_selection` it doesn't know, so a
  `multi_sfu` ghost was invisible to it (it elected nothing and fell back to
  its own SFU — live-observed), while multi-SFU Element Call resolves an
  `oldest_membership` member to the oldest member's SFU, which is where the
  ghost publishes. Live-verified 2026-09-29 with Commet on matrix.org in both
  orders (ghost first: Commet followed to nether.im; Commet first: ghost
  published on matrix.org, then migrated home when Commet left).
- **Defensive subscribe.** Not every client follows the election, so the bridge
  also subscribes on each human member's own `foci_preferred[0]`, and the
  reaper only judges a member once both its resolved and its preferred SFU are
  watched. A mis-electing client is heard anyway and can never be reaped alive.

### Fixed
- **The #25 reaper only judges members on SFUs it is connected to.** A member
  whose resolved SFU the bridge has no live session on (or whose transport
  can't be derived) is *unobservable*, not absent: never reaped, no phantom
  clock. Losing a foreign session mid-grace clears its members' clocks.
## [0.3.7] - 2026-09-28

**Commet callers in encrypted rooms can now be heard on Discord, and the next
client whose media-key event doesn't fit our type shows up as a WARN instead
of silent one-way audio.**

### Fixed
- **Matrix→Discord silent for Commet callers in encrypted rooms.** Commet's
  experimental encrypted calls send `io.element.call.encryption_keys` without
  `member.id`; the bridge's strict content type rejected it, matrix-sdk dropped
  the event without a log line, and the caller's frames stayed undecryptable
  (`MissingKey`) while the ghosts' keys reached them fine — one-way audio.
  `member.id` and `sent_ts` are now optional; neither is used for identity
  (that is `sender:claimed_device_id`).

### Added
- To-device interop tripwire: an `io.element.call.encryption_keys` event that
  fails to parse is now logged at WARN with its field shape (names only, never
  key bytes) instead of vanishing.

## [0.3.6] - 2026-09-27

**Follow-through on the 0.3.5 stall: the next silent sync stall gets logged,
and the device pile that made it slow can finally be cleaned up — by the
harness, by the bridge's own prune, and by a new operator tool.**

### Added
- **Management bot sync-loop stall watchdog.** A sync iteration that never
  completes is silent — no error, no log line — which is how the v0.3.5
  stall hid for three days. The bot now stamps every completed `/sync`
  iteration; a side task WARNs once when none has completed for 60 s and
  INFOs on recovery with the stall's length, so the next one lands in the
  journal and in nvb-watch's WARN count.
- **The e2e harness deletes the bot devices it mints.** Every bridge-under-
  test store mints a fresh device and they never went away: the e2e bot had
  3,340 devices by 2026-09-27, and the SDK claims one-time keys for *all* of a
  user's own devices on every encrypted send while holding the lock the sync
  loop needs — that pile is what made a 1 s greeting a 30 s stall. After each
  scenario's bridge exits, the harness deletes the devices recorded in its
  `data/*/device_id` markers (best effort, only ever after the process is
  gone).

### Fixed
- **Device pruning works on Tuwunel again (no MSC4190 needed).** The #53
  prune's fallback logged in as the retired device and then issued a self
  `DELETE /devices/{id}`, which Tuwunel refuses with an empty UIA challenge —
  so retired devices were never actually deleted. The fallback now calls
  `POST /logout` with that session instead, which removes the device without
  UIA (re-probed on 1.9.2). Shared by the startup prune, the harness cleanup
  above, and `ops/nvb-prune-devices.sh`.
- **`ops/nvb-prune-devices.sh`** — reports (and with `--apply` deletes) a
  bot's dead devices: anything not in a `device_id` marker under the data
  dir and unseen by the server for 24 h. Dry run by default; live ids are
  never touched (the #53 wedge). Deletes via masqueraded `DELETE`, else
  login-as-device + `/logout`.
- **nvb-watch counts the bot's devices daily** (`| devices: …` in the digest)
  and alerts at `DEVICE_TRIPWIRE` (default 150); an optional `E2E_BOT_MXID`
  watches the harness identity too.

## [0.3.5] - 2026-09-27

**A hotfix for the management bot: a DM invite accepted right after a
rejected room invite was never greeted, because the bot's own "I only work
in DMs" notice stalled its sync loop for half a minute.**

### Fixed
- **The management bot no longer goes deaf for ~30 s after declining a
  non-direct invite.** The #101 explanation ("I only work in direct
  messages…") was sent through the SDK as an encrypted DM. That send claims
  one-time keys for the bot's own sibling devices and then holds the crypto
  store transaction while the bot's sync loop waits on the same lock at the
  end of its iteration — traced live on 2026-09-27: the sync loop went silent
  for ~31 s. Any DM invite the bot accepted in that window was joined
  server-side but never seen as joined by the SDK, so the newcomer got no
  greeting and no replies until the stall cleared (nightly
  `mgmt_self_serve_link` red from 2026-09-24, every run at the second DM,
  with no code change — the trigger surfaced with Tuwunel 1.9.2). The
  explanation is a one-line, non-sensitive notice, so it now goes out as a
  **plaintext** direct room created and posted over the appservice-asserted
  client API, which touches no SDK state at all: no key claim, no Olm
  session, no store transaction, nothing to stall. Operator outreach
  (`msg`, `announce`, misconfiguration heads-ups) is unchanged and stays
  end-to-end encrypted.

## [0.3.4] - 2026-08-20

**The opt-out promise now holds everywhere — including the operator's own
diagnostics — and newcomers get a guided walkthrough instead of a wall of
text.**

### Fixed
- **Opted-out users are never written to diagnostic capture WAVs (#70).**
  With `capture_wav` diagnostics enabled, the per-SSRC WAV tap ran before the
  `/bridge optout` gate, so an opted-out user's voice was recorded to disk
  every 20 ms even while being correctly dropped from Matrix. The tap now
  sits behind both speaker attribution and the opt-out check — consent must
  be provable before any frame touches disk, so opted-out and
  not-yet-attributed speakers are never recorded.
- **A voice channel linked at runtime no longer strands phantom participants
  (#70).** The capture anchor's ghost seeding never recorded presence, so on
  a channel created by the self-serve `link` flow a seeded Discord user's
  leave was an unobserved edge: their ghost stayed in the Element Call
  indefinitely and kept the anchor bot in voice as if speakers were present.
  Seeding now records presence exactly like the gateway paths (bots skipped,
  idempotent across the pool).
- **A user who leaves during the ghost member-lookup no longer gets a
  phantom ghost (#70).** The mint path re-checks presence after its awaited
  member HTTP lookup, shrinking the leave-vs-mint race from lookup-sized to
  lock-acquisition-sized.
- **Echo-exclusion survives songbird's in-driver full reconnect (#70).** A
  reconnect's re-Identify may carry a fresh SSRC that was never added to the
  puppet's exclusion set — while the stale SSRC stayed excluded and would
  silently mute any human later handed it by Discord's SSRC recycling. Both
  connect events now register the SSRC, swapping the stale one out.

### Added
- **`tutorial` — a guided, step-by-step walkthrough over DM (#102).** Seven
  screens from "what this does" to the Discord-side approval, one per
  message: `next` advances, `stop` ends, and unknown chatter in a DM now
  offers the tutorial instead of a terse "Unrecognized command". Instance
  specifics (guide URL, approval bot name, open-beta notice) come from
  config. Illustrated steps follow once the screenshot assets land.
- **Declined room invites are explained (#101).** Inviting the bot to a
  regular room (instead of a direct message) is still declined — the
  DMs-only contract — but the bot now DMs the inviter the why and the fix,
  rate-limited to once per inviter per hour.
- **The greeting waits out slow federated joins (#101).** The join greeting's
  retry window grew from ~14 s to ~60 s, so a new user on a slow federating
  homeserver no longer lands in a silent room.

### CI / ops
- Every code push to a feature branch now runs a fast live management-suite
  subset (#107); nvb-watch gained a canary that alerts on invites to the
  dormant conventional bot address nobody serves (#96); `crates/audio`
  adopts `as_chunks` for Rust 1.98's new clippy lint.

## [0.3.3] - 2026-08-20

**Two more silent-death modes closed: a homeserver that isn't up yet can no
longer permanently kill the management bot, and a Discord gateway loop can no
longer burn a puppet's token.**

### Fixed
- **The management bot survives a homeserver-unreachable boot (#126).** If the
  homeserver (or its front proxy) was down when the process started, the bot's
  appservice login failed once and the bot stayed dead until a manual restart —
  masked, because the bridges and puppets carry on and `systemctl is-active`
  reports healthy while self-serve DMs and the operator console are silently
  gone (observed live 2026-07-22; bridges got their half of the fix from
  0.3.2's restart backoff). Login and the initial sync now retry
  network-shaped failures (5xx, timeouts, transport errors) with capped
  exponential backoff (5 s → 60 s, indefinitely, a WARN per attempt), while
  definite config errors (401/403 bad `as_token`, 404 wrong URL) still fail
  fast and loudly — decided structurally on a typed error, never on its text.

### Added
- **Gateway identify-storm circuit breaker (#129).** Discord auto-resets a
  bot's token after ~1000 gateway connections in a day, and serenity retries
  IDENTIFYs forever with only a ~5 s floor — so a server-side rejection loop
  (the failure that killed a neighbouring bridge's bot on 2026-08-08) would
  cost a puppet its token inside a day. Every gateway `ready` is one completed
  IDENTIFY, so the pool now counts readies per bot: ten within ten minutes
  parks the bot — its assignments fail over through the per-(bot, guild)
  allocator (anchors included), its gateway is shut down, and a loud ERROR is
  logged — then a one-hour cooldown rebuilds the client and re-arms it. Worst
  case a still-broken bot spends ~200 identifies/day, safely inside the budget,
  while a healthy pool covers its channels.
- **Gateway lifecycle logging by default (#129).** The built-in log filter now
  includes `serenity=info`, so reconnect/session-invalidation lines reach the
  journal on every deployment — previously the storm risk class was entirely
  invisible (14 days of production journal contained zero serenity lines).
- **Live-suite hardening (#124/#100, harness-only).** The three-night
  `mgmt_self_serve_link` red was the harness's own transaction ids colliding
  across nightly runs — the homeserver deduplicates them per token *forever*,
  silently swallowing the command sends. Transaction ids now carry a per-run
  nonce, reply scans require the command marker to be visible before matching,
  and `mgmt_dm_pending_reap` is expected to pass (its cold-outreach failure
  mode is fixed on Tuwunel 1.8.2).

## [0.3.2] - 2026-08-14

**A network blip could no longer kill a bridge — and a dead bridge can no longer
hide.** On 2026-08-11 five bridges on the public instance died inside ten
minutes; three stayed dead for three days before anyone noticed.

### Fixed
- **Transient `/sync` errors are no longer misread as fatal auth failures
  (#130).** The sync loops classified errors by substring-matching the
  *stringified* error for `"401"`/`"403"`/`"M_UNKNOWN_TOKEN"` — and a
  `matrix_sdk` error stringifies to include the request URL, which for `/sync`
  carries `?since=<token>`. While the homeserver's sync stream sat at position
  `5401xxxx`, every bridge's token contained the substring `401`, so an ordinary
  transport error ("error sending request for url …") was declared a fatal auth
  failure and the supervisor killed the bridge with no retry. Classification is
  now structural — HTTP status plus Matrix errcode, with a transport error
  (no HTTP response at all) never fatal — behind a pure, unit-tested decision
  function. The same bug and fix apply to the management bot's sync loop, and
  the raw appservice-join path now branches on a typed status instead of
  `contains("403")`.
- **A failed bridge restarts itself with exponential backoff (#130).** Any
  bridge task that returns an error or panics is now respawned after
  `bridge_restart_base_ms * 2^n` (capped at `bridge_restart_max_ms`, both new
  `[timing]` keys; 5 s → 5 min by default), with the streak reset once a bridge
  has been up past the cap. Operator stops, reconcile replacements and shutdown
  are never auto-restarted. This also closes the long-standing reboot race
  where the bridge starts before the homeserver is serving, every appservice
  login 502s, and *all* bridges die permanently.

### Added
- **`<data_dir>/bridges.json` — a machine-readable bridge roster (#131).**
  Written on every state transition and refreshed every 60 s, it names each
  bridge with `alive` / `restarting` / `down`, its origin, and its failure
  count. `nvb-watch` now alerts on any bridge that is not alive (re-alerting
  **daily** until it recovers, since the 08-11 outage produced exactly one
  alert, at 01:30 on a Monday), on a stale roster (dead-man's switch for the
  supervisor loop), and reports `bridges=N/M` in the daily digest — so a quiet
  digest asserts bridge liveness instead of merely implying it.
- **The ops scripts are in the repo** under [`ops/`](ops/) —
  `nvb-watch.sh`, `nvb-backup.sh`, `nvb-ops.conf.example`, plus
  `ops/test-nvb-watch.sh`, an offline harness that drives the real watcher
  against stubbed `journalctl`/`systemctl`/`curl`. They previously existed only
  on the production host, unreviewed and unbackuped.

## [0.3.1] - 2026-07-26

**One-way-deafness fix.** A latent race in the vendored voice stack could
permanently silence a speaker toward Matrix after a fast leave+rejoin; it was
also the true mechanism behind the #124 nightly recovery-cluster failures.

### Fixed
- **Fast leave+rejoin no longer permanently deafens the receive side (#127).**
  The vendored songbird tracked voice-gateway disconnects with a lazy tombstone
  swept every 5 s; a user who left and rejoined inside one sweep interval (a
  mod voice-kick + the #46 same-bot recovery, a quick channel hop, a Discord
  blip) had their *fresh* SSRC mapping reaped by the sweep, after which every
  DAVE-encrypted packet from them was silently discarded — the anchor went
  permanently deaf to that speaker (Discord→Matrix one-way silence) until the
  bridge cycled, with zero log evidence. A re-speaking user now cancels the
  pending tombstone and eagerly retires their previous SSRC. Root of the #124
  nightly recovery-cluster reds: the failures were timing-gated on Discord's
  voice-state propagation speed, which is why they vanished on the 07-19 quiet
  host (laggy week) and returned deterministic-looking after it sped up.
- **e2e: the listener recorder no longer double-writes a rejoined puppet.**
  After the fix above, a fast-rejoining target is briefly reachable via two
  mapped SSRCs (the dead one lingers in songbird's `silent` set for the decode
  timeout); the recorder wrote 20 ms of audio *plus* 20 ms of zero-fill per
  tick, stretching captures to ~2× wall clock as half-rate chop (oracle
  glitches ≈200). Real audio now wins; zero-fill only applies to targets with
  no write in that tick.
- **e2e: `steal_release_repromotion` no longer races the stale-membership
  reaper.** The scenario's membership-only phantoms are, to the #25 reaper,
  exactly what it exists to retract (an `m.call.member` with no LiveKit
  participant) — under the compressed 3 s `reap_grace` it emptied Alice's and
  Bob's memberships ~4 s after the subscriber connected, racing the
  steal→release→re-promotion choreography and producing both of the scenario's
  #124-era failure shapes. Its bridge-under-test now runs with `reap_grace_ms`
  pushed past the scenario's runtime via a new per-scenario `[timing]`-override
  hook (the reaper itself is covered by `reap_release` under `--profile
  real`). The cross-channel re-promotion logic was verified innocent all
  along.

### Added
- **DAVE-layer diagnostics in the vendored songbird (#127).** Every MLS
  gateway arm (prepare_epoch — including the previously-invisible ignored
  `epoch != 1` case — transitions, external sender, proposals, commit,
  welcome, session reinit) and every silent packet-drop branch in the receive
  path now logs at `debug`; the mixer WARNs (throttled) when DAVE is
  negotiated but frames leave without frame encryption — the exact
  send-side state mandatory-DAVE SFUs punish with silent non-delivery. The
  e2e harness and nightly workflow enable `songbird=debug` so artifacts
  capture all of it.

## [0.3.0] - 2026-07-14

**The 0.3.0 series goes stable.** This is the first non-prerelease since 0.2.x:
the releases page, `/releases/latest`, and the `:latest` container tags now
resolve here instead of the months-old 0.2.4. Functionally it is beta.4 plus
two verified fixes; the full story of the series is in the alpha/beta entries
below. (Semver note: 0.x still signals a young project — 1.0 is planned after
the post-stability refactor.)

### Fixed
- **Operator invites into populated rooms stick (#120).** The #85 anti-spoof
  greet-leave ran after every accepted invite, so an operator inviting the bot
  into a >2-member room saw it join and silently leave seconds later,
  breaking the manual invite-then-relink flow. Operator-accepted invites now
  keep the join (the greeting is skipped; the self-serve anti-spoof leave is
  unchanged). Guarded by the new `mgmt_operator_room_invite` scenario.

### Added
- **Per-participant LiveKit connection-quality logging (#116).** The SFU's
  view of every participant's link (WARN on Poor/Lost) — the packet-loss
  signal that choppy-audio triage previously lacked.
- Ops: the nvb-watch daily digest now reports the puppet-pool guild footprint
  with a Discord App-Verification tripwire alert (#117); the nightly e2e run
  moved to 08:30 UTC, clear of the runner host's apt-daily-upgrade window
  that was garbling its audio oracles.

## [0.3.0-beta.4] - 2026-07-13

**Invite-only rooms are now self-serve linkable.** The last structural gap in
self-serve onboarding: rooms the bot cannot join on its own no longer require
operator intervention.

### Added
- **Awaiting-invite link resume (#111).** A `link` of a room the bot cannot
  join now arms a single-use intent (default 30 min, `[limits]
  invite_intent_ttl_secs` to tune): invite the bot to that room — from the
  same account that ran `link` — and the link continues automatically to the
  normal Discord-approval step. Works for every tier; strictly matched to the
  exact requester and room; failed or abandoned resumes leave the room again.
- **`list` now shows your in-flight state**: links awaiting Discord approval
  (with the code and time left) and links awaiting your invite — so the DM
  always tells you where things stand.

### Changed
- Docs (self-serve walkthrough, hosted instance): the restricted-room flow is
  now "run `link` first, invite the bot when it asks"; puppet-pool invite
  guidance clarified (any subset in any order works; spread partial invites
  across the pool).

## [0.3.0-beta.3] - 2026-07-13

**Beta polish from live feedback.** A self-serve usability fix surfaced by a real
beta user, plus a CI-infrastructure change that stops the test harness from
flaking itself. No change to the shipped bridge's runtime behaviour beyond the
reworded reply.

### Fixed
- **Self-serve `link` failure no longer gives unfollowable advice (#111).** When
  the bot cannot join a room a self-serve user asked to bridge, the reply used to
  say "invite the bot to it first, then run `link` again" — but a self-serve
  user's invite to the bot is auto-rejected (the DMs-only gate), so that was a
  dead end. The reply is now tier-aware: operators still get the invite-me path;
  self-serve users are told to make the room publicly joinable or ask the
  instance operator to link it. (Join-failure remedy only — the pending-link
  invite-gate exemption remains future work, so #111 stays open.)

### Changed
- **CI and the live E2E harness now serialize on the shared runner host.** They
  share one physical machine, and a CPU-heavy CI build running alongside an e2e
  audio scenario garbled its timing scores (spurious reds). Both workflows now
  share a fixed concurrency group so at most one runs at a time, with `nice` on
  the CI build steps as a fallback. CI/infra only — no effect on the release
  binaries.

## [0.3.0-beta.2] - 2026-07-12

**The federation fixture pays for itself.** A second, operator-run test
homeserver (`nether.autos`, issue #109) now lets the live suite mint genuinely
REMOTE users — and it caught two real bridge bugs within its first hour that
23 same-server scenarios were structurally blind to. Both are fixed here.

### Fixed
- **Federated invite redelivery no longer makes the management bot decline
  the DM it just joined (#110).** Over federation, a still-pending invite is
  redelivered ~half a second after the bot's accept-join — inside the window
  where the SDK's local room state lags the raw appservice join. The
  redelivered invite then hit the per-sender cooldown its own first pass had
  just stamped and was declined (the #97 join-then-leave, alive over
  federation; deterministic with `mgmt_cooldown_secs` set). The invite
  handler now asks the server — the ground truth — whether the bot is
  already joined before treating a delivery as a new invite.
- **v12 room creators can link their own rooms (#112).** Room v12 gives
  creators implicit infinite power and forbids them from `users[]` — so the
  moderator-grant authority check could structurally never authorize a v12
  creator. The check now resolves the room's creator set from the create
  event (v12 room ids ARE the create event id) and accepts joined creators.
  Fail-closed: any read failure falls back to the explicit-grant path.

### Added
- **Federated live scenarios (#109).** `fed_dm_invite_lifecycle` and
  `fed_link_v12` (both passing, guarding the fixes above) plus
  `fed_cold_outreach_utd` (known-fail pinning #104 with a genuinely remote
  crypto recipient). The suite grows to 26; the `fed_*` scenarios report a
  new first-class `skipped` outcome on deployments without the fixture, so
  the suite still runs anywhere. Fixture runbook: `docs/e2e-homeserver.md`.

### Known issues
- Self-serve `link` of an invite-only room remains unsatisfiable — the bot's
  "invite me first" guidance conflicts with its own invite gate (#111).
- Cold-outreach first messages to remote encrypted recipients can still be
  undecryptable (#104) — now deterministically reproduced by the fixture.

## [0.3.0-beta.1] - 2026-07-12

**Alpha → beta.** The self-serve hosted flow has now been exercised end-to-end
by real strangers on three homeservers — every failure they found in one long
night is fixed or honestly tracked, and the live test suite grew to cover the
whole invite/DM lifecycle, encrypted DMs included. Thank you, first testers.

### Added
- **Modern (v12, domainless) room ids are fully supported (#103).** `link`,
  `ban`, and `report` accept both room-id generations, and first-contact
  federated joins send explicit `via` hints (the requester's homeserver) —
  domainless ids carry no implicit routing. Found by a real user whose
  homeserver mints the new form; their literal room id is now a unit test.
- **Invite/DM lifecycle test coverage (#99).** A crypto-capable E2EE DM test
  actor and scenarios for encrypted-DM commands, offline-invite handling, and
  bot-initiated-DM survival — the classes of failure only real federation
  exposed. Suite: 23 live scenarios.

### Fixed
- **The empty-DM reaper now asks the server before reaping (#97).** The
  alpha.25 exemption read the SDK's lagging room summary and could still
  retract a seconds-old outreach invite; the gate is now a server-side
  pending-invite query, failing safe on error.
- Shape-rejected `link` attempts are now logged (previously reply-only —
  invisible in triage).

### Known
- The FIRST message of an operator `msg`/`announce` to a user with no
  existing DM can be undecryptable for them (#104; tracked by a live
  known-fail scenario). Self-serve users who DM the bot themselves are
  unaffected.

## [0.3.0-alpha.25] - 2026-07-12

### Fixed
- **Federated DM invites no longer self-destruct (#97).** Sync redelivers a
  still-pending invite while a slow federated join is in flight; the duplicate
  used to hit the per-sender invite cooldown stamped by its own first pass and
  *decline* the just-joined room (join-then-leave, hit by the first real beta
  user). Invite handling now dedupes per room, and the cooldown arms only
  after a join actually succeeds.
- **Bot-initiated DMs survive until the invitee answers (#97).** The
  empty-room reaper no longer collects rooms that still have a pending
  invited member — previously it retracted `msg`/`announce`/outreach invites
  if the human took more than one sweep interval to accept.

## [0.3.0-alpha.24] - 2026-07-12

### Added
- **The bridge detects and announces broken room call-permissions itself
  (#95 tiers 1–2).** Every maintenance sweep checks each bridged room's
  `m.room.power_levels` for the `org.matrix.msc3401.call.member = 0`
  requirement — the most common silent bridge-breaker. On failure: a fix-it
  chat notice is posted into the room, the bridge owner gets a DM, a
  `room_misconfigured` audit event is appended, and a WARN feeds the ops
  tooling; once per room per day. The sweep now runs for every deployment
  (static bridges included) — the open-registration reap duties stay gated.
  `NVB_MAINTENANCE_INTERVAL_SECS` overrides the 5-minute tick for drills.

## [0.3.0-alpha.23] - 2026-07-12

### Added
- **Operator observability + outreach commands (#94).** `status` renders the
  puppet pool's per-bot guild footprint (connected state, guild counts, the
  distinct-guild total, tenant bridges per guild) with a ⚠ warning as any app
  nears Discord's 100-server App Verification gate. `stats` shows per-bridge
  uptime, owner, and link age plus whole-process memory. `msg <@mxid> <text>`
  DMs a user as the bridge bot, and `announce <text>` + `announce confirm`
  mass-DMs every bridge owner (two-step, 2-minute confirm window, audit-logged)
  for rare service notices. All operator-only.
- **`[management] instance_notice`.** Optional free-text line appended to the
  DM greeting and `help` — the place for an open-beta disclaimer and the
  instance's help-room address.

## [0.3.0-alpha.22] - 2026-07-11

### Added
- **The bot's replies link the user guide (#92).** New optional `[management]
  docs_url`: when set, a `Guide: <url>` line is appended to the DM greeting,
  `help` (both tiers), the pending-approval reply, and the errors whose fix the
  guide walks through (room-id / snowflake shape, the `call.member` power-level
  probe) — so someone who discovers the bot cold can find the walkthrough. The
  greeting now also says what the bot does instead of just "management".

### Fixed
- The snowflake shape error sent users to "Server Settings" for Developer Mode —
  it lives under **User** Settings → Advanced. The room-id shape error now says
  where Element shows the internal room ID.

## [0.3.0-alpha.21] - 2026-07-11

### Added
- **Self-serve allowlist — an invite-only middle tier (#82).** Between operators
  and fully-open registration: set `[management] self_serve_allow` and only those
  MXIDs (plus operators) get the self-serve tier; everyone else is ignored. The
  config value seeds `<data_dir>/self_serve_allow.toml` once; thereafter operators
  edit it live over DM with `serve-allow` / `serve-remove` / `serve-open` /
  `serve-list`. Unset ⇒ open to every Matrix user (the pre-#82 behavior). The
  file's existence is what enables restriction, so removing the last entry does
  not silently re-open the surface.

### Changed
- **The `link` pending-code reply names the approval bot (#80).** New optional
  `[management] approval_bot_name` is surfaced in the reply so a self-serve user
  knows which Discord bot to run `/bridge approve` on, and the reply now points at
  the `applications.commands` scope when the command doesn't appear in the guild.

## [0.3.0-alpha.20] - 2026-07-11

### Security
- **Stronger room-authority bar for self-serve linking (#86).** `require_room_authority`
  no longer passes anyone whose power merely meets `state_default`. The sender must
  now hold an **explicit individual moderator grant**: a `users[sender]` power-level
  entry that is both above the room's `users_default` and at least 50 (the Element
  "Moderator" level). Keying on an explicit, above-default grant closes the #79
  finding-6 vectors — a room can no longer authorize *every* member by lowering
  `state_default` or raising `users_default`. Authority is also **re-checked at
  Discord-approval time**, not just at the `link` probe, so a requester demoted or
  gone between `link` and approval can't slip a bridge through.

## [0.3.0-alpha.19] - 2026-07-11

### Security
- **Open-registration hardening residuals (#87/#88/#89).** Follow-ups to the #79
  review's medium findings, around a new periodic management-bot maintenance task.
  - **#87** — a `link` probe that succeeds but is never approved no longer leaves
    the shared bot joined to that room forever. `PendingLink` records whether the
    probe created the join; a 5-minute maintenance sweep leaves rooms from expired,
    unapproved, probe-created pending links — never a room a bridge holds (the
    "safe set" is the running bridges via `ControlMsg::Status`, unioned with the
    configured tenant rooms; the sweep is skipped entirely if that set can't be
    read).
  - **#88** — a per-foreign-domain cap on pending links bounds how much of the
    shared 16-slot pending table one homeserver's users can hold, so a
    federated-MXID Sybil set sharing a homeserver can't starve honest onboarding.
    The home server is exempt (invite-gated, not a Sybil vector).
  - **#89** — a per-sender cooldown on invite auto-joins (reusing
    `mgmt_cooldown_secs`), and the same maintenance sweep leaves DM rooms that
    have gone empty (only the bot remains).

## [0.3.0-alpha.18] - 2026-07-11

### Security
- **Open-registration hardening from the #79 adversarial review (#83/#84/#85).**
  The review confirmed the consent model is structurally sound; these close the
  highest-severity DoS/abuse gaps on the untrusted public DM surface.
  - **#83 — the `link` probe no longer pins the shared bot into attacker-named
    rooms.** The probe must join a room to read its state; it now records whether
    the bot was *already* a member (a running bridge keeps it joined — all bridges
    share one appservice MXID) and, on any post-join failure, leaves the room
    **only if the probe itself created the membership**. A room with an active
    bridge is never torn out (the correct form of a fix reverted in alpha.16).
  - **#84 — a global (cross-MXID) velocity limiter.** New `[limits]
    max_global_link_attempts` (+ optional `global_link_window_secs`, default 60 s)
    bounds total `link`-probe + invite auto-join volume across *all* self-serve
    senders, so a federated-MXID Sybil swarm can't get fresh budget per throwaway
    identity. **Required when `open_registration` is on.** The per-owner cooldown
    and link-attempt maps are now swept so they can't grow unbounded keyed by
    throwaway MXIDs.
  - **#85 — the audit trail rotates and invite auto-joins are member-checked.**
    `audit.jsonl` rotates to `.1` at 64 MiB (bounded disk against report/link
    spam), and an accepted DM invite that turns out to have >2 members (a spoofed
    `is_direct`) is left instead of greeted.

## [0.3.0-alpha.16] - 2026-07-11

### Added
- **Abuse observability + report path for open registration (#78).** Reactive
  abuse tooling — voice content is unmoderatable by design (never recorded or
  transcribed), so the bridge consolidates a durable audit trail and a report
  path that feeds the existing deny-list.
  - **`report <owner|guild|room> <value> [reason]`** — a self-serve-tier
    management command (available to non-operators, the people most likely to be
    harmed) that files an abuse report to the audit trail for an operator to
    review. It never acts on its own; auto-banning untrusted input would itself
    be an abuse vector, so it is advisory and feeds *manual* bans. Rate-limited
    via the shared `mgmt_cooldown_secs` budget so it can't flood the trail;
    free-text reasons are trimmed and length-capped.
  - **`reports [count]`** — an operator-only review command that renders the most
    recent report events from `<data_dir>/audit.jsonl` over DM (no host shell
    needed), each next to the exact `ban` command to run — wiring the report path
    straight into the deny-list tooling.
  - The audit trail now also records **invite accept/reject** decisions
    (operator / open-registration / banned / not-direct / not-allowlisted),
    closing the reachability-attempt coverage gap alongside the existing
    link/approve/unlink/ban events.
- **Anti-Sybil admission controls for open registration (#77).** Two new
  `[limits]` knobs harden the self-serve `link` flow beyond the per-owner tenant
  cap, which keys on the federation-reachable (thus throwaway) owner MXID:
  - `max_tenants_per_guild` caps how many tenant bridges may target one Discord
    guild — the Manage-Server requirement is a stronger anti-Sybil anchor than a
    federated account. Pending (unapproved) links count toward it; operators are
    exempt.
  - `max_link_attempts` (+ optional `link_attempt_window_secs`, default one hour)
    adds a per-owner sliding-window velocity budget on `link` attempts that reach
    the probe stage, on top of the single-shot `mgmt_cooldown_secs`. Operators
    are exempt. Rejections are recorded in the audit trail.

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
