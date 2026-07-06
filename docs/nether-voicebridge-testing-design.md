# Automated Regression-Testing Framework — Design (issue #38)

**Status: committed design, v2 (2026-07-06).** Supersedes the v1 feasibility
assessment (2026-06-23). v1 established feasibility and the three-tier split;
v2 bakes in the decisions, corrects claims the codebase has since outgrown,
and lays out the phased implementation plan. Phases are implemented in order,
each independently shippable and user-tested before merge.

## Context

The hosted multi-tenant instance is opening to public users (#43/#47).
Regressions must be caught before they reach users. This framework simulates
normal behavior and edge cases with fake bot users on both sides — Discord
speaker bots and Matrix test users — plus cheap hermetic tiers that run on
every PR.

## Decisions (locked)

1. **Scope: all three tiers**, built in order:
   - **Tier 1** — hermetic audio-integrity oracle over the in-process pipeline
     (per-PR, targets #32/#36-class corruption).
   - **Tier 2** — hermetic allocation/lifecycle scenario tests over the
     existing `test_*` seams (per-PR).
   - **Tier 3** — live E2E orchestrator (`nvb-e2e`) with fake users against
     the standing test infrastructure (nightly + manual dispatch, never a
     merge gate).
2. **Discord fake users:** 2–3 hand-created speaker bot apps + 1 privileged
   mod bot (Move Members). Created deliberately, never scripted (abuse
   heuristics — see CLAUDE.md "Do NOT").
3. **Matrix fake users:** a dedicated second appservice namespace
   (`@nvbtest_*`) registered on Tuwunel; the harness masquerades test users
   via its `as_token`. No password-login plumbing. Test localparts use
   **alphabetic** suffixes (`nvbtest_alice`) — `is_ghost_identity`
   (`ghost.rs:124`) and the legacy matcher only exclude all-digit suffixes,
   so test users can never be echo-excluded or misclassified as bridge-owned.
4. **CI:** per-PR hermetic job (`cargo test` + clippy) + nightly & manual
   live job, both on the self-hosted Forgejo runner. **A red nightly is a
   triage ticket, never a merge block.**
5. **Gaps → known-fail issues (carried from v1).** When the suite surfaces a
   bridge gap, the scenario encodes the *expected* behavior, is marked
   known-fail referencing a separately-filed issue, and the fix happens
   outside test work. The framework changes **no bridge behavior**. The two
   plumbing exceptions, both behavior-neutral at defaults: timer config
   threading (Phase C1) and a JSON tracing output mode (Phase D1).
6. **Timers → two-speed, config-threaded (carried from v1).** Grace constants
   move to a `[timing]` config with defaults byte-identical to today's
   values; a compressed profile (1–3 s) drives fast live runs; a small
   real-timing subset runs nightly. Hermetic Tier-2 tests prefer tokio
   paused time over compression (deterministic, instant).
7. **Prod tenant conflict:** the three Auto Test tenants stay in prod
   `tenants.toml` until Phase D starts, then are unlinked (mgmt-bot `unlink`
   ×3) so the harness's own bridge-under-test owns those rooms/channels.
   Running both would double-bridge (two anchors, echo, allocation fights).

## Corrections to v1 (what today's code changes)

- **External-disconnect recovery is FIXED** (#46): `DriverDisconnect` now
  drives an epoch-checked rejoin (`puppet.rs:2356-2420`) with a periodic
  `sweep_voice_health` backstop (`puppet.rs:1452`). v1 listed it as a gap;
  it is now a **must-PASS regression guard** (scenario 9). The one remaining
  gap is **mod force-move**: the puppet's own `voice_state_update` is ignored
  (`puppet.rs:1982` early-return on `is_bridge_bot`), and a move keeps the
  voice connection alive so no `DriverDisconnect` fires → no rejoin.
  Known-fail scenario 10 + separate issue.
- **The supervisor control plane exists** (#4/#45, post-v1):
  `Supervisor::spawn_bridge`/`stop_bridge`, SIGHUP → `reconcile_tenants`
  re-reading `tenants.toml`, `ControlMsg::{Reconcile,Status,GuildCoverage}`
  (`crates/bridge/src/supervisor.rs`, `crates/matrix-rtc/src/mgmt.rs`). The
  Tier-3 harness gets bridge lifecycle **for free** by spawning the real
  binary with a generated `tenants.toml` and driving SIGHUP/SIGTERM —
  exercising the exact production boot/reconcile/drain paths.
- **v1's "test Matrix user accounts" are replaced** by the test-appservice
  masquerade (decision 3) — which is precisely the code path the ghost
  machinery already implements (`ghost.rs`: register → invite/join → openid →
  lk-jwt → LiveKit connect → `m.call.member` post → key distribution). Far
  less new code than v1 assumed.
- **MFCC/spectral distance is dropped** from the oracle. It needs an FFT
  dependency, and every corruption class we have actually shipped (#32
  intermittent glitches, #36 DAVE-trailer decode corruption) is caught by
  time-domain metrics + dropout/glitch detectors. Zero new deps in
  `nvb-audio`. Revisit only if a spectral-tilt regression ever appears.
- **Tier-2 timing tests use `#[tokio::test(start_paused = true)]`** where they
  exercise real run-loops — instant and deterministic without compression.
  The compressed `[timing]` profile's real customer is Tier 3 live runs.
- **Standing infrastructure exists** (post-v1): Discord test guild
  "Nether Voice Bridge" (`1523348036040790087`, channels Auto Test 01–03) and
  the Matrix "Automated Testing" space (Auto Test 01/03 unencrypted, 02
  encrypted; all verified `org.matrix.msc3401.call.member` PL 0, `invite` 0).
  v1's provisioning list shrinks to the new bot apps + test appservice.

## What already exists (reuse, don't rebuild)

- **wav harness** — `crates/audio/src/wav.rs`: `read/write_wav_mono_48k`,
  `WavMonoWriter`. Pure PCM, no SDKs.
- **Audio core decoupled by design** — `downmix_stereo_to_mono`, `mix_mono`,
  `rms_dbfs`, `JitterBuffer` (20 ms→10 ms), `PlaybackMixer`, routing — all
  unit-testable in isolation; 30 audio tests already exist.
- **Debug taps (#32)** — Tap A per-SSRC ingress WAV
  (`crates/discord/src/capture.rs`), Tap B per-ghost publish WAV
  (`crates/matrix-rtc/src/rtc/ghost.rs:937`), gated on config `capture_wav`
  (env `NETHER_DISCORD_CAPTURE_WAV`).
- **Allocation test seam** — `puppet.rs:2807-2861`: guild-scoped helpers
  `test_slot_of(bridge, key)`, `test_anchor(bridge)`, `test_is_overflow`,
  `test_free_count`, `test_is_unhealthy(idx, guild)`, `test_set_unhealthy`,
  `test_is_busy`, `test_is_recovering`, `test_session_mixer`,
  `test_has_channel`, `test_arm_idle_token` — plus a 37-test suite.
- **Stale-ghost logic is pure** — `matrix.rs::classify_stale_memberships`
  (decision) already takes the grace as a parameter; `sweep_stale_ghosts`
  handles startup residue.
- **Ghost join/publish machinery** — everything a fake Matrix speaker needs,
  crate-private in `rtc::ghost` / `rtc::session` (wrapped by the Phase-D2
  `testkit` feature, not refactored).
- **188 unit tests** across config/audio/discord/matrix-rtc/bridge, including
  the supervisor `diff_tenants`/reconcile suite and the mgmt control plane.
- **Rich structured tracing** — every assign/steal/promote/failover/
  ghost-retire is a `tracing` event with bridge/participant/idx fields; the
  live harness scrapes these via a JSON output mode.

## Hard constraints (unchanged from v1)

- **No mock exists for Discord voice or MatrixRTC.** Discord is DAVE-only;
  matrix-rust-sdk can't join calls. Live tests need real bot apps, a real
  homeserver, and real LiveKit. The vendored DAVE-patched songbird makes
  every harness bot a full MLS participant, same as the bridge's own bots.
- **Audio assertions are tolerance-based, never bit-exact** (Opus, resample,
  20↔10 ms reframe, jitter).
- **Discord bot supply is hand-provisioned and small**; scenarios pace their
  joins/moves to stay clear of rate limits and abuse heuristics.
- **Platform extensibility** (self-bots, Signal, Telegram) stays a design
  seam (`ParticipantDriver` trait), not built work.

---

# The two algorithms

## 1. The audio-integrity oracle (Phase B)

The assertion primitive deciding whether audio crossed *intact*. Location:
`crates/audio/src/oracle.rs` (new `pub mod`), pure `&[i16]` math, zero new
dependencies. A sibling `probe.rs` synthesizes deterministic test signals.

```rust
pub struct OracleConfig {
    pub min_ncc: f32,          // normalized cross-correlation floor
    pub min_seg_snr_db: f32,   // segmental SNR floor
    pub max_dropouts: usize,
    pub max_glitches: usize,
    pub max_align_ms: u32,     // sanity bound on discovered delay
}
pub struct OracleReport {
    pub delay_samples: isize,
    pub gain: f32,
    pub ncc: f32,
    pub seg_snr_db: f32,
    pub dropouts: Vec<Anomaly>,
    pub glitches: Vec<Anomaly>,
    pub pass: bool,
}
pub fn compare_mono_48k(source: &[i16], captured: &[i16], cfg: &OracleConfig) -> OracleReport;
```

Pipeline:
1. **Align** — cross-correlate 10 ms-hop RMS envelopes (cheap: 100 fps, not
   48 kHz), refine ±1 hop at sample rate. Absorbs latency offset.
2. **Normalize** — least-squares gain over the aligned overlap.
3. **Score** — normalized cross-correlation coefficient + segmental SNR
   (10 ms segments, source-silent segments skipped). Thresholds gate
   pass/fail; the **continuous scores are always emitted** so nightly runs
   trend them — a slow drift downward is a regression even while "passing".
4. **Dropout detector** — segments where source RMS > −40 dBFS but captured
   < −60 dBFS (reuses `rms_dbfs`).
5. **Glitch detector** — sample-to-sample discontinuities, clipping runs,
   segment-energy spikes where the source was smooth.

**Probe signals, not golden WAVs.** `probe.rs` provides
`synth_probe_mono_48k(seed, dur_ms) -> Vec<i16>` (and a stereo variant): a
seeded composition of a log chirp sweep (spectral distinctiveness), tone
bursts separated by true silence (arms the dropout detector), and an
amplitude ramp (arms gain normalization). Same seed ⇒ bit-identical signal ⇒
no binary blobs in git, and Tier 3 writes the identical probe to WAV at
runtime via `write_wav_mono_48k`.

**Oracle self-tests (negative)** calibrate the thresholds and prevent an
always-green oracle: an injected 200 ms gap must fire the dropout detector; an
injected click/level-jump must fire the glitch detector; a time-shifted,
gain-scaled but otherwise clean copy must still PASS. #32-class intermittent
faults need duration: an `#[ignore]`d long-probe test (3–5 min) runs nightly
via `cargo test -- --ignored`.

## 2. The scenario director (Phase D)

A scenario is an ordered async body driving **actors** and checking
**assertions** via **observers**, bracketed by a baseline pre/postcondition.

```
Actors    = DiscordSpeaker[i]  (speaker bot apps: join, speak(probe), mute, leave)
            DiscordMod         (Move Members: force_move, disconnect)
            DiscordListener    (songbird receive → per-SSRC WAV of puppet output)
            MatrixSpeaker[j]   (testkit join_and_publish: probe frames, mute, leave_clean)
            MatrixPhantom      (post m.call.member, never join LiveKit → feeds the reaper)
            MatrixListener     (testkit subscribe → WAV; unencrypted first)
Observers = LogScraper         (bridge child-process stdout, NVB_LOG_JSON=1)
            RoomStatePoller    (raw CS-API m.call.member: active vs Empty, by sender class)
            ConnCountProbe     (gateway voice-state cache per test channel)
            WavComparator      (= the Phase-B oracle over listener WAV vs probe)

run(scenario):
  setup    -> assert BASELINE (channels empty, no active m.call.member, conn count 0)
  body     -> actions; every assertion POLLS with timeout (grace timers mean nothing is instant)
  teardown -> drive actors out; SIGTERM the bridge; assert RETURN-TO-BASELINE
  report   -> pass/fail + artifacts (bridge JSON log, WAVs, state snapshots, report.json)
```

Scenarios are independent and idempotent; the baseline assertion doubles as
the universal no-dangling-ghost / no-zombie-connection check. Expectation is
`Pass` or `KnownFail { issue }` — a *passing* known-fail reports
"unexpectedly fixed" (warn, not fail). The DSL is a Rust builder (type-safe,
actors are async handles anyway); a declarative TOML DSL waits until a second
platform actually exists.

---

# Implementation phases

Each phase = one or more PRs, user-tested before merge (standing rule).

## Phase A — CI hermetic job  *(~half session)*

**New:** `.forgejo/workflows/ci.yml`. Today CI only builds release images on
tags (`docker.yml`); there is no test job at all.

- Triggers: `push` (all branches) + `pull_request`;
  `concurrency: ci-${{ github.ref }}` with cancel-in-progress — one runner is
  shared with release builds.
- One job, `runs-on: docker`, container `rust:1-bookworm`, apt deps matching
  the Dockerfile (`cmake clang libclang-dev libopus-dev pkg-config
  protobuf-compiler libssl-dev` — livekit pulls prebuilt libwebrtc).
- Steps: checkout → cargo/target cache keyed on `Cargo.lock` →
  `cargo test --workspace --locked` →
  `cargo clippy --workspace --all-targets --locked -- -D warnings`.
  Debug profile; `--locked` guards the songbird/livekit pins; the vendored
  songbird `[patch.crates-io]` is in-tree and needs nothing special.
- No `cargo fmt --check` (tree not verified rustfmt-clean; a reformat doesn't
  belong in test work).
- Fallback if `target/` blows the cache backend: cache `~/.cargo/*` only.

**Accept:** a PR runs the job; a deliberately broken test/lint reds it;
warm-cache runs materially faster than cold (~12 min).

## Phase B — Tier 1 audio oracle  *(~1 session)*

**New:** `crates/audio/src/oracle.rs`, `crates/audio/src/probe.rs`,
`crates/audio/tests/oracle_pipeline.rs`. **Modified:** two `pub mod` lines in
`crates/audio/src/lib.rs`.

Chains under test (plain `#[test]`s — Phase A picks them up automatically):
1. `downmix_stereo_to_mono` → `JitterBuffer` push-20 ms/pop-10 ms →
   `PlaybackMixer` → 10 ms drain → oracle vs source (the Discord→Matrix
   in-process shape).
2. Two probes through `PlaybackMixer` → oracle vs `mix_mono` reference
   (overflow-mix integrity).
3. WAV round-trip via `wav.rs` (validates the Tier-3 artifact path itself).
4. The negative self-tests + `#[ignore]`d long-probe (above).

**Accept:** chains green; each negative test red-flags its fault; thresholds
documented; `cargo test -p nvb-audio` < ~30 s excluding `--ignored`.

## Phase C — Timer threading + Tier 2 scenarios  *(~1–2 sessions, two PRs)*

### C1 — `TimingConfig` (config-plumbing only, zero behavior change)

Today, all grace timers are hardcoded consts: `LEAVE_GRACE=12s`
(`matrix.rs:41`), `REAP_GRACE=45s` (`matrix.rs:1032`), `REAP_INTERVAL=20s`
(`rtc/mod.rs:58`), `ANCHOR_IDLE_GRACE=90s` (`puppet.rs:88`),
`QUARANTINE_BASE/MAX=15/120s` (`puppet.rs:73-74`), `VOICE_HEALTH_SWEEP=15s`
(`puppet.rs:81`).

New `TimingConfig` in `nvb-config` (7 `_ms` fields; `Default` = exactly
today's values; `compressed()` preset at 1–3 s). Optional `[timing]` TOML
section + `NVB_TIMING=compressed` env preset (preset first, explicit keys
override). Startup emits a **loud warn when timing ≠ default** — the guard
against accidentally-compressed production.

Threading is by **explicit parameter, not a global**: `SharedPool::new`
gains a `&TimingConfig` (stores four `Duration` fields),
`nvb_matrix_rtc::run` gains a `timing` arg (leave-grace debounce, reaper call
site — `classify_stale_memberships` already takes grace as a parameter — and
the reap ticker), the supervisor resolves once and passes through
`spawn_bridge`. ~9 call sites total.

**Accept:** defaults byte-identical (user smoke-tests a real bridge);
`NVB_TIMING=compressed` visibly shortens leave-grace in a manual test.

### C2 — Tier 2 named scenarios

Extend existing test modules (no new seams that touch non-test code):
- **Allocation** (`puppet.rs` tests, via the `test_*` seam):
  `scenario_steal_release_repromotion` (#24), `scenario_overflow_fcfs_promotion_order`,
  `scenario_guild_scoped_never_starves` (#46),
  `scenario_external_disconnect_epoch_recovery` (#46-fix regression guard),
  `scenario_quarantine_backoff_progression` (15→30→60→120 capped),
  `scenario_anchor_idle_release`.
- **Ghost reap** (`matrix.rs` tests): table-driven
  `classify_stale_memberships` cases — phantom reaped (#54 class), live
  member never reaped, flap-within-grace survives.
- **Supervisor**: reconcile *sequences* (add→replace→remove across three
  reconciles, cumulative registry asserts); run-loop tests under
  `#[tokio::test(start_paused = true)]`.

**Accept:** +15–25 tests, all green in CI.

## Phase D — Tier 3 live orchestrator  *(~4–5 sessions, three slices)*

### D1 — Harness skeleton + bridge-under-test + smoke scenario

**New crate `crates/e2e`** (package `nvb-e2e`, binary). Workspace member;
`default-members` pins the existing five crates so plain `cargo build` and
the Dockerfile are unaffected. Deps stay inside the existing workspace set.

```
crates/e2e/src/main.rs           CLI: run --scenario <n>|--all [--profile compressed|real] [--artifacts DIR]; list
crates/e2e/src/config.rs         E2eConfig: bridge_bin, guild/channel/room ids, tokens (env-first, e2e.toml fallback)
crates/e2e/src/bridge_proc.rs    BridgeUnderTest: temp workdir (config.toml w/ [server.test] + [puppets] + [timing];
                                 tenants.toml), NVB_DATA_DIR, NVB_LOG_JSON=1, spawn; sighup()/sigterm_and_wait()/kill()/
                                 reconfigure_tenants() (rewrite + SIGHUP — the production reconcile path)
crates/e2e/src/logscrape.rs      LogScraper: JSON lines → LogEvent{target,level,fields,message}; ring buffer +
                                 broadcast; await_event(pred, timeout), events_matching(pred)
crates/e2e/src/observers.rs      RoomStatePoller (raw CS-API, curl-ish UA — Cloudflare 403s python-urllib;
                                 sender-class by prefix), ConnCountProbe (gateway voice-state cache), baseline assert
crates/e2e/src/scenario.rs       runner, artifacts, report.json, Expectation::{Pass, KnownFail{issue}}
crates/e2e/src/actors/…          ParticipantDriver trait; discord.rs (DiscordSpeaker: join + RawAdapter probe playback)
crates/e2e/src/scenarios/…       registry; smoke.rs
```

**The one bridge change:** `main.rs` honors `NVB_LOG_JSON=1` →
`tracing_subscriber` JSON layer (needs the `json` feature on the workspace
`tracing-subscriber` dep). Default output unchanged.

Bridge-under-test = **the real binary as a child process** — exercises
`main.rs` boot, tenants loading, SIGHUP reconcile, SIGTERM drain, and panic
containment exactly as production; stdout becomes the log artifact; no test
linkage in the shipped binary.

Baseline teardown notes: the harness cannot retract *foreign*
`m.call.member` state (sender-locked state keys) — it relies on the bridge's
own startup `sweep_stale_ghosts` for residue, and (from D3) the mod bot to
clear lingering Discord voice states.

**D1 scenario:** `smoke_open_close` — DiscordSpeaker joins Auto Test 01 →
ghost membership + log events appear → speaks a probe → leaves → ghost
retracts after leave-grace → baseline.

**Prereq (runbook step 5):** unlink the three Auto Test tenants from prod.

### D2 — Matrix actors (testkit) + audio-crossing scenarios

**New:** `crates/matrix-rtc/src/testkit.rs` behind
`#[cfg(feature = "testkit")]` (`[features] testkit = []`). A thin public
wrapper over the crate-private ghost join/publish machinery — **no
production signature changes**; `--no-default-features` builds are
byte-identical. The ghost path is as_token-masquerade-native, exactly
matching the test-appservice decision.

```rust
pub struct TestUser { pub localpart: String, pub display_name: String } // alphabetic suffixes enforced
pub async fn join_and_publish(cfg: &TestAsConfig, user: &TestUser, room_id: &str,
    frames: mpsc::Receiver<Vec<i16>>) -> Result<TestCallHandle>;
impl TestCallHandle {
    pub async fn set_muted(&self, muted: bool) -> Result<()>;
    pub async fn leave_clean(self) -> Result<()>; // retract membership + disconnect
    pub async fn kill(self);                      // die WITHOUT retracting → stale-member injector
}
```

Actors: `MatrixSpeaker`, `MatrixPhantom` (membership, no LiveKit → reaper
feed), `DiscordListener` (songbird receive → WAV, modeled on `capture.rs`),
`MatrixListener` (testkit subscribe → WAV; **unencrypted rooms first**).

**Encrypted D→M limitation (explicit):** a Matrix listener in an encrypted
room must receive the ghost's Olm-delivered media key — the bridge-bot
subscriber's machinery. Until a decrypting listener exists (D2.5/D3
stretch), the encrypted D→M scenario asserts via **Tap B WAV** (pre-encryption
PCM) + key-delivery log events (`reached NO recipient devices` WARN = fail).
Encrypted **M→D is fully oracle-verifiable already** (the bridge decrypts;
the DiscordListener records the puppet).

### D3 — Adversarial actors + full catalogue + nightly workflow

- `DiscordMod` actor (serenity REST): `force_move`, `disconnect`.
- Remaining catalogue scenarios, incl. the force-move **known-fail** (file
  the issue first).
- **New:** `.forgejo/workflows/e2e.yml` — `schedule` (nightly) +
  `workflow_dispatch` (scenario filter, profile); build
  `-p nether-voicebridge -p nvb-e2e` (cached like Phase A); run
  `--all --profile compressed`, then the real-timing subset
  (`reap_release`, `leave_grace_debounce`, `anchor_idle_release`); always
  upload artifacts. Secrets via Forgejo Actions secrets. Red = triage
  ticket (manually filed from the artifact bundle in v1).

---

# Scenario catalogue v1 (priority order)

| # | Scenario | Regression of | Tier | Timing | Room |
|---|----------|---------------|------|--------|------|
| 1 | `smoke_open_close` | lifecycle base | live | compressed | unenc |
| 2 | `audio_m2d_unenc` (oracle) | #32/#36 class | live | compressed | unenc |
| 3 | `audio_d2m_unenc` (oracle) | #32/#36 class | live | compressed | unenc |
| 4 | `audio_m2d_enc` (full oracle) | #30/#51 key path | live | compressed | **enc** |
| 5 | `audio_d2m_enc` (Tap B + key logs) | #51 | live | compressed | **enc** |
| 6 | `steal_release_repromotion` | #24 | hermetic (T2) | paused-time | — |
| 7 | `overflow_fcfs_promotion` | allocation | T2 + live 3-speaker variant | compressed | unenc |
| 8 | `reap_release` (phantom member) | #54 | T2 + live | **real-timing nightly** (45 s grace) | unenc |
| 9 | `external_disconnect_recovery` | #46 fix — **must PASS** | live | compressed | unenc |
| 10 | `force_move_no_rejoin` | gap `puppet.rs:1982` — **KnownFail** | live | compressed | unenc |
| 11 | `fresh_store_rekey` (wipe store → audio still flows) | #30/#51 | live | compressed | **enc** |
| 12 | `ghost_session_rotation` (log-asserted) | #52 | live | compressed | **enc** |
| 13 | `leave_grace_debounce` (flap < grace keeps ghost) | debounce | live | **real-timing nightly** (EC's ~90 s heartbeat is external, uncompressible) | unenc |
| 14 | `shutdown_drain` (SIGTERM mid-call → baseline) | lifecycle | live | compressed | unenc |
| 15 | `multi_bridge_isolation` (Auto Test 01+03, no cross-talk) | isolation | live | compressed | unenc ×2 |
| 16 | `long_glitch_sweep` (3–5 min probe) | #32/#36 | hermetic `--ignored` + live | nightly only | unenc |

The baseline pre/postcondition wraps **every** live scenario for free.

# Provisioning runbook (operator-manual, one-time, needed from Phase D)

1. **Discord apps** (create deliberately, never scripted):
   `nvb-test-speaker-1..2` (optionally `-3`), `nvb-test-mod`, plus **2–3
   dedicated harness puppet bots** `nvb-test-puppet-1..3` for the
   bridge-under-test. **Never share prod puppet tokens** — one token in two
   processes = two fighting voice sessions per guild. No privileged intents
   needed anywhere (`GUILD_VOICE_STATES` is unprivileged).
2. **Invites to guild `1523348036040790087`:** speakers → Connect, Speak,
   Use Voice Activity; mod → Move Members (covers disconnect too); harness
   puppets → Connect, Speak, Use Voice Activity, Change Nickname.
3. **Tuwunel test appservice** (same `appservice_dir` mechanism as
   mautrix-discord, then reload):
   ```yaml
   id: nvbtest
   url: null            # passive — we only masquerade outbound; verify Tuwunel accepts null
   as_token: <64-hex random>
   hs_token: <64-hex random>
   sender_localpart: nvbtest_bot
   rate_limited: false
   namespaces:
     users: [ { exclusive: true, regex: '@nvbtest_.*:nether\.im' } ]
   ```
4. **Matrix rooms:** invite the fixed test users
   (`@nvbtest_alice/bob/carol/listener:nether.im`) into the Automated
   Testing space rooms once; memberships persist.
5. **Unlink the 3 Auto Test tenants from prod** (mgmt-bot `unlink` ×3)
   before the first live harness run.
6. **Forgejo Actions secrets:** `NVB_E2E_SPEAKER_TOKENS`, `NVB_E2E_MOD_TOKEN`,
   `NVB_E2E_PUPPET_TOKENS`, `NVB_E2E_TEST_AS_TOKEN`,
   `NVB_E2E_BRIDGE_AS_TOKEN`; non-secret ids in a committed
   `e2e.example.toml`.

# Risks / open items

- **Runner capacity:** one runner serves per-PR CI + releases + nightly E2E;
  the debug `target/` cache is large (libwebrtc). Fallbacks: registry-only
  caching, sccache, or a host-volume `CARGO_TARGET_DIR`.
- **Encrypted D→M full-oracle listener** (Olm-receiving MatrixListener) is
  the hardest single piece and deliberately deferred past D2.
- **Tuwunel passive appservice** (`url: null`) needs one-time verification;
  fallback: point it at a black-hole port.
- **Forgejo `schedule` trigger support** on the installed version — if
  absent, an external cron hits the dispatch API.
- **Discord pacing:** live runs pace joins/moves (sleeps between actor
  actions); nightly cadence only.
