# Public Instance — Feasibility (issue #43)

Status: **analysis only** (not a build order). Companion to
`nether-voicebridge-architecture.md`. Written 2026-06-24.

## The ask

[Issue #43](https://nether.codes/nether/nether-voicebridge/issues/43) (enhancement
/ question) asks whether nether-voicebridge could offer a **hosted, mautrix-style
public instance**: rather than self-hosting, a user invites a management bot,
links their Discord voice channel to a Matrix room, and controls the bridge
remotely — the way the public mautrix-telegram instances let you `!login` and go.

This doc assesses whether that's viable, what the real blockers are, and in what
order they'd have to fall. The headline question it answers — **does a small bot
pool scale to many tenants?** — turns out to be the load-bearing one.

## Verdict

A public instance is **conceivable but materially harder than a mautrix text
bridge**, for structural reasons (below). The single biggest enabler is a
**puppet-pool refactor to per-guild allocation** — without it, the shared pool
can't be shared across tenants at all. The honest sequencing:

1. **Tier 0 — managed hosting** works *today* with no code (operator-driven
   onboarding).
2. **Multi-guild puppet refactor** is the precondition that makes a shared pool
   economical across many guilds/tenants.
3. **Tier 1 / Tier 2 self-serve** are progressively larger, and Tier 2 carries
   open external risk (Discord ToS, resource isolation).

## The multi-guild scaling finding (the core correction)

The intuitive worry is that a pool of N bots can serve only N callers *total*,
making a public instance uneconomical. **That worry is unfounded by Discord's
design — but our current code imposes exactly that artificial cap.** This gap is
the crux of #43.

### Discord allows it

A single Discord bot application maintains **one voice gateway connection per
guild**, and those connections are independent. One bot can therefore sit in
voice in many guilds at once. A pool of N bots scales to **unlimited guilds, each
guild showing up to N simultaneous distinct puppets**, bounded only by CPU and
bandwidth — not by a global slot count. This is precisely how public music bots
(Jockie, the former Groovy/Rythm) serve thousands of guilds from one application.

The one real ceiling is **policy, not architecture**: a bot must be
*verified* by Discord to join more than 100 guilds. That's a per-application
onboarding step, not a runtime wall.

### Our code does not exploit it (yet)

The allocator models a bot's availability as a **single process-global flag**:

- `PuppetSlot.busy: AtomicBool` (`crates/discord/src/puppet.rs:202`) has **no
  guild qualifier**. `take_free_bot` (`puppet.rs:603`) skips any bot whose `busy`
  is set and sets it globally on claim (`puppet.rs:621`).
- Guild-awareness — `PuppetSlot.guilds: RwLock<HashSet<GuildId>>`
  (`puppet.rs:207`) — is only an **eligibility filter** ("may this bot serve
  guild X?"), *not* a per-guild slot. It does not let one bot be busy-in-A and
  free-in-B.
- Consequence: if guild A's channel claims all N bots, guild B **starves**, even
  though Discord would happily let those same bots also serve B. The exhaustion
  is artificial — an artifact of the data model, not of Discord.

### The SDK layer is already capable

Good news: nothing below the allocator stands in the way.

- Each bot runs its own songbird manager, and voice Calls are keyed by guild:
  `manager.get_or_insert(channel.guild_id)` (`puppet.rs:2260`). One manager can
  hold many live Calls, one per guild, simultaneously.
- So the blocker is **purely the allocator's allocation unit**, not the voice
  plumbing.

### What the enabling refactor entails (feasibility level, not designed here)

Two pieces of very different size:

- **(a) Allocation unit: per-bot → per-(bot, guild).** Replace the global `busy`
  flag with per-guild assignment state so a bot can be assigned independently in
  each guild it belongs to. Contained to the allocator and its tests — moderate.
- **(b) Per-bot runtime multiplexing (the hard part).** Today a bot's
  `ChannelCtx` carries a **single** `guild_id`/`channel_id` (`puppet.rs` struct,
  ~line 100+), and its anchor only captures while the channel's `anchor` marker
  equals its own slot index — i.e. one bot anchors/captures *one* channel. To
  serve guilds concurrently, one bot must run **capture** (SSRC→user attribution,
  the capture sink) **and** playback for several guilds at once, with all audio
  routing keyed by guild. The capture/anchor path is the real refactor.

Treating per-guild concurrent Calls as proven (they're standard for public bots),
this is a sizeable but well-bounded change — and it is the precondition for any
multi-tenant offering.

## Why the mautrix analogy only partly transfers

Even with the pool refactor, three structural facts make this harder than a
mautrix text bridge.

### Discord: bot applications, not user login

mautrix puppets remote users for free because you `!login` with your own
Telegram/WhatsApp account. There is no equivalent here: puppets are **real
Discord bot applications**, and each must be **invited to each guild** with
Connect / Speak / Use Voice Activity / Change Nickname. A tenant onboarding is
therefore "invite the operator's pool bots + bind a channel," not a login flow.
Bot **avatars are global per application** (architecture constraint #4) — there
are no per-guild bot avatars — so all tenants see the same generic puppet
identities. And the 100-guild verification gate applies per bot.

### Matrix: federates, but the tenant's homeserver must run Element Call

The appservice lives on the **operator's** homeserver; its ghosts federate into a
tenant's room fine. But two requirements survive federation:

- Each tenant room still needs **PL 0 for `org.matrix.msc3401.call.member`** and
  the bot **invited** (restricted/invite-only rooms) — per the architecture doc's
  room requirement.
- Critically, the call is anchored to the tenant's room, so its
  LiveKit SFU / lk-jwt come from the **tenant's homeserver's** `.well-known`
  `rtc_foci`. **If the tenant's homeserver doesn't run MatrixRTC / Element Call,
  there is no call to join at all.** Unlike a text bridge that works on any
  homeserver, the addressable audience is "users whose HS supports Element Call"
  — growing, but far from universal.

### Architecture: static config, single process, no runtime provisioning

- All bridges are spawned **once at startup** from the loaded TOML
  (`crates/bridge/src/main.rs:108`, the `for (idx, bridge)` loop), and the only
  lifecycle control is the SIGINT/SIGTERM shutdown path (`main.rs:198`,
  `shutdown_signal` at `:228`). There is **no config reload, no command channel,
  no way to add or remove a bridge while running** — provisioning a tenant today
  means editing TOML and restarting.
- The puppet pool *can* already register channels dynamically
  (`SharedPool::register_channel` returns an `Arc<ChannelView>` with no
  lifetime constraint), but nothing calls it after startup. That's an
  encouraging seam for Tier 1, not a finished feature.
- Each ghost runs its own Olm crypto client with its own SQLite store. Under
  single-tenant load this already hit a **file-descriptor exhaustion ceiling**
  (live-diagnosed: per-ghost stores blew past the 1024 soft `nofile` limit →
  crypto sync died → inaudible ghosts; fixed operationally by raising
  `LimitNOFILE` to 65536). At multi-tenant scale this is a real resource wall
  that wants a deliberate isolation/sharding strategy, not just a higher ulimit.

## Phased recommendation

**Tier 0 — managed hosting (≈0 code, works today).** Operator runs the binary;
onboarding is a support ticket: operator adds a `[[bridge]]`, invites the pool
bots to the tenant's guild, restarts. This is a legitimate public-*ish* offering
and the honest near-term answer to #43. Cost is docs + the room-prereq checklist
+ ops. Cap = pool size *per guild* (already fine), but tenant count is bounded by
the no-runtime-provisioning limitation and manual toil.

**Multi-guild puppet refactor — the precondition.** Per-(bot, guild) allocation +
per-bot capture/playback multiplexing (above). Without this, "shared pool across
tenants" doesn't actually share. Gates everything below.

**Tier 1 — semi-self-serve (medium-large).** A Matrix management bot + a Discord
slash-command to bind a room↔channel, **runtime dynamic bridge create/destroy**,
tenant-state persistence, and an auth model (who may bridge what), over an
operator-approved fixed pool. This is the real engineering chunk; the dynamic
lifecycle is the part that doesn't exist yet.

**Tier 2 — full mautrix-style self-serve (large + open external risk).** Anyone
invites bots and auto-provisions, with **per-tenant resource isolation** plus the
**cheap-ghost crypto rework** (OlmMachine + `MemoryStore`, see "Scaling encrypted
callers" below) that retires the FD ceiling. Gate this on a deliberate read of
**Discord's ToS** for a public voice-relay service and on pool economics
(verification logistics, bandwidth, abuse surface).

## Scaling encrypted callers (the per-ghost crypto cost)

The FD ceiling above is a symptom; the underlying question is what an *encrypted*
caller actually costs. In a plaintext call a ghost is cheap (a LiveKit participant
publishing a track). In an **encrypted** call each ghost
(`build_ghost_crypto_client`, `crates/matrix-rtc/src/rtc/ghost.rs:1260`) today
spins up a complete `matrix-sdk` crypto client: its own SQLite store, a bounded
initial `sync_once`, a **perpetual `/sync` loop** (`ghost.rs:1322`), and a
key-distribution loop. So one 20-speaker encrypted call = 20 crypto stores, 20
homeserver `/sync` long-polls, and 40 long-lived tasks. Two cost classes:

- **Media plane** (HKDF frame keys → LiveKit frame crypto, constraint #3): per
  active stream, steady, linear. Not the worry.
- **Control plane** (Olm identity, `/sync`, device tracking, key distribution):
  heavy, bursty, and super-linear — O(ghosts × in-call recipient devices) Olm
  sends on each membership change. This is the scaling concern.

### What cannot be collapsed (verified against Element Call)

The tempting fix — one shared crypto identity distributing every ghost's key — is
**impossible**, and this is settled from matrix-js-sdk's actual code, not
inference. Element Call attributes a received media key to the **authenticated
to-device sender** (`event.getSender()`) and deliberately ignores the `member.id`
field inside the payload (a literal `// TODO ... claimed information` comment). A
key only ever lands in the sender's own participant slot, so **N distinct
attributable E2EE participants require N distinct sending identities** — the same
reason Element Call's own key distribution is O(n²). We can make each identity
cheap; we cannot make them one.

### What *can* be fixed — and it retires the FD wall

The per-ghost cost is an artifact of using a full `Client` + `SqliteCryptoStore` +
perpetual `/sync`, not anything inherent to encryption. The ghost is **send-only**
— it never receives or decrypts; its `/sync` exists purely to refresh recipient
device-lists (results are discarded at `ghost.rs:1314`). So each ghost can drop
from a full `Client` to a standalone **`matrix-sdk-crypto::OlmMachine` backed by a
`MemoryStore`**:

- **Perpetual `/sync` → gone**, replaced by on-demand `/keys/query` before a key
  send (`update_tracked_users` → `outgoing_requests` → raw PUT). Removes the
  O(ghosts) sync fan-out on the homeserver.
- **All crypto FDs → gone.** `MemoryStore` uses zero file descriptors. The FD cost
  was worse than first estimated: `SqliteCryptoStore` uses a deadpool pool sized
  `max(2, physical_cores × 4)` — ~32 connections per ghost store on an 8-core box,
  not ~3 — which is the real reason ~20 ghosts blew past 1024. `MemoryStore` makes
  crypto FD cost **independent of ghost count** and is the proper fix for the
  live-diagnosed exhaustion (the `LimitNOFILE=65536` bump is a stopgap).
- Ghosts are **ephemeral** (alive only while a Discord user speaks in a live call;
  a restart tears down all calls anyway), so `MemoryStore`'s "keys lost on drop"
  fits — nothing is worth persisting across restarts. The send primitive
  (`encrypt_content_for_devices`, behind the `experimental-send-custom-to-device`
  feature the bridge **already** enables) is exactly what
  `encrypt_and_send_raw_to_device` wraps today, so this is a lower-level rewiring
  of the same operation, not new crypto.

### Residual floor and optional wins

- **Irreducible:** O(ghosts × in-call recipient devices) pairwise Olm sessions per
  call — bounded by call size, now in-memory and cheap rather than eliminated.
- **Optional — a per-room device tracker.** Every ghost currently queries the
  *same* recipient device-list independently; one shared per-room `/keys/query`
  feeding all ghosts collapses device *discovery* from O(ghosts) to O(1) per room
  (the Olm sessions stay per-ghost).
- **Plaintext escape hatch.** Path 2 (plaintext to-device masqueraded as the ghost
  via `as_token`) needs *zero* per-ghost crypto — one appservice token sends every
  ghost's key, correctly attributed, and current EC *accepts* it (the
  plaintext-rejection check is commented out, rust-sdk #4883). Legitimate for a
  **trusted/self-hosted** homeserver, but for a **public instance bridging
  third-party encrypted rooms** it lets the operator's homeserver read the media
  keys — an E2EE downgrade the room's members never consented to. So it scales
  freely but is off the table for the multi-tenant case.

### Implication for Tier 2

Tier 2 multi-tenant = **cheap ghosts** (OlmMachine + `MemoryStore` + on-demand
key-query, contained to the ghost path in `matrix-rtc`, no protocol change)
**plus per-tenant process/runtime isolation** so one tenant's call can't starve
another's FD/CPU/homeserver budget. The per-ghost footprint shrinking from "~32
FDs + a sync loop" to "some memory + occasional HTTP" is what makes that isolation
affordable in the first place. Caveats: the OlmMachine rewire hand-rolls the
HTTP/retry/lock logic `Client` gives for free, and a fresh in-memory account means
a new device per ghost lifecycle — peer-safe per the #30 notes, but a conscious
change from today's persistent-store-reused-across-rejoins behaviour.

## Open questions / external gates

- Discord ToS posture for a *public* voice relay (vs. a private self-hosted one).
- Bot-verification logistics past 100 guilds per application.
- Whether to **spike** the per-guild capture/playback multiplexing before
  committing to the refactor (deferred for now; per-guild Calls treated as
  proven).
- Per-tenant isolation strategy (process/runtime per tenant) for multi-tenant
  scale. (The per-ghost crypto *cost* itself is now understood — OlmMachine +
  `MemoryStore` retires the FD ceiling; see "Scaling encrypted callers".)
- Whether to keep ephemeral ghost crypto in-memory (re-key per lifecycle) vs.
  persistent — interacts with the #30 device-identity churn rule.
- Whether the per-room shared device-tracker optimisation is worth its complexity.
