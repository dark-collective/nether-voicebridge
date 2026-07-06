# Discord Terms-of-Service posture (issue #47)

Status: **research / verdict** (not legal advice — a primary-source reading, get
counsel before going commercial). Companion to
`nether-voicebridge-public-instance-feasibility.md`. Written 2026-07-06.

This is the external gate the isolation work (the per-bridge panic boundary
landed in #47) and the feasibility doc both defer to: *is a hosted, multi-tenant
Discord↔Matrix voice relay, built on operator-owned bot applications, within
Discord's terms — and how public can it go before it hits a real wall?* The
answer is read off Discord's four governing documents, cross-checked against how
this codebase actually drives Discord. Tracked as issue #61.

Sources (all fetched 2026-07-06):
- **Developer Terms of Service** — eff. 2024-07-08
  ([link](https://support-dev.discord.com/hc/en-us/articles/8562894815383-Discord-Developer-Terms-of-Service)).
- **Developer Policy** — eff. 2024-07-08
  ([link](https://support-dev.discord.com/hc/en-us/articles/8563934450327-Discord-Developer-Policy)).
- **Automated User Accounts (Self-Bots)**
  ([link](https://support.discord.com/hc/en-us/articles/115002192352-Automated-User-Accounts-Self-Bots)).
- **Changes to Privileged Intent Access for Discord Apps** — 2026 update
  ([link](https://support-dev.discord.com/hc/en-us/articles/40281523410967-Changes-to-Privileged-Intent-Access-for-Discord-Apps)).

## Verdict

**A private, self-hosted bridge is squarely inside Discord's developer terms; a
public multi-tenant instance is *not prohibited* but crosses into "operating an
Application that processes other people's users as a service" — which turns on a
set of concrete, satisfiable obligations rather than a ban.** The driving facts:

1. **We are on the sanctioned automation path.** Discord's own words: automation
   is done with **bot accounts** (token-authenticated apps), and "automating
   normal user accounts (generally called 'self-bots') outside of the OAuth2/bot
   API is forbidden, and can result in an account termination" (Self-Bots
   article). Every puppet is a real bot application (`crates/config/src/lib.rs:128`;
   gateway client built with a bot token, `crates/discord/src/puppet.rs:482`). The
   only self-bot temptation in the roadmap is the **video** relay (#8) — README:107
   already flags that as a ToS violation and it stays out of scope.
2. **We touch no privileged data.** The bots connect with exactly
   `GatewayIntents::GUILDS | GUILD_VOICE_STATES` (`puppet.rs:482`) — neither is a
   *privileged* intent (privileged = Guild Members / Presence / Message Content).
   The 2026 privileged-intent review (now gated at **10,000 users**) therefore
   **does not apply to us at all**. Voice receive is a supported songbird/DAVE
   capability and the anchor bot decrypts because it is a **legitimate MLS group
   member** (CLAUDE.md constraint #1; capture at `crates/discord/src/lib.rs:167,241`)
   — participation, not circumvention.
3. **The one hard ceiling is policy, not code:** an app must be **verified by
   Discord to exceed 100 servers**, and as of the 2026 update App Verification is
   now a **separate process** from privileged-intent review. This is per bot
   application and bounds how wide any single puppet can spread (feasibility:48-50,
   110).
4. **Going public/multi-tenant adds duties, not a prohibition.** No clause forbids
   a Discord↔Matrix voice relay. But once we relay *other people's* users' voice
   off-platform as a service, the Developer Policy's permission/transparency rules
   and the Developer Terms' privacy-policy, retention, and security obligations all
   bind — see "Gates" below. The closest friction (Developer Policy §7,
   off-platform transmission of API Data) is cleared by disclosure + consent +
   opt-out, not blocked.
5. **Monetizing has extra strings** (Developer Policy §18 + Monetization
   Requirements) — treat a paid instance as a separate, counsel-reviewed decision.

## How we actually use Discord (the compliance-relevant facts)

- **Legitimate bot applications, deliberately created.** N separate apps, one raw
  token each (`config/src/lib.rs:128`, `config.example.toml:78`), and a standing
  rule against scripted mass-creation (CLAUDE.md:195 — "create them
  deliberately"). No user-account automation anywhere.
- **Non-privileged intents only** (`puppet.rs:482`), so no Message Content / Members
  / Presence data is ingested.
- **Voice capture as an MLS participant.** The anchor puppet is a real DAVE (MLS)
  member; audio decodes because it legitimately holds the ratcheted sender keys
  (CLAUDE.md constraint #1; architecture:154-160; `lib.rs:167,241`).
- **Manual invites, four voice permissions.** Each bot is invited out-of-band per
  guild with **Connect / Speak / Use Voice Activity / Change Nickname**
  (README:146; `config.example.toml:83`). No OAuth invite-link generation in code.
- **Identity by nickname only.** Puppets set a per-guild **nickname**
  (`puppet.rs:2331`) and never a runtime avatar (grep confirms no `set_avatar`/
  `edit_profile` in `crates/discord/`); avatars are global-per-app and rate-limited
  (constraint #4). *Accuracy note: we do not throttle nickname edits in code — the
  ~1/10min figure is Discord's avatar limit, not a nickname limiter we enforce.*
- **Operator owns the bots; tenants bring guilds.** There is no per-tenant Discord
  login — a tenant onboarding is "invite the operator's pool bots + bind a
  channel" (feasibility:101-110). This keeps us operating *one* set of Applications
  rather than syndicating API access to third-party apps.
- **Audio is ephemeral by default.** The core is PCM in-memory; WAV taps are an
  opt-in debug path, not a product feature — i.e. no default long-term storage of
  users' voice.

## What Discord's terms actually say

- **Bots are the supported path; self-bots are banned** (Self-Bots article). We
  are on the right side of this line; #8 (video via self-bots) is the line.
- **License to operate your Application** is granted for exactly that — integrate,
  develop, and operate your app (Developer Terms §2a). **Restrictions** (§2b)
  forbid reverse-engineering, circumventing security/limits, and "modify … rent,
  lease, sell, or syndicate access to the APIs." Operating our own pool for tenants
  is running one Application, not syndicating API access — but a model where tenants
  supplied their *own* tokens would read very differently. Keep the pool operator-owned.
- **Developer credentials** (§2d) must be used solely with their own app, kept
  **encrypted**, and **"may not be embedded in open source projects."** The repo's
  never-commit-secrets convention is a direct compliance control here.
- **User privacy & security (§5) is the load-bearing section for going public:**
  - **§5a — you must publish a privacy policy** compliant with GDPR/CCPA/etc.,
    **publicly linked in the Developer Portal**, describing what data you collect,
    how you use/share it, and how users request deletion. A public instance cannot
    ship without this.
  - **§5b — data sharing & retention:** API Data may be shared only with a Service
    Provider, where legally required, or where the user directs it; and must be
    **deleted when no longer necessary, when you stop operating, or on user/Discord
    request.** A relay that keeps voice ephemeral satisfies this cleanly; any
    recording feature would need an explicit retention/deletion story.
  - **§5c — security:** commercially reasonable safeguards incl. **encryption at
    rest** and breach notification.
- **Developer Policy — "Protect Discord Users":** §2 "do not initiate processes on
  a user's behalf without first obtaining their permission … clearly labeled …
  accurate description"; §3 respect users' ability to opt out / remove the app; **§7
  do not contact users *outside* Discord without explicit permission** using API
  Data. Relaying a participant's voice to Matrix is off-platform transmission of
  their content — the compliant posture is **clear disclosure that the channel is
  bridged + a way to not be captured**, which the bridge's lifecycle (bots are
  visible members; leaving the channel stops capture) largely already provides.
- **Developer Policy — "Handle Data with Care":** §15 use API Data only for stated
  functionality; §16 no profiling / no under-13 / no health/financial/sensitive
  data; **§18 do not sell, license, or commercialize API Data or Discord's
  services**; §21 no training ML/AI on message content. We do none of the
  prohibited things; §18 is the clause to respect if the instance ever charges.
- **Monitoring & App Review (§6):** Discord may require **App Review** at
  thresholds or on material functionality changes, and you may not expand beyond
  your reviewed stated purpose without re-review. An off-platform voice relay is
  unusual enough that App Review could scrutinize it — keep the Developer-Portal
  description accurate and current.
- **API limits (Developer Policy "API Limits"):** Discord may limit "the number of
  users you may serve" at its discretion and you must not circumvent it; exceeding
  needs **express written consent**. Scaling a public instance is subject to this.
- **Termination housekeeping (§9a):** access tokens unused for 30+ days may be
  suspended — a live-ops note for a large, mostly-idle puppet pool.

## Gates for a public instance

- **Per-app 100-server verification** — the real architectural ceiling; per bot
  application, now separate from privileged-intent review. Plan pool sizing and a
  verification runbook around it (feasibility:48-50, 259).
- **A published, portal-linked privacy policy** (§5a) — a hard prerequisite, not
  optional, the moment third parties' users are processed.
- **Transparency + consent + opt-out for being bridged** (Policy §2/§3/§7) — make
  "this channel is relayed to Matrix" unmistakable and leave a way to abstain.
  Design the management/link flow so the bridge's presence is disclosed in-channel.
- **Retention/deletion discipline** (§5b) — keep voice ephemeral by default; if any
  recording is ever added, it needs a deletion path and policy text.
- **Monetization is a separate decision** (Policy §18 + Monetization Requirements) —
  charging for the *service/infrastructure* differs from commercializing API Data,
  but the boundary wants counsel; note the Premium-Apps requirement in supported
  regions.
- **GDPR/UK data-transfer terms** (Developer Terms §11) bind if EU/UK users are
  bridged — the SCC/Addendum clauses flow through automatically.

## Open questions / where to get confirmation

- **App Review outcome for an off-platform voice relay** — not resolvable from the
  public docs; only a Developer-Portal submission / developer-support ticket
  settles whether Discord treats a multi-tenant voice bridge as an approved use
  case. Worth a pre-emptive inquiry before Tier-2 investment.
- **Monetization boundary** (§18 vs. charging for hosting) — needs legal review,
  not a doc read.
- **"Users you may serve" limit** (API Limits) — undefined publicly; if a public
  instance scales, request written consent proactively rather than discover the cap.
- **Verdict for sequencing:** a private/self-hosted bridge and an operator-run,
  invite-only Tier-1 instance are low-risk on these terms today. Full Tier-2
  self-serve should be gated on (a) a privacy policy + consent UX, (b) a 100-server
  verification plan per app, and (c) ideally a developer-support confirmation on the
  relay use case — the same external gate the isolation and feasibility docs point
  at, now with the specific clauses named.
