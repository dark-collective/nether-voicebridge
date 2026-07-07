# Discord App Verification runbook (the 100-server gate)

_Companion to `nether-voicebridge-discord-tos.md` (#47 slice 2). That doc holds
the analysis; this one is the operational plan. Nothing here is urgent until a
puppet application approaches ~75 servers._

## The gate

Each Discord **application** (= each puppet bot) can join at most **100
servers** unverified. Past that, the app must pass **App Verification**
(separate from the privileged-intent review, which does NOT apply to us — the
pool uses only `GUILDS | GUILD_VOICE_STATES`). Verification is per-app and
involves: identity verification of the team owner (government ID via Stripe
Identity, once per team), an application questionnaire (what the app does, why
it needs its access), and an accurate Developer-Portal listing.

Pool math: N puppet apps × 100 servers = the instance's unverified guild
ceiling. The allocation unit is (bot, guild), so every app invited to a guild
consumes one of that app's 100 slots — an elastically-shared 7-bot pool burns
7 slots per onboarded guild. **Partitioning the pool (inviting only some bots
per guild) stretches the ceiling** at the cost of fewer simultaneous speakers
in that guild.

## Tripwire

- Add per-app guild counts to routine ops checks (each bot's `ready` logs its
  guild list; `note_bot_guild` is the seam if we ever want a metric).
- **At ~75 servers on any app: start verification for that app.** Do not wait
  for 100 — new joins hard-fail at the cap, and verification turnaround is
  weeks, not days.
- Put all puppet apps under one Discord **Team** so the owner identity check
  happens once.

## Keeping App Review painless (do this continuously)

- Every puppet app's Developer-Portal description must accurately say what it
  is: a **voice relay bridging Discord voice channels to Matrix rooms
  (off-platform), operated at nether.im**. Don't let descriptions drift
  from functionality (Developer Terms §6: expanding beyond the reviewed purpose
  requires re-review).
- Every app links the published privacy policy (Developer Terms §5a) and, once
  it exists, the terms-of-service page.
- Voice stays ephemeral (no recording) — if that ever changes, §5b retention
  duties and the policy text must change FIRST.

## Pre-emptive developer-support inquiry (send once, before Tier-2 scale)

The public docs never confirm whether an off-platform voice relay is an
approved use case; only a support ticket settles it. Suggested text
(dev support → https://dis.gd/contact, "Developers" category):

> Subject: Use-case confirmation — Discord↔Matrix voice relay bridge
>
> Hi — I operate an open-source bridge (AGPL, https://nether.codes/dark/nether-voicebridge) that relays live
> voice between a Discord voice channel and a Matrix room, with the explicit
> approval of a Manage Server member for every linked channel. Bridged Matrix
> participants appear as clearly-labelled bots; Discord participants appear as
> named participants in the Matrix call. Audio is relayed live only — never
> recorded, stored, transcribed, or used for ML. The bots use only the GUILDS
> and GUILD_VOICE_STATES intents. A published privacy policy is linked from
> each application.
>
> Before scaling a hosted multi-tenant instance, I'd like to confirm:
> (1) is this off-platform relay use case acceptable under the Developer
> Terms/Policy as described; and (2) is there anything specific App
> Verification will need from a voice-relay app beyond the standard
> questionnaire? Thank you!

Log the ticket id and answer here when sent.

## Also worth knowing (from the ToS doc)

- **"Users you may serve" (API Limits):** Discord may cap a bot's reach at its
  discretion; exceeding needs written consent. If the instance grows fast, ask
  proactively.
- **Token hygiene (§9a):** access tokens idle 30+ days may be suspended — a
  mostly-idle spare puppet pool should still connect periodically (our pool
  connects at process start, which suffices while the service runs).
- **Monetization (§18):** charging for hosting/infrastructure is a separate
  legal question from commercializing API data — get counsel before charging.
