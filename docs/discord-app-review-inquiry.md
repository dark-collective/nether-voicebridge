# Discord developer-support inquiry — off-platform voice relay use case

_A pre-emptive inquiry to send to Discord Developer Support / App Review before
scaling the hosted instance, per the "open questions" in
`nether-voicebridge-discord-tos.md`. The goal is to confirm Discord's stance on
the use case (an off-platform voice relay) BEFORE investing in Tier-2 scale or
crossing the 100-server verification gate on any bot._

**How to send:** Discord Developer Support ticket
(<https://dis.gd/developers-support> / the Developer Portal "Get Help" flow),
category "App Review / Verification" or "Other." Send it from the account that
owns the puppet applications. Attach the app IDs and the Privacy Policy + ToS
URLs (below). Keep the description matching what you put in each app's portal
"Description" field.

---

## Subject

Use-case confirmation before verification: a consent-gated Discord↔Matrix live
voice relay (operator-owned bot pool)

## Message

Hello Developer Support,

I operate a free, non-commercial community service that bridges **live voice**
between a Discord voice channel and a linked Matrix (Element Call) room, and I'd
like to confirm the use case is acceptable and understand the verification path
before I approach the 100-server threshold on any of my applications.

**What the apps do.** I run a small pool of my own bot applications (bot tokens
only — no user-account automation / self-bots). Each bot joins a voice channel
as a normal member with **Connect, Speak, Use Voice Activity, and Change
Nickname**, and connects with **only** the `GUILDS` and `GUILD_VOICE_STATES`
gateway intents (**no privileged intents** — no Message Content, Members, or
Presence). A bot captures the voice channel's audio as a legitimate voice
participant and relays it in real time to a linked Matrix room, and relays the
Matrix side back into the channel as named bot participants. **Audio is
ephemeral — never recorded, stored, or transcribed.**

**Consent and transparency.** A bridge is only created with two-sided consent: a
moderator of the Matrix room requests it, and a person with **Manage Server** on
the Discord side approves it with a slash command. The bridge's presence is
disclosed in-channel (the bots are visible members, relayed users appear with a
`[Matrix]` prefix, and a notice is posted when a channel is bridged). Any
participant can exclude themselves at any time with a `/bridge optout` command,
and there is an abuse-report + deny-list path.

**Compliance posture.** I have published a Privacy Policy and Terms of Service
and linked both in each application's Developer Portal (URLs below). I process no
privileged data, I don't sell or commercialize any API data, I don't train
ML/AI on any content, and I keep voice ephemeral by default.

**My questions:**

1. Is a consent-gated, off-platform **voice relay** like this an acceptable use
   of the API and gateway for a bot application, given the transparency and
   opt-out described above?
2. For **App Verification** to exceed 100 servers on these apps (non-privileged
   intents only): what does the process require for this kind of app, and is
   there anything about the off-platform-relay purpose I should address in the
   verification submission?
3. Is there a **"number of users you may serve"** limit I should be aware of for
   a service like this, and if so, how do I request written consent proactively
   rather than hit it unexpectedly?
4. Anything else you'd want me to put in place before scaling this to more
   servers?

I'd rather confirm the posture with you now than discover an issue at scale.
Thank you.

**App IDs:** _<fill in the puppet application IDs>_
**Privacy Policy:** https://nether.wiki/matrix/voice-bridge-privacy/
**Terms of Service:** https://nether.wiki/matrix/voice-bridge-terms/
**Project (open source):** https://nether.codes/nether/nether-voicebridge

---

## Developer Portal — what to put where (per app, ×7 puppet apps)

Under each application → **General Information**:

- **Privacy Policy URL:** `https://nether.wiki/matrix/voice-bridge-privacy/`
- **Terms of Service URL:** `https://nether.wiki/matrix/voice-bridge-terms/`
- **Description** (keep accurate + matching the inquiry, e.g.): "Bridges live
  voice between this server's voice channels and a linked Matrix (Element Call)
  room, with two-sided consent and an opt-out. Audio is relayed live only —
  never recorded. Non-privileged intents only."
