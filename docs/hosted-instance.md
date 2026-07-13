# Use the hosted instance

Don't want to run your own copy of the bridge? The project operator runs a
shared instance on the `nether.im` homeserver that you can ask to bridge your
Discord voice channel to a Matrix room, without installing or configuring
anything yourself.

> [!IMPORTANT]
> **The hosted instance is in open beta.** It works in daily use, but the
> bridge is young and under heavy development — expect the occasional rough
> edge rather than perfection. Thank you for testing it with us! Report
> anything odd in
> [`#nether-voicebridge-help:nether.im`](https://matrix.to/#/%23nether-voicebridge-help%3Anether.im)
> (or with the bot's `report` command for abuse).

> [!NOTE]
> The hosted instance supports **self-serve bridging**: DM the bridge bot
> (`@voicebridge_as:nether.im`) and link your own room — see the
> **[self-serve walkthrough](self-serve-bridging.md)**. Consent is required
> from both sides: you must be a moderator of the Matrix room, and someone
> with Manage Server must approve the link inside Discord. (Prefer a human?
> Ask in [`#nether-voicebridge-help:nether.im`](https://matrix.to/#/%23nether-voicebridge-help%3Anether.im).)

## What it is

One Rust process, run by the project operator, that bridges Discord voice
channels to Matrix/[Element Call](https://github.com/element-hq/element-call)
rooms on `nether.im`. Speak in Discord and be heard in the linked Matrix
call — and vice versa — with every speaker showing up as their own named
participant on the far side: Matrix speakers appear in Discord as named
puppet bots, Discord speakers appear in the Matrix call as named appservice
ghosts. See the [README](../README.md) for the full feature list and
[the architecture doc](nether-voicebridge-architecture.md) for how it works
under the hood.

## Cost

**Free.** The hosted instance is a community service — there is no charge to
link a channel, and no plan to charge for it.

## What data flows through it

Only what's needed to relay the call and know who's in it: live voice audio
(relayed, never recorded), Discord/Matrix display names and user ids for
call participants, and the bridge-link admin record (who requested it, who
approved it). Full detail, retention, and your rights are in the live
**[privacy policy](https://nether.wiki/matrix/voice-bridge-privacy/)** and
**[terms of service](https://nether.wiki/matrix/voice-bridge-terms/)**
(the in-repo [template](privacy-policy.md) is their hostable source) — read
them before linking a room, especially if your server or room has
participants who care about this. In short: no recording, no transcripts, no ML training, no
data sale. You can opt out of being relayed at any time with `/bridge optout`
in a bridged Discord server.

## Invite the puppet bots to your Discord server

The hosted instance runs a shared pool of puppet bot applications. Each one
needs to be invited to your Discord server — with **Connect, Speak, Use Voice
Activity, Change Nickname** — before it can serve your server's bridges (see
[How the puppet pool works](discord-bot-setup.md#how-the-puppet-pool-works)
for why: the bridge allocates puppets **per (bot, guild)**, so a bot can only
speak in a guild it has actually joined). How many you invite is up to you:

- **Any subset, in any order, works.** The bridge discovers which bots are in
  your server automatically — there is no required order and nothing to
  configure. Inviting bots 2, 5 and 7 works exactly like inviting 1, 2 and 3.
- **K invited bots ≈ K simultaneously-named Matrix speakers** in a voice
  channel. Invite the whole pool for maximum capacity; a Matrix speaker beyond
  your invited count still gets through, just without its own named puppet.
- **If you only want a few, please pick them at random** rather than starting
  from the top of the list — Discord caps each bot application at 100 servers,
  and spreading the load keeps every bot below that ceiling for longer.

- [Voice Bridge 01](https://discord.com/oauth2/authorize?client_id=1515386525653930055&permissions=103809024&integration_type=0&scope=bot)
- [Voice Bridge 02](https://discord.com/oauth2/authorize?client_id=1515387208046088364&permissions=103809024&integration_type=0&scope=bot)
- [Voice Bridge 03](https://discord.com/oauth2/authorize?client_id=1515738552007135282&permissions=103809024&integration_type=0&scope=bot)
- [Voice Bridge 04](https://discord.com/oauth2/authorize?client_id=1523341447959543910&permissions=103809024&integration_type=0&scope=bot)
- [Voice Bridge 05](https://discord.com/oauth2/authorize?client_id=1522762904233967716&permissions=103809024&integration_type=0&scope=bot)
- [Voice Bridge 06](https://discord.com/oauth2/authorize?client_id=1522762505842065549&permissions=103809024&integration_type=0&scope=bot)
- [Voice Bridge 07](https://discord.com/oauth2/authorize?client_id=1522763042654257173&permissions=103809024&integration_type=0&scope=bot)

Each link is a standard Discord OAuth2 `bot`-scope invite pre-scoped to:

- **Connect** — join the voice channel
- **Speak** — transmit audio
- **Use Voice Activity** — voice-activity input mode (not push-to-talk)
- **Change Nickname** — show the bridged Matrix speaker's name

No other permission is requested, and the bots use only the `GUILDS` and
`GUILD_VOICE_STATES` gateway intents (no message content, no member list, no
presence).

## Get the bridge bot into your Matrix room

1. **Restricted or invite-only room?** Run `link` first — the bot can't join
   on its own, and its reply will ask you to **invite it within a time
   window**; send that invite from your own account, from the room, and the
   link continues automatically. (The bot joins as a full appservice member —
   no `guest_access` needed; a room in the allow-space needs no invite.) The
   hosted instance's bridge bot is `@voicebridge_as:nether.im`.
2. **Make sure your room allows power level 0 for `org.matrix.msc3401.call.member`.**
   This is the standard Element Call setup — without it, ghosts (and, in an
   encrypted room, the E2EE subscriber) can't post their call membership, and
   Discord→Matrix audio fails with a 403 *"not enough power for
   org.matrix.msc3401.call.member"*. The easiest fix: **start or join a proper
   Element Call in the room once** — Element configures this automatically.

   > [!WARNING]
   > **Element's UI is misleading here.** Roles & Permissions →
   > "Join Element calls" shows **"Default"** both when the power level is
   > *unset* (falls through to `state_default`, usually 50 — too high) *and*
   > when an explicit override of `0` is already present. The label can't
   > tell you which state you're in. Don't trust it — check the **raw**
   > `m.room.power_levels` event instead: open **Element → room settings →
   > Advanced → View source** (or `/devtools` → room state →
   > `m.room.power_levels`) and confirm
   > `events["org.matrix.msc3401.call.member"] == 0`.
3. Once both are true, the room is ready — no other Matrix-side setup is
   needed on your end; the operator handles the appservice registration.

## How to request a bridge

The fastest path is the **[self-serve DM flow](self-serve-bridging.md)** — DM
`@voicebridge_as:nether.im`, run `link`, and have a Discord server
manager approve the code — self-serve is **enabled on the hosted instance**
(open beta). If you'd rather have a human walk you through it, ask in
[#nether-voicebridge-help:nether.im](https://matrix.to/#/%23nether-voicebridge-help%3Anether.im).
Either way, have ready:

- **Discord:** the guild ID and the voice channel ID (right-click each →
  "Copy Server ID" / "Copy Channel ID" — enable Developer Mode first under
  User Settings → Advanced → Developer Mode).
- **Matrix:** the room's internal room ID (`!...:nether.im` — Element → room
  Settings → Advanced → "Internal room ID"; **not** the `#alias` form).
- Confirmation that you hold **Manage Server** on the Discord guild (or can
  get someone who does to approve) — a bridge link is only created with the
  explicit approval of a Discord server manager, and confirmation that the
  Matrix room meets the power-level requirement above.

The management bot's `link` flow (or the operator, for a hand-held request)
verifies both sides' prerequisites before wiring anything up, and
you'll be told exactly what's missing if something isn't ready yet.
