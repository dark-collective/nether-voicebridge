# Discord bot setup (self-hosting)

The bridge's Discord side needs one or more **puppet bot applications** — a
shared pool of Discord bots that the bridge allocates dynamically to
represent Matrix speakers in your voice channel(s). Registering them is the
fiddliest part of self-hosting, so this is a full walkthrough. It covers
Discord only; the Matrix-side requirements (appservice registration, room
power levels) are in the [README's Requirements](../README.md#requirements)
section and [the architecture doc](nether-voicebridge-architecture.md).

If you're not self-hosting, see [Use the hosted instance](hosted-instance.md)
instead — you don't need any of this.

## How the puppet pool works (read this first)

The bridge doesn't need one bot per Matrix *user* — it needs enough bots that
every **simultaneous** Matrix speaker in a channel can be represented at
once, plus one to act as the channel's capture "anchor" (the bot that
listens for Discord speakers and plays a blended mix of everyone else). The
pool is shared across all your `[[bridge]]` entries and allocated **per
(bot, guild)**: each bot holds one independent voice connection per Discord
guild, so:

- **N bots ⇒ up to N simultaneous Matrix speakers per guild** (one of which
  doubles as that channel's anchor when nobody else needs the slot).
- **Two bridged channels in the *same* guild need distinct bots** — a bot can
  only be in one voice channel per guild at a time.
- A bot only serves guilds it's actually been invited to, so you can either
  invite the whole pool to every guild (share elastically) or invite subsets
  per guild (partition the pool).

Size your pool for your **peak concurrent speaker count**, not your total
member count. A small Discord server bridging one voice channel with usually
2-3 people talking at once is comfortably served by 2-3 bots; add more if you
expect louder or bigger calls, or you're bridging multiple channels in the
same guild.

## Before you start: how many applications to create

Decide your pool size (see above), then repeat every step below **once per
bot**, creating each application separately and deliberately.

> [!WARNING]
> **Create applications by hand, one at a time — never in a scripted loop.**
> Discord's abuse heuristics watch for bulk-application creation; a script
> that mints N applications back-to-back risks the whole batch (or your
> developer account) getting flagged. A few minutes of manual clicking per
> bot is the cost of a healthy pool.

## 1. Create the application

1. Go to the [Discord Developer Portal](https://discord.com/developers/applications)
   and sign in.
2. Click **New Application**, give it a name (see
   [Naming and avatars](#naming-and-avatars) below), accept the terms, and
   click **Create**.

   ![Creating a new application in the Discord Developer Portal](images/discord-setup-01-new-application.png)

3. You land on the application's **General Information** page. Nothing to
   configure here yet, but this is where the app id (client id) shown in the
   URL and on this page will be used later for the OAuth2 invite link.

   ![The application's General Information page](images/discord-setup-02-general-information.png)

## 2. The Bot tab

1. Open the **Bot** tab in the left sidebar.

   ![The Bot tab in the Developer Portal sidebar](images/discord-setup-03-bot-tab.png)

2. A bot user is created automatically with the application. Click
   **Reset Token** to generate a bot token, and copy it immediately —
   **it is shown once**; if you lose it, you'll need to reset it again (which
   invalidates the old one).

   ![Resetting and copying the bot token](images/discord-setup-04-reset-token.png)

   Paste the token straight into your password manager or `config.toml` (see
   [Wiring tokens in](#wiring-tokens-into-configtoml) below) — don't leave it
   sitting in a scratch file.

3. **Privileged Gateway Intents:** leave these all **off**. The bridge only
   requests the `GUILDS` and `GUILD_VOICE_STATES` intents — neither is
   privileged, and the bridge needs none of **Presence Intent**, **Server
   Members Intent**, or **Message Content Intent**. Don't enable them; there's
   no reason to, and it only adds attack surface / review burden.

   ![Privileged Gateway Intents left off](images/discord-setup-05-privileged-intents.png)

4. **Public Bot:** leave this on (default) unless you have a specific reason
   to restrict who can invite it — it doesn't affect the bridge.

## 3. Generate the OAuth2 invite URL

1. Open the **OAuth2** tab, then the **URL Generator** sub-page.

   ![The OAuth2 URL Generator](images/discord-setup-06-oauth2-url-generator.png)

2. Under **Scopes**, check **`bot`**. (You do not need `applications.commands`
   for a plain puppet bot — that scope is only needed if you're separately
   setting up the [management bot's two-sided Discord approval](../README.md#two-sided-link-auth),
   which registers a `/bridge approve` slash command.)
3. Under **Bot Permissions**, check exactly:
   - **Connect**
   - **Speak**
   - **Use Voice Activity**
   - **Change Nickname**

   ![Selecting the bot scope and the four voice permissions](images/discord-setup-07-scopes-and-permissions.png)

4. Copy the **Generated URL** at the bottom of the page, or build it directly
   from the template below — substitute your application's **client id**
   (from the General Information page):

   ```
   https://discord.com/oauth2/authorize?client_id=YOUR_CLIENT_ID&permissions=103809024&scope=bot
   ```

   `103809024` is the exact permission integer for Connect + Speak + Use
   Voice Activity + Change Nickname — no more, no less. If you ever add scopes
   or permissions in the portal, re-copy the generated URL rather than hand-editing
   the integer.

## 4. Invite the bot to every guild it should serve

Open the invite URL from step 3 in a browser you're logged into Discord with,
pick the server, and authorize. Repeat **for every guild** you want this bot
usable in (see [How the puppet pool works](#how-the-puppet-pool-works) — a bot
only serves guilds it has joined). If you're bridging two channels in the
same guild, make sure each has enough *distinct* bots available; sharing the
whole pool across both is fine as long as the pool is big enough to cover
their combined peak speaker counts.

The bot needs no further per-guild setup — the bridge sets its nickname per
Matrix speaker at runtime.

## Naming and avatars

- **Application/bot name:** anything recognizable to you as an operator (e.g.
  `Voice Bridge Puppet 1`) — Matrix speakers won't see this name; the bridge
  overrides each puppet's **nickname** per guild at runtime (default prefix
  `[Matrix] `, configurable via `puppets.nickname_prefix`).
- **Avatar:** optional, and **global to the application** — Discord
  rate-limits avatar changes to about **once per 10 minutes**, and there is no
  way to set a different avatar per guild or per represented Matrix user. Pick
  a static avatar per bot (or leave the default) and don't expect it to
  reflect who's currently speaking through it; only the nickname does that.

## Wiring tokens into `config.toml`

Add every bot's token to the shared `[puppets]` pool (raw token, no `Bot `
prefix, no duplicates):

```toml
[puppets]
tokens = [
    "YOUR_PUPPET_BOT_TOKEN_1",
    "YOUR_PUPPET_BOT_TOKEN_2",
    # ...one entry per bot
]
```

See `config.example.toml` for the full annotated schema (nickname prefix,
standby nickname, etc.).

**Docker / systemd deployments:** instead of `config.toml`, set the
`NETHER_PUPPET_TOKENS` environment variable to a comma-separated list of
tokens (see [`docs/docker-usage.md`](docker-usage.md) for the full environment
variable reference). Either way, **never commit tokens** to version control —
`config.toml` is already git-ignored for this reason.

---

<!--
Screenshots still needed (capture at 1:1 in the Developer Portal, redact
nothing sensitive is visible by default on these pages, but double-check
before publishing — application ids are not secret, tokens are):

- docs/images/discord-setup-01-new-application.png
  "New Application" modal (name field + Create button)
- docs/images/discord-setup-02-general-information.png
  General Information page after creation (showing where the client id lives)
- docs/images/discord-setup-03-bot-tab.png
  Left sidebar with the "Bot" tab highlighted
- docs/images/discord-setup-04-reset-token.png
  Bot tab: Reset Token button + the one-time token reveal (use a fake/already-
  reset token in the screenshot, never a live one)
- docs/images/discord-setup-05-privileged-intents.png
  Bot tab: Privileged Gateway Intents section, all three toggles OFF
- docs/images/discord-setup-06-oauth2-url-generator.png
  OAuth2 > URL Generator page, blank/initial state
- docs/images/discord-setup-07-scopes-and-permissions.png
  URL Generator with "bot" scope checked and Connect/Speak/Use Voice
  Activity/Change Nickname checked under Bot Permissions, generated URL
  visible at the bottom
-->
