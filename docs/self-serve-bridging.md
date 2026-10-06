# Bridge your room yourself — the self-serve DM flow

How to link a Discord voice channel to a Matrix room by DMing the bridge's
management bot, on an instance where the operator has enabled
[open registration](../README.md#open-registration). No operator involvement is
needed — but the bridge will only create the link with proof of consent from
**both** sides: you must be a **moderator of the Matrix room**, and someone with
**Manage Server** on the Discord side must approve the link with a one-time
code.

> [!NOTE]
> This page is written for **users** of a hosted bridge instance. If you're an
> operator setting up your own, see the [README](../README.md) and
> [hosted-instance notes](hosted-instance.md). Whether self-serve is enabled —
> and which bot to DM — is up to each instance's operator; for the `nether.im`
> instance, see [Use the hosted instance](hosted-instance.md). The `nether.im`
> instance is in **open beta**: rough edges happen, and we appreciate every
> tester — if something misbehaves, say so in
> [`#nether-voicebridge-help:nether.im`](https://matrix.to/#/%23nether-voicebridge-help%3Anether.im).

## What you'll need

- **A Matrix account.** Any account works, including one on another homeserver —
  the bot is reachable over federation. (It's the *room* that must be reachable
  by the instance's homeserver, not you.)
- **Moderator power in the Matrix room** you want to bridge: you must be a
  joined member with an **explicit personal power-level grant** — at least 50
  (Element's "Moderator" role), and above the room's default level. Power
  everyone inherits from the room default does **not** count, even if that
  default is high. This is checked, not assumed.
- **The Discord IDs**: the server (guild) ID and the voice channel ID.
  Right-click each → "Copy Server ID" / "Copy Channel ID" (enable Developer
  Mode first: User Settings → Advanced → Developer Mode).
- **The Matrix room's internal ID** (`!something:example.org` — Element → room
  Settings → Advanced → "Internal room ID"; **not** the `#alias` form).
- **Someone with Manage Server** on the Discord guild (you, or someone willing) —
  they'll approve the link inside Discord.
- **The instance's puppet bots invited to the Discord server** with Connect,
  Speak, Use Voice Activity and Change Nickname (the operator publishes the
  invite links — for the hosted instance see
  [here](hosted-instance.md#invite-the-puppet-bots-to-your-discord-server)).

## Step 1 — prepare the Matrix room

Two room prerequisites, both verified by the bot before anything is created:

1. **The room must allow power level 0 for `org.matrix.msc3401.call.member`** —
   the standard Element Call setup. Easiest fix: start (or join) a proper
   Element Call in the room once; Element configures it automatically.

   This is the single most common bridge-breaker, and its failure mode is
   silent: the link succeeds and Matrix→Discord works, but **Discord speakers
   never appear in the Matrix call** (the ghosts' membership posts get a 403
   and retry forever). Two traps:

   - Element's "Roles & Permissions → Join Element calls" label is
     **unreliable** — it shows "Default" whether or not the room is correctly
     configured. Don't trust it in either direction.
   - The ground truth is the raw state: `/devtools` → Explore room state →
     `m.room.power_levels` → `"org.matrix.msc3401.call.member": 0` must be
     present under `"events"`; add it there if missing
     ([details](hosted-instance.md#get-the-bridge-bot-into-your-matrix-room)).

   The `link` probe checks this for you at link time — but power levels can
   regress later (room upgrades, permission edits), and the check only runs
   when linking.
   <!-- TODO(screenshots): same 3 shots as the wiki page — call button,
        devtools power_levels with the override highlighted, misleading
        Roles & Permissions dropdown. -->

2. **Restricted/invite-only room? Run `link` first, then invite the bot when
   it asks.** The bot can't join such a room on its own, so the `link` reply
   will tell you to invite it (default window: 30 minutes). Send that invite
   **from your own account, from the room itself** — the moment it arrives,
   the link continues automatically. (The bot joins as a full appservice
   member; no `guest_access` needed. A room in your server's allow-space
   needs no invite at all.)

## Step 2 — DM the management bot

Open a **direct message** with the instance's bridge bot (its Matrix ID is
published by the operator). The bot only auto-joins *direct* invites — it will
refuse to be pulled into group rooms — and it stays silent in any room with more
than two members.

Send `help` to see your command set:

```
link <name> <matrix-room-id> <discord-guild-id> <discord-channel-id>
unlink <name>
list
report <owner|guild|room> <value> [reason]
help
```

## Step 3 — run `link`

```
link "Game Night" !abcdef:example.org 123456789012345678 234567890123456789
```

Pick any `name` you like (quote it if it contains spaces) — it's how you'll
refer to the bridge later in `unlink` and `list`.

The bot now runs its preflight checks: it joins the room, verifies the call
power levels and MatrixRTC discovery, and checks that you're a moderator of the
room. If something is missing you get a specific message telling you what to
fix. If everything passes, **nothing is created yet** — instead you receive a
one-time approval code:

```
Discord-side approval required: someone with the Manage Server permission
in guild 123456789012345678 must run /bridge approve code:NVB-XXXXXX
```

## Step 4 — approve on the Discord side

Someone holding **Manage Server** (or Administrator) in that Discord server runs
the slash command, in any text channel of that server:

```
/bridge approve code:NVB-XXXXXX
```

- The command is served by one of the instance's puppet bots; Discord hides it
  from members without Manage Server, and the bridge re-checks the permission
  server-side.
- The code is **single-use, expires** (10 minutes by default — operator
  configurable), and is only valid in the guild the link named. An expired code
  just means running `link` again.
- On success, the bridge starts immediately: the approver sees the result in
  Discord, you get a confirmation in your DM, and a **public disclosure notice**
  is posted in the bridged voice channel's text chat — every server member can
  see the channel is now relayed to Matrix, and who approved it.

That's it. Join the Discord voice channel or the room's Element Call and talk —
each speaker appears on the far side as their own named participant.

## Managing your bridges

- `list` — shows the bridges **you** created and whether they're running.
- `unlink <name>` — removes a bridge you created (a neutral notice is posted in
  the Discord channel so its members know the relay ended).
- You can only see and remove your own bridges; other people's are invisible to
  you.

## Limits you may hit

Instances cap self-serve usage (all operator-configured, so exact numbers
vary). The `nether.im` instance currently sets:

- **2 bridges per account** (pending approvals count toward it) and **3 per
  Discord server**;
- a **30-second cooldown** between commands that change something, and **5
  `link` attempts per hour** (plus a global rate limit shared by everyone);
- the **approval code expires after 10 minutes** — if the Discord side missed
  the window, just run `link` again for a fresh code.

If you hit one of those, the refusal message says exactly which limit and (for
the rate limits) when to retry.

Two limits are about call size rather than commands, and they **don't produce
an error message** — worth knowing so they don't look like breakage:

- **Discord speakers per Matrix call:** each bridge relays up to **16**
  concurrent Discord speakers into the Element Call (each as their own named
  ghost). A 17th simultaneous speaker is **not relayed at all** until a slot
  frees (someone leaves or goes quiet long enough to be released) — they're
  fine on the Discord side, just absent from Matrix.
- **Named Matrix speakers per Discord channel:** the shared pool of **7**
  puppet bots means up to 7 Matrix speakers get their own named participant in
  a Discord channel at once — an 8th simultaneous speaker is still heard, but
  their audio comes out of the channel's anchor bot instead of a bot with
  their name (it blends back once someone stops talking).

## Consent, disclosure, and opting out

- Voice is relayed **live only** — never recorded, stored, or transcribed. See
  the instance's privacy policy (linked from the bot's own messages when
  configured).
- The link exists only because a room moderator asked for it *and* a Discord
  server manager approved it, and its creation is disclosed publicly in the
  channel.
- Any Discord user can exclude themselves from relay entirely with
  `/bridge optout` in a bridged server (`/bridge optin` reverses it).
- To flag an abusive bridge to the operator, DM the bot:
  `report <owner|guild|room> <value> [reason]` — e.g.
  `report guild 123456789012345678 harassment`. Reports are reviewed by a
  human; nothing is removed automatically.

## Being yourself on both sides (linked identities)

If you are in the Discord voice channel with your own Discord account **and** in
the Matrix room's call with your own Matrix account, the bridge would normally
show a ghost of your Discord account on Matrix and a bot puppet of you on
Discord. Pairing the two accounts tells the bridge they are one person:

1. DM the management bot `pair`. It replies with a one-time code.
2. On Discord, **as the account you want to link**, run `/bridge pair code:<code>`
   in any server this bridge serves (within 10 minutes). Nobody else can do this
   step for you, which is what makes the pairing yours.

From then on, whenever that Discord account is in a bridged voice channel *and*
your Matrix account is in that room's call, the bridge creates no ghost and no
puppet for you and doesn't count you twice. On every other occasion you are an
ordinary participant — nothing is stored about where you are, only that the two
accounts belong together. If your Discord side drops out while you stay in the
call, you get a puppet again automatically and the bot tells you once. You keep hearing the
Discord side in the call the whole time: while you are paired and present, a bridge bot may sit in the
voice channel under its standby name to carry the Discord speakers to you.

- `pairings` lists your pairings; `unpair [discord-user-id]` removes one or all;
  `/bridge unpair` on Discord does the same from that side.
- You may pair several Discord accounts (e.g. a main and a secondary account); a Discord account
  pairs with one Matrix account at a time.
- `pair delegate <@mxid>` names an account that receives your notices (for people
  who run a helper for you); `pair delegate none` clears it.

Room moderators (or the bridge's owner) can also approve a **service account** in
their room — a Matrix account that sits in the call but should get no Discord
puppet and not count as a participant, e.g. a relay helper: `relay allow
<room-id> <@mxid>`, `relay deny …` to revoke, `relay list <room-id>` to see them.
Operators can additionally pin exclusions in the bridge config
(`excluded_discord_user_ids` / `excluded_matrix_user_ids`).

Design record: `docs/adr/0001-linked-identities.md`.

## Troubleshooting

| Symptom | Likely cause / fix |
| --- | --- |
| The bot never joins your DM | The instance may not have open registration enabled — ask the operator. Or you're banned. |
| The bot joined but never answers | You're messaging it in a group room — commands only work in a 2-person DM. |
| "not a moderator" style refusal on `link` | You need a personal Moderator role (an explicit power-level grant ≥ 50, above the room default) in the target room — ask a room admin to promote *you specifically*; a permissive room-wide default isn't enough. |
| Probe failure about power levels | The room doesn't allow PL 0 for `org.matrix.msc3401.call.member` — see Step 1. |
| Probe failure joining the room | Restricted room: the reply asks you to invite the bot — do that from the room, from the same account you ran `link` with, within the window it names. `list` shows any link still awaiting your invite. |
| `/bridge approve` says unknown/expired | The code timed out, was already used, was minted for a different guild — or you ran it on a different bridge instance's bot (each instance only knows its own codes). Run `link` again for a fresh code. |
| Approved, but no audio | Usually the puppet bots aren't in the guild or lack Connect/Speak on the channel — recheck the invite links and channel permission overrides. |
