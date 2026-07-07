# Privacy Policy — Nether Voice Bridge (hosted instance)

_Last updated: 2026-07-07. This is the hostable source for the policy required
by Discord Developer Terms §5a; publish it at a public URL and link that URL in
the **Privacy Policy** field of every Discord application in the bridge's
puppet pool._

**Operator:** Dark Collective ("we") · **Contact:** dark_collective@riseup.net

This policy covers the hosted Nether Voice Bridge service, which relays live
voice between Discord voice channels and Matrix rooms (Element Call). It does
not cover Discord or any Matrix homeserver themselves, which have their own
policies.

## What the bridge processes

- **Live voice audio.** Audio from participants in a bridged Discord voice
  channel is relayed in real time to the linked Matrix room's call, and vice
  versa. **Audio is ephemeral: it is never recorded, stored, or used for any
  purpose other than live relay.** No transcripts are made. Audio is not used
  to train any machine-learning system.
- **Discord identity data.** For participants in a bridged channel: user id,
  display name/nickname, and voice state (joined/left, mute). These are used to
  represent you in the Matrix call as a named "ghost" participant and are
  visible to the Matrix room's members. Avatars are not copied.
- **Matrix identity data.** For participants in a bridged call: Matrix user id
  and display name, used to represent you in the Discord channel as a named bot
  participant.
- **Bridge administration records.** When someone links a channel: the Matrix
  user id of the requester, the Discord guild and channel ids, the Discord
  username/id of the approving server manager, and timestamps. Kept in the
  service's configuration and an append-only audit log for abuse prevention and
  operational forensics. Deny lists (banned users/guilds/rooms) are kept for
  enforcement.
- **Operational logs.** Service logs contain connection events and the
  identifiers above (never audio). Logs rotate and are not retained
  indefinitely.

## What the bridge does NOT do

- No recording or storage of voice audio; no transcripts.
- No message content processing (the bridge does not read text channels).
- No sale, sharing, or commercialization of any data.
- No advertising, profiling, or automated decision-making.
- No training of AI/ML models on any relayed content.

## Where data goes

Relayed audio and identity data flow between Discord's infrastructure and the
Matrix homeserver/SFU serving the linked room (for this instance:
**nether.im** and its LiveKit media server). Members of the linked Matrix room
and the bridged Discord channel can see and hear the relayed participants —
that is the service's purpose. Infrastructure providers hosting this instance
act as processors: Akamai/Linode (cloud hosting), Cloudflare (network/TLS
proxy); parts of the service run on privately operated server hardware.

## Transparency and consent

- A bridge link is only created with the explicit approval of a person holding
  the **Manage Server** permission in the Discord server.
- Bridged Matrix participants appear in Discord with a distinguishing prefix
  (default `[Matrix]`), so their presence is disclosed in-channel.
- If you do not want your voice relayed, leave the bridged voice channel or ask
  a server manager to unlink it. (A per-user opt-out command is planned.)

## Retention and deletion

- **Voice audio:** not retained at all (live relay only).
- **Link/audit records:** retained while the link exists and afterwards as
  operational history; deleted on request (see contact) where not needed for
  abuse enforcement.
- **Logs:** rotated on a short operational schedule.

## Your rights (GDPR/UK GDPR/CCPA)

Where these laws apply to you, you may request access to, correction of, or
deletion of personal data we hold (practically: link records, audit entries,
and deny-list entries naming you), and you may lodge a complaint with your
supervisory authority. Contact dark_collective@riseup.net; we respond within 30 days.
We process the data above to provide the service you or your community
requested (legitimate interest / performance of the service).

## Changes

We will update this page when the service's data handling changes; the
"last updated" date reflects the current revision.
