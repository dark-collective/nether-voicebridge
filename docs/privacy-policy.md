# Privacy Policy — Nether Voice Bridge (hosted instance)

_Last updated: 2026-10-29. This is the hostable source for the policy required
by Discord Developer Terms §5a; publish it at a public URL and link that URL in
the **Privacy Policy** field of every Discord application in the bridge's
puppet pool. The published copy for the nether.im instance is at
<https://nether.wiki/matrix/voice-bridge-privacy/>._

**Operator:** Dark Collective ("we"), operating from the United States ·
**Contact:** dark_collective@riseup.net

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
  username/id of the approving server manager, and timestamps. When someone
  files an abuse report: the reporter, the reported target, and the reason
  given. The service also records bridge-lifecycle and reachability events
  (link / approve / unlink / ban, and invite accept/reject decisions). These are
  kept in the service's configuration and an append-only audit log for abuse
  prevention and operational forensics. Deny lists (banned users/guilds/rooms)
  are kept for enforcement.
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
Matrix homeserver/SFU serving the linked room (for this instance: **nether.im**
and its LiveKit media server). Members of the linked Matrix room and the bridged
Discord channel can see and hear the relayed participants — that is the
service's purpose. **All processing takes place in the United States:** the
LiveKit media server runs on Akamai/Linode in North Carolina, and the Matrix
homeserver runs on a Florida-based host (Sepfy); Cloudflare provides network/TLS
proxying. These providers act as processors.

## Transparency and consent

- A bridge link is only created with the explicit approval of a person holding
  the **Manage Server** permission in the Discord server, and its creation is
  disclosed with a notice in the bridged channel's text chat.
- Bridged Matrix participants appear in Discord with a distinguishing prefix
  (default `[Matrix]`), so their presence is disclosed in-channel.
- If you do not want your voice relayed, run **`/bridge optout`** in the
  Discord server — your voice will never be relayed and you will not appear in
  bridged calls, on any channel this instance bridges, until you run
  `/bridge optin`. (Leaving the bridged voice channel works too, of course.)
- To report an abusive bridge, DM the bridge bot (`report <owner|guild|room>
  <value> [reason]`) or email the contact above. A human reviews every report;
  nothing is actioned automatically.

## Age

This service is intended for users who meet Discord's minimum age (13, or older
where local law requires) and any age requirement of the Matrix room or
community it is used in (the nether.im community is 18+). We do not knowingly
process data from anyone under these ages.

## Retention and deletion

- **Voice audio:** not retained at all (live relay only).
- **Link/audit records:** retained while the link exists and afterwards as
  operational history; deleted on request (see contact) where not needed for
  abuse enforcement.
- **Logs & audit trail:** service logs and the append-only audit trail are
  size-bounded and rotate on a short operational schedule — not kept
  indefinitely.
- **Pairings and service-account records:** kept until you `unpair` (or the
  room's moderators `relay deny`).
- **Companion keys and your permissions for them:** kept until the key is
  revoked or its room is unlinked; a permission you gave with `/bridge
  companion allow` until you withdraw it (`/bridge companion deny` or
  `/bridge optout`) or the key is gone.
- **Your enrolled devices:** kept until you unlink or revoke the device, or a
  newer enrollment of the same device replaces it; an enrollment you never
  confirm is deleted after 10 minutes.

### Linked identities (optional)

If you choose to pair your Matrix account with a Discord account (`pair` /
`/bridge pair`), the bridge stores that mapping — your Matrix user ID, the
Discord user ID, an optional delegate account you name, and when it was created
— so it can recognise you as one person when both accounts are present. It is
created only by you, from both accounts, and `unpair` deletes it immediately.
Room moderators may likewise record that a specific Matrix account is a service
account in their room (`relay allow`); `relay deny` deletes that record.

### Companion tools and your own devices (optional)

Instances may offer an interface for companion tools (it is off unless the
operator enables it). People with authority over a bridged room may issue
keys to companion tools for that room. For each key the bridge stores its
issuer (a Matrix user id), its scope, a hash of its secret (never the secret
itself), its label and when it was created; the personal permissions you grant
with `/bridge companion allow` (your Discord user id, the key and when); and
audit lines for each lease (the key id, the room, the Discord user id and ghost
it represents, the consent basis, and timestamps). A room's authorized
companions may learn which Discord members the bridge represents in the call
(Discord user id, display name and ghost), whether you are streaming on Discord
and whether your ghost is present. In a room without media encryption, a companion holding a
lease can receive the call's audio while the lease lasts. The bridge never
carries a companion's media.

If you enroll a listen-only device of your own Matrix account, the bridge
stores your Matrix user id and the device id you chose as a key (and, until you
confirm the enrollment from your account, the direct-message room the
confirmation code was sent to), until you unlink or revoke the device or enroll
it again; plus audit lines of the enrollment, your confirmation and each
connection (room, device, time). The one-time sign-in token your device
presents is checked once with your homeserver and is never stored or logged.
When the device enrolls and while its event stream is open, the bridge sees its
network address; it is kept only briefly in memory for rate limiting, not with
the key or in the audit log. The bridge sends that device only your own media
key, your own call state and whether your paired Discord account is in the
channel — never anyone else's key, and never audio.

## Your rights (GDPR/UK GDPR/CCPA)

Where these laws apply to you, you may request access to, correction of, or
deletion of personal data we hold (practically: link records, audit entries,
and deny-list entries naming you), and you may lodge a complaint with your
supervisory authority. Contact dark_collective@riseup.net; we respond within 30 days.
We process the data above to provide the service you or your community
requested (legitimate interest / performance of the service).

## Governing law

Dark Collective operates from the United States, and all data is processed in
the U.S. This policy is governed by the laws of the United States. The GDPR /
UK GDPR / CCPA rights described above are honored regardless of where you live.

## Changes

We will update this page when the service's data handling changes; the
"last updated" date reflects the current revision.
