<!-- Draft issue text (not filed yet). -->
# Your own call data for your own tools

**What:** let a device of your own Matrix account — a recorder, a broadcast-software source, a second client that only
listens — get *your own* call data from the bridge without joining the call itself: your call state and the SFUs
you're on, your own media key in encrypted rooms, and whether your paired Discord account is in the bridged channel.

**Why:** in an encrypted call, clients send media keys only to call members. A listen-only device of yours would have
to post its own call membership (showing up as an extra participant, receiving everyone's keys) just to decrypt your
own media. The bridge is already a call member and already receives your key.

**How (ADR 0005):**
- enroll once: `POST /_nvb/v1/self/enroll` with an OpenID token from your homeserver, verified on its federation API;
  you get a device-bound key, revocable, replaced on re-enroll;
- `GET /_nvb/v1/self/events?room_id=…` while you're in the call: `self_call`, `key`, `paired`, `revoked`;
- the bridge advertises its API in the room (`im.nether.voicebridge.api`) when `[companions] public_url` is set;
- the stale-membership reaper ignores enrolled listen-only devices, so they can't keep a crashed client's membership
  alive.

**Never:** another member's key, a ghost's key, any media, or a stored user credential.

Off unless `[companions]` is enabled.
