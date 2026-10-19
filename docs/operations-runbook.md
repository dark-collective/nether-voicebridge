# Operations runbook — running a hosted instance

How to keep a hosted nether-voicebridge instance healthy: what to watch, what
the alerts mean, and the moderation recipes. Written for the `nether.im`
deployment but generic — paths assume the deb install (`/etc/nether-voicebridge`,
`NVB_DATA_DIR=/var/lib/nether-voicebridge`). Companion docs:
[hosted-instance.md](hosted-instance.md) (user-facing),
[nether-voicebridge-verification-runbook.md](nether-voicebridge-verification-runbook.md)
(the Discord 100-server gate).

## The monitoring loop (nvb-watch)

A small ops-side watcher (not part of the bridge binary — it keeps working even
if the bridge wedges) runs from a systemd timer every 15 minutes and posts to a
private Matrix **ops room** as the bridge bot. The scripts live in
[`ops/`](../ops/) in this repo — `nvb-watch.sh`, `nvb-backup.sh` and
`nvb-ops.conf.example` — and are deployed to `/opt/nvb-ops/` on the host, with
config in `/opt/nvb-ops/nvb-ops.conf`; auth is a root-only curl config holding
the appservice token. Edit them **in the repo and redeploy**, not in place.
`ops/test-nvb-watch.sh` exercises the alerting logic offline against stubbed
`journalctl`/`systemctl`/`curl` — run it after any change (it needs `jq`).

It checks, per window:

| Signal | Source | Alert |
| --- | --- | --- |
| Abuse reports | `audit.jsonl` delta | 🚩 any `report` event — always actionable; follow up with the bot's `reports` command |
| Code guessing | audit delta | 🔑 ≥3 `approve_rejected` / 15 min |
| Sybil probing | audit delta | 🛡 ≥5 `link_rejected`/`invite_rejected` from one actor **or** one homeserver domain |
| Bridge errors | journal cursor | ❌ any ERROR line (quoted) |
| Log noise | journal cursor | ⚠️ WARN rate over threshold (restart bursts trip this once — expected) |
| Service down | systemd | 💀 unit not active |
| **Bridge down** (#131) | `bridges.json` roster + journal latch | 🧟 any configured bridge not `alive` — **re-alerted daily** until it recovers; ✅ when it comes back |
| Roster stale (#131) | `bridges.json` `updated` stamp | 🕰 roster older than `ROSTER_STALE_SECS` (default 300 s) — the supervisor loop itself may be wedged |
| e2e nightly red | Actions API (`e2e.yml` scheduled runs) | 🌙❌ new failing run (once per run); 🌙✅ green again; 🌙⏰ no scheduled run in >30 h (dead cron) |
| **CI red on master** (#131) | GitHub mirror's public Actions API (`.github/workflows/ci.yml`, master only; the Forgejo copy was removed 2026-10-18) | 🔨❌ new failing run, then **daily re-alert while red** — master CI only runs on a push, so without the reminder a standing red alerts exactly once (it sat red 19 days unnoticed, 07-26→08-14); 🔨✅ green again after a red |
| Egress anomaly | `ss` on the bridge PID | 🔌 TCP count outside envelope; 👁 any remote endpoint not in the allow-set (Cloudflare/Discord edge, homeserver, private ranges, `EXTRA_ALLOW_HOSTS`, plus every SFU / lk-jwt host the bridge itself logged connecting to in the last 24 h — multi-SFU calls legitimately reach other homeservers' LiveKit, e.g. `matrix-org.livekit.cloud`) |
| Verification gate (#117) | Discord API, once daily with the digest | ⚠ any puppet bot's guild count ≥ `TRIPWIRE` (default 60; Discord's hard cap is 100 servers/app) — start the App Verification paperwork early |

Plus one 📊 **daily digest** even when everything is quiet — audit events in
the last 24 h, journal counts, `bridges=N/M` liveness, connection counts, the
latest `e2e:#N` / `ci:#N` conclusions, and
the **puppet-pool guild footprint** (`pool: b1=4 b2=3 … distinct=4` — per-bot
guild counts from the Discord API, #117). **If the digest stops appearing, the
monitor itself is broken** — that's the dead-man's-switch.

### Why bridge liveness is checked separately (#130 / #131)

`💀 unit not active` only proves the **process** is alive. On 2026-08-11 five
bridges died *inside* a healthy-looking process and three stayed dead for three
days, while every daily digest read `err=0 warn=0 svc=active` — because the
ERROR scan is an *event* check over a 15-minute window and a dead bridge is
silent. Since v0.3.2 the supervisor publishes `<data_dir>/bridges.json`:

```json
{"updated": 1755000000, "pid": 570490,
 "bridges": [{"name":"Light Voice","origin":"static","state":"alive",
              "since":1754990000,"consecutive_failures":0}]}
```

`state` is `alive`, `restarting` (serving a restart backoff), `down`, or
`parked` — the bridge's Matrix room is banned (a homeserver room ban, or the bot
banned from it), so it logged one ERROR and stopped instead of retrying the
same 403 forever. A parked bridge stays parked across tenant reloads until its
entry changes or is unlinked (or the process restarts). The fix is almost
always `unlink <name>`, since the room is gone. The
watcher alerts on anything not `alive`, on a stale `updated` stamp, and — for
builds without the roster — on a journal latch over
`channel unregistered from puppet pool`, cleared only by a bridge log line that
*follows* the unregistration. `bridges=N/M` in the digest means a quiet digest
now positively asserts bridge liveness instead of merely implying it.

Since v0.3.0-alpha.24 the bridge also **detects the most common breakage
itself** (#95 tiers 1–2): every maintenance sweep (~5 min,
`NVB_MAINTENANCE_INTERVAL_SECS` to override) it checks each bridged room's
`m.room.power_levels` for the `org.matrix.msc3401.call.member = 0`
requirement. A broken room gets a chat notice posted into it (with the fix),
its owner gets a DM, a `room_misconfigured` audit event is appended, and a
`bridged room misconfigured` WARN reaches the ops alerts — rate-limited to
once per room per day. Static bridges are covered too; no configuration
needed.

The audit trail (`<data_dir>/audit.jsonl`, rotated to `.1` at 64 MiB) records
every link/approve/unlink/ban/report/invite decision with the actor's MXID —
`jq` over it answers "who did what" directly.

## The check-in ritual

Weekly (or after any alert):

1. Read the ops room — digests + alerts since last time.
2. DM the bot `reports` — any abuse reports pending a decision?
3. DM the bot `list` — do the tenants look like what you expect?
4. Check the guild footprint against the **Discord verification tripwire**:
   each puppet application caps at 100 servers unverified; **start App
   Verification for any app at ~75** (see the
   [verification runbook](nether-voicebridge-verification-runbook.md)).
5. Skim `journalctl -u nether-voicebridge -p warning --since -7d | head` for
   new noise classes.

## Moderation recipes (operator DM commands)

- **Kill a user's access AND delete all their bridges:**
  `ban owner @user:example.org` — one command does both: deny-lists the MXID
  and unlinks every tenant they own. `unban owner …` reverses the ban (their
  bridges stay gone). A banned owner's pending links die too.
- **Take down a specific bridge:** `unlink <name>`.
- **Edit a bridge** (wrong channel, renamed room): there is no in-place edit —
  `unlink <name>` then `link` it again with the corrected IDs. Bridge entries
  carry no state worth preserving; re-linking re-runs all the safety probes.
- **Block a guild or room instead of a person:** `ban guild <id>` /
  `ban room <id>`.
- **Restrict self-serve to invited users:** `serve-allow @user:…` (first use
  flips from open to allowlist), `serve-open` to reopen, `serve-list` to show.
- **See recent abuse reports with ready-made ban commands:** `reports`.
- The audit trail records all of the above with your MXID as the actor.

### Removing a user from the homeserver

Do this **before** deleting or banning their rooms on the homeserver. A tenant
whose room disappears underneath it keeps running until the next restart, then
parks with an ERROR (and a 🧟 alert every day until it is unlinked).

1. Find everything they linked:
   `jq -c 'select(.actor | test("^@name:")) | select(.event=="link_ok") | .detail' audit.jsonl`
   (match on the localpart; people often have accounts on several homeservers).
   Note each `room_id` and `guild_id`.
2. `ban owner @name:server` for **every** account they use. Each ban also
   unlinks every tenant that account owns.
3. `ban guild <id>` for Discord guilds that are theirs, and `ban room <id>` for
   their Matrix rooms. A ban guards against a **re-link** even from an account
   you didn't know about. It also unlinks any tenant on that guild or room,
   whoever owns it.
4. `list` — confirm nothing of theirs is left.
5. Only now delete or ban the rooms on the homeserver.

## Backups

`/opt/nvb-ops/nvb-backup.sh` (daily timer, 04:15) snapshots the admin state —
`config.toml`, `tenants.toml`, `banlist.toml`, `self_serve_allow.toml`,
`audit.jsonl*` — to `/var/backups/nether-voicebridge/nvb-YYYYMMDD.tgz`
(mode 600: it contains tokens), keeping 14 days.

**Deliberately excluded:** the crypto stores. Losing one is harmless by
design — the bridge re-provisions a fresh device id on a fresh store and peers
just see a new device (never a changed key). Restoring an old crypto store
over a newer one is actually the dangerous direction; don't.

**Restore:** stop the service, untar the pieces you need back into place
(`tar xzf … --absolute-names`), start, and check `list` matches expectations.

## Known limits (what users can hit)

The `[limits]` block plus pool geometry bound every abuse surface; the values
are the operator's choice. What a user can bump into, and what they see, is
documented user-side in [self-serve-bridging.md](self-serve-bridging.md#limits)
— keep that section in sync when changing prod limits.
