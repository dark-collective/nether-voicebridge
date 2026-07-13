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
private Matrix **ops room** as the bridge bot. On the nether.im instance it
lives at `/opt/nvb-ops/nvb-watch.sh` with config in `/opt/nvb-ops/nvb-ops.conf`;
auth is a root-only curl config holding the appservice token.

It checks, per window:

| Signal | Source | Alert |
| --- | --- | --- |
| Abuse reports | `audit.jsonl` delta | 🚩 any `report` event — always actionable; follow up with the bot's `reports` command |
| Code guessing | audit delta | 🔑 ≥3 `approve_rejected` / 15 min |
| Sybil probing | audit delta | 🛡 ≥5 `link_rejected`/`invite_rejected` from one actor **or** one homeserver domain |
| Bridge errors | journal cursor | ❌ any ERROR line (quoted) |
| Log noise | journal cursor | ⚠️ WARN rate over threshold (restart bursts trip this once — expected) |
| Service down | systemd | 💀 unit not active |
| Egress anomaly | `ss` on the bridge PID | 🔌 TCP count outside envelope; 👁 any remote endpoint not in the allow-set (Cloudflare/Discord edge, homeserver, private ranges) |
| Verification gate (#117) | Discord API, once daily with the digest | ⚠ any puppet bot's guild count ≥ `TRIPWIRE` (default 60; Discord's hard cap is 100 servers/app) — start the App Verification paperwork early |

Plus one 📊 **daily digest** even when everything is quiet — audit events in
the last 24 h, journal counts, connection counts, and the **puppet-pool guild
footprint** (`pool: b1=4 b2=3 … distinct=4` — per-bot guild counts from the
Discord API, #117). **If the digest stops appearing, the monitor itself is
broken** — that's the dead-man's-switch.

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
