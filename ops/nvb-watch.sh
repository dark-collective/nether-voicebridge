#!/bin/bash
# nvb-watch — abuse / health / egress monitor for nether-voicebridge.
# Runs every 15 min from nvb-watch.timer. Alerts to the NVB Ops Matrix room;
# one digest per day even when quiet (so silence != dead monitor).
# See docs/operations-runbook.md in the nether-voicebridge repo.
set -u
. "${NVB_OPS_CONF:-/opt/nvb-ops/nvb-ops.conf}"
mkdir -p "$STATE_DIR"

ALERTS=()

post() { # post one plain-text notice to the ops room as the bridge bot
    local txn="nvbops$(date +%s%N)"
    curl -s -m 20 -K "${NVB_CURL_AS:-/opt/nvb-ops/curl-as.conf}" \
        -X PUT "$HS_URL/_matrix/client/v3/rooms/$OPS_ROOM/send/m.room.message/$txn?user_id=%40voicebridge_as%3Anether.im" \
        -H "Content-Type: application/json" \
        --data "$(python3 -c 'import json,sys; print(json.dumps({"msgtype":"m.notice","body":sys.argv[1]}))' "$1")" \
        >/dev/null || logger -t nvb-watch "failed to post to ops room"
}

# ── 1. audit.jsonl deltas since last run ─────────────────────────────────────
OFF_FILE="$STATE_DIR/audit.offset"
NEW_EVENTS="$STATE_DIR/audit.delta"; : > "$NEW_EVENTS"
if [ -f "$AUDIT" ]; then
    size=$(stat -c %s "$AUDIT")
    off=$(cat "$OFF_FILE" 2>/dev/null || echo 0)
    if [ "$size" -lt "$off" ]; then
        # rotated: audit.jsonl.1 holds the old generation — finish it, restart
        [ -f "$AUDIT.1" ] && tail -c +"$((off + 1))" "$AUDIT.1" >> "$NEW_EVENTS"
        off=0
    fi
    tail -c +"$((off + 1))" "$AUDIT" >> "$NEW_EVENTS"
    echo "$size" > "$OFF_FILE"
fi
if [ -s "$NEW_EVENTS" ]; then
    reports=$(jq -r 'select(.event=="report") | "  \(.actor) reported \(.detail.kind // "?") \(.detail.value // "?"): \(.detail.reason // "")"' "$NEW_EVENTS" 2>/dev/null)
    [ -n "$reports" ] && ALERTS+=("🚩 ABUSE REPORT(S) filed — review with the bot's \`reports\` command:
$reports")
    ar=$(grep -c '"event":"approve_rejected"' "$NEW_EVENTS" || true)
    [ "$ar" -ge "$APPROVE_REJECT_ALERT" ] && \
        ALERTS+=("🔑 $ar rejected /bridge approve attempts in 15 min — possible code guessing (see $AUDIT)")
    # rejection clustering per actor AND per homeserver domain
    cluster=$(jq -r 'select(.event=="link_rejected" or .event=="invite_rejected") | .actor' "$NEW_EVENTS" 2>/dev/null \
              | awk -F: 'NF{print $0; if (NF>1) print "domain "$2}' | sort | uniq -c | sort -rn \
              | awk -v t="$REJECT_CLUSTER_ALERT" '$1>=t {print "  "$1"x "$2" "$3}')
    [ -n "$cluster" ] && ALERTS+=("🛡 Rejection cluster (link/invite refusals ≥$REJECT_CLUSTER_ALERT/15min from one actor/domain):
$cluster")
fi

# ── 2. journal health since last run ─────────────────────────────────────────
CUR_FILE="$STATE_DIR/journal.cursor"
if [ -s "$CUR_FILE" ]; then
    J=$(journalctl -u "$UNIT" --after-cursor="$(cat "$CUR_FILE")" --show-cursor --no-pager 2>/dev/null)
else
    J=$(journalctl -u "$UNIT" --since "-15 min" --show-cursor --no-pager 2>/dev/null)
fi
cur=$(echo "$J" | grep -oP -- '-- cursor: \K.*' | tail -1)
[ -n "$cur" ] && echo "$cur" > "$CUR_FILE"
errn=$(echo "$J" | grep -cE " ERROR " || true)
warnn=$(echo "$J" | grep -cE " WARN " || true)
if [ "$errn" -gt 0 ]; then
    ALERTS+=("❌ $errn ERROR line(s) in the bridge journal:
$(echo "$J" | grep -E " ERROR " | head -3 | cut -c1-300)")
fi
[ "$warnn" -gt "$WARN_RATE_ALERT" ] && ALERTS+=("⚠️ WARN rate high: $warnn in 15 min — journalctl -u $UNIT")
# Discord gateway identify-storm tripwire (2026-08-08 mautrix-discord incident:
# a 5h reconnect loop crossed Discord's 1000-IDENTIFY/day limit -> token
# auto-reset). Serenity logs gateway lifecycle at info (needs serenity=info in
# RUST_LOG). Sustained storm = ~60+ lines/15min; normal = a handful/day.
gwn=$(echo "$J" | grep -cE "serenity.*(Attempting to reconnect|auto-reconnect|session invalidation|Invalid session|Failed to resume|Restarting shard)" || true)
[ "$gwn" -gt "${GW_STORM_ALERT:-20}" ] && ALERTS+=("🌪 Discord gateway churn: $gwn reconnect/invalid-session lines in 15 min — identify-storm risk (1000/day ⇒ Discord auto-resets the bot token). journalctl -u $UNIT | grep serenity")
# Known failure signature: the #95 sweep's detection line, or a ghost 403 on
# m.call.member = the room lost/never had the PL-0 override → Discord→Matrix
# silently broken in that room. A bare `post_membership failed` is NOT enough:
# the ghost retries any error (2 s backoff), so a one-off transport blip that
# recovers on the next attempt used to raise a false misconfig alert (10-08).
plmiss=$( {
    echo "$J" | grep "bridged room misconfigured" | grep -oP 'bridge=\K.*?(?= room=)'
    echo "$J" | grep "ghost post_membership failed" | grep -E "error=.*(403 Forbidden|M_FORBIDDEN)" | grep -oP 'bridge\{name=\K[^}]+'
} | sort -u | paste -sd', ')
[ -n "$plmiss" ] && ALERTS+=("🔧 Room misconfigured — m.call.member needs PL 0: bridge(s) [$plmiss]. Discord speakers will NOT appear in the Matrix call there. Fix: start an Element Call in the room once, or /devtools → m.room.power_levels → events[\"org.matrix.msc3401.call.member\"]=0 (the Roles&Permissions UI shows \"Default\" either way — check the raw event).")
# Any other cause only matters if it persists: a ghost whose LAST
# ${GHOST_FAIL_STREAK:-3}+ membership attempts in the window all failed (no
# posted/refreshed after them) is stuck out of the call.
stuck=$(echo "$J" \
    | sed -nE 's/.*bridge\{name=([^}]+)\}:ghost\{mxid=([^}]+)\}.*ghost post_membership failed.*/\1\t\2\tF/p
               s/.*bridge\{name=([^}]+)\}:ghost\{mxid=([^}]+)\}.*ghost call membership (posted|refreshed).*/\1\t\2\tS/p' \
    | awk -F'\t' -v n="${GHOST_FAIL_STREAK:-3}" '{k=$1"\t"$2; s[k] = ($3=="F") ? s[k]+1 : 0}
          END {for (k in s) if (s[k] >= n) {split(k, a, "\t"); print a[1]}}' \
    | sort -u | paste -sd', ')
[ -n "$stuck" ] && ALERTS+=("👻 Ghost membership STUCK — repeated m.call.member post failures with no recovery: bridge(s) [$stuck]. Discord speakers there are missing from the Matrix call. Not a 403 (that alerts as misconfigured) — check the error= in: journalctl -u $UNIT | grep 'post_membership failed'
$(echo "$J" | grep "ghost post_membership failed" | tail -1 | cut -c1-300)")
alive=$(systemctl is-active "$UNIT")
[ "$alive" != "active" ] && ALERTS+=("💀 $UNIT is $alive")

# ── 2b. bridge-level liveness (#131) ─────────────────────────────────────────
# `systemctl is-active` only proves the PROCESS is alive. On 2026-08-11 five
# bridges died INSIDE a healthy-looking process (#130); three stayed dead for
# three days while every daily digest read "err=0 warn=0 svc=active", because
# the ERROR scan above is an *event* check over a 15-minute window and a dead
# bridge is silent. Two independent state checks now:
#
#   (a) the roster the supervisor publishes (v0.3.2+) — authoritative when
#       fresh: it names every configured bridge and whether it is actually up.
#   (b) FALLBACK, only when (a) is absent or stale: a journal latch on the death
#       signature. `channel unregistered from puppet pool` names a bridge that
#       just went away; it stays latched until that bridge logs again.
#
# Either way, re-alert DAILY while a bridge stays down: the 08-11 outage produced
# one alert, at 01:30 on a Monday, and nothing ever repeated it.
BRIDGES_LINE=""
DOWN_FILE="$STATE_DIR/bridges.down"
ALERT_STAMP="$STATE_DIR/bridges.alert"
ROSTER="${ROSTER:-/var/lib/nether-voicebridge/bridges.json}"

# (a) first: is there a fresh roster? If so it is authoritative and the journal
# latch below is skipped entirely. The latch is a *fallback* for builds without
# a roster — running both is noisy, because an ordinary `systemctl restart`
# unregisters every channel and would latch (then clear) all of them, emitting
# one ✅ per bridge and risking a false down-alert whenever the restart straddles
# a window boundary.
ROSTER_FRESH=0
if [ -f "$ROSTER" ] && r_upd_probe=$(jq -r '.updated // 0' "$ROSTER" 2>/dev/null) \
   && [ "$(( $(date +%s) - ${r_upd_probe:-0} ))" -le "${ROSTER_STALE_SECS:-300}" ]; then
    ROSTER_FRESH=1
    # Drop any latch left over from a pre-roster build — the roster now answers
    # this question directly, so a stale latch could only ever misfire.
    rm -f "$DOWN_FILE"
fi

# (b) maintain the latch from this window's journal (fallback only).
touch "$DOWN_FILE"
if [ "$ROSTER_FRESH" = "0" ]; then
    while read -r b; do
        [ -z "$b" ] && continue
        grep -qxF "$b" "$DOWN_FILE" || echo "$b" >> "$DOWN_FILE"
    done < <(echo "$J" | grep -oP 'channel unregistered from puppet pool bridge="\K[^"]+')
    # Proof of life clears the latch — but only evidence that comes AFTER the death.
    # A bridge's dying window contains its own lines (the `bridge failed` ERROR is
    # emitted from inside its span), so "any line from this bridge" would have the
    # corpse clearing its own latch. Compare positions instead: the bridge is alive
    # only if its last log line follows the last unregistration in this window.
    # Iterate over a snapshot, since the loop rewrites the file it reads from.
    mapfile -t _latched < <(sort -u "$DOWN_FILE" | grep . || true)
    for b in "${_latched[@]:-}"; do
        [ -z "$b" ] && continue
        last_unreg=$(echo "$J" | grep -nF "channel unregistered from puppet pool bridge=\"$b\"" | tail -1 | cut -d: -f1)
        last_life=$(echo "$J" | grep -nF "bridge{name=$b}" | tail -1 | cut -d: -f1)
        if [ -n "$last_life" ] && { [ -z "$last_unreg" ] || [ "$last_life" -gt "$last_unreg" ]; }; then
            # `grep -v` exits 1 when it filters out EVERY line — i.e. exactly when
            # the last latched bridge recovers — so the move must not be chained
            # behind its exit status, or the latch would stick forever and alarm
            # daily about a bridge that is fine.
            grep -vxF "$b" "$DOWN_FILE" > "$DOWN_FILE.tmp" || true
            mv "$DOWN_FILE.tmp" "$DOWN_FILE"
            ALERTS+=("✅ bridge [$b] is logging again — clearing its down-latch.")
        fi
    done
fi

# The roster's own verdict (when the running build publishes one).
roster_down=""
if [ -f "$ROSTER" ] && r_upd=$(jq -r '.updated // 0' "$ROSTER" 2>/dev/null); then
    r_age=$(( $(date +%s) - ${r_upd:-0} ))
    r_total=$(jq -r '.bridges | length' "$ROSTER" 2>/dev/null || echo 0)
    r_alive=$(jq -r '[.bridges[] | select(.state=="alive")] | length' "$ROSTER" 2>/dev/null || echo 0)
    BRIDGES_LINE="| bridges=$r_alive/$r_total"
    if [ "$r_age" -gt "${ROSTER_STALE_SECS:-300}" ]; then
        # Dead-man's switch: the supervisor refreshes this every 60 s.
        ALERTS+=("🕰 bridge roster is STALE (${r_age}s old, >${ROSTER_STALE_SECS:-300}s) — the supervisor loop may be wedged. $ROSTER")
        BRIDGES_LINE="$BRIDGES_LINE(stale)"
    else
        roster_down=$(jq -r '.bridges[] | select(.state!="alive") | "  \(.name) [\(.state)] failures=\(.consecutive_failures)"' "$ROSTER" 2>/dev/null)
    fi
fi

# Alert on the union of both signals, at most once a day per distinct set.
down_sig=$(printf '%s\n%s' "$roster_down" "$(sort -u "$DOWN_FILE" 2>/dev/null | sed 's/^/  /')" | grep . | sort -u)
if [ -n "$down_sig" ]; then
    stamp="$(date +%F)|$(printf '%s' "$down_sig" | md5sum | cut -c1-12)"
    if [ "$(cat "$ALERT_STAMP" 2>/dev/null)" != "$stamp" ]; then
        ALERTS+=("🧟 BRIDGE(S) DOWN inside a running process — this is invisible to \`systemctl is-active\`:
$down_sig
The process auto-restarts a failed bridge with backoff (v0.3.2+); a bridge listed here across several checks is NOT recovering. Triage: journalctl -u $UNIT | grep -E 'bridge failed|scheduling automatic restart'")
        echo "$stamp" > "$ALERT_STAMP"
    fi
else
    rm -f "$ALERT_STAMP"
fi

# ── 3. egress canary ─────────────────────────────────────────────────────────
pid=$(systemctl show -p MainPID --value "$UNIT")
tcpn=0; udpn=0
if [ -n "$pid" ] && [ "$pid" != "0" ]; then
    remotes=$(ss -tnp state established 2>/dev/null | grep "pid=$pid," | awk '{print $4}' | sed 's/:[0-9]*$//')
    tcpn=$(printf '%s' "$remotes" | grep -c . || true)
    udpn=$(ss -unp 2>/dev/null | grep -c "pid=$pid," || true)
    # allow-set: Cloudflare edge (homeserver + Discord gateway) + private + resolved HS
    allow='^(104\.1[6-9]|104\.2[0-9]|172\.6[4-9]|172\.7[01]|162\.15[89]|108\.162|141\.101|173\.245|188\.114|190\.93|197\.234|198\.41|10\.|192\.168\.|100\.6[4-9]|100\.[7-9][0-9]|100\.1[01][0-9]|100\.12[0-7]|127\.)'
    # Multi-SFU (MSC4195, bridge v0.3.8+): the bridge also subscribes on OTHER
    # homeservers' LiveKit SFUs (e.g. matrix-org.livekit.cloud) and ghosts may
    # publish there. Those hosts are dynamic, so allow exactly the SFU / lk-jwt
    # hosts the bridge itself logged connecting to in the last 24 h
    # (`sfu_url=wss://…` from the session, `sfu=https://…` from the election).
    sfu_hosts=$(journalctl -u "$UNIT" --since "-24h" --no-pager 2>/dev/null \
        | grep -oE '(sfu_url=wss|sfu=https)://[A-Za-z0-9._-]+' | sed -E 's#^[a-z_]+=[a-z]+://##' | sort -u)
    hs_ips=$(for h in nether.im ${EXTRA_ALLOW_HOSTS:-} $sfu_hosts; do getent ahostsv4 "$h" 2>/dev/null | awk '{print $1}'; done | sort -u | sed 's/\./\\./g' | paste -sd'|')
    [ -n "$hs_ips" ] && allow="$allow|^($hs_ips)\$"
    strangers=$(printf '%s\n' "$remotes" | sort -u | grep -Ev "$allow" | grep . || true)
    [ "$tcpn" -gt "$TCP_MAX" ] && ALERTS+=("🔌 TCP connection count $tcpn exceeds envelope ($TCP_MAX) — leak or unexpected load")
    [ -n "$strangers" ] && ALERTS+=("👁 Unexpected egress endpoints (not Cloudflare/Discord/homeserver/private):
$(echo "$strangers" | head -5 | sed 's/^/  /')")
fi

# ── 3a. dormant-MXID invite canary (#96) ─────────────────────────────────────
# @nether-voicebridge_bot is the conventional bot address users may still try
# (old docs, muscle memory) but NOTHING serves it — prod's bot is
# @voicebridge_as. An invite to it sits pending forever, silently: exactly how
# the first real beta user got stranded (2026-07-12). Sync as it via the
# appservice token (same exclusive namespace) and alert on any pending invite;
# state-latched per room so each stranded invite alerts once, and the state
# clears when the invite set empties (a re-invite re-alerts).
DORMANT_MXID="${DORMANT_MXID:-@nether-voicebridge_bot:nether.im}"
DORMANT_STATE="$STATE_DIR/dormant.invites"
enc_mxid=$(python3 -c 'import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1],safe=""))' "$DORMANT_MXID")
dsync=$(curl -s -m 20 -K "${NVB_CURL_AS:-/opt/nvb-ops/curl-as.conf}" \
    "$HS_URL/_matrix/client/v3/sync?user_id=$enc_mxid&timeout=0&filter=%7B%22room%22%3A%7B%22timeline%22%3A%7B%22limit%22%3A0%7D%7D%7D" 2>/dev/null)
if [ -n "$dsync" ]; then
    pend=$(printf '%s' "$dsync" | jq -r '.rooms.invite // {} | keys[]' 2>/dev/null)
    if [ -n "$pend" ]; then
        new=$(comm -13 <(sort "$DORMANT_STATE" 2>/dev/null) <(printf '%s\n' "$pend" | sort))
        [ -n "$new" ] && ALERTS+=("📬 Pending invite(s) to the DORMANT bot address $DORMANT_MXID — nobody serves it, the inviter is stranded (#96). Point them at @voicebridge_as:nether.im, then reject the invite:
$(printf '%s\n' "$new" | sed 's/^/  /')")
        printf '%s\n' "$pend" | sort > "$DORMANT_STATE"
    else
        : > "$DORMANT_STATE"
    fi
fi

# ── 3b. Actions conclusions: e2e nightly + master CI ─────────────────────────
# The live e2e suite (Forgejo Actions e2e.yml) went RED for 8 days unnoticed
# (#124) — nothing watched its conclusion. Poll the newest *scheduled* run via
# the public Actions API and alert on a NEW failing run (state-tracked → at
# most one alert per nightly, not every 15 min) or a stale suite (no scheduled
# run in >30h = the 08:30 cron silently stopped firing). Read-only; no token.
# The same response also feeds the master-CI check further down (#131).
E2E_STATE="$STATE_DIR/e2e.lastrun"
E2E_DIGEST=""
CI_DIGEST=""
e2e_json=$(curl -sL -m 20 -A "nvb-watch/1.0" \
    "https://nether.codes/api/v1/repos/nether/nether-voicebridge/actions/tasks?limit=40" 2>/dev/null)
if [ -n "$e2e_json" ]; then
    latest=$(printf '%s' "$e2e_json" | jq -r '
        [.workflow_runs[]? | select(.workflow_id=="e2e.yml" and .event=="schedule")]
        | sort_by(.run_started_at) | last
        | if . == null then empty else "\(.run_number) \(.status) \(.run_started_at)" end' 2>/dev/null)
    if [ -n "$latest" ]; then
        e2e_rn=$(echo "$latest"   | awk '{print $1}')
        e2e_st=$(echo "$latest"   | awk '{print $2}')
        e2e_at=$(echo "$latest"   | awk '{print $3}')
        # State = "<run_number> <conclusion>" (a bare run number = the pre-fix format, conclusion unknown).
        e2e_prev=$(cat "$E2E_STATE" 2>/dev/null || echo "")
        e2e_prev_rn=${e2e_prev%% *}
        e2e_prev_st=""; case "$e2e_prev" in *" "*) e2e_prev_st=${e2e_prev#* } ;; esac
        E2E_DIGEST="| e2e:#$e2e_rn $e2e_st"
        run_url="https://nether.codes/nether/nether-voicebridge/actions/runs/$e2e_rn"
        if [ "$e2e_rn" != "$e2e_prev_rn" ]; then
            # First time we see this run number: alert once on its conclusion.
            if [ "$e2e_st" = "failure" ]; then
                ALERTS+=("🌙❌ e2e nightly #$e2e_rn FAILED (scheduled $e2e_at) — the live regression harness is RED. Triage: $run_url  (standing cluster: issue #124).")
            elif [ "$e2e_st" = "success" ] && [ "$e2e_prev_st" = "failure" ]; then
                # Green-again ONLY after a latched red (like the CI check below) — the old run-number-only latch
                # announced "GREEN again" after every green night (2026-09-28 → 10-03: six false notices).
                ALERTS+=("🌙✅ e2e nightly #$e2e_rn is GREEN again (scheduled $e2e_at).")
            fi
            # Only latch a terminal conclusion so an in-progress run re-alerts once it finishes.
            case "$e2e_st" in success|failure|cancelled) echo "$e2e_rn $e2e_st" > "$E2E_STATE" ;; esac
        fi
        # Stale-suite: newest scheduled run older than 30h → cron likely dead
        # (one alert/day via a date stamp so it doesn't spam every 15 min).
        e2e_epoch=$(date -d "$e2e_at" +%s 2>/dev/null || echo 0)
        if [ "$e2e_epoch" -gt 0 ]; then
            e2e_age_h=$(( ( $(date +%s) - e2e_epoch ) / 3600 ))
            E2E_STALE_STAMP="$STATE_DIR/e2e.stale.date"
            if [ "$e2e_age_h" -ge 30 ] && [ "$(cat "$E2E_STALE_STAMP" 2>/dev/null)" != "$(date +%F)" ]; then
                ALERTS+=("🌙⏰ e2e nightly STALE — newest scheduled run #$e2e_rn was ${e2e_age_h}h ago (>30h). The 08:30 cron may have stopped firing.")
                echo "$(date +%F)" > "$E2E_STALE_STAMP"
            fi
        fi
    fi

    # ── ci.yml on master (#131 follow-up) ────────────────────────────────────
    # The hermetic gate (tests + clippy -D warnings) sat RED on master for 19
    # days (ad53ccd 07-26 → 08-14) unnoticed: this guardrail watched only
    # e2e.yml. Same API response, no extra call. One structural difference from
    # the nightly: a nightly re-alerts by itself (a NEW failing run number
    # appears every day), but master CI only runs on a push — a standing red
    # would alert exactly once, ever. That one-shot shape is precisely what hid
    # the #130 outage, so while master stays red this re-alerts DAILY.
    CI_STATE="$STATE_DIR/ci.lastrun"
    CI_RED_STAMP="$STATE_DIR/ci.red.date"
    latest_ci=$(printf '%s' "$e2e_json" | jq -r '
        [.workflow_runs[]? | select(.workflow_id=="ci.yml" and .head_branch=="master")]
        | sort_by(.run_started_at) | last
        | if . == null then empty else "\(.run_number) \(.status)" end' 2>/dev/null)
    if [ -n "$latest_ci" ]; then
        ci_rn=${latest_ci%% *}
        ci_st=${latest_ci##* }
        ci_prev=$(cat "$CI_STATE" 2>/dev/null || echo "")
        CI_DIGEST="| ci:#$ci_rn $ci_st"
        ci_url="https://nether.codes/nether/nether-voicebridge/actions/runs/$ci_rn"
        if [ "$latest_ci" != "$ci_prev" ]; then
            case "$ci_st" in
                failure)
                    ALERTS+=("🔨❌ CI #$ci_rn FAILED on master — the hermetic gate (tests + clippy) is RED and every branch will inherit it. Triage: $ci_url")
                    echo "$(date +%F)" > "$CI_RED_STAMP" ;;
                success)
                    # Green-again only after a latched red — master CI runs on
                    # every merge, so a ✅ per green push would be pure noise.
                    case "$ci_prev" in *failure) ALERTS+=("🔨✅ CI #$ci_rn is GREEN again on master.") ;; esac
                    rm -f "$CI_RED_STAMP" ;;
            esac
            # Only latch a terminal conclusion so an in-progress run re-checks.
            case "$ci_st" in success|failure|cancelled) echo "$latest_ci" > "$CI_STATE" ;; esac
        elif [ "$ci_st" = "failure" ] && [ "$(cat "$CI_RED_STAMP" 2>/dev/null)" != "$(date +%F)" ]; then
            ALERTS+=("🔨❌ CI on master is STILL RED (#$ci_rn) — daily reminder; no new push has fixed it. Triage: $ci_url")
            echo "$(date +%F)" > "$CI_RED_STAMP"
        fi
    fi
fi

# ── 4. deliver ───────────────────────────────────────────────────────────────
for a in "${ALERTS[@]:-}"; do [ -n "$a" ] && post "🚨 nvb-watch: $a"; done

DIG_FILE="$STATE_DIR/digest.date"
today=$(date +%F)
if [ "$(cat "$DIG_FILE" 2>/dev/null)" != "$today" ]; then
    # digest counts the last 24h from the audit file itself (ts is epoch secs)
    cutoff=$(( $(date +%s) - 86400 ))
    day_summary=$(cat "$AUDIT.1" "$AUDIT" 2>/dev/null \
        | jq -r --argjson c "$cutoff" 'select(.ts >= $c) | .event' 2>/dev/null \
        | sort | uniq -c | sort -rn | awk '{printf "%s×%s ", $2, $1}')
    # #117: puppet-pool guild footprint — once daily (7 Discord API calls),
    # against the 100-server App Verification gate. Tokens are read from the
    # bridge config and NEVER echoed/logged; only counts leave this block.
    pool_line=""
    if [ -f "${NVB_CONFIG:-/etc/nether-voicebridge/config.toml}" ]; then
        mapfile -t _toks < <(awk '/^\[puppets\]/{f=1} f && /^\]/{f=0} f' \
            "${NVB_CONFIG:-/etc/nether-voicebridge/config.toml}" \
            | grep -oE '"[^"]{50,}"' | tr -d '"')
        _i=0; _max=0; _ids="$STATE_DIR/pool.ids"; : > "$_ids"
        for _t in "${_toks[@]}"; do
            _i=$((_i + 1))
            _g=$(curl -s -m 15 -H "Authorization: Bot $_t" \
                "https://discord.com/api/v10/users/@me/guilds" 2>/dev/null)
            _n=$(printf '%s' "$_g" | jq 'if type=="array" then length else -1 end' 2>/dev/null || echo -1)
            if [ "${_n:--1}" -ge 0 ]; then
                printf '%s' "$_g" | jq -r '.[].id' >> "$_ids" 2>/dev/null
                pool_line="$pool_line b$_i=$_n"
                [ "$_n" -gt "$_max" ] && _max=$_n
            else
                pool_line="$pool_line b$_i=?"
            fi
            sleep 1
        done
        _distinct=$(sort -u "$_ids" | grep -c . || true)
        pool_line="| pool:${pool_line} distinct=$_distinct"
        if [ "$_max" -ge "${TRIPWIRE:-60}" ]; then
            post "🚨 nvb-watch: ⚠ puppet-bot guild count $_max ≥ tripwire ${TRIPWIRE:-60} — nearing Discord's 100-server App Verification gate; start verification (docs/nether-voicebridge-verification-runbook.md). Bot's 'status' command has the per-app detail."
        fi
    fi
    # Bot device count (v0.3.5 post-mortem): every fresh crypto store mints a
    # new device for the bot, and the SDK claims one-time keys for ALL of a
    # user's own devices on every encrypted send while holding a lock the sync
    # loop needs — 3,313 dead e2e-bot devices turned a 1 s greeting into a
    # 29 s stall. Counted once daily from the masqueraded `/devices` listing;
    # the tripwire fires well before the count can hurt (prod: 19 live).
    dev_line=""
    for _who in "${AS_USER:-}" "${E2E_BOT_MXID:-}"; do
        [ -n "$_who" ] || continue
        _enc=$(python3 -c 'import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1],safe=""))' "$_who")
        _n=$(curl -s -m 20 -K "${NVB_CURL_AS:-/opt/nvb-ops/curl-as.conf}" \
            "$HS_URL/_matrix/client/v3/devices?user_id=$_enc" 2>/dev/null \
            | jq -r '.devices | length' 2>/dev/null || true)
        [[ "${_n:-}" =~ ^[0-9]+$ ]] || _n="?"
        dev_line="$dev_line ${_who%%:*}=$_n"
        if [[ "$_n" =~ ^[0-9]+$ ]] && [ "$_n" -ge "${DEVICE_TRIPWIRE:-150}" ]; then
            post "🚨 nvb-watch: ⚠ $_who has $_n devices on the homeserver (tripwire ${DEVICE_TRIPWIRE:-150}). Every encrypted send claims keys for all of them under the SDK's key-claim lock and stalls the management bot's sync loop; prune the dead ones (live ids = the device_id markers under the data dir). See CHANGELOG 0.3.5."
        fi
    done
    [ -n "$dev_line" ] && dev_line="| devices:$dev_line"
    # `bridges=N/M` is deliberately in the digest: before #131 a healthy-looking
    # digest proved only that the PROCESS was up, which is exactly how a 3-day
    # outage went unnoticed. Now a green digest states bridge liveness too.
    post "📊 nvb-watch daily digest $today — audit 24h: ${day_summary:-quiet} | journal(15m): err=$errn warn=$warnn svc=$alive ${BRIDGES_LINE:-| bridges=?} | conns: tcp=$tcpn udp=$udpn $pool_line ${dev_line:-} ${E2E_DIGEST:-} ${CI_DIGEST:-}"
    echo "$today" > "$DIG_FILE"
fi
exit 0
