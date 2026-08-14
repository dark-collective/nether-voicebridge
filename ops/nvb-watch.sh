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
# Known failure signature: ghost 403 on m.call.member (runtime) or the #95
# sweep's detection line = the room lost/never had the PL-0 override →
# Discord→Matrix silently broken in that room.
plmiss=$(echo "$J" | grep -E "post_membership failed|bridged room misconfigured" | grep -oP '(bridge\{name=|bridge=)\K[^}: ]+' | sort -u | paste -sd', ')
[ -n "$plmiss" ] && ALERTS+=("🔧 Room misconfigured — m.call.member needs PL 0: bridge(s) [$plmiss]. Discord speakers will NOT appear in the Matrix call there. Fix: start an Element Call in the room once, or /devtools → m.room.power_levels → events[\"org.matrix.msc3401.call.member\"]=0 (the Roles&Permissions UI shows \"Default\" either way — check the raw event).")
alive=$(systemctl is-active "$UNIT")
[ "$alive" != "active" ] && ALERTS+=("💀 $UNIT is $alive")

# ── 2b. bridge-level liveness (#131) ─────────────────────────────────────────
# `systemctl is-active` only proves the PROCESS is alive. On 2026-08-11 five
# bridges died INSIDE a healthy-looking process (#130); three stayed dead for
# three days while every daily digest read "err=0 warn=0 svc=active", because
# the ERROR scan above is an *event* check over a 15-minute window and a dead
# bridge is silent. Two independent state checks now:
#
#   (a) the roster the supervisor publishes (v0.3.2+) — authoritative: it names
#       every configured bridge and whether it is actually up.
#   (b) a journal latch on the death signature — covers older builds, and
#       cross-checks (a). `channel unregistered from puppet pool` names a bridge
#       that just went away; it stays latched until that bridge logs again.
#
# Both re-alert DAILY while a bridge stays down: the 08-11 outage produced one
# alert, at 01:30 on a Monday, and nothing ever repeated it.
BRIDGES_LINE=""
DOWN_FILE="$STATE_DIR/bridges.down"
ALERT_STAMP="$STATE_DIR/bridges.alert"
ROSTER="${ROSTER:-/var/lib/nether-voicebridge/bridges.json}"

# (b) maintain the latch from this window's journal.
touch "$DOWN_FILE"
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

# (a) the authoritative roster, when the running build publishes one.
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
    hs_ips=$(for h in nether.im ${EXTRA_ALLOW_HOSTS:-}; do getent ahostsv4 "$h" 2>/dev/null | awk '{print $1}'; done | sort -u | sed 's/\./\\./g' | paste -sd'|')
    [ -n "$hs_ips" ] && allow="$allow|^($hs_ips)\$"
    strangers=$(printf '%s\n' "$remotes" | sort -u | grep -Ev "$allow" | grep . || true)
    [ "$tcpn" -gt "$TCP_MAX" ] && ALERTS+=("🔌 TCP connection count $tcpn exceeds envelope ($TCP_MAX) — leak or unexpected load")
    [ -n "$strangers" ] && ALERTS+=("👁 Unexpected egress endpoints (not Cloudflare/Discord/homeserver/private):
$(echo "$strangers" | head -5 | sed 's/^/  /')")
fi

# ── 3b. e2e nightly conclusion (#124 / #123 step 4 guardrail) ────────────────
# The live e2e suite (Forgejo Actions e2e.yml) went RED for 8 days unnoticed
# (#124) — nothing watched its conclusion. Poll the newest *scheduled* run via
# the public Actions API and alert on a NEW failing run (state-tracked → at
# most one alert per nightly, not every 15 min) or a stale suite (no scheduled
# run in >30h = the 08:30 cron silently stopped firing). Read-only; no token.
E2E_STATE="$STATE_DIR/e2e.lastrun"
E2E_DIGEST=""
e2e_json=$(curl -s -m 20 -A "nvb-watch/1.0" \
    "https://nether.codes/api/v1/repos/dark/nether-voicebridge/actions/tasks?limit=40" 2>/dev/null)
if [ -n "$e2e_json" ]; then
    latest=$(printf '%s' "$e2e_json" | jq -r '
        [.workflow_runs[]? | select(.workflow_id=="e2e.yml" and .event=="schedule")]
        | sort_by(.run_started_at) | last
        | if . == null then empty else "\(.run_number) \(.status) \(.run_started_at)" end' 2>/dev/null)
    if [ -n "$latest" ]; then
        e2e_rn=$(echo "$latest"   | awk '{print $1}')
        e2e_st=$(echo "$latest"   | awk '{print $2}')
        e2e_at=$(echo "$latest"   | awk '{print $3}')
        e2e_prev=$(cat "$E2E_STATE" 2>/dev/null || echo "")
        E2E_DIGEST="| e2e:#$e2e_rn $e2e_st"
        run_url="https://nether.codes/dark/nether-voicebridge/actions/runs/$e2e_rn"
        if [ "$e2e_rn" != "$e2e_prev" ]; then
            # First time we see this run number: alert once on its conclusion.
            if [ "$e2e_st" = "failure" ]; then
                ALERTS+=("🌙❌ e2e nightly #$e2e_rn FAILED (scheduled $e2e_at) — the live regression harness is RED. Triage: $run_url  (standing cluster: issue #124).")
            elif [ "$e2e_st" = "success" ] && [ -n "$e2e_prev" ]; then
                ALERTS+=("🌙✅ e2e nightly #$e2e_rn is GREEN again (scheduled $e2e_at).")
            fi
            # Only latch a terminal conclusion so an in-progress run re-alerts once it finishes.
            case "$e2e_st" in success|failure|cancelled) echo "$e2e_rn" > "$E2E_STATE" ;; esac
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
    # `bridges=N/M` is deliberately in the digest: before #131 a healthy-looking
    # digest proved only that the PROCESS was up, which is exactly how a 3-day
    # outage went unnoticed. Now a green digest states bridge liveness too.
    post "📊 nvb-watch daily digest $today — audit 24h: ${day_summary:-quiet} | journal(15m): err=$errn warn=$warnn svc=$alive ${BRIDGES_LINE:-| bridges=?} | conns: tcp=$tcpn udp=$udpn $pool_line ${E2E_DIGEST:-}"
    echo "$today" > "$DIG_FILE"
fi
exit 0
