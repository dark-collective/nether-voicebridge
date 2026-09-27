#!/bin/bash
# Delete a bot user's DEAD devices from the homeserver — dry run by default.
#
# Why: every fresh crypto store mints a new `<base>-<rand>` device for the bot
# and the old ones never go away. The SDK claims one-time keys for ALL of a
# user's own devices on every encrypted send while holding a lock the sync
# loop needs, so a pile of dead devices makes every encrypted send — and the
# management bot's responsiveness — proportionally slower (v0.3.5 post-mortem:
# 3,313 dead e2e-bot devices = a 29 s stall).
#
# What counts as LIVE (never deleted):
#   * any device id found in a `device_id` marker under $NVB_DATA_DIR
#     (`<store>/device_id`) — the ids the running process actually uses;
#   * any device the server saw in the last $RECENT_HOURS hours — a second
#     deployment sharing this identity (staging, a fixture) syncs with its own
#     ids, and deleting a LIVE device silences its encrypted audio for good
#     (#53 wedge). Widen the window if such a deployment can idle longer.
#
#   ops/nvb-prune-devices.sh                 # report only
#   ops/nvb-prune-devices.sh --apply         # delete the dead ones
#   AS_USER=@other:example.org ops/nvb-prune-devices.sh
#
# Reads HS_URL / AS_USER from $NVB_OPS_CONF (default /opt/nvb-ops/nvb-ops.conf)
# and authenticates with the curl config $NVB_CURL_AS (the appservice token,
# never echoed). Delete path, probed live on Tuwunel 1.9.2 (2026-09-27): the
# masqueraded `DELETE /devices/{id}` is refused (403, MSC4190 not enabled), but
# an appservice login *as that device* (`m.login.application_service` +
# `device_id`) followed by `POST /logout` removes it, no UIA involved. That is
# what this tool does; the DELETE is tried first in case MSC4190 is on.
set -euo pipefail
# Environment overrides win over the conf file (so `AS_USER=@e2e:… ./script`
# targets another identity without editing the conf).
_env_hs="${HS_URL:-}"; _env_user="${AS_USER:-}"
NVB_OPS_CONF="${NVB_OPS_CONF:-/opt/nvb-ops/nvb-ops.conf}"
[ -f "$NVB_OPS_CONF" ] && . "$NVB_OPS_CONF"
HS_URL="${_env_hs:-${HS_URL:-}}"; AS_USER="${_env_user:-${AS_USER:-}}"
NVB_CURL_AS="${NVB_CURL_AS:-/opt/nvb-ops/curl-as.conf}"
NVB_DATA_DIR="${NVB_DATA_DIR:-/var/lib/nether-voicebridge}"
RECENT_HOURS="${RECENT_HOURS:-24}"
APPLY=0; [ "${1:-}" = "--apply" ] && APPLY=1
: "${HS_URL:?HS_URL unset}"; : "${AS_USER:?AS_USER unset}"

enc=$(python3 -c 'import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1],safe=""))' "$AS_USER")
listing=$(curl -fsS -m 60 -K "$NVB_CURL_AS" "$HS_URL/_matrix/client/v3/devices?user_id=$enc")
total=$(jq -r '.devices | length' <<<"$listing")

live_file=$(mktemp); trap 'rm -f "$live_file"' EXIT
# One id per marker file; the files carry no trailing newline, so read them
# one at a time rather than concatenating.
for _m in "$NVB_DATA_DIR"/*/device_id; do
    [ -f "$_m" ] && tr -d '[:space:]' < "$_m" && echo
done 2>/dev/null | grep . > "$live_file" || true
cutoff_ms=$(( ( $(date +%s) - RECENT_HOURS * 3600 ) * 1000 ))

# dead = not in a marker AND not seen recently
dead=$(jq -r --argjson c "$cutoff_ms" --rawfile live "$live_file" '
  ($live | split("\n") | map(select(length > 0))) as $l
  | .devices[]
  | select((.device_id as $d | $l | index($d) | not) and ((.last_seen_ts // 0) < $c))
  | .device_id' <<<"$listing")
n_dead=$(grep -c . <<<"$dead" || true)
n_live=$(grep -c . "$live_file" || true)
echo "$AS_USER: $total devices on the homeserver; $n_live live marker(s) under $NVB_DATA_DIR; $n_dead dead (unmarked and unseen for ${RECENT_HOURS}h)"
[ "$n_dead" -gt 0 ] || exit 0
if [ "$APPLY" -ne 1 ]; then
    echo "dry run — would delete:"; echo "$dead" | sed 's/^/  /'
    echo "re-run with --apply to delete them"
    exit 0
fi
ok=0; bad=0
while IFS= read -r id; do
    [ -n "$id" ] || continue
    encid=$(python3 -c 'import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1],safe=""))' "$id")
    code=$(curl -s -o /dev/null -w '%{http_code}' -m 30 -K "$NVB_CURL_AS" -X DELETE \
        -H 'Content-Type: application/json' -d '{}' \
        "$HS_URL/_matrix/client/v3/devices/$encid?user_id=$enc")
    if [ "$code" != 200 ] && [ "$code" != 404 ]; then
        # No MSC4190: log in as the device, then log it out (deletes it).
        lp="${AS_USER#@}"; lp="${lp%%:*}"
        at=$(curl -s -m 30 -K "$NVB_CURL_AS" -X POST -H 'Content-Type: application/json' \
            -d "{\"type\":\"m.login.application_service\",\"identifier\":{\"type\":\"m.id.user\",\"user\":\"$lp\"},\"device_id\":\"$id\"}" \
            "$HS_URL/_matrix/client/v3/login" | jq -r '.access_token // empty' 2>/dev/null || true)
        if [ -n "$at" ]; then
            code=$(curl -s -o /dev/null -w '%{http_code}' -m 30 -X POST -H "Authorization: Bearer $at" \
                -H 'Content-Type: application/json' -d '{}' "$HS_URL/_matrix/client/v3/logout")
            unset at
        fi
    fi
    case "$code" in 200|404) ok=$((ok+1));; *) bad=$((bad+1)); echo "  $id -> HTTP $code";; esac
done <<<"$dead"
echo "deleted $ok, failed $bad; remaining: $(curl -fsS -m 60 -K "$NVB_CURL_AS" "$HS_URL/_matrix/client/v3/devices?user_id=$enc" | jq -r '.devices | length')"
