#!/bin/bash
# Offline harness for nvb-watch's bridge-liveness section (#131).
#
# Runs the REAL nvb-watch.sh against stubbed `journalctl`/`systemctl`/`curl`
# (earlier on PATH) and a scratch state dir, so the alerting logic can be
# exercised without a homeserver, a bridge, or the prod host. Every alert the
# script would post is captured to a file and asserted on.
#
#   ./ops/test-nvb-watch.sh        # prints PASS/FAIL per case
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
T=$(mktemp -d)
trap 'rm -rf "$T"' EXIT
mkdir -p "$T/bin" "$T/state"

# ── stubs ────────────────────────────────────────────────────────────────────
cat > "$T/bin/journalctl" <<EOF
#!/bin/bash
cat "$T/journal.txt" 2>/dev/null
echo "-- cursor: s=deadbeef"
EOF
cat > "$T/bin/systemctl" <<'EOF'
#!/bin/bash
case "$*" in
    *is-active*) echo active ;;
    *MainPID*)   echo 0 ;;
    *)           echo "" ;;
esac
EOF
# Capture ops-room posts; serve a canned Actions API response when one is
# staged at \$T/api.json (absent ⇒ empty reply, section 3b skips — most cases).
cat > "$T/bin/curl" <<EOF
#!/bin/bash
for a in "\$@"; do
    case "\$a" in
        *'"msgtype"'*) printf '%s\n---\n' "\$a" >> "$T/posts.txt" ;;
        *actions/tasks*) [ -f "$T/api.json" ] && cat "$T/api.json" ;;
    esac
done
exit 0
EOF
printf '#!/bin/bash\nexit 0\n' > "$T/bin/logger"
printf '#!/bin/bash\nexit 0\n' > "$T/bin/ss"
chmod +x "$T"/bin/*
export PATH="$T/bin:$PATH"

cat > "$T/ops.conf" <<EOF
HS_URL=https://example.invalid
AS_USER=@bot:example.invalid
OPS_ROOM=!ops:example.invalid
UNIT=nether-voicebridge
AUDIT=$T/audit.jsonl
STATE_DIR=$T/state
BACKUP_DIR=$T/backups
BACKUP_KEEP=3
APPROVE_REJECT_ALERT=3
REJECT_CLUSTER_ALERT=5
WARN_RATE_ALERT=100
TCP_MAX=40
EXTRA_ALLOW_HOSTS=
TRIPWIRE=60
NVB_CONFIG=$T/config.toml
GW_STORM_ALERT=20
EOF
export NVB_OPS_CONF="$T/ops.conf" NVB_CURL_AS="$T/ops.conf"

PASS=0; FAIL=0
run() { # run(journal_text, roster_json_or_empty)
    : > "$T/posts.txt"
    printf '%s\n' "$1" > "$T/journal.txt"
    if [ -n "${2:-}" ]; then printf '%s\n' "$2" > "$T/roster.json"; else rm -f "$T/roster.json"; fi
    # Keep the daily digest out of the way unless a case asks for it.
    echo "${DIGEST_DATE:-$(date +%F)}" > "$T/state/digest.date"
    ROSTER="$T/roster.json" bash "$HERE/nvb-watch.sh" >/dev/null 2>&1
}
# NB: bodies are JSON-encoded with ensure_ascii, so emoji arrive as \uXXXX
# escapes — assert on the ASCII part of an alert, never on its emoji.
posts() { cat "$T/posts.txt" 2>/dev/null; }
check() { # check(name, pattern, want: yes|no)
    if posts | grep -qF "$2"; then got=yes; else got=no; fi
    if [ "$got" = "$3" ]; then PASS=$((PASS+1)); echo "  PASS  $1";
    else FAIL=$((FAIL+1)); echo "  FAIL  $1 (wanted $3, got $got for '$2')"; fi
}

now=$(date +%s)
roster() { # roster(state_of_second_bridge, age_secs)
    cat <<EOF
{"updated":$((now - ${2:-0})),"pid":1,"bridges":[
 {"name":"Light Voice","origin":"static","state":"alive","since":$now,"consecutive_failures":0},
 {"name":"Dark Voice","origin":"static","state":"$1","since":$now,"consecutive_failures":2}]}
EOF
}

echo "== case 1: all bridges alive ⇒ no down alert =="
run "Aug 14 01:00 host nvb[1]: INFO bridge{name=Light Voice}: all good" "$(roster alive 0)"
check "no down alert" "BRIDGE(S) DOWN" no
check "no stale alert" "roster is STALE" no

echo "== case 2: roster says a bridge is down ⇒ alert names it =="
run "Aug 14 01:00 host nvb[1]: INFO bridge{name=Light Voice}: all good" "$(roster down 0)"
check "down alert posted" "BRIDGE(S) DOWN" yes
check "names the dead bridge" "Dark Voice [down]" yes

echo "== case 3: same failure again the same day ⇒ NOT re-alerted =="
run "Aug 14 01:15 host nvb[1]: INFO bridge{name=Light Voice}: all good" "$(roster down 0)"
check "no duplicate alert" "BRIDGE(S) DOWN" no

echo "== case 4: still down tomorrow ⇒ re-alerted (the 08-11 miss) =="
sed -i "s/^$(date +%F)/1970-01-01/" "$T/state/bridges.alert" 2>/dev/null
run "Aug 15 01:00 host nvb[1]: INFO bridge{name=Light Voice}: all good" "$(roster down 0)"
check "re-alerted on a new day" "BRIDGE(S) DOWN" yes

echo "== case 5: stale roster ⇒ dead-man's switch fires =="
rm -f "$T/state/bridges.alert"
run "Aug 14 01:00 host nvb[1]: INFO bridge{name=Light Voice}: all good" "$(roster alive 999)"
check "stale alert" "roster is STALE" yes

echo "== case 6: no roster (pre-0.3.2) — journal latch catches the death =="
rm -f "$T/state/bridges.alert" "$T/state/bridges.down"
run 'Aug 11 01:25 host nvb[1]: ERROR bridge{name=Light Voice}: nether_voicebridge::supervisor: bridge failed error=Matrix sync fatal auth error
Aug 11 01:25 host nvb[1]: INFO nvb_discord::puppet: channel unregistered from puppet pool bridge="Light Voice"' ""
check "down alert from the journal latch" "BRIDGE(S) DOWN" yes
check "names the bridge" "Light Voice" yes

echo "== case 7: latch persists while the bridge stays silent =="
sed -i "s/^$(date +%F)/1970-01-01/" "$T/state/bridges.alert" 2>/dev/null
run "Aug 12 01:00 host nvb[1]: INFO some unrelated line" ""
check "still alerting a day later" "BRIDGE(S) DOWN" yes

echo "== case 8: the bridge logs again ⇒ latch clears, ✅ posted =="
run "Aug 14 01:08 host nvb[1]: INFO bridge{name=Light Voice}: joined Matrix room" ""
check "recovery notice" "is logging again" yes
if [ -s "$T/state/bridges.down" ]; then FAIL=$((FAIL+1)); echo "  FAIL  latch file cleared"; else PASS=$((PASS+1)); echo "  PASS  latch file cleared"; fi

echo "== case 10: a fresh roster suppresses the journal latch (restart noise) =="
# An ordinary `systemctl restart` unregisters every channel. With a roster
# present that is authoritative, so the fallback latch must stay out of the way
# — otherwise every restart emits one ✅ per bridge, and a restart straddling a
# window boundary raises a false down-alert.
rm -f "$T/state/bridges.alert" "$T/state/bridges.down"
run 'Aug 14 18:16 host nvb[1]: INFO nvb_discord::puppet: channel unregistered from puppet pool bridge="Light Voice"
Aug 14 18:16 host nvb[1]: INFO nvb_discord::puppet: channel unregistered from puppet pool bridge="Dark Voice"' "$(roster alive 0)"
check "no latch chatter when the roster is fresh" "down-latch" no
check "no false down alert on restart" "BRIDGE(S) DOWN" no
if [ -s "$T/state/bridges.down" ]; then FAIL=$((FAIL+1)); echo "  FAIL  latch not used while roster is fresh"; else PASS=$((PASS+1)); echo "  PASS  latch not used while roster is fresh"; fi

echo "== case 11: a STALE roster falls back to the latch =="
rm -f "$T/state/bridges.alert" "$T/state/bridges.down"
run 'Aug 14 18:16 host nvb[1]: INFO nvb_discord::puppet: channel unregistered from puppet pool bridge="Light Voice"' "$(roster alive 999)"
check "latch still catches a death when the roster is stale" "BRIDGE(S) DOWN" yes

echo "== case 9: digest carries bridges=N/M =="
DIGEST_DATE=1970-01-01 run "Aug 14 01:00 host nvb[1]: INFO bridge{name=Light Voice}: ok" "$(roster alive 0)"
check "digest states bridge liveness" "bridges=2/2" yes

ci_api() { # ci_api(run_number, status) — a master ci.yml run, no e2e runs
    cat > "$T/api.json" <<EOF
{"workflow_runs":[
 {"run_number":$1,"workflow_id":"ci.yml","status":"$2","event":"push","head_branch":"master","run_started_at":"2026-08-14T12:00:00Z"},
 {"run_number":99,"workflow_id":"ci.yml","status":"failure","event":"push","head_branch":"some-branch","run_started_at":"2026-08-14T13:00:00Z"}
]}
EOF
}
QUIET_J="Aug 14 12:00 host nvb[1]: INFO bridge{name=Light Voice}: ok"

echo "== case 12: master CI failure ⇒ one alert (branch reds ignored) =="
rm -f "$T/state/ci.lastrun" "$T/state/ci.red.date"
ci_api 77 failure
run "$QUIET_J" "$(roster alive 0)"
check "CI red alert" "CI #77 FAILED on master" yes

echo "== case 13: same red run again the same day ⇒ silent =="
run "$QUIET_J" "$(roster alive 0)"
check "no same-day repeat" "CI" no

echo "== case 14: still red tomorrow, NO new run ⇒ daily reminder (the 19-day miss) =="
echo "1970-01-01" > "$T/state/ci.red.date"
run "$QUIET_J" "$(roster alive 0)"
check "daily STILL RED reminder" "STILL RED (#77)" yes

echo "== case 15: a green master run after the red ⇒ green-again, then quiet =="
ci_api 78 success
run "$QUIET_J" "$(roster alive 0)"
check "green-again notice" "CI #78 is GREEN again" yes
ci_api 79 success
run "$QUIET_J" "$(roster alive 0)"
check "no notice per ordinary green push" "GREEN again" no
rm -f "$T/api.json"

echo
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
