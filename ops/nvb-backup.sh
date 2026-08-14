#!/bin/bash
# nvb-backup — daily snapshot of the bridge's admin/config state.
# Crypto stores are deliberately EXCLUDED: losing one just re-provisions a
# fresh device (peer-safe, #30). The archive contains tokens → mode 600.
# See docs/operations-runbook.md in the nether-voicebridge repo.
set -eu
. "${NVB_OPS_CONF:-/opt/nvb-ops/nvb-ops.conf}"
mkdir -p "$BACKUP_DIR"
out="$BACKUP_DIR/nvb-$(date +%Y%m%d).tgz"
files=()
for f in /etc/nether-voicebridge/config.toml \
         /var/lib/nether-voicebridge/tenants.toml \
         /var/lib/nether-voicebridge/banlist.toml \
         /var/lib/nether-voicebridge/self_serve_allow.toml \
         /var/lib/nether-voicebridge/audit.jsonl \
         /var/lib/nether-voicebridge/audit.jsonl.1 \
         /opt/nvb-ops/nvb-ops.conf; do
    [ -f "$f" ] && files+=("$f")
done
tar czf "$out" --absolute-names "${files[@]}"
chmod 600 "$out"
# prune: keep the newest $BACKUP_KEEP
ls -1t "$BACKUP_DIR"/nvb-*.tgz 2>/dev/null | tail -n +"$((BACKUP_KEEP + 1))" | xargs -r rm -f
logger -t nvb-backup "wrote $out ($(stat -c %s "$out") bytes)"
