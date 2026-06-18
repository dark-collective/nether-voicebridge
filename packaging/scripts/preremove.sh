#!/bin/sh
# Runs before removal on both deb (prerm) and rpm (%preun). Stop+disable the unit
# only on a real removal, NOT during an upgrade (deb: "upgrade", rpm: arg "1").
set -e

case "${1:-}" in
    remove|purge|0)
        if [ -d /run/systemd/system ]; then
            systemctl --quiet disable --now nether-voicebridge.service || true
        fi
        ;;
esac

exit 0
