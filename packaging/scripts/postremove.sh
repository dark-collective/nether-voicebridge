#!/bin/sh
# Runs after removal on both deb (postrm) and rpm (%postun).
set -e

# Only on a full purge (deb "purge"): drop the service user/group. We deliberately
# LEAVE /var/lib/nether-voicebridge in place — it holds Olm crypto stores, and
# silently destroying them on remove would be surprising. The admin can rm it.
case "${1:-}" in
    purge)
        if getent passwd nether-voicebridge >/dev/null 2>&1; then
            userdel nether-voicebridge || true
        fi
        if getent group nether-voicebridge >/dev/null 2>&1; then
            groupdel nether-voicebridge || true
        fi
        ;;
esac

if [ -d /run/systemd/system ]; then
    systemctl daemon-reload || true
fi

exit 0
