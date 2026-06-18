#!/bin/sh
# Runs after install/upgrade on both deb (postinst) and rpm (%post).
# Idempotent: safe to run on every upgrade.
set -e

SVC_USER=nether-voicebridge
SVC_GROUP=nether-voicebridge
STATE_DIR=/var/lib/nether-voicebridge
CONF_DIR=/etc/nether-voicebridge

# Dedicated, unprivileged system user/group for the daemon.
if ! getent group "$SVC_GROUP" >/dev/null 2>&1; then
    groupadd --system "$SVC_GROUP"
fi
if ! getent passwd "$SVC_USER" >/dev/null 2>&1; then
    useradd --system --gid "$SVC_GROUP" --home-dir "$STATE_DIR" \
        --shell /usr/sbin/nologin --comment "Nether Voice Bridge" "$SVC_USER"
fi

# State dir for the matrix-sdk SQLite stores (Olm crypto + state). systemd's
# StateDirectory= also ensures this on start; create it now so a pre-seeded store
# works on first boot.
mkdir -p "$STATE_DIR"
chown "$SVC_USER:$SVC_GROUP" "$STATE_DIR"
chmod 0750 "$STATE_DIR"

# config.toml holds Discord bot tokens + Matrix appservice tokens — keep the dir
# readable only by root and the service group, never world-readable.
if [ -d "$CONF_DIR" ]; then
    chown root:"$SVC_GROUP" "$CONF_DIR"
    chmod 0750 "$CONF_DIR"
fi

if [ -d /run/systemd/system ]; then
    systemctl daemon-reload || true
fi

# First install (no live config yet): point the admin at the next steps.
if [ ! -f "$CONF_DIR/config.toml" ]; then
    echo "nether-voicebridge: copy $CONF_DIR/config.example.toml -> $CONF_DIR/config.toml"
    echo "  fill in your Discord/Matrix credentials, then:"
    echo "    chown root:$SVC_GROUP $CONF_DIR/config.toml && chmod 640 $CONF_DIR/config.toml"
    echo "    systemctl enable --now nether-voicebridge"
fi

exit 0
