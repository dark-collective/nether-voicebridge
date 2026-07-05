#!/usr/bin/env bash
# Bootstrap an arm64 Forgejo Actions runner for nether.codes (issue #49).
#
# Target: a fresh Ubuntu 22.04/24.04 arm64 VM (e.g. Oracle Always-Free Ampere
# A1.Flex). Run as root. Idempotent — safe to re-run; already-done steps are
# skipped. Full runbook (Oracle provisioning, token, verification):
# docs/arm64-runner-setup.md.
#
# Usage:
#   sudo NVB_RUNNER_TOKEN=<registration-token> ./setup-arm64-runner.sh
#
# Optional env:
#   NVB_FORGEJO_URL    Instance URL the runner registers against and polls.
#                      Default http://100.64.0.13:3000 — Forgejo over the
#                      tailnet, skipping Cloudflare (whose ~100 MB body cap and
#                      flaky chunked uploads break registry pushes). Requires
#                      this host to be on the tailnet first.
#   NVB_RUNNER_NAME    Display name in Forgejo (default: oracle-arm64).
#   NVB_RUNNER_LABELS  Runner labels (default:
#                      docker-arm64:docker://ghcr.io/catthehacker/ubuntu:act-22.04
#                      — the arch-suffixed label .forgejo/workflows/docker.yml
#                      targets; the image is multi-arch, arm64 included).
#   TS_AUTHKEY         Tailscale auth key for non-interactive `tailscale up`.
#                      Without it, the script prints the interactive login URL.
set -euo pipefail

FORGEJO_URL="${NVB_FORGEJO_URL:-http://100.64.0.13:3000}"
RUNNER_NAME="${NVB_RUNNER_NAME:-oracle-arm64}"
RUNNER_LABELS="${NVB_RUNNER_LABELS:-docker-arm64:docker://ghcr.io/catthehacker/ubuntu:act-22.04}"
# Must match the workflow's `env.REGISTRY` (plain-HTTP registry on the tailnet).
REGISTRY_HOST="100.64.0.13:3000"
# Pin the runner (same policy as songbird/livekit — don't float CI-critical deps).
RUNNER_VERSION="12.12.0"

RUNNER_USER="forgejo-runner"
RUNNER_HOME="/var/lib/forgejo-runner"
RUNNER_CONFIG="/etc/forgejo-runner/config.yml"

[ "$(id -u)" -eq 0 ] || { echo "run as root (sudo)" >&2; exit 1; }
[ "$(uname -m)" = "aarch64" ] || { echo "this host is $(uname -m), expected aarch64" >&2; exit 1; }

# ── Docker Engine ─────────────────────────────────────────────────────────────
if ! command -v docker >/dev/null 2>&1; then
    echo ">> installing Docker Engine"
    curl -fsSL https://get.docker.com | sh
else
    echo ">> docker already installed: $(docker --version)"
fi

# The tailnet registry is plain HTTP — the daemon must trust it or every
# pull/push in CI fails on TLS. Merge (don't clobber) /etc/docker/daemon.json.
echo ">> ensuring ${REGISTRY_HOST} is in docker insecure-registries"
python3 - "$REGISTRY_HOST" <<'PY'
import json, os, sys
host = sys.argv[1]
path = "/etc/docker/daemon.json"
conf = {}
if os.path.exists(path):
    with open(path) as f:
        conf = json.load(f)
regs = set(conf.get("insecure-registries", []))
if host not in regs:
    regs.add(host)
    conf["insecure-registries"] = sorted(regs)
    with open(path, "w") as f:
        json.dump(conf, f, indent=2)
    print("updated", path)
else:
    print(path, "already lists", host)
PY
systemctl restart docker

# ── Tailscale (needed to reach the Forgejo/registry tailnet address) ─────────
case "$FORGEJO_URL" in
  *100.64.*)
    if ! command -v tailscale >/dev/null 2>&1; then
        echo ">> installing tailscale"
        curl -fsSL https://tailscale.com/install.sh | sh
    fi
    if ! tailscale status >/dev/null 2>&1; then
        echo ">> bringing tailscale up"
        if [ -n "${TS_AUTHKEY:-}" ]; then
            tailscale up --authkey "$TS_AUTHKEY"
        else
            # Prints a login URL — open it in a browser, then re-run this script.
            tailscale up
        fi
    fi
    echo ">> tailscale is up"
    ;;
esac

# ── forgejo-runner binary ─────────────────────────────────────────────────────
if [ ! -x /usr/local/bin/forgejo-runner ] || \
   ! /usr/local/bin/forgejo-runner --version 2>/dev/null | grep -q "$RUNNER_VERSION"; then
    echo ">> installing forgejo-runner v${RUNNER_VERSION} (arm64)"
    curl -fsSL -o /usr/local/bin/forgejo-runner \
        "https://code.forgejo.org/forgejo/runner/releases/download/v${RUNNER_VERSION}/forgejo-runner-${RUNNER_VERSION}-linux-arm64"
    chmod 755 /usr/local/bin/forgejo-runner
fi
/usr/local/bin/forgejo-runner --version

# ── Runner user + directories ────────────────────────────────────────────────
if ! id "$RUNNER_USER" >/dev/null 2>&1; then
    useradd --system --create-home --home-dir "$RUNNER_HOME" --shell /usr/sbin/nologin "$RUNNER_USER"
fi
usermod -aG docker "$RUNNER_USER"
mkdir -p "$RUNNER_HOME" "$(dirname "$RUNNER_CONFIG")"
chown "$RUNNER_USER": "$RUNNER_HOME"

# ── Runner config ─────────────────────────────────────────────────────────────
# Generate the full default config once, then set the one non-default we need:
# docker_host "automount" mounts the host Docker socket into job containers
# (docker.yml's build-push/login steps drive the host daemon, same as the
# existing amd64 runner).
if [ ! -f "$RUNNER_CONFIG" ]; then
    echo ">> writing $RUNNER_CONFIG"
    /usr/local/bin/forgejo-runner generate-config > "$RUNNER_CONFIG"
    sed -i 's|docker_host: ""|docker_host: "automount"|' "$RUNNER_CONFIG"
fi
grep -q 'docker_host: "automount"' "$RUNNER_CONFIG" \
    || { echo "ERROR: $RUNNER_CONFIG lacks docker_host: \"automount\" — set it manually" >&2; exit 1; }

# ── Registration ──────────────────────────────────────────────────────────────
# One-time: creates ${RUNNER_HOME}/.runner. Get the token from Forgejo (as the
# repo owner): Settings → Actions → Runners → Create new runner.
if [ ! -f "$RUNNER_HOME/.runner" ]; then
    [ -n "${NVB_RUNNER_TOKEN:-}" ] || {
        echo "ERROR: not yet registered and NVB_RUNNER_TOKEN is unset." >&2
        echo "Re-run with: sudo NVB_RUNNER_TOKEN=<token> $0" >&2
        exit 1
    }
    echo ">> registering '$RUNNER_NAME' with $FORGEJO_URL"
    (cd "$RUNNER_HOME" && sudo -u "$RUNNER_USER" /usr/local/bin/forgejo-runner register \
        --no-interactive \
        --instance "$FORGEJO_URL" \
        --token "$NVB_RUNNER_TOKEN" \
        --name "$RUNNER_NAME" \
        --labels "$RUNNER_LABELS")
else
    echo ">> already registered ($RUNNER_HOME/.runner exists)"
fi

# ── systemd service ───────────────────────────────────────────────────────────
cat > /etc/systemd/system/forgejo-runner.service <<EOF
[Unit]
Description=Forgejo Actions runner (arm64)
After=network-online.target docker.service tailscaled.service
Wants=network-online.target

[Service]
Type=simple
User=${RUNNER_USER}
WorkingDirectory=${RUNNER_HOME}
ExecStart=/usr/local/bin/forgejo-runner daemon -c ${RUNNER_CONFIG}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable --now forgejo-runner
sleep 2
systemctl --no-pager --lines 5 status forgejo-runner || true

echo
echo ">> done. Verify the runner shows 'Idle' under the repo's"
echo ">> Settings → Actions → Runners, then dispatch docker.yml to test."
