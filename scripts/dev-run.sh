#!/usr/bin/env bash
# Dev runner for the voice bridge.
#
# The bridge does nothing unless this process is running — a stopped bridge was
# the #1 source of "why isn't it working?" during testing. Run this in a
# dedicated terminal while testing; logs stream to the console AND to
# /tmp/nvb.log so you can `grep` them from another shell.
#
# Usage:  scripts/dev-run.sh
# Stop:   Ctrl-C  (clean shutdown → finalises any capture_*.wav files)
#
# Env overrides:
#   RUST_LOG     log filter (default: nether_voicebridge=debug,info)
#   CONFIG_PATH  path to config.toml (default: ./config.toml)
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ ! -f "${CONFIG_PATH:-config.toml}" ]]; then
    echo "error: ${CONFIG_PATH:-config.toml} not found — copy config.example.toml and fill it in" >&2
    exit 1
fi

export RUST_LOG="${RUST_LOG:-nether_voicebridge=debug,info}"

echo "building…"
cargo build

echo "starting bridge (Ctrl-C to stop) — logs also at /tmp/nvb.log"
exec cargo run 2>&1 | tee /tmp/nvb.log
