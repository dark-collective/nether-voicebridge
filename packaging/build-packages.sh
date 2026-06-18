#!/usr/bin/env bash
# Build the release binary and produce .deb + .rpm into dist/.
# Run from anywhere; resolves the repo root itself.
#
# Requirements: a Rust toolchain + the native build deps (cmake, clang,
# libclang-dev, libopus-dev, protobuf-compiler, libssl-dev, pkg-config) and nfpm
# (https://nfpm.goreleaser.com/install/) on PATH.
#
# Usage:
#   packaging/build-packages.sh            # version from Cargo.toml
#   NVB_VERSION=0.2.0 packaging/build-packages.sh   # explicit override
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# Version: explicit env wins, else read the workspace package version.
if [ -z "${NVB_VERSION:-}" ]; then
    NVB_VERSION="$(cargo metadata --format-version 1 --no-deps \
        | python3 -c 'import sys,json; print(next(p["version"] for p in json.load(sys.stdin)["packages"] if p["name"]=="nether-voicebridge"))')"
fi
export NVB_VERSION
echo ">> building nether-voicebridge $NVB_VERSION"

cargo build --release --locked -p nether-voicebridge

mkdir -p dist
nfpm package -f packaging/nfpm.yaml -p deb -t dist/
nfpm package -f packaging/nfpm.yaml -p rpm -t dist/

echo ">> artifacts:"
ls -1 dist/*.deb dist/*.rpm
