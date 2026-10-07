#!/usr/bin/env bash
# Tar up a static binary (from the Dockerfile `static` target) with the
# example config, systemd unit, license and README, and upload it to the
# Release for $TAG via publish-release.sh (needs API/TOKEN/TAG).
#
#   packaging/static/package-static.sh <static-binary>
#
# Run from the REPOSITORY ROOT on the binary's own architecture.
set -euo pipefail

bin=$1
: "${TAG:?}"
# v0.2.1-rc1 -> 0.2.1~rc1, matching the deb/rpm/tarball/AppImage names.
v="${TAG#v}"
version="${v/-/\~}"
arch=$(uname -m)

stage="nether-voicebridge-${version}-${arch}-linux-static"
rm -rf "dist/${stage}" && mkdir -p "dist/${stage}"
install -m 0755 "$bin" "dist/${stage}/nether-voicebridge"
cp config.example.toml packaging/nether-voicebridge.service LICENSE README.md "dist/${stage}/"
tar -C dist -czf "dist/${stage}.tar.gz" "${stage}"
rm -rf "dist/${stage}"
ls -l "dist/${stage}.tar.gz"

packaging/publish-release.sh "dist/${stage}.tar.gz"
