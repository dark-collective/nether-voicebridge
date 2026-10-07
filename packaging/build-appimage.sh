#!/usr/bin/env bash
# Wrap an already-built nether-voicebridge binary in an AppImage.
#
#   packaging/build-appimage.sh <binary> <arch> <version> <outdir>
#
# <arch> is the uname-style arch of <binary> (x86_64 | aarch64). Run from the
# REPOSITORY ROOT. The host only needs curl, sha256sum and file(1) (which
# appimagetool shells out to): the x86_64
# appimagetool packs either architecture (the target arch comes from the
# pinned type2 runtime handed to it), and it runs via extract-and-run, so no
# FUSE is needed in CI containers.
#
# Tools are pinned by version AND sha256 (a moved/compromised upstream asset
# fails the build instead of silently shipping). Downloads are cached in
# $NVB_APPIMAGE_TOOLS (default: target/appimage-tools).
set -euo pipefail

bin=$1 arch=$2 version=$3 outdir=$4

command -v file >/dev/null || { echo "build-appimage.sh: file(1) is required by appimagetool" >&2; exit 1; }

APPIMAGETOOL_VERSION=1.9.1
APPIMAGETOOL_SHA256=ed4ce84f0d9caff66f50bcca6ff6f35aae54ce8135408b3fa33abfc3cb384eb0
RUNTIME_VERSION=20251108
case "$arch" in
  x86_64)  RUNTIME_SHA256=2fca8b443c92510f1483a883f60061ad09b46b978b2631c807cd873a47ec260d ;;
  aarch64) RUNTIME_SHA256=00cbdfcf917cc6c0ff6d3347d59e0ca1f7f45a6df1a428a0d6d8a78664d87444 ;;
  *) echo "unsupported arch: $arch" >&2; exit 1 ;;
esac

tools=${NVB_APPIMAGE_TOOLS:-target/appimage-tools}
mkdir -p "$tools" "$outdir"

fetch() { # <url> <dest> <sha256>
  if [ ! -f "$2" ] || ! echo "$3  $2" | sha256sum -c --quiet - 2>/dev/null; then
    curl -fsSL -o "$2.tmp" "$1"
    echo "$3  $2.tmp" | sha256sum -c --quiet -
    mv "$2.tmp" "$2"
  fi
}

tool="$tools/appimagetool-${APPIMAGETOOL_VERSION}-x86_64.AppImage"
runtime="$tools/runtime-${RUNTIME_VERSION}-${arch}"
fetch "https://github.com/AppImage/appimagetool/releases/download/${APPIMAGETOOL_VERSION}/appimagetool-x86_64.AppImage" \
  "$tool" "$APPIMAGETOOL_SHA256"
fetch "https://github.com/AppImage/type2-runtime/releases/download/${RUNTIME_VERSION}/runtime-${arch}" \
  "$runtime" "$RUNTIME_SHA256"
chmod +x "$tool"

appdir=$(mktemp -d)/nether-voicebridge.AppDir
trap 'rm -rf "$(dirname "$appdir")"' EXIT
mkdir -p "$appdir/usr/bin" "$appdir/usr/share/doc/nether-voicebridge"
install -m 0755 "$bin" "$appdir/usr/bin/nether-voicebridge"
install -m 0755 packaging/appimage/AppRun "$appdir/AppRun"
install -m 0644 packaging/appimage/nether-voicebridge.desktop packaging/appimage/nether-voicebridge.svg "$appdir/"
install -m 0644 config.example.toml LICENSE README.md "$appdir/usr/share/doc/nether-voicebridge/"

out="$outdir/nether-voicebridge-${version}-${arch}.AppImage"
ARCH="$arch" APPIMAGE_EXTRACT_AND_RUN=1 "$tool" --no-appstream \
  --runtime-file "$runtime" "$appdir" "$out"
ls -l "$out"
