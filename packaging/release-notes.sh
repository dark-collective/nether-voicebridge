#!/usr/bin/env bash
# Print the release notes for a tag: its section of CHANGELOG.md, without the
# heading (the release is already titled by the tag).
#
#   packaging/release-notes.sh v1.2.3 [CHANGELOG.md]
#
# REPO_URL (optional, e.g. https://nether.codes/nether/nether-voicebridge)
# adds a link to the full changelog at that tag. A tag without its own section
# (folded into a later one) gets only that link, or a pointer to CHANGELOG.md.
set -euo pipefail

tag=${1:?usage: release-notes.sh <tag> [changelog]}
changelog=${2:-CHANGELOG.md}
ver=${tag#v}

section=$(VER="$ver" awk '
  /^## \[/ { if (found) exit; if (index($0, "## [" ENVIRON["VER"] "]") == 1) { found = 1; next } }
  found { print }
' "$changelog" | sed -e '/./,$!d' | sed -e ':a' -e '/^\n*$/{$d;N;ba' -e '}')

if [ -n "${REPO_URL:-}" ]; then
  link="Full changelog: ${REPO_URL}/src/tag/${tag}/CHANGELOG.md"
else
  link="Full changelog: CHANGELOG.md"
fi

if [ -n "$section" ]; then
  printf '%s\n\n%s\n' "$section" "$link"
else
  printf 'No separate notes for this release; its changes are listed under a neighboring version.\n\n%s\n' "$link"
fi
