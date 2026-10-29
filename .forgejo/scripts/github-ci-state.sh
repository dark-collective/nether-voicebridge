#!/bin/sh
# Prints the GitHub mirror's CI verdict for a commit: red | green | pending | unknown.
# Used by e2e.yml to skip live runs on pushes whose hermetic gate (.github/workflows/ci.yml)
# already failed. Unauthenticated public API: anything unexpected (mirror lag, rate limit,
# network) prints "unknown" and the caller runs as before — this only ever skips on a
# definite red.
# usage: github-ci-state.sh <sha>
sha=${1:?sha}
repo=${GITHUB_MIRROR_REPO:-dark-collective/nether-voicebridge}
body=$(curl -fsS --max-time 15 -H 'Accept: application/vnd.github+json' \
  "https://api.github.com/repos/$repo/commits/$sha/check-runs" 2>/dev/null) || { echo unknown; exit 0; }
# One check run per line of interest: "status" and "conclusion" values in order.
states=$(printf '%s' "$body" | tr ',' '\n' | sed -n 's/^ *"\(status\|conclusion\)": *"\{0,1\}\([a-z_]*\)"\{0,1\} *$/\1=\2/p')
[ -n "$states" ] || { echo unknown; exit 0; }
if printf '%s\n' "$states" | grep -qE '^conclusion=(failure|timed_out|startup_failure)$'; then
  echo red
elif printf '%s\n' "$states" | grep -qE '^status=(queued|in_progress|waiting|pending|requested)$'; then
  echo pending
elif printf '%s\n' "$states" | grep -qE '^conclusion=success$'; then
  echo green
else
  echo unknown
fi
