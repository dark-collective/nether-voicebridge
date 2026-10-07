#!/usr/bin/env bash
# Upload files to the Forgejo Release for a tag, creating the release if needed.
#
#   API=… TOKEN=… TAG=v1.2.3 packaging/publish-release.sh <file>...
#   API=… TOKEN=… TAG=v1.2.3 packaging/publish-release.sh --publish
#
# API is the repo API base (…/api/v1/repos/<owner>/<repo>); TOKEN needs
# `write:repository`. A newly created release is a DRAFT (hidden from the
# public) so jobs can attach assets as they finish; the last release job runs
# `--publish` to make it visible once everything is attached. Several jobs run
# this concurrently, so creation tolerates losing the race (re-looks the
# release up), and an asset that already exists under the same name is
# replaced — re-runs and the final SHA256SUMS refresh are idempotent.
set -euo pipefail

: "${API:?}" "${TOKEN:?}" "${TAG:?}"
auth="Authorization: token ${TOKEN}"

# Semver: any tag with a hyphen pre-release identifier (e.g. v0.2.0-rc5,
# -beta, -alpha) is a pre-release; a bare v0.2.0 is a stable release.
case "${TAG}" in
  *-*) prerelease=true ;;
  *)   prerelease=false ;;
esac

lookup() { # prints the release id, or nothing if there is none (404)
  { curl -fsS -H "$auth" "${API}/releases/tags/${TAG}" \
      | python3 -c 'import sys,json;print(json.load(sys.stdin).get("id",""))'; } 2>/dev/null || true
}

rel_id=$(lookup)
if [ -z "$rel_id" ]; then
  rel_id=$(curl -fsS -X POST -H "$auth" -H 'Content-Type: application/json' \
    -d "{\"tag_name\":\"${TAG}\",\"name\":\"${TAG}\",\"draft\":true,\"prerelease\":${prerelease}}" \
    "${API}/releases" \
    | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])' 2>/dev/null || true)
  # Another job created it between our lookup and POST.
  [ -n "$rel_id" ] || rel_id=$(lookup)
  [ -n "$rel_id" ] || { echo "could not create or find release ${TAG}" >&2; exit 1; }
else
  # Reuse path: correct a pre-existing release's flag to match the tag
  # (e.g. one pre-created in the UI), so the rule holds either way.
  curl -fsS -X PATCH -H "$auth" -H 'Content-Type: application/json' \
    -d "{\"prerelease\":${prerelease}}" \
    "${API}/releases/${rel_id}" >/dev/null
fi
echo "release id: $rel_id"

if [ "${1:-}" = "--publish" ]; then
  curl -fsS -X PATCH -H "$auth" -H 'Content-Type: application/json' \
    -d '{"draft":false}' "${API}/releases/${rel_id}" >/dev/null
  echo "published ${TAG}"
  exit 0
fi

for f in "$@"; do
  name=$(basename "$f")
  old=$(curl -fsS -H "$auth" "${API}/releases/${rel_id}/assets" \
    | NAME="$name" python3 -c 'import os,sys,json;print(" ".join(str(a["id"]) for a in json.load(sys.stdin) if a["name"]==os.environ["NAME"]))')
  for id in $old; do
    echo "replacing existing $name"
    curl -fsS -X DELETE -H "$auth" "${API}/releases/${rel_id}/assets/${id}" >/dev/null
  done
  echo "uploading $name"
  curl -fsS -X POST -H "$auth" \
    -F "attachment=@${f}" \
    "${API}/releases/${rel_id}/assets?name=${name}" >/dev/null
done
