#!/usr/bin/env bash
# Checks that every version in the tree is VERSION, then prints VERSION's
# section of CHANGELOG.md, which becomes the release notes. Fails on any
# mismatch or a missing section, before anything is published.
#
#   scripts/release-notes.sh 0.1.0
set -euo pipefail
cd "$(dirname "$0")/.."
version=${1:?usage: release-notes.sh VERSION}
version=${version#v}

failed=0
check() {
  if ! grep -qE "$2" "$1"; then
    echo "$1 does not set $3 to $version" >&2
    failed=1
  fi
}
escaped=${version//./\\.}
check core/Cargo.toml "^version = \"$escaped\"$" version
check relay/Cargo.toml "^version = \"$escaped\"$" version
check deploy/helm/felix-webhook-relay/Chart.yaml "^version: $escaped$" version
check deploy/helm/felix-webhook-relay/Chart.yaml "^appVersion: \"$escaped\"$" appVersion
# Every place the compose file names the relay image.
if grep -o 'felix-webhook-relay:\${RELAY_VERSION:-[^}]*}' deploy/compose/docker-compose.yml |
  grep -v ":-$escaped}" >&2; then
  echo "deploy/compose/docker-compose.yml does not default RELAY_VERSION to $version" >&2
  failed=1
fi
[ "$failed" = 0 ] || exit 1

notes=$(awk -v heading="## [$version]" '
  index($0, heading) == 1 { found = 1; next }
  found && /^## / { exit }
  found { print }
' CHANGELOG.md)
if [ -z "${notes//[[:space:]]/}" ]; then
  echo "CHANGELOG.md has no section for $version" >&2
  exit 1
fi
printf '%s\n' "$notes"
