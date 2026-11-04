#!/usr/bin/env bash
# The pin patterns are literal text, not shell expansions.
# shellcheck disable=SC2016
# Moves this repo's Felix pins from one version to another:
#   .github/felix-bump.sh <old> <new>
# The felix-release workflow runs it, then updates .felix-version and the
# lockfiles. Every pin listed here must be found, so a moved or renamed pin
# fails the bump instead of being skipped.
set -euo pipefail
OLD=${1:?usage: felix-bump.sh <old> <new>}
NEW=${2:?usage: felix-bump.sh <old> <new>}
cd "$(dirname "$0")/.."

die() { echo "felix-bump: $*" >&2; exit 1; }

# pin FILE PATTERN: replace PATTERN with {V}=OLD by PATTERN with {V}=NEW.
pin() {
  local file=$1 pattern=$2
  [ -f "$file" ] || die "$file does not exist"
  FROM=${pattern//'{V}'/$OLD} TO=${pattern//'{V}'/$NEW} \
    perl -0777 -pi -e '$n += s/\Q$ENV{FROM}\E/$ENV{TO}/g; END { exit($n ? 0 : 3) }' "$file" \
    || die "$file: expected '${pattern//'{V}'/$OLD}'"
}

# changelog_fragment TEXT: write the bump's changelog bullet as a fixed-name
# fragment, so repeated bumps before a release overwrite one file.
changelog_fragment() {
  mkdir -p changelog.d
  printf -- '- %s\n' "$1" > changelog.d/felix-version.changed.md
}

pin relay/Cargo.toml 'felix-client = "{V}"'
pin relay/Cargo.toml 'felix-wire = "{V}"'
pin dev/docker-compose.yml 'ghcr.io/getfelix/felix-controlplane:{V}'
pin dev/docker-compose.yml 'ghcr.io/getfelix/felix-broker:{V}'
pin deploy/compose/docker-compose.yml 'ghcr.io/getfelix/felix-controlplane:${FELIX_VERSION:-{V}}'
pin deploy/compose/docker-compose.yml 'ghcr.io/getfelix/felix-broker:${FELIX_VERSION:-{V}}'
pin .github/workflows/images.yml 'FELIX_CHART_REF: v{V}'
pin README.md 'a Felix {V} broker'
pin docs/self-hosting.md '--branch v{V} https://github.com/GetFelix/felix'
changelog_fragment "Built on Felix $NEW: the client crates, the compose files, CI and the Felix chart the self-hosting guide installs."
