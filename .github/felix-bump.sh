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

# changelog_entry HEADING SUBSECTION PREFIX TEXT: in the unreleased section,
# replace the bullet that starts with PREFIX by "- TEXT", or add it under
# SUBSECTION (or at the top when SUBSECTION is empty). Released sections are
# never touched.
changelog_entry() {
  python3 - CHANGELOG.md "$@" <<'PY'
import re, sys
path, heading, sub, prefix, text = sys.argv[1:]
lines = open(path).read().split("\n")
bullet = ["- " + text]
start = next((i for i, l in enumerate(lines) if re.match(r"## \[?Unreleased\]?\s*$", l)), None)
if start is None:
    first = next((i for i, l in enumerate(lines) if l.startswith("## ")), len(lines))
    lines[first:first] = [heading, ""]
    start = first
end = next((i for i in range(start + 1, len(lines)) if lines[i].startswith("## ")), len(lines))
hit = next((i for i in range(start + 1, end) if lines[i].startswith("- " + prefix)), None)
if hit is not None:
    stop = hit + 1
    while stop < end and lines[stop].startswith("  "):
        stop += 1
    lines[hit:stop] = bullet
else:
    anchor = start
    if sub:
        anchor = next((i for i in range(start + 1, end) if lines[i] == sub), None)
        if anchor is None:
            lines[start + 1:start + 1] = ["", sub]
            anchor, end = start + 2, end + 2
    # Put the bullet first under the anchor heading, one blank line below it.
    j = anchor + 1
    while j < end and lines[j].strip() == "":
        j += 1
    tail = [] if j < end and lines[j].startswith("- ") else [""]
    lines[anchor + 1:j] = [""] + bullet + tail
out = "\n".join(lines)
open(path, "w").write(out)
PY
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
changelog_entry '## Unreleased' '' 'Built on Felix ' \
  "Built on Felix $NEW: the client crates, the compose files, CI and the Felix chart the self-hosting guide installs."
