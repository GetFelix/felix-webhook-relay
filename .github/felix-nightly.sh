#!/usr/bin/env bash
# The nightly run against Felix main: starts the dev stack from the Felix images
# tagged $FELIX_IMAGE_TAG (default: nightly) and runs the integration, crash and
# retention tests that CI runs against it. The long demonstrations (10,000
# webhooks, 10 million records) are left to CI. The felix-nightly workflow calls
# this after pointing Cargo.toml at the same Felix commit.
#
# It rewrites the image tags in dev/docker-compose.yml, so run it from a
# throwaway checkout.
set -euo pipefail
cd "$(dirname "$0")/.."

tag=${FELIX_IMAGE_TAG:-nightly}
TAG=$tag perl -pi -e 's{(ghcr\.io/getfelix/felix-(?:broker|controlplane|felixctl)):[^\s"\x27]+}{$1:$ENV{TAG}}' \
  dev/docker-compose.yml

logs() {
  [ $? -eq 0 ] || docker compose -f dev/docker-compose.yml -f dev/docker-compose.cluster.yml logs --no-color
}
trap logs EXIT

export RELAY_FELIX_CA_FILE="$PWD/dev/state/broker-cert.pem"

cargo test --no-run

dev/up.sh
cargo test --tests -- --include-ignored

dev/up.sh --cluster
RELAY_TEST_CLUSTER=1 cargo test --test crash -- --include-ignored --nocapture

dev/up.sh --retention
RELAY_TEST_RETENTION=1 cargo test --test retention -- --include-ignored --nocapture
