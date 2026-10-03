#!/usr/bin/env bash
# Start the development stack from scratch and wait for the broker. The
# control plane keeps its state in memory, so the broker's log is reset with
# it: a log that outlived its control plane would no longer match it.
set -euo pipefail
cd "$(dirname "$0")"

docker compose down --volumes --remove-orphans >/dev/null 2>&1 || true
mkdir -p state/idp
if ! docker compose up --detach; then
  docker compose logs >&2
  exit 1
fi

for _ in $(seq 1 90); do
  if curl -fsS http://127.0.0.1:8080/ready >/dev/null 2>&1 &&
    curl -fsS http://127.0.0.1:5556/dex/.well-known/openid-configuration >/dev/null 2>&1; then
    echo "Felix is ready. For the relay:"
    echo "  export RELAY_FELIX_CA_FILE=\"$PWD/state/broker-cert.pem\""
    echo "  export RELAY_IDP_TOKEN_FILE=\"$PWD/state/relay-idp.token\""
    exit 0
  fi
  sleep 2
done

echo "the broker did not become ready" >&2
docker compose ps --all >&2
docker compose logs >&2
exit 1
