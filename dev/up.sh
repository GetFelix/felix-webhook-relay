#!/usr/bin/env bash
# Start the development stack from scratch and wait for the broker. The
# control plane keeps its state in memory, so the broker's log is reset with
# it: a log that outlived its control plane would no longer match it.
# `--cluster` starts three replicating brokers instead of one
# (docker-compose.cluster.yml); `--retention` keeps records only seconds
# (docker-compose.retention.yml).
set -euo pipefail
cd "$(dirname "$0")"

files=(-f docker-compose.yml)
health=(8080)
case "${1:-}" in
  --cluster)
    files+=(-f docker-compose.cluster.yml)
    health=(8080 8081 8082)
    ;;
  --retention) files+=(-f docker-compose.retention.yml) ;;
esac

docker compose -f docker-compose.yml -f docker-compose.cluster.yml down --volumes --remove-orphans >/dev/null 2>&1 || true
rm -f state/broker-*.pem 2>/dev/null || true
mkdir -p state/idp
if ! docker compose "${files[@]}" up --detach; then
  docker compose "${files[@]}" logs >&2
  exit 1
fi

ready() {
  for port in "${health[@]}"; do
    curl -fsS "http://127.0.0.1:$port/ready" >/dev/null 2>&1 || return 1
  done
  curl -fsS http://127.0.0.1:5556/dex/.well-known/openid-configuration >/dev/null 2>&1
}

for _ in $(seq 1 90); do
  if ready; then
    echo "Felix is ready. For the relay:"
    echo "  export RELAY_FELIX_CA_FILE=\"$PWD/state/broker-cert.pem\""
    echo "  export RELAY_IDP_TOKEN_FILE=\"$PWD/state/relay-idp.token\""
    exit 0
  fi
  sleep 2
done

echo "the brokers did not become ready" >&2
docker compose "${files[@]}" ps --all >&2
docker compose "${files[@]}" logs >&2
exit 1
