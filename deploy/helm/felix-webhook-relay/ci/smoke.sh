#!/usr/bin/env bash
# deploy/ci/smoke.sh against the chart on kind, through port-forwards that
# are opened afresh, since an upgrade replaces the pods they point at.
set -euo pipefail
pkill -f 'kubectl port-forward' || true
kubectl port-forward svc/felix-webhook-relay-intake 8091:80 >/dev/null &
kubectl port-forward svc/felix-webhook-relay-admin 8090:80 >/dev/null &
kubectl port-forward svc/dex 5556:5556 >/dev/null &
for _ in $(seq 1 30); do
  curl -fs http://127.0.0.1:8091/healthz >/dev/null && curl -fs http://127.0.0.1:8090/healthz >/dev/null &&
    curl -fs http://127.0.0.1:5556/dex/.well-known/openid-configuration >/dev/null && break
  sleep 2
done
RELAY_URL=http://127.0.0.1:8091 ADMIN_URL=http://127.0.0.1:8090 RECEIVER_URL=http://receiver:9000/hook \
  "$(dirname "$0")/../../../ci/smoke.sh"
