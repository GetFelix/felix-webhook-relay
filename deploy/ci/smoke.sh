#!/usr/bin/env bash
# Checks a running install the way an operator would: sign in to the
# stand-in Dex as alice, create a source and an endpoint through the admin
# API, send a webhook, and wait until the endpoint has it. With
# RECEIVER_STOP and RECEIVER_START, also takes the endpoint down, sends
# another, and checks it arrives once the endpoint is back.
#
#   RELAY_URL     where intake answers (default http://127.0.0.1:8090)
#   ADMIN_URL     where the admin API answers (default RELAY_URL)
#   DEX_URL       the stand-in Dex (default http://127.0.0.1:5556/dex)
#   RECEIVER_URL  the endpoint, as the relay reaches it; it must answer 2xx
#   TENANT        default acme
set -euo pipefail

RELAY_URL=${RELAY_URL:-http://127.0.0.1:8090}
ADMIN_URL=${ADMIN_URL:-$RELAY_URL}
DEX_URL=${DEX_URL:-http://127.0.0.1:5556/dex}
TENANT=${TENANT:-acme}
: "${RECEIVER_URL:?set RECEIVER_URL to the endpoint, as the relay reaches it}"
name="smoke$(date +%s)"

token=$(curl -fsS -u relay-admin:dev-admin-secret "$DEX_URL/token" \
  -d grant_type=password -d username=alice@example.com -d password=password \
  -d scope='openid email' | jq -er .id_token)
api() {
  curl -fsS -H "authorization: Bearer $token" -H 'content-type: application/json' "$@"
}

source_token=$(api -X PUT -d '{"scheme": {"type": "token"}}' \
  "$ADMIN_URL/api/$TENANT/sources/$name" | jq -er .secret)
api -X PUT -d "$(jq -n --arg source "$name" --arg url "$RECEIVER_URL" '{source: $source, url: $url}')" \
  "$ADMIN_URL/api/$TENANT/endpoints/$name" >/dev/null
echo "created source and endpoint $name"

send() {
  curl -fsS -H 'content-type: application/json' -d "{\"n\": $1}" \
    "$RELAY_URL/in/$TENANT/$name/$source_token" | jq -er .offset
}

# delivered OFFSET SECONDS: waits for a 2xx attempt at that offset.
delivered() {
  for _ in $(seq 1 "$2"); do
    if api "$ADMIN_URL/api/$TENANT/events/$name/$1" |
      jq -e '[.attempts[] | select(.status >= 200 and .status < 300)] | length > 0' >/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "webhook at offset $1 was not delivered" >&2
  api "$ADMIN_URL/api/$TENANT/events/$name/$1" >&2 || true
  return 1
}

# A new endpoint's worker waits out the claim wait before its first poll.
offset=$(send 1)
delivered "$offset" 120
echo "delivered offset $offset"

if [ -n "${RECEIVER_STOP:-}" ]; then
  eval "$RECEIVER_STOP"
  offset=$(send 2)
  for _ in $(seq 1 60); do
    api "$ADMIN_URL/api/$TENANT/events/$name/$offset" | jq -e '.attempts | length > 0' >/dev/null && break
    sleep 1
  done
  echo "the endpoint is down and refused offset $offset"
  eval "$RECEIVER_START"
  delivered "$offset" 180
  echo "delivered offset $offset after the endpoint came back"
fi
