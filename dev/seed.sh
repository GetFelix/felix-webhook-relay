#!/bin/sh
# Seeds the development stack the way a deployment would: bootstrap a Felix
# tenant that trusts two IdPs, Dex for people and a stand-in for services,
# create each relay tenant's namespace, streams and caches and its admin role,
# and write the broker's credential and the relay's tokens to the state
# directory. Safe to run again: existing objects are kept.
set -eu

CONTROL_PLANE=${CONTROL_PLANE:-http://controlplane:8443}
BOOTSTRAP=${BOOTSTRAP:-http://controlplane:9095}
BOOTSTRAP_TOKEN=${BOOTSTRAP_TOKEN:-dev-bootstrap}
ISSUER=${IDP_ISSUER:-http://idp:9400}
STATE=${STATE_DIR:-/state}
TENANT=${RELAY_FELIX_TENANT:-relay}
# Relay tenants and who administers each, as Dex users' emails.
TENANTS=${RELAY_TENANTS:-acme globex}
ADMINS=${RELAY_TENANT_ADMINS:-acme=alice@example.com globex=bob@example.com}
AUDIENCE=felix-webhook-relay
PEOPLE=${DEX_ISSUER:-http://127.0.0.1:5556/dex}
PEOPLE_JWKS=${DEX_JWKS:-http://dex:5556/dex/keys}
PEOPLE_AUDIENCE=${DEX_CLIENT_ID:-relay-admin}

b64url() { openssl base64 -A | tr '+/' '-_' | tr -d '='; }

# A new signing key per run. The private half stays out of the served directory.
key="$STATE/idp-key.pem"
openssl genrsa -out "$key" 2048 2>/dev/null
# The modulus of a 2048-bit key sits at a fixed place in its DER public key.
modulus=$(openssl rsa -in "$key" -pubout -outform DER 2>/dev/null | tail -c +34 | head -c 256 | b64url)
jq -n --arg n "$modulus" \
  '{keys: [{kty: "RSA", kid: "dev", alg: "RS256", use: "sig", n: $n, e: "AQAB"}]}' \
  >"$STATE/idp/jwks.json"

# id_token SUBJECT [LIFETIME_SECONDS]
id_token() {
  now=$(date +%s)
  lifetime=${2:-3600}
  header=$(printf '{"alg":"RS256","typ":"JWT","kid":"dev"}' | b64url)
  claims=$(jq -cn --arg iss "$ISSUER" --arg sub "$1" --arg aud "$AUDIENCE" --argjson now "$now" \
    --argjson lifetime "$lifetime" \
    '{iss: $iss, sub: $sub, aud: $aud, iat: $now, exp: ($now + $lifetime)}' | b64url)
  signature=$(printf '%s.%s' "$header" "$claims" | openssl dgst -sha256 -sign "$key" -binary | b64url)
  echo "$header.$claims.$signature"
}

# Felix keys RBAC on sha256(issuer|subject), not on the subject itself.
# principal SUBJECT [ISSUER]
principal() { printf '%s|%s' "${2:-$ISSUER}" "$1" | sha256sum | cut -d' ' -f1; }

# call METHOD URL BODY [curl args]: prints the answer; a 409 means it exists.
call() {
  method=$1 url=$2 body=$3
  shift 3
  code=$(curl -sS -o /tmp/answer -w '%{http_code}' -X "$method" \
    -H 'content-type: application/json' "$@" --data "$body" "$url")
  case $code in
    2*) cat /tmp/answer ;;
    409) echo "  $method $url: already exists" >&2 ;;
    *) echo "$method $url -> $code: $(cat /tmp/answer)" >&2; exit 1 ;;
  esac
}

exchange() {
  call POST "$CONTROL_PLANE/v1/tenants/$TENANT/token/exchange" "$2" \
    -H "authorization: Bearer $(id_token "$1")" | jq -er .felix_token
}

# The relay's service grant spans every relay tenant; each process narrows its
# token to one tenant's namespace when it exchanges it.
streams="stream:$TENANT/*/*"
caches="cache:$TENANT/*/*"

echo "bootstrap tenant $TENANT"
call POST "$BOOTSTRAP/internal/bootstrap/tenants/$TENANT/initialize" "$(jq -n \
  --arg issuer "$ISSUER" --arg audience "$AUDIENCE" \
  --arg people "$PEOPLE" --arg people_jwks "$PEOPLE_JWKS" --arg people_audience "$PEOPLE_AUDIENCE" \
  --arg streams "$streams" --arg caches "$caches" \
  --arg admin "$(principal relay-admin)" \
  --arg broker "$(principal relay-broker)" \
  --arg relay "$(principal relay-service)" \
  '{
    display_name: "Felix Webhook Relay (development)",
    idp_issuers: [
      {
        issuer: $issuer,
        audiences: [$audience],
        jwks_url: ($issuer + "/jwks.json"),
        claim_mappings: {subject_claim: "sub"}
      },
      {
        issuer: $people,
        audiences: [$people_audience],
        jwks_url: $people_jwks,
        claim_mappings: {subject_claim: "email"}
      }
    ],
    initial_admin_principals: [$admin],
    policies: [
      {subject: "role:admin", object: $streams, action: "stream.manage"},
      {subject: "role:admin", object: $caches, action: "cache.manage"},
      {subject: "role:broker", object: "cluster:*", action: "node.view"},
      {subject: "role:relay", object: $streams, action: "stream.publish"},
      {subject: "role:relay", object: $streams, action: "stream.subscribe"},
      {subject: "role:relay", object: $streams, action: "group.manage"},
      {subject: "role:relay", object: $caches, action: "cache.read"},
      {subject: "role:relay", object: $caches, action: "cache.write"}
    ],
    groupings: [
      {user: $admin, role: "role:admin"},
      {user: $broker, role: "role:broker"},
      {user: $relay, role: "role:relay"}
    ]
  }')" -H "x-felix-bootstrap-token: $BOOTSTRAP_TOKEN" >/dev/null

admin=$(exchange relay-admin '{"audience": "felix-controlplane"}')
auth="authorization: Bearer $admin"
base="$CONTROL_PLANE/v1/tenants/$TENANT/namespaces"

stream_body() {
  jq -n --arg stream "$1" '{
    stream: $stream,
    kind: "Stream",
    shards: 1,
    replication_factor: 1,
    retention: {max_age_seconds: null, max_size_bytes: null},
    consistency: "Leader",
    delivery: "AtLeastOnce",
    durable: true
  }'
}

rbac="$CONTROL_PLANE/v1/tenants/$TENANT/rbac"
for ns in $TENANTS; do
  echo "namespace $ns"
  call POST "$base" "{\"namespace\": \"$ns\", \"display_name\": \"$ns\"}" -H "$auth" >/dev/null
  for stream in dead attempts; do
    call POST "$base/$ns/streams" "$(stream_body "$stream")" -H "$auth" >/dev/null
  done
  # One shard each: a retained prefix watch reads a single shard.
  for cache in config idem state stats; do
    call POST "$base/$ns/caches" \
      "{\"cache\": \"$cache\", \"display_name\": \"$cache\", \"shards\": 1}" -H "$auth" >/dev/null
  done
  # Who may administer this tenant: everything in its namespace, including
  # creating sources' streams.
  role="role:relay-tenant-$ns"
  for action in stream.publish stream.subscribe stream.manage group.manage; do
    call POST "$rbac/policies" "{\"subject\": \"$role\", \"object\": \"stream:$TENANT/$ns/*\", \"action\": \"$action\"}" -H "$auth" >/dev/null
  done
  for action in cache.read cache.write cache.manage; do
    call POST "$rbac/policies" "{\"subject\": \"$role\", \"object\": \"cache:$TENANT/$ns/*\", \"action\": \"$action\"}" -H "$auth" >/dev/null
  done
done
call POST "$base/acme/streams" "$(stream_body src.demo)" -H "$auth" >/dev/null

for pair in $ADMINS; do
  ns=${pair%%=*}
  email=${pair#*=}
  echo "$email administers $ns"
  call POST "$rbac/groupings" "$(jq -n --arg user "$(principal "$email" "$PEOPLE")" \
    --arg role "role:relay-tenant-$ns" '{user: $user, role: $role}')" -H "$auth" >/dev/null
done

# The broker runs as uid 65532 and writes its certificate here too.
chmod 777 "$STATE"
# Creating a source's stream goes through the control plane with this token.
echo "$admin" >"$STATE/admin.token"
exchange relay-broker '{"audience": "felix-controlplane"}' >"$STATE/node.token"
exchange relay-service \
  '{"requested": ["stream.publish", "stream.subscribe", "group.manage", "cache.read", "cache.write"]}' \
  >"$STATE/relay.token"
# The relay's own IdP token, which it exchanges per tenant. Felix has no
# client-credentials grant (felix#954), so a deployment's IdP or a sidecar
# writes this file; here it is minted for 30 days.
id_token relay-service 2592000 >"$STATE/relay-idp.token"
chmod 644 "$STATE/node.token" "$STATE/relay.token" "$STATE/admin.token" "$STATE/relay-idp.token"
echo "wrote node.token, relay.token, admin.token and relay-idp.token to $STATE"
