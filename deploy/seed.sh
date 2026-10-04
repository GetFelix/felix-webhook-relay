#!/bin/sh
# Sets up Felix for the relay: bootstrap a Felix tenant that trusts two IdPs,
# one for admins and one for the relay's own services, create each relay
# tenant's namespace, streams, caches and admin role, and write the broker's
# credential and the relay's IdP token. Safe to run again: existing objects
# are kept, and tenants or admins added to the settings are created.
#
# Felix issues tokens only in exchange for an IdP token (felix#954), so the
# services sign in with a key this script makes. `seed.sh` seeds once, for
# the development stack. `seed.sh serve` is the install's `tokens` service:
# it serves the key's JWKS, seeds, and then keeps the relay's IdP token and
# the broker's token fresh. docs/self-hosting.md describes every setting.
set -eu

CONTROL_PLANE=${FELIX_CONTROL_PLANE:-http://controlplane:8443}
BOOTSTRAP=${FELIX_BOOTSTRAP_URL:-http://controlplane:9095}
BOOTSTRAP_TOKEN=${FELIX_BOOTSTRAP_TOKEN:-dev-bootstrap}
ISSUER=${TOKENS_ISSUER:-http://tokens:9400}
STATE=${STATE_DIR:-/state}
IDP_DIR=${IDP_DIR:-$STATE/idp}
IDP_KEY=${IDP_KEY:-$STATE/idp-key.pem}
TENANT=${RELAY_FELIX_TENANT:-relay}
TENANTS=$(echo "${RELAY_TENANTS:-acme}" | tr ',' ' ')
# tenant=admin,admin separated by spaces; an admin is a value of the admins'
# subject claim.
ADMINS=${RELAY_TENANT_ADMINS:-}
REPLICAS=${RELAY_STREAM_REPLICAS:-1}
PEOPLE=${RELAY_OIDC_ISSUER:-}
PEOPLE_JWKS=${RELAY_OIDC_JWKS_URL:-}
PEOPLE_AUDIENCE=${RELAY_OIDC_CLIENT_ID:-relay-admin}
PEOPLE_CLAIM=${RELAY_OIDC_SUBJECT_CLAIM:-email}
TOKEN_LIFETIME=${RELAY_TOKEN_LIFETIME_SECONDS:-86400}
AUDIENCE=felix-webhook-relay
if [ "$REPLICAS" -gt 1 ]; then CONSISTENCY=Quorum; else CONSISTENCY=Leader; fi

b64url() { openssl base64 -A | tr '+/' '-_' | tr -d '='; }

# Keeps an existing key. A new one gets a new key id, so the control plane
# fetches the JWKS again rather than checking against the old key.
make_key() {
  mkdir -p "$IDP_DIR"
  [ -s "$IDP_KEY" ] || openssl genrsa -out "$IDP_KEY" 2048 2>/dev/null
  chmod 600 "$IDP_KEY"
  # The modulus of a 2048-bit key sits at a fixed place in its DER public key.
  modulus=$(openssl rsa -in "$IDP_KEY" -pubout -outform DER 2>/dev/null | tail -c +34 | head -c 256 | b64url)
  KID=$(printf '%s' "$modulus" | openssl dgst -sha256 | sed 's/.*= //' | cut -c1-16)
  jq -n --arg n "$modulus" --arg kid "$KID" \
    '{keys: [{kty: "RSA", kid: $kid, alg: "RS256", use: "sig", n: $n, e: "AQAB"}]}' \
    >"$IDP_DIR/jwks.json"
  chmod 644 "$IDP_DIR/jwks.json"
}

# id_token SUBJECT [LIFETIME_SECONDS]
id_token() {
  now=$(date +%s)
  header=$(jq -cn --arg kid "$KID" '{alg: "RS256", typ: "JWT", kid: $kid}' | b64url)
  claims=$(jq -cn --arg iss "$ISSUER" --arg sub "$1" --arg aud "$AUDIENCE" --argjson now "$now" \
    --argjson lifetime "${2:-3600}" \
    '{iss: $iss, sub: $sub, aud: $aud, iat: $now, exp: ($now + $lifetime)}' | b64url)
  signature=$(printf '%s.%s' "$header" "$claims" | openssl dgst -sha256 -sign "$IDP_KEY" -binary | b64url)
  echo "$header.$claims.$signature"
}

# Felix keys RBAC on sha256(issuer|subject), not on the subject itself.
# principal SUBJECT [ISSUER]
principal() { printf '%s|%s' "${2:-$ISSUER}" "$1" | sha256sum | cut -d' ' -f1; }

# call METHOD URL BODY [curl args]: prints the answer; a 409 means it exists.
call() {
  method=$1 url=$2 body=$3
  shift 3
  answer=$(mktemp)
  code=$(curl -sS -o "$answer" -w '%{http_code}' -X "$method" \
    -H 'content-type: application/json' "$@" --data "$body" "$url")
  case $code in
    2*) cat "$answer" ;;
    409) echo "  $method $url: already exists" >&2 ;;
    *) echo "$method $url -> $code: $(cat "$answer")" >&2; rm -f "$answer"; return 1 ;;
  esac
  rm -f "$answer"
}

exchange() {
  reply=$(call POST "$CONTROL_PLANE/v1/tenants/$TENANT/token/exchange" "$2" \
    -H "authorization: Bearer $(id_token "$1")")
  echo "$reply" | jq -er .felix_token
}

# write NAME CONTENT: replaced in one step, since the relay may be reading it.
write() {
  printf '%s\n' "$2" >"$STATE/.$1"
  chmod 644 "$STATE/.$1"
  mv "$STATE/.$1" "$STATE/$1"
}

# In Kubernetes a token also goes to a Secret, which the pods that use it
# mount. store_secret NAME TOKEN
store_secret() {
  account=/var/run/secrets/kubernetes.io/serviceaccount
  secrets="https://kubernetes.default.svc/api/v1/namespaces/$(cat "$account/namespace")/secrets"
  body=$(jq -n --arg name "$1" --arg token "$2" \
    '{apiVersion: "v1", kind: "Secret", metadata: {name: $name}, stringData: {token: $token}}')
  kube() {
    curl -sS -o /dev/null -w '%{http_code}' -X "$1" --cacert "$account/ca.crt" \
      -H "authorization: Bearer $(cat "$account/token")" -H 'content-type: application/json' \
      --data "$body" "$2"
  }
  code=$(kube POST "$secrets")
  if [ "$code" = 409 ]; then code=$(kube PUT "$secrets/$1"); fi
  case $code in
    2*) echo "stored a token in Secret $1" ;;
    *) echo "store Secret $1 -> $code" >&2; return 1 ;;
  esac
}

stream_body() {
  jq -n --arg stream "$1" --argjson replicas "$REPLICAS" --arg consistency "$CONSISTENCY" '{
    stream: $stream,
    kind: "Stream",
    shards: 1,
    replication_factor: $replicas,
    retention: {max_age_seconds: null, max_size_bytes: null},
    consistency: $consistency,
    delivery: "AtLeastOnce",
    durable: true
  }'
}

seed() {
  # The relay's service grant spans every relay tenant; each process narrows
  # its token to one tenant's namespace when it exchanges it.
  streams="stream:$TENANT/*/*"
  caches="cache:$TENANT/*/*"
  echo "bootstrap tenant $TENANT"
  call POST "$BOOTSTRAP/internal/bootstrap/tenants/$TENANT/initialize" "$(jq -n \
    --arg issuer "$ISSUER" --arg audience "$AUDIENCE" \
    --arg streams "$streams" --arg caches "$caches" \
    --arg admin "$(principal relay-admin)" \
    --arg broker "$(principal relay-broker)" \
    --arg relay "$(principal relay-service)" \
    '{
      display_name: "Felix Webhook Relay",
      idp_issuers: [{
        issuer: $issuer,
        audiences: [$audience],
        jwks_url: ($issuer + "/jwks.json"),
        claim_mappings: {subject_claim: "sub"}
      }],
      initial_admin_principals: [$admin],
      policies: [
        {subject: "role:admin", object: $streams, action: "stream.manage"},
        {subject: "role:admin", object: $caches, action: "cache.manage"},
        {subject: "role:broker", object: "cluster:*", action: "node.view"},
        {subject: "role:broker", object: "cluster:*", action: "node.manage"},
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
  base="$CONTROL_PLANE/v1/tenants/$TENANT"

  if [ -n "$PEOPLE" ]; then
    echo "trust $PEOPLE for admins"
    call POST "$base/idp-issuers" "$(jq -n --arg issuer "$PEOPLE" --arg jwks "$PEOPLE_JWKS" \
      --arg audience "$PEOPLE_AUDIENCE" --arg claim "$PEOPLE_CLAIM" '{
        issuer: $issuer,
        audiences: [$audience],
        jwks_url: (if $jwks == "" then null else $jwks end),
        claim_mappings: {subject_claim: $claim}
      }')" -H "$auth" >/dev/null
  fi

  for ns in $TENANTS; do
    echo "tenant $ns"
    call POST "$base/namespaces" "{\"namespace\": \"$ns\", \"display_name\": \"$ns\"}" -H "$auth" >/dev/null
    for stream in dead attempts; do
      call POST "$base/namespaces/$ns/streams" "$(stream_body "$stream")" -H "$auth" >/dev/null
    done
    # One shard each: a retained prefix watch reads a single shard.
    for cache in config idem state stats; do
      call POST "$base/namespaces/$ns/caches" "$(jq -n --arg cache "$cache" --argjson replicas "$REPLICAS" \
        --arg consistency "$CONSISTENCY" \
        '{cache: $cache, display_name: $cache, shards: 1, replication_factor: $replicas, consistency: $consistency}')" \
        -H "$auth" >/dev/null
    done
    # Who may administer this tenant: everything in its namespace, including
    # creating sources' streams.
    role="role:relay-tenant-$ns"
    for action in stream.publish stream.subscribe stream.manage group.manage; do
      call POST "$base/rbac/policies" "{\"subject\": \"$role\", \"object\": \"stream:$TENANT/$ns/*\", \"action\": \"$action\"}" -H "$auth" >/dev/null
    done
    for action in cache.read cache.write cache.manage; do
      call POST "$base/rbac/policies" "{\"subject\": \"$role\", \"object\": \"cache:$TENANT/$ns/*\", \"action\": \"$action\"}" -H "$auth" >/dev/null
    done
  done

  for pair in $ADMINS; do
    ns=${pair%%=*}
    for who in $(echo "${pair#*=}" | tr ',' ' '); do
      echo "$who administers $ns"
      call POST "$base/rbac/groupings" "$(jq -n --arg user "$(principal "$who" "$PEOPLE")" \
        --arg role "role:relay-tenant-$ns" '{user: $user, role: $role}')" -H "$auth" >/dev/null
    done
  done

  # A certificate for the broker, made once, so a broker that restarts keeps
  # the identity the relay trusts. Skipped without BROKER_CERT_SAN, as in
  # Kubernetes, where the operator brings one.
  if [ -n "${BROKER_CERT_SAN:-}" ] && [ ! -s "$STATE/broker-cert.pem" ]; then
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 3650 \
      -subj /CN=felix-broker -addext "subjectAltName=$BROKER_CERT_SAN" \
      -addext basicConstraints=critical,CA:FALSE -addext extendedKeyUsage=serverAuth \
      -keyout "$STATE/broker-key.pem" -out "$STATE/broker-cert.pem" 2>/dev/null
    # The broker runs as uid 65532.
    chmod 644 "$STATE/broker-cert.pem" "$STATE/broker-key.pem"
  fi

  node_token
  # The tests act as an operator and as the relay with these.
  if [ "${SEED_TEST_TOKENS:-}" = 1 ]; then
    write admin.token "$admin"
    write relay.token "$(exchange relay-service \
      '{"requested": ["stream.publish", "stream.subscribe", "group.manage", "cache.read", "cache.write"]}')"
  fi
}

# The broker re-reads its token file, so renewing it here keeps the broker's
# control-plane credential current without a restart.
node_token() {
  node=$(exchange relay-broker '{"audience": "felix-controlplane"}') || return 1
  write node.token "$node"
  if [ -n "${NODE_TOKEN_SECRET:-}" ]; then store_secret "$NODE_TOKEN_SECRET" "$node" || return 1; fi
  echo "wrote node.token to $STATE"
}

# The relay reads this file again before each exchange, so a fresh one is
# all it needs.
relay_token() {
  token=$(id_token relay-service "$TOKEN_LIFETIME")
  write relay-idp.token "$token"
  if [ -n "${RELAY_TOKEN_SECRET:-}" ]; then store_secret "$RELAY_TOKEN_SECRET" "$token"; fi
}

mkdir -p "$STATE"
make_key
case "${1:-}" in
  serve)
    rm -f "$STATE/seeded"
    busybox httpd -p 9400 -h "$IDP_DIR"
    seed
    touch "$STATE/seeded"
    while :; do
      relay_token
      node_token || echo "renewing node.token failed; the broker keeps the current one" >&2
      sleep $((TOKEN_LIFETIME / 4))
    done
    ;;
  *)
    seed
    relay_token
    # So the host user can clear the development stack's state directory.
    chmod 777 "$STATE"
    ;;
esac
