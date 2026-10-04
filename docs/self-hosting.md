# Self-hosting Felix Webhook Relay

How to run the relay on your own machines: the compose install, signing admins
in with your own identity provider, exposing intake and admin safely, the
broker settings the relay depends on, backups, upgrades, Kubernetes, and every
setting.

The install is the relay and Felix. There is no Postgres, Redis or other store
beside them: every webhook, cursor, setting and secret lives in Felix.

## What runs

| Service | Image | Holds state? | Job |
|---|---|---|---|
| `relay` | `ghcr.io/getfelix/felix-webhook-relay` | No | Intake, delivery and admin in one process; `RELAY_ROLES` splits them |
| `broker` | `ghcr.io/gabloe/felix-broker` | Yes, `felix-data` | Felix: every source's log, the groups, the caches |
| `controlplane` | `ghcr.io/gabloe/felix-controlplane` | Yes, `controlplane-data` | Felix: the tenant, namespaces, roles and token exchange, in its own Raft log |
| `tokens` | the relay image | No | At each start: seeds Felix and makes the broker a certificate the first time. Then keeps the relay's IdP token fresh. Never published |
| `dex` | `ghcr.io/dexidp/dex` | No | The stand-in sign-in for a first run |

Felix 0.6.0-preview, which the install pins, is published under `ghcr.io/gabloe`; Felix releases after it publish under `ghcr.io/getfelix`.

The relay image is built for `linux/amd64` and `linux/arm64` and signed with
cosign by the release workflow, as is the Helm chart. To check either before
you run it (for the chart, `ghcr.io/getfelix/charts/felix-webhook-relay:0.1.0`):

```bash
cosign verify ghcr.io/getfelix/felix-webhook-relay:0.1.0 \
  --certificate-identity-regexp 'https://github.com/GetFelix/felix-webhook-relay/.github/workflows/.*@refs/tags/v.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

## Install with Docker Compose

You need Docker with Compose 2.20 or later, about 2 cores and 2 GB of memory,
and disk for the webhooks. Each source is a Felix stream, and each stream
reserves a log segment up front (`FELIX_SEGMENT_BYTES`, 16 MiB here), so a
source costs 16 MiB before its first webhook.

1. Get the compose bundle for a release. It is the compose file, its `.env`
   and the stand-in's `dex.yaml`, with the release's image tag written in:

   ```bash
   curl -fsSLO https://github.com/GetFelix/felix-webhook-relay/releases/download/v0.1.0/felix-webhook-relay-compose-0.1.0.tar.gz
   tar xzf felix-webhook-relay-compose-0.1.0.tar.gz
   cd felix-webhook-relay-compose-0.1.0
   ```

2. Edit `.env`. Set `FELIX_BOOTSTRAP_TOKEN`, `FELIX_RAFT_PEER_TOKEN` and
   `RELAY_SECRET_KEY` before the first start, each from
   `openssl rand -base64 32`. Keep `RELAY_SECRET_KEY` safe: it seals every
   source and endpoint secret, and without it they cannot be opened.

3. Start it:

   ```bash
   docker compose up -d --wait
   ```

4. Open <http://127.0.0.1:8090/admin/acme> and sign in as `alice@example.com`
   with the password `password`. Create a source and an endpoint, and send a
   webhook to the URL the page shows.

`deploy/ci/smoke.sh` does step 4 from a shell: it signs in as alice, creates a
source and an endpoint, sends a webhook and waits until the endpoint has it.
CI runs it against the install on every pull request.

`docker compose logs -f relay` shows the relay, and `docker compose ps` shows
what is healthy.

## Your own identity provider

Admins sign in with any OpenID Connect provider (Keycloak, Dex, Entra ID,
Okta, Auth0, Google and others), and Felix decides who may administer which
tenant.

**At the provider**, register a confidential web application:

| Setting | Value |
|---|---|
| Grant | Authorization code, with a client secret |
| Redirect URI | `RELAY_PUBLIC_URL` followed by `/auth/callback`, such as `https://relay.example.com/auth/callback` |
| Scopes | `openid email profile` |

**In `.env`**, point the install at it and stop the stand-in:

```bash
COMPOSE_PROFILES=
RELAY_PUBLIC_URL=https://relay.example.com
RELAY_OIDC_ISSUER=https://login.example.com/realms/relay
RELAY_OIDC_INTERNAL_URL=
RELAY_OIDC_JWKS_URL=
RELAY_OIDC_CLIENT_ID=felix-webhook-relay
RELAY_OIDC_CLIENT_SECRET=...
RELAY_OIDC_SUBJECT_CLAIM=email
RELAY_TENANT_ADMINS=acme=ana@example.com,ben@example.com
```

`RELAY_OIDC_ISSUER` must match the ID token's `iss` exactly. Leave
`RELAY_OIDC_INTERNAL_URL` and `RELAY_OIDC_JWKS_URL` empty when the containers
reach the provider at the issuer's address. Set them when they reach it
somewhere else, as the stand-in does: browsers reach Dex at `127.0.0.1:5556`,
and the relay and the control plane at `dex:5556`.

Then `docker compose up -d`. The `tokens` service adds the provider to the
Felix tenant's trusted issuers, with the client ID as the audience.

## Tenants and the Felix roles they need

A relay tenant is a Felix namespace in the Felix tenant `relay`. For each name
in `RELAY_TENANTS`, the `tokens` service creates:

- the namespace;
- the streams `dead` and `attempts` and the caches `config`, `idem`, `state`
  and `stats`, one shard each;
- the role `role:relay-tenant-<tenant>`, with `stream.publish`,
  `stream.subscribe`, `stream.manage` and `group.manage` on
  `stream:relay/<tenant>/*`, and `cache.read`, `cache.write` and `cache.manage`
  on `cache:relay/<tenant>/*`;
- an assignment of that role to each admin listed for the tenant in
  `RELAY_TENANT_ADMINS`, as a value of `RELAY_OIDC_SUBJECT_CLAIM`.

The relay's own service account holds `role:relay`: `stream.publish`,
`stream.subscribe` and `group.manage` on `stream:relay/*/*`, and `cache.read`
and `cache.write` on `cache:relay/*/*`. Each relay process narrows that to one
tenant's namespace at every token exchange, so the broker refuses it anything
of another tenant's. [design.md](design.md#multi-tenancy-and-auth) explains
the model.

To add a tenant or an admin, add it to `RELAY_TENANTS` or
`RELAY_TENANT_ADMINS` and run `docker compose up -d`. The `tokens` service
runs again and creates what is missing. It never removes anything: to take
someone's access away, delete their assignment through the Felix control
plane, and their next exchange is refused, within five minutes for a
signed-in admin.

## Exposing intake and admin

The relay listens on `127.0.0.1:8090` by default; the image listens on
`0.0.0.0:8090`, and the compose file publishes it on `RELAY_BIND`, which is
`127.0.0.1`. Every admin route needs a signed-in admin or an ID token as a
bearer token, and what an admin may touch is the control plane's answer at
the token exchange, not the relay's. Only intake (`/in/`), `/healthz` and
`/metrics` answer without signing in.

To take webhooks from the internet:

1. Put a reverse proxy with TLS in front, and set `RELAY_PUBLIC_URL` to its
   `https://` address. The session cookie is marked `Secure` only then. With
   [Caddy](https://caddyserver.com), add a `tls.yaml` next to the compose file:

   ```yaml
   services:
     proxy:
       image: caddy:2
       command: caddy reverse-proxy --from relay.example.com --to relay:8090
       ports: ["80:80", "443:443"]
       volumes: [caddy-data:/data]
       restart: unless-stopped
   volumes:
     caddy-data:
   ```

   and start with `-f docker-compose.yml -f tls.yaml`.

2. Keep the admin surface off the internet where you can, even though it is
   signed in. Either have the proxy pass only `/in/` from outside and reach
   `/admin`, `/api` and `/auth` over a VPN, or run two relays: one with
   `RELAY_ROLES=intake,deliver` behind the public proxy, and one with
   `RELAY_ROLES=admin` published on `127.0.0.1` or a private network.

3. Do not publish `/metrics` to the internet: it shows traffic per hop.

A `token` source's token is in its URL path. The relay never logs paths, but
a proxy may, so turn off its access log or strip paths from it.

## Broker settings the relay depends on

The compose file sets these on the broker, and `ci/felix-values.yaml` sets
them in the felix chart:

| Setting | Here | Why |
|---|---|---|
| `FELIX_DURABLE_RETENTION_SECONDS` | `1209600` (14 days) | Retention must be longer than `RELAY_DISABLE_AFTER` plus `RELAY_REPLAY_WINDOW` (10 days by default), or an endpoint that was down, or a replay, loses webhooks. The relay warns at startup and on the admin page when a source was trimmed too young. Retention is broker-wide |
| `FELIX_GROUP_VISIBILITY_TIMEOUT_MS` | `30000` | How long a claim lasts. A restarted worker waits `RELAY_CLAIM_WAIT_MS` before its first poll, so its predecessor's claims lapse first ([felix#962](https://github.com/GetFelix/felix/issues/962)). Keep the two equal |
| `FELIX_DURABLE_SEGMENT_BYTES` | `16777216` | Every source is a stream, and each log reserves a whole segment on disk up front: 256 MiB per source at Felix's default. Turning off `FELIX_DURABLE_PREALLOCATE` does the same |
| `FELIX_DURABLE_FSYNC_MODE`, `FELIX_ACK_ON_COMMIT` | `on_commit`, `true` | Intake answers `202` only after the broker acknowledges, and this makes the acknowledgement wait for the device |
| `FELIX_SUB_QUEUE_BOUND` | `8192` | The writer queue is per connection, and the relay holds many subscriptions on one |

The control plane also needs `FELIX_CONTROLPLANE_OIDC_ALLOWED_ALGORITHMS`
to include `RS256` on Felix 0.6.0-preview, which accepts only ES256 by
default; later releases accept RS256 on their own.

## TLS between the relay and Felix

QUIC is always TLS. On the first start the `tokens` service writes a
self-signed certificate for the name `broker` to the `state` volume, and the
relay trusts exactly that certificate. To use your own, put
`broker-cert.pem` and `broker-key.pem` in the volume before the first start;
the certificate must name `broker`.

The control plane and the `tokens` service are plain HTTP on the compose
network, which nothing outside reaches.

## Backups

| Volume | What | Lose it and |
|---|---|---|
| `controlplane-data` | The tenant, namespaces, roles and trusted issuers | Felix no longer knows the streams in its log |
| `felix-data` | Every webhook, cursor, dead letter, setting and sealed secret | Everything the relay knew is gone |
| `state` | The service tokens and the broker certificate | Nothing: the `tokens` service makes them again |

Back up `controlplane-data` and `felix-data` together, with Felix stopped, and
keep `RELAY_SECRET_KEY` with them: without it the backup's secrets do not open.

```bash
docker compose stop relay broker controlplane
docker run --rm -v "$PWD":/out \
  -v felix-webhook-relay_controlplane-data:/backup/controlplane \
  -v felix-webhook-relay_felix-data:/backup/broker \
  debian:bookworm-slim tar czf /out/relay-backup.tar.gz -C /backup controlplane broker
docker compose up -d
```

To restore, `docker compose down -v`, untar into the empty volumes with the
same command and `tar xzf`, and start everything. A log restored without its
metadata, or the other way round, does not match.

## Upgrading

Each release pins the relay image and the Felix version it was tested with in
`docker-compose.yml`. Back up, replace `docker-compose.yml` with the new
release's, keep `.env`, and:

```bash
docker compose pull
docker compose up -d
```

A restarted delivery process waits `RELAY_CLAIM_WAIT_MS`, 30 seconds by
default, before it delivers again, so every upgrade pauses delivery that long.

On Felix 0.6.0-preview the broker reads its token once, at start
([felix#955](https://github.com/GetFelix/felix/issues/955)), and the token lasts
`FELIX_TOKEN_TTL_SECONDS`, 30 days. Restart at least that often, which also
mints a new one:

```bash
docker compose up -d --force-recreate
```

## Kubernetes

`deploy/helm/felix-webhook-relay` runs the relay next to a release of the
[felix chart](https://github.com/GetFelix/felix/tree/main/deploy/helm/felix):
intake as a Deployment, delivery as a StatefulSet whose ordinal is
`RELAY_WORKER_INDEX`, admin as a Deployment behind an ingress, and the
`tokens` service. The [chart's README](../deploy/helm/felix-webhook-relay/README.md)
lists what it renders. CI runs the sequence below on kind with the values in
`deploy/helm/felix-webhook-relay/ci/`, sends a webhook through an outage, and
upgrades to three delivery workers.

1. Secrets: the control plane's bootstrap and Raft peer tokens, the brokers'
   certificate, which must name what the relay dials (`felix-broker` here),
   and the relay's secret key and OIDC client secret:

   ```bash
   kubectl create secret generic felix-bootstrap --from-literal=token="$(openssl rand -hex 24)"
   kubectl create secret generic felix-raft-peer --from-literal=token="$(openssl rand -hex 24)"
   kubectl create secret tls felix-broker-tls --cert=broker.crt --key=broker.key
   kubectl create secret generic felix-webhook-relay \
     --from-literal=secret-key="$(openssl rand -base64 32)" --from-literal=client-secret=...
   ```

2. The felix chart with its brokers off. `ci/felix-values.yaml` is a starting
   point: a three-member Raft control plane, so the metadata needs no
   database, and the broker settings above.

   ```bash
   git clone --depth 1 --branch v0.6.0-preview https://github.com/GetFelix/felix
   helm install felix felix/deploy/helm/felix -f felix-values.yaml
   ```

3. This chart, which each release publishes as a signed OCI chart and
   attaches to the release. Its `tokens` Deployment seeds Felix and stores
   the broker credential in the Secret `felix-webhook-relay-broker-credential`:

   ```bash
   helm install felix-webhook-relay oci://ghcr.io/getfelix/charts/felix-webhook-relay \
     --version 0.1.0 -f relay-values.yaml
   kubectl wait --for=condition=available deployment/felix-webhook-relay-tokens
   ```

   where `relay-values.yaml` sets at least:

   ```yaml
   felix:
     controlPlaneUrl: http://felix-controlplane:8443
     brokers: [felix-broker:5000]
     serverName: felix-broker
     caSecret: { name: felix-broker-tls, key: tls.crt }
   tokens:
     bootstrapUrl: http://felix-controlplane-bootstrap:9095
     bootstrapSecret: { name: felix-bootstrap }
   secretKey: { existingSecret: felix-webhook-relay }
   tenants: acme
   tenantAdmins: acme=ana@example.com
   oidc:
     issuer: https://login.example.com/realms/relay
     clientId: felix-webhook-relay
     clientSecret: { existingSecret: felix-webhook-relay }
   admin:
     ingress: { enabled: true, host: relay-admin.example.com, tlsSecret: relay-admin-tls }
   intake:
     ingress: { enabled: true, host: hooks.example.com, tlsSecret: hooks-tls }
   ```

4. The brokers:

   ```bash
   helm upgrade felix felix/deploy/helm/felix -f felix-values.yaml --set broker.enabled=true
   ```

The relay pods wait for the token Secret on a first install and restart until
the brokers answer, so they settle a minute or two after the brokers.

With three or more brokers, set `felix.replicas: 3` before the first install,
so every stream and cache is on three brokers and survives losing one.

Every `helm upgrade` of this chart restarts the `tokens` pod, which seeds
again and mints new tokens. It rewrites the relay's IdP token in its Secret
every six hours, and the relay reads it from the mounted file before each
exchange. The broker reads its credential only at start
([felix#955](https://github.com/GetFelix/felix/issues/955)), so restart the
brokers within `FELIX_EXCHANGE_TOKEN_TTL_SECONDS` of the last upgrade.

Changing `deliver.replicas` moves endpoints between workers. Each moved
endpoint waits `RELAY_CLAIM_WAIT_MS` on its new worker and resumes from its
group's cursor.

## Configuration reference

### Compose (`.env`)

| Variable | Default | Meaning |
|---|---|---|
| `FELIX_BOOTSTRAP_TOKEN` | required | The control plane's day-0 token, which the `tokens` service creates the Felix tenant with |
| `FELIX_RAFT_PEER_TOKEN` | required | The control plane's Raft peer token, 32 characters or more |
| `RELAY_SECRET_KEY` | required | 32 bytes, base64 or hex, that seal every secret the relay stores |
| `RELAY_BIND`, `RELAY_PORT` | `127.0.0.1`, `8090` | The host address and port the relay is published on |
| `RELAY_PUBLIC_URL` | `http://127.0.0.1:8090` | Where browsers reach the relay |
| `RELAY_TENANTS` | `acme` | Relay tenants, comma-separated |
| `RELAY_TENANT_ADMINS` | `acme=alice@example.com` | Who administers each, `tenant=admin,admin` separated by spaces |
| `COMPOSE_PROFILES` | `dev-idp` | `dev-idp` runs the stand-in Dex. Empty once you use your own provider |
| `RELAY_OIDC_*` | the stand-in Dex | As for the relay and the `tokens` service below |
| `RELAY_VERSION` | the release | The relay image's tag |
| `FELIX_VERSION` | the release's Felix | The Felix images' tag |
| `FELIX_OIDC_ALGORITHMS` | `ES256,RS256` | ID token algorithms Felix accepts |
| `FELIX_SEGMENT_BYTES` | `16777216` | The broker's log segment size, which each source reserves up front |
| `FELIX_RETENTION_SECONDS` | `1209600` | How long the broker keeps records |
| `FELIX_TOKEN_TTL_SECONDS` | `2592000` | How long a token from the control plane lasts |
| `RELAY_REPLAY_WINDOW`, `RELAY_DISABLE_AFTER` | `7d`, `72h` | As below |
| `FELIX_LOG`, `RELAY_LOG` | `info` | Log filters |

### The relay

| Variable | Default | What |
|---|---|---|
| `RELAY_ROLES` | `intake,deliver,admin` | Which roles this process runs |
| `RELAY_LISTEN` | `127.0.0.1:8090` (`0.0.0.0:8090` in the image) | HTTP address for intake, the admin API, `/healthz` and `/metrics` |
| `RELAY_PUBLIC_URL` | `http://<RELAY_LISTEN>` | Where browsers reach the relay, for the sign-in redirect |
| `RELAY_OIDC_ISSUER` | none; required for `admin` | The IdP admins sign in with |
| `RELAY_OIDC_CLIENT_ID`, `RELAY_OIDC_CLIENT_SECRET` | none; required for `admin` | The relay's client at that IdP |
| `RELAY_OIDC_INTERNAL_URL` | the issuer | Where the relay reaches the IdP, when it cannot at the issuer's address. Replaces the issuer at the start of the discovery and token endpoint URLs |
| `RELAY_SECRET_KEY` | none, required | 32 bytes, base64 or hex, that seal every secret the relay stores in Felix |
| `RELAY_FELIX_BROKERS` | `127.0.0.1:5000` | Comma-separated broker addresses, `host:port`, resolved at each connection |
| `RELAY_FELIX_SERVER_NAME` | `localhost` | Name the broker certificate is checked against |
| `RELAY_FELIX_CA_FILE` | platform roots | PEM certificates to trust for the broker |
| `RELAY_IDP_TOKEN_FILE` | none, required | An ID token for the relay's service principal, read again before each token exchange |
| `RELAY_FELIX_CONTROL_PLANE` | `http://127.0.0.1:8443` | Where tokens are exchanged and streams created |
| `RELAY_FELIX_TENANT` | `relay` | The Felix tenant of the deployment |
| `RELAY_STREAM_REPLICAS` | `1` | Brokers that hold each new source's stream; above 1 its writes wait for a majority |
| `RELAY_REPLAY_WINDOW` | `7d` | How far back replays should reach |
| `RELAY_TENANTS` | `acme` | Comma-separated relay tenants this process serves |
| `RELAY_WORKER_INDEX` | `0` | This delivery process's index; it owns the endpoints whose id hashes to it |
| `RELAY_WORKER_COUNT` | `1` | How many delivery processes share the endpoints |
| `RELAY_ENDPOINT_PREFIXES` | all | Comma-separated; only endpoints whose ids start with one of these |
| `RELAY_CLAIM_WAIT_MS` | `30000` | Wait before the first poll; at least the broker's `FELIX_GROUP_VISIBILITY_TIMEOUT_MS` |
| `RELAY_BACKOFF` | `5s,15s,1m,2m,5m` | Waits between probes of a paused endpoint; the last repeats |
| `RELAY_REFUSED_RETRIES` | `5s,30s` | Waits before each retry of a refused record, then it is dead-lettered |
| `RELAY_DISABLE_AFTER` | `72h` | How long an endpoint may fail without a break before it is disabled |
| `RELAY_WORKER_NAME` | host name and pid | Names this process in endpoint health entries |
| `RUST_LOG` | `info` | Log filter |

### The `tokens` service

`relay-seed serve` in the relay image runs it; `relay-seed` alone seeds once
and exits, which is how `dev/` uses it. The script is `deploy/seed.sh`.

| Variable | Default | Meaning |
|---|---|---|
| `FELIX_CONTROL_PLANE` | `http://controlplane:8443` | The control plane's API |
| `FELIX_BOOTSTRAP_URL` | `http://controlplane:9095` | Its bootstrap listener |
| `FELIX_BOOTSTRAP_TOKEN` | `dev-bootstrap` | Its token |
| `TOKENS_ISSUER` | `http://tokens:9400` | The address the control plane fetches this service's keys at, which is also its issuer |
| `STATE_DIR` | `/state` | Where it writes `node.token`, `relay-idp.token` and the broker certificate |
| `IDP_DIR`, `IDP_KEY` | under `STATE_DIR` | Where the served JWKS and the signing key go. Keep the key out of anything the relay mounts |
| `BROKER_CERT_SAN` | unset | Makes the broker a certificate with these subject alternative names, once |
| `RELAY_FELIX_TENANT`, `RELAY_TENANTS`, `RELAY_TENANT_ADMINS`, `RELAY_STREAM_REPLICAS` | `relay`, `acme`, none, `1` | What to create |
| `RELAY_OIDC_ISSUER`, `RELAY_OIDC_CLIENT_ID` | none, `relay-admin` | The admins' provider and the audience their ID tokens carry |
| `RELAY_OIDC_JWKS_URL` | from the issuer's discovery document | Where Felix fetches that provider's keys |
| `RELAY_OIDC_SUBJECT_CLAIM` | `email` | The claim that names an admin |
| `RELAY_TOKEN_LIFETIME_SECONDS` | `86400` | How long the relay's IdP token lasts; it is renewed every quarter of that |
| `NODE_TOKEN_SECRET`, `RELAY_TOKEN_SECRET` | unset | In Kubernetes, Secrets to store the broker credential and the relay's token in |

## Felix gaps this works around

| What | Why | Felix issue |
|---|---|---|
| The `tokens` service | Felix issues tokens only in exchange for an IdP token, so the relay's service accounts need a provider of their own | [#954](https://github.com/GetFelix/felix/issues/954) |
| `FELIX_TOKEN_TTL_SECONDS` of 30 days and a restart within it | A standalone 0.6.0-preview broker reads its token once; fixed on Felix main, not yet released | [#955](https://github.com/GetFelix/felix/issues/955) |
| `FELIX_OIDC_ALGORITHMS=ES256,RS256` | 0.6.0-preview accepts only ES256; fixed on Felix main, not yet released | [#984](https://github.com/GetFelix/felix/issues/984) |
| `RELAY_CLAIM_WAIT_MS` equal to the visibility timeout | A restarted group member cannot take back its predecessor's claims | [#962](https://github.com/GetFelix/felix/issues/962) |
| Small segments | Segment size and preallocation are broker-wide, so each source's stream reserves a whole segment | Not filed |
| Retention set on the broker | Retention is broker-wide only | Not filed |
