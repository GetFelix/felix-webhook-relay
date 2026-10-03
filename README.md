<p align="center">
  <img src="docs/brand/felix-webhook-relay-mark.png" alt="Felix Webhook Relay: the Felix cat with one webhook coming in and three deliveries going out, one retrying" width="320">
</p>

<h1 align="center">Felix Webhook Relay</h1>

<p align="center">
  A self-hosted webhook relay whose entire backend is <a href="https://github.com/gabloe/felix">Felix</a>.<br>
  No Postgres, Redis or job queue beside it.
</p>

It takes webhooks in over HTTP, verifies their signatures, and stores them
durably before answering. It delivers each one to its endpoints, signed, with
retries and backoff. It sets aside what an endpoint keeps refusing, and it can
replay any endpoint over a time range after an outage.

**Status: M6 done.** Sources and endpoints live in Felix, written through
the admin API with their secrets sealed. Intake verifies Standard Webhooks,
GitHub, Stripe and generic HMAC signatures before anything is stored, and
dedupes retries on the sender's event id. Deliveries are signed with
Standard Webhooks. A delivery process runs every endpoint in config that
hashes to its index, each as its own task, ordered or with a window of
concurrent requests, filtered by event type, and starting at the source's
tail unless it backfills. An
endpoint that is down pauses and probes on a backoff schedule while its
backlog waits in the log, a record it keeps refusing goes to the `dead`
stream, and an endpoint that answers `410` or fails for three days is
disabled until an operator enables it. Any endpoint can replay a time or
offset range from the log beside live delivery, and dead letters can be
redriven or discarded. One process serves many tenants, each confined to
its own Felix namespace by narrowed tokens, and admins sign in to a plain
HTML page or use the JSON API. Killing intake, a worker, or any broker of a
three-broker cluster under load loses nothing that was acknowledged, and the
measured performance is in [docs/performance.md](docs/performance.md).
Packaging for self-hosting is still to come.
The design and the plan are in [docs/design.md](docs/design.md).

## Why it exists

A webhook relay is almost nothing but a queue: accept, store, deliver, retry,
give up, replay. It is usually built as a queue plus Postgres plus a worker
pool plus a scheduler, with an outbox to keep the queue and the database
agreeing.

Felix already has the pieces as one system. One durable stream per source
holds every webhook. One consumer group per endpoint gives it its own cursor,
acknowledgements and redelivery. What an endpoint keeps refusing goes to the
relay's own dead-letter stream. Replay is a subscription from an offset over
the same log. An endpoint that is down for an hour costs
nothing while it waits: its backlog is the stretch of log its cursor has not
reached yet.

The design chapter [How these are normally built](docs/design.md#how-these-are-normally-built)
compares this with the usual stack and with hosted services like Svix,
Hookdeck and Convoy, and says what the trade costs: no queries, no
compare-and-set, and retention set once per broker.

## How it works

One binary, `felix-relay`, runs as intake, delivery, admin, or all three.

| What | Felix primitive | Name |
|---|---|---|
| Accepted webhooks for a source | Durable stream, one shard | `src.<source>` |
| An endpoint's delivery position | Consumer group | `ep.<endpoint>` |
| Records an endpoint refused | Durable stream | `dead` |
| Sources, endpoints, encrypted secrets | Cache | `config` |
| Idempotency keys | Cache with TTL | `idem` |
| Endpoint health and replay jobs | Cache | `state` |

Each relay tenant is a Felix namespace, and every connection the relay opens
for a tenant carries a token narrowed to it, so the broker refuses cross-tenant
access on its own.

## Running locally

You need Rust (the toolchain is pinned in `rust-toolchain.toml`) and, for
anything that talks to Felix, Docker. Unit tests need neither Docker nor a
broker:

```bash
cargo test
```

`dev/up.sh` starts a Felix broker and control plane from the published
0.6.0-preview images, Dex for admins to sign in with, and a stand-in identity
provider for the relay's own token. It seeds two relay tenants, `acme` and
`globex`, with their caches and admin roles, and writes the broker's
certificate and the relay's IdP token to `dev/state/`:

```bash
dev/up.sh
export RELAY_FELIX_CA_FILE="$PWD/dev/state/broker-cert.pem"
export RELAY_IDP_TOKEN_FILE="$PWD/dev/state/relay-idp.token"
export RELAY_OIDC_ISSUER=http://127.0.0.1:5556/dex
export RELAY_OIDC_CLIENT_ID=relay-admin RELAY_OIDC_CLIENT_SECRET=dev-admin-secret
export RELAY_SECRET_KEY="$(openssl rand -base64 32)"
RELAY_TENANTS=acme,globex cargo run -p felix-relay
```

Open <http://127.0.0.1:8090/admin/acme> and sign in as `alice@example.com`
with the password `password`. Alice administers `acme`, `bob@example.com`
administers `globex`, and `carol@example.com` administers nothing, so the
control plane refuses her. The page creates sources and endpoints, shows
health, lag and counts, and replays, retries, redrives and rotates.

The same actions are a JSON API under `/api/<tenant>/`, which takes an ID
token as a bearer token:

```bash
token=$(curl -s -u relay-admin:dev-admin-secret http://127.0.0.1:5556/dex/token \
  -d grant_type=password -d username=alice@example.com -d password=password \
  -d scope='openid email' | jq -r .id_token)
api() { curl -s -H "authorization: Bearer $token" -H 'content-type: application/json' "$@"; }
api -X PUT -d '{"scheme": {"type": "token"}}' http://127.0.0.1:8090/api/acme/sources/demo
api -X PUT -d '{"source": "demo", "url": "http://127.0.0.1:9000/hook"}' \
  http://127.0.0.1:8090/api/acme/endpoints/demo
```

Creating a source creates its stream through the control plane, with the
admin's own token. A source with a `token` scheme gets a long random token in
its URL, and the answer shows it once. The endpoint's answer holds its
`whsec_` signing secret. An endpoint also
takes `"mode": "unordered"` with a `"window"` (default 16) of requests in
flight, `"event_types"` to receive only some, and `"backfill": true` to start
from the beginning of the source's log instead of its tail. Send a webhook to
`/in/acme/demo/<token>`, and it arrives at the endpoint URL signed with
Standard Webhooks headers:

```bash
curl -i -H 'content-type: application/json' -d '{"hello":"world"}' \
  http://127.0.0.1:8090/in/acme/demo/<token>
```

A source verifies one of these schemes, named in its `scheme`:

| `type` | The sender signs with |
|---|---|
| `standard-webhooks` | `webhook-id`, `webhook-timestamp`, `webhook-signature`, under a `whsec_` secret |
| `github` | `X-Hub-Signature-256` |
| `stripe` | `Stripe-Signature`, with its timestamp |
| `hmac` | HMAC-SHA256 of the body in the header named by `header`, `hex` or `base64` per `encoding` |
| `token` | Nothing; the token in the URL is the secret, which is weaker |

`event_id` says where the sender puts its event id, `{"header": "webhook-id"}`
or `{"json": "data.id"}`. With one, every delivery carries the sender's id and
a retry of a stored webhook answers `200` without storing it again.

Every Felix connection the relay opens is for one tenant, with a token the
control plane narrowed to that tenant's namespace, and every admin request
uses the admin's own token narrowed the same way. A tenant cannot reach
another's data even through a bug in the relay: the broker refuses it.

A `token` source's token is a secret in the URL path. The relay never writes
request paths to its log or its metrics, but a proxy or load balancer in
front of it may log them; prefer a signing scheme where the sender has one.

The integration tests run the relay against that stack:

```bash
cargo test -- --include-ignored
```

`dev/up.sh --cluster` starts three replicating brokers instead, for the crash
test, and `dev/up.sh --retention` a broker that keeps records for seconds, for
the retention guard:

```bash
dev/up.sh --cluster && RELAY_TEST_CLUSTER=1 cargo test --test crash -- --include-ignored
dev/up.sh --retention && RELAY_TEST_RETENTION=1 cargo test --test retention -- --include-ignored
```

| Variable | Default | What |
|---|---|---|
| `RELAY_ROLES` | `intake,deliver,admin` | Which roles this process runs |
| `RELAY_LISTEN` | `127.0.0.1:8090` | HTTP address for intake, the admin API, `/healthz` and `/metrics` |
| `RELAY_PUBLIC_URL` | `http://<RELAY_LISTEN>` | Where browsers reach the relay, for the sign-in redirect |
| `RELAY_OIDC_ISSUER` | none; required for `admin` | The IdP admins sign in with |
| `RELAY_OIDC_CLIENT_ID`, `RELAY_OIDC_CLIENT_SECRET` | none; required for `admin` | The relay's client at that IdP |
| `RELAY_SECRET_KEY` | none, required | 32 bytes, base64 or hex, that seal every secret the relay stores in Felix |
| `RELAY_FELIX_BROKERS` | `127.0.0.1:5000` | Comma-separated broker addresses |
| `RELAY_FELIX_SERVER_NAME` | `localhost` | Name the broker certificate is checked against |
| `RELAY_FELIX_CA_FILE` | platform roots | PEM certificates to trust for the broker |
| `RELAY_IDP_TOKEN_FILE` | none, required | An ID token for the relay's service principal, read again before each token exchange |
| `RELAY_FELIX_CONTROL_PLANE` | `http://127.0.0.1:8443` | Where tokens are exchanged and streams created |
| `RELAY_FELIX_TENANT` | `relay` | The Felix tenant of the deployment |
| `RELAY_STREAM_REPLICAS` | `1` | Brokers that hold each new source's stream; above 1 its writes wait for a majority |
| `RELAY_REPLAY_WINDOW` | `7d` | How far back replays should reach; the relay warns when broker retention is shorter than this plus `RELAY_DISABLE_AFTER` |
| `RELAY_TENANTS` | `acme` | Comma-separated relay tenants this process serves, each a Felix namespace |
| `RELAY_WORKER_INDEX` | `0` | This delivery process's index; it owns the endpoints whose id hashes to it |
| `RELAY_WORKER_COUNT` | `1` | How many delivery processes share the endpoints |
| `RELAY_ENDPOINT_PREFIXES` | all | Comma-separated; only endpoints whose ids start with one of these |
| `RELAY_CLAIM_WAIT_MS` | `30000` | Wait before the first poll; at least the broker's `FELIX_GROUP_VISIBILITY_TIMEOUT_MS` (5000 on the dev stack) |
| `RELAY_BACKOFF` | `5s,15s,1m,2m,5m` | Waits between probes of a paused endpoint; the last repeats |
| `RELAY_REFUSED_RETRIES` | `5s,30s` | Waits before each retry of a refused record, then it is dead-lettered |
| `RELAY_DISABLE_AFTER` | `72h` | How long an endpoint may fail without a break before it is disabled |
| `RELAY_WORKER_NAME` | host name and pid | Names this process in endpoint health entries |

`GET /metrics` serves three Prometheus histograms, one per hop:
`relay_intake_ack_seconds`, `relay_poll_wakeup_seconds` and
`relay_outbound_request_seconds`, and a counter of group polls,
`relay_group_polls_total`, which stands still while an endpoint is paused.

Replay an endpoint over a time range (Unix milliseconds), then follow the job:

```bash
api -X POST -d '{"since": 1791030000000, "until": 1791030600000}' \
  http://127.0.0.1:8090/api/acme/endpoints/demo/replays
api http://127.0.0.1:8090/api/acme/jobs/<id>
```

`GET /api/acme/dead` lists dead letters, and
`POST /api/acme/dead/<offset>/redrive` or `.../discard` acts on one.

A disabled endpoint is enabled again with
`POST /api/<tenant>/endpoints/<id>/enable`, and its health is in the `state`
cache under `health/<endpoint>`.

## Build order

| M | Milestone | Proves | Status |
|---|---|---|---|
| 0 | One source, one endpoint, through Felix | A webhook goes in and comes out | Done |
| 1 | Signatures in and out, idempotency keys | Senders are verified, deliveries verify with standard libraries | Done |
| 2 | Retries, pausing, backoff, dead letters | An endpoint down for an hour gets everything back in order | Done |
| 3 | Many sources and endpoints, ordered and unordered | One slow endpoint does not delay the others | Done |
| 4 | Replay and redrive | Any endpoint replays a time range from the log | Done |
| 5 | Tenants, narrowed tokens, the admin page | Tenant isolation enforced by the broker | Done |
| 6 | Crash and failover tests, performance targets | Nothing acknowledged is lost | Done |
| 7 | Images, compose, Helm, a self-hosting guide | Anyone can self-host it | |

Each milestone is a [GitHub milestone](https://github.com/gabloe/felix-webhook-relay/milestones)
with an issue per piece of work.

## Links

- [Design](docs/design.md)
- [Contributing](CONTRIBUTING.md)
- [Felix](https://github.com/gabloe/felix)
- [Felix Canvas](https://github.com/gabloe/felix-canvas), the other application built only on Felix

## License

MIT
