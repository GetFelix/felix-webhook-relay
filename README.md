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

**Status: M1 done.** Sources and endpoints live in Felix, written through
the admin API with their secrets sealed. Intake verifies Standard Webhooks,
GitHub, Stripe and generic HMAC signatures before anything is stored, and
dedupes retries on the sender's event id. Deliveries are signed with
Standard Webhooks and go to one endpoint per delivery process, in order.
Retries with backoff, dead letters and everything after are still to come.
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
0.6.0-preview images, with a stand-in identity provider. It seeds the `acme`
tenant with the relay's caches and a `src.demo` stream, and writes the
broker's certificate, a relay token and an operator token to `dev/state/`:

```bash
dev/up.sh
export RELAY_FELIX_CA_FILE="$PWD/dev/state/broker-cert.pem"
export RELAY_FELIX_TOKEN_FILE="$PWD/dev/state/relay.token"
export RELAY_SECRET_KEY="$(openssl rand -base64 32)"
cargo run -p felix-relay
```

Sources and endpoints are written through the admin API. A source's stream
must exist first; `dev/up.sh` made `src.demo`. A source with a `token` scheme
gets a long random token in its URL, and the answer shows it once:

```bash
curl -s -X PUT -H 'content-type: application/json' \
  -d '{"scheme": {"type": "token"}}' http://127.0.0.1:8090/api/acme/sources/demo
curl -s -X PUT -H 'content-type: application/json' \
  -d '{"source": "demo", "url": "http://127.0.0.1:9000/hook"}' \
  http://127.0.0.1:8090/api/acme/endpoints/demo
```

The endpoint's answer holds its `whsec_` signing secret. Send a webhook to
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

The admin API has no sign-in yet, so keep `RELAY_LISTEN` on a private address.

The integration tests run the relay against that stack:

```bash
cargo test -- --include-ignored
```

| Variable | Default | What |
|---|---|---|
| `RELAY_ROLES` | `intake,deliver,admin` | Which roles this process runs |
| `RELAY_LISTEN` | `127.0.0.1:8090` | HTTP address for intake, the admin API, `/healthz` and `/metrics` |
| `RELAY_SECRET_KEY` | none, required | 32 bytes, base64 or hex, that seal every secret the relay stores in Felix |
| `RELAY_FELIX_BROKERS` | `127.0.0.1:5000` | Comma-separated broker addresses |
| `RELAY_FELIX_SERVER_NAME` | `localhost` | Name the broker certificate is checked against |
| `RELAY_FELIX_CA_FILE` | platform roots | PEM certificates to trust for the broker |
| `RELAY_FELIX_TOKEN_FILE` | none, required | Felix token |
| `RELAY_FELIX_TENANT` | `relay` | The Felix tenant of the deployment |
| `RELAY_TENANT` | `acme` | The relay tenant, which is a Felix namespace |
| `RELAY_ENDPOINT` | `demo` | The endpoint this process delivers to; its consumer group is `ep.<id>` |

`GET /metrics` serves three Prometheus histograms, one per hop:
`relay_intake_ack_seconds`, `relay_poll_wakeup_seconds` and
`relay_outbound_request_seconds`.

## Build order

| M | Milestone | Proves | Status |
|---|---|---|---|
| 0 | One source, one endpoint, through Felix | A webhook goes in and comes out | Done |
| 1 | Signatures in and out, idempotency keys | Senders are verified, deliveries verify with standard libraries | Done |
| 2 | Retries, pausing, backoff, dead letters | An endpoint down for an hour gets everything back in order | |
| 3 | Many sources and endpoints, ordered and unordered | One slow endpoint does not delay the others | |
| 4 | Replay and redrive | Any endpoint replays a time range from the log | |
| 5 | Tenants, narrowed tokens, the admin page | Tenant isolation enforced by the broker | |
| 6 | Crash and failover tests, performance targets | Nothing acknowledged is lost | |
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
