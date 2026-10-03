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

**Status: M0 done.** One hardcoded source and one endpoint work end to
end through Felix: intake answers only once a webhook is durable, and a
delivery worker posts it to the endpoint in order. Signatures, retries and
everything after are still to come. The design and the plan are in
[docs/design.md](docs/design.md).

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
tenant with the `src.demo` stream and the relay's other streams and caches,
and writes the broker's certificate and a relay token to `dev/state/`:

```bash
dev/up.sh
export RELAY_FELIX_CA_FILE="$PWD/dev/state/broker-cert.pem"
export RELAY_FELIX_TOKEN_FILE="$PWD/dev/state/relay.token"
RELAY_ENDPOINT_URL=http://127.0.0.1:9000/hook cargo run -p felix-relay
```

Then send it a webhook, and it arrives at the endpoint URL with a
`webhook-id` header:

```bash
curl -i -H 'content-type: application/json' -d '{"hello":"world"}' \
  http://127.0.0.1:8090/in/acme/demo
```

The integration tests run the relay against that stack, once as one process
and once as separate intake and delivery processes:

```bash
cargo test -- --include-ignored
```

| Variable | Default | What |
|---|---|---|
| `RELAY_ROLES` | `intake,deliver,admin` | Which roles this process runs |
| `RELAY_LISTEN` | `127.0.0.1:8090` | HTTP address for intake, `/healthz` and `/metrics` |
| `RELAY_FELIX_BROKERS` | `127.0.0.1:5000` | Comma-separated broker addresses |
| `RELAY_FELIX_SERVER_NAME` | `localhost` | Name the broker certificate is checked against |
| `RELAY_FELIX_CA_FILE` | platform roots | PEM certificates to trust for the broker |
| `RELAY_FELIX_TOKEN_FILE` | none | Felix token; required for `intake` and `deliver` |
| `RELAY_FELIX_TENANT` | `relay` | The Felix tenant of the deployment |
| `RELAY_TENANT` | `acme` | The relay tenant, which is a Felix namespace |
| `RELAY_SOURCE` | `demo` | The one source intake accepts |
| `RELAY_EVENT_TYPE_HEADER` | none | Request header that names the event type |
| `RELAY_KEEP_HEADERS` | none | Comma-separated request headers stored with the body |
| `RELAY_ENDPOINT` | `demo` | The one endpoint's id; its consumer group is `ep.<id>` |
| `RELAY_ENDPOINT_URL` | none | Where deliveries go; required for `deliver` |

`GET /metrics` serves three Prometheus histograms, one per hop:
`relay_intake_ack_seconds`, `relay_poll_wakeup_seconds` and
`relay_outbound_request_seconds`.

## Build order

| M | Milestone | Proves | Status |
|---|---|---|---|
| 0 | One source, one endpoint, through Felix | A webhook goes in and comes out | Done |
| 1 | Signatures in and out, idempotency keys | Senders are verified, deliveries verify with standard libraries | |
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
