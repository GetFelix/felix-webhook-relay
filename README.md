# Felix Webhook Relay

A self-hosted webhook relay whose entire backend is [Felix](https://github.com/gabloe/felix).
No Postgres, Redis or job queue beside it.

It takes webhooks in over HTTP, verifies their signatures, and stores them
durably before answering. It delivers each one to its endpoints, signed, with
retries and backoff. It sets aside what an endpoint keeps refusing, and it can
replay any endpoint over a time range after an outage.

**Status: design stage.** Nothing is built yet. The design and the plan are in
[docs/design.md](docs/design.md).

## Why it exists

A webhook relay is almost nothing but a queue: accept, store, deliver, retry,
give up, replay. It is usually built as a queue plus Postgres plus a worker
pool plus a scheduler, with an outbox to keep the queue and the database
agreeing.

Felix already has the pieces as one system. One durable stream per source
holds every webhook. One consumer group per endpoint gives it its own cursor,
acknowledgements, redelivery and dead letters. Replay is a subscription from
an offset over the same log. An endpoint that is down for an hour costs
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

## Build order

| M | Milestone | Proves | Status |
|---|---|---|---|
| 0 | One source, one endpoint, through Felix | A webhook goes in and comes out | |
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
