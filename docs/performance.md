# Performance

Each row of the design's [performance targets](design.md#performance-targets),
measured, with the conditions it was measured under. Everything here was
measured on **one host**: the Felix broker, its control plane, the relay, the
load generator and the receiving endpoints shared four cores. A target met
here is not proof it holds in production, and rows that need dedicated hardware
say so.

## Conditions

| | |
|---|---|
| Host | GitHub Codespace, 4 cores, 16 GB, its overlay disk |
| Felix | 0.6.0-preview images, one broker in Docker (`dev/up.sh`) |
| Durability | `FELIX_DURABLE_FSYNC_MODE=on_commit` and `FELIX_ACK_ON_COMMIT=true`: an acknowledged webhook is on the device |
| Relay | Release build, `RUST_LOG=warn`, one process |
| Bodies | 1 KiB |
| How | `RELAY_TEST_PERF=1 cargo test --release --test load -- --include-ignored --nocapture --test-threads 1 measure` and the demonstration tests named below |

The `demonstrations` CI job runs the same measurements on a GitHub-hosted
runner on every pull request, so their numbers are in each run's log.

## Results

| Path | Target | Measured | Met? |
|---|---|---|---|
| Intake, request to `202` | < 10 ms p50, < 30 ms p99 | 2.2 ms p50, 3.5 ms p99, one sender | Yes, on one host |
| Intake throughput, one process | 5,000/s at 1 KiB until p99 crosses 30 ms | 11,330/s at 128 in flight, p99 16.3 ms | Yes, on one host |
| Accept to endpoint receipt, healthy endpoint | < 50 ms p50, < 250 ms p99 | 2.5 ms p50, 4.1 ms p99, measured from sending, so it includes intake | Yes, on one host |
| One endpoint slowed to 10 s | Others' p99 within 10% | 6.53 ms alone, 6.27 ms beside it (1,000 webhooks, `--test isolation`) | Yes; needs dedicated hardware to resolve 10% of a few ms |
| Drain after an outage, ordered | One request per round trip | 10,000 webhooks after a one-hour outage, in order, no repeats (`--test outage`, real backoff) | Yes |
| Drain after an outage, unordered, window 16 | 16 in flight | Exactly 16 concurrent requests (`--test endpoints`, window test) | Yes |
| Endpoints per delivery process | 1,000 idle, 200 active | 1,000 idle and 200 active, 2 webhooks/s to the active ones' source (400 deliveries/s): 15.5 ms p50, 40.9 ms p99, 46 MiB idle, 71 MiB busy | Yes at that rate; see the limit below |
| Offset for a time, 10M-record source | < 1 s | 137 to 341 ms, 23 or 24 reads, on a GitHub-hosted runner | Yes |

Intake at other concurrencies:

| In flight | Rate (/s) | p50 | p99 |
|---|---|---|---|
| 1 | 438 | 2.2 ms | 3.5 ms |
| 8 | 1,796 | 4.2 ms | 6.9 ms |
| 32 | 5,400 | 5.5 ms | 7.7 ms |
| 128 | 11,330 | 10.2 ms | 16.3 ms |

Intake reaches these rates by gathering the webhooks that arrive while an
append is in flight into one idempotent `publish_batch`, which takes one
sequence number whatever its size. One at a time, as before, it was bound to
one broker round trip with fsync per webhook.

## Where it stops

**Group operations per source.** Every endpoint of a source is a consumer
group on the source's one shard, and every delivery costs that shard a poll
and an acknowledgement, each committed before it answers. On this host the
shard handled about 1,000 group operations a second, so about 500 deliveries a
second per source across all its endpoints. Beyond that latency climbs into
seconds: 200 active endpoints at 10 webhooks/s (2,000 deliveries/s) measured
8 to 12 s p50. A new endpoint's walk past a source's history runs at the same
rate, 1,093 records a second here, because Felix cannot start a group at an
offset and the relay acknowledges each older record unsent. This is the first
number to measure on dedicated hardware, and the reason a busy source should
not carry hundreds of endpoints.

**Streams per connection.** Each idle endpoint holds a waiting group poll, and
each poll holds one of a QUIC connection's 1,024 streams. A thousand idle
endpoints on one connection left the busy endpoints queueing for a stream,
with p50 delivery near the 10 s poll wait. The relay spreads group traffic
over four connections per tenant, which is room for about 4,000 endpoints per
process.

**Short-lived subscriptions.** A dropped Felix subscription keeps its stream
until the broker next writes to it, which on a quiet stream is never, so a
connection stopped accepting new subscriptions after about 500 tail lookups.
The relay does its short reads on a separate connection that it replaces
every 400 subscriptions.
