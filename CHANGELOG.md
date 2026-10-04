# Changelog

Each release's section is its GitHub release notes.

## [0.1.0]

The first release: a self-hosted webhook relay whose only backend is Felix
0.6.0-preview.

### Intake

- Takes webhooks over HTTP at `/in/<tenant>/<source>` and answers `202` only
  after Felix has stored them.
- Verifies Standard Webhooks, GitHub, Stripe and generic HMAC signatures, or a
  token in the URL, before anything is stored.
- Drops a sender's retries of a webhook it already stored, keyed on the
  sender's event id.

### Delivery

- Signs every delivery with Standard Webhooks, so receivers verify it with an
  existing library.
- Delivers each endpoint in order, or unordered with a window of requests in
  flight, filtered by event type, from the source's tail or its start.
- Pauses an endpoint that is down and probes it on a backoff schedule while its
  backlog waits in the log. Sets aside a webhook an endpoint keeps refusing, in
  the `dead` stream, and disables an endpoint that answers `410` or fails for
  three days.
- Spreads endpoints over delivery processes by hashing their ids.

### Replay and redrive

- Replays any endpoint over a time or offset range beside live delivery.
- Redrives or discards dead letters, the relay's own and Felix's.

### Tenants and admin

- Serves many tenants from one process, each a Felix namespace reached only
  with tokens narrowed to it, so the broker refuses cross-tenant access.
- Admins sign in through OpenID Connect to a plain HTML page and a JSON API.
  Secrets are sealed under `RELAY_SECRET_KEY` before they reach Felix.
- `/metrics` has a histogram for each hop: intake, the group poll and the
  outbound request.

### Reliability

- Killing intake, a delivery worker or any broker of a three-broker cluster
  under load loses nothing that was acknowledged. Measured performance is in
  `docs/performance.md`.

### Self-hosting

- One image for amd64 and arm64, `ghcr.io/getfelix/felix-webhook-relay`, signed
  with cosign.
- A compose install with Felix and nothing else, and a Helm chart, published
  as `oci://ghcr.io/getfelix/charts/felix-webhook-relay`. `docs/self-hosting.md`
  is the guide.
