# Contributing

This page is how code and pull requests should read. The design and the
milestone plan are in [docs/design.md](docs/design.md).

## Code

- Rust only, so the whole project builds with `cargo` and nothing else. No npm,
  no frontend build step. The admin page is server-rendered HTML with a
  stylesheet and a few lines of inline JavaScript at most.
- Write the simplest thing that meets the design. No abstraction a milestone
  does not need yet.
- Keep `core` pure. The envelope, the signature schemes, the retry and
  endpoint-health state machine and the ordering rules live there with no I/O,
  and are tested there. The `relay` crate wires them to axum and
  `felix-client`.
- Keep every role stateless beyond what it has in flight. If something must
  survive a restart, it goes in Felix.
- No second datastore. If a feature cannot be expressed in Felix streams,
  caches, counters and consumer groups, it is out of scope, or it is a Felix
  gap to file.
- Format with `cargo fmt` and lint with `cargo clippy -D warnings`. CI checks
  both.

## Comments and docs

- Comment only where the code is unclear: a sentence or two on a constraint the
  code cannot show, such as an ordering requirement or a failure mode.
- No narration of what the code does, no templated headers, no comments that
  restate a test's name.
- Document public items briefly (`///`): what it does and what it guarantees.
- Write docs in plain sentences. No em-dashes, no filler words, no hedging.
- Docs change with the code. If a pull request changes behaviour, update the
  page that describes it.

## Pull requests

- One pull request per milestone, closing that milestone's issues.
- CI must pass before review.
- No AI attribution in commits or pull request descriptions.
- List any design calls under "Review notes", and any Felix gaps you hit under
  "Felix gaps found", with the Felix issue each one is filed as.

## Milestones and issues

Work follows the build order in [design.md](docs/design.md#build-order). Each
milestone is a [GitHub milestone](https://github.com/gabloe/felix-webhook-relay/milestones),
each piece of it is an issue, and each milestone lands as one pull request that
closes its issues. When Felix gets in the way, file an issue on
[Felix](https://github.com/gabloe/felix/issues) and link it from the pull request.

## Releases

Bump the version in `core/Cargo.toml`, `relay/Cargo.toml`, the chart's
`version` and `appVersion`, and the compose file's `RELAY_VERSION` defaults,
and add the version's section to `CHANGELOG.md`. A `v<version>` tag then runs
`.github/workflows/release.yml`, which refuses a tag that disagrees with any
of them (`scripts/release-notes.sh` is the check). Rehearse first by running
the workflow by hand with `dry_run`, and `ref` set to the branch.
