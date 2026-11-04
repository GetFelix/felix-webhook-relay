# Changelog fragments

Add one file here per change instead of editing `CHANGELOG.md`, so pull requests
stop conflicting on the same lines.

## Naming

`changelog.d/<name>.<section>.md`

- `<name>` is free. Use the issue or PR number and a slug: `123-durable-mark`.
- `<section>` is one of `security`, `added`, `changed`, `deprecated`, `removed`,
  `fixed`, `performance`, `documentation`.

## Body

Markdown bullets, written exactly as they should read under the heading. A change
that belongs in two sections gets two files. No headings: the file name sets the
section, and `--check` refuses a `#`, `##` or `###` line.

`changelog.d/123-durable-mark.fixed.md`:

```markdown
- A durable publish no longer acknowledges before its mark is written (#123).
  The ack now waits for the commit, so a crash cannot lose an acknowledged record.
```

## Releasing

`python3 .github/scripts/changelog-assemble.py <version> [--date YYYY-MM-DD]` merges the
fragments into the `## Unreleased` section of `CHANGELOG.md` in a fixed section order,
retitles it as the release, leaves an empty Unreleased section, and deletes the
fragments. Entries already written in `CHANGELOG.md` are kept. The release workflow
refuses a tag while fragments are still here.

CI runs `python3 .github/scripts/changelog-assemble.py --check` to validate names,
sections and non-empty bodies, and `python3 .github/scripts/test_changelog_assemble.py`
for the assembler itself.
