#!/usr/bin/env python3
"""Fold changelog.d/ fragments into CHANGELOG.md for a release.

Each pull request adds `changelog.d/<name>.<section>.md` instead of editing
CHANGELOG.md, so concurrent PRs stop conflicting on the same lines. At release
time this merges the fragments into the current `## [Unreleased]` section
(hand-written entries already there are kept), retitles it as the release, leaves
a fresh empty Unreleased heading above it, and deletes the fragments. The
heading style (`## [Unreleased]` or `## Unreleased`) carries over to the release.

    changelog-assemble.py <version> [--date YYYY-MM-DD]
    changelog-assemble.py --check
"""

import argparse
import datetime
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
FRAGMENT_DIR = ROOT / "changelog.d"
CHANGELOG = ROOT / "CHANGELOG.md"

SECTIONS = [
    "security",
    "added",
    "changed",
    "deprecated",
    "removed",
    "fixed",
    "performance",
    "documentation",
]
NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*(\.[A-Za-z0-9_-]+)*$")


def fragment_files(directory: Path) -> list[Path]:
    if not directory.is_dir():
        return []
    return sorted(
        p for p in directory.iterdir() if p.is_file() and p.name != "README.md" and not p.name.startswith(".")
    )


def parse_name(path: Path) -> tuple[str, str] | str:
    """Return (name, section), or an error message."""
    stem, dot, ext = path.name.rpartition(".")
    if ext != "md" or not dot:
        return f"{path.name}: fragments end in .md"
    name, dot, section = stem.rpartition(".")
    if not dot or not name:
        return f"{path.name}: expected <name>.<section>.md"
    if section not in SECTIONS:
        return f"{path.name}: unknown section '{section}' (use one of {', '.join(SECTIONS)})"
    if not NAME.match(name):
        return f"{path.name}: the name may only use letters, digits, '-', '_' and '.'"
    return name, section


def load_fragments(directory: Path) -> tuple[dict[str, list[str]], list[str]]:
    by_section: dict[str, list[str]] = {s: [] for s in SECTIONS}
    errors: list[str] = []
    for path in fragment_files(directory):
        parsed = parse_name(path)
        if isinstance(parsed, str):
            errors.append(parsed)
            continue
        body = path.read_text(encoding="utf-8").strip()
        if not body:
            errors.append(f"{path.name}: empty")
            continue
        # A heading would end the section early in the assembled file.
        if re.search(r"^#{1,3} ", body, re.MULTILINE):
            errors.append(f"{path.name}: has a heading; the file name sets the section")
            continue
        by_section[parsed[1]].append(body)
    return by_section, errors


def split_unreleased(text: str) -> tuple[str, str, str]:
    """Split into (before, unreleased body, after the Unreleased section)."""
    found = re.search(r"^## \[?Unreleased\]?[^\n]*\n", text, re.MULTILINE)
    if not found:
        raise SystemExit("CHANGELOG.md has no 'Unreleased' heading")
    rest = text[found.end():]
    nxt = re.search(r"^## ", rest, re.MULTILINE)
    end = nxt.start() if nxt else len(rest)
    return text[: found.end()], rest[:end], rest[end:]


def merge(unreleased: str, fragments: dict[str, list[str]]) -> str:
    """Merge fragments into an Unreleased body; return the new body."""
    parts = re.split(r"^### (.+?)[ \t]*\n", unreleased, flags=re.MULTILINE)
    preamble = parts[0].strip()
    existing: dict[str, str] = {}
    order: list[str] = []
    for title, body in zip(parts[1::2], parts[2::2]):
        if title not in existing:
            order.append(title)
            existing[title] = ""
        existing[title] = (existing[title] + "\n" + body.strip("\n")).strip("\n")

    known = {s.capitalize(): s for s in SECTIONS}
    titles = [s.capitalize() for s in SECTIONS]
    out = [preamble] if preamble else []
    for title in titles + [t for t in order if t not in known]:
        chunks = [existing[title]] if existing.get(title) else []
        if title in known:
            chunks += fragments[known[title]]
        if chunks:
            out.append(f"### {title}\n" + "\n".join(chunks))
    return "\n\n".join(out)


def assemble(version: str, date: str) -> int:
    fragments, errors = load_fragments(FRAGMENT_DIR)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    text = CHANGELOG.read_text(encoding="utf-8")
    head, body, tail = split_unreleased(text)
    merged = merge(body, fragments)
    if not merged:
        print("nothing to release: no Unreleased entries and no fragments", file=sys.stderr)
        return 1
    if re.search(rf"^## \[?{re.escape(version)}(\]|\s|$)", text, re.MULTILINE):
        print(f"CHANGELOG.md already has a section for {version}", file=sys.stderr)
        return 1
    title = f"[{version}]" if re.search(r"^## \[Unreleased\]", head, re.MULTILINE) else version
    CHANGELOG.write_text(f"{head}\n## {title} - {date}\n\n{merged}\n\n{tail}", encoding="utf-8")
    for path in fragment_files(FRAGMENT_DIR):
        path.unlink()
    return 0


def check() -> int:
    _, errors = load_fragments(FRAGMENT_DIR)
    if errors:
        print("invalid changelog fragments in changelog.d/:", file=sys.stderr)
        print("\n".join(f"  {e}" for e in errors), file=sys.stderr)
        return 1
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("version", nargs="?")
    ap.add_argument("--date", default=datetime.date.today().isoformat())
    ap.add_argument("--check", action="store_true", help="validate fragments and exit")
    args = ap.parse_args()
    if args.check:
        return check()
    if not args.version:
        ap.error("a version is required unless --check is given")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", args.date):
        ap.error("--date must be YYYY-MM-DD")
    return assemble(args.version.removeprefix("v"), args.date)


if __name__ == "__main__":
    raise SystemExit(main())
