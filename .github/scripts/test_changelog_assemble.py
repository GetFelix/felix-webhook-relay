#!/usr/bin/env python3
"""Tests for changelog-assemble.py. Run: python3 .github/scripts/test_changelog_assemble.py"""

import importlib.util
import re
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("changelog_assemble", HERE / "changelog-assemble.py")
ca = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ca)


def section(text, version):
    """The body of one `## ` section, matching the heading with or without brackets."""
    found = re.search(rf"^## \[?{re.escape(version)}\]?[^\n]*\n(.*?)(?=^## |\Z)", text, re.MULTILINE | re.DOTALL)
    return found.group(1).strip() if found else ""

CHANGELOG = """# Changelog

## [Unreleased]

### Fixed
- hand-written fix

### Known limitations
- a limit

### Added
- hand-written add

## [0.1.0] - 2026-01-01

### Added
- old
"""


class Assemble(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        ca.CHANGELOG = root / "CHANGELOG.md"
        ca.FRAGMENT_DIR = root / "changelog.d"
        ca.FRAGMENT_DIR.mkdir()
        ca.CHANGELOG.write_text(CHANGELOG)

    def tearDown(self):
        self.tmp.cleanup()

    def frag(self, name, body):
        (ca.FRAGMENT_DIR / name).write_text(body)

    def test_merges_orders_and_deletes(self):
        self.frag("2-b.fixed.md", "- fix b\n")
        self.frag("1-a.fixed.md", "- fix a\n")
        self.frag("3-s.security.md", "- sec\n")
        self.frag("README.md", "ignored")
        self.assertEqual(ca.assemble("1.0.0", "2026-02-02"), 0)
        text = ca.CHANGELOG.read_text()
        self.assertIn("## [Unreleased]\n\n## [1.0.0] - 2026-02-02\n", text)
        body = section(text, "1.0.0")
        heads = [l for l in body.splitlines() if l.startswith("### ")]
        self.assertEqual(heads, ["### Security", "### Added", "### Fixed", "### Known limitations"])
        self.assertLess(body.index("hand-written fix"), body.index("fix a"))
        self.assertLess(body.index("fix a"), body.index("fix b"))
        self.assertEqual(section(text, "0.1.0"), "### Added\n- old")
        self.assertEqual(section(text, "Unreleased"), "")
        self.assertEqual([p.name for p in ca.fragment_files(ca.FRAGMENT_DIR)], [])
        self.assertTrue((ca.FRAGMENT_DIR / "README.md").exists())

    def test_check_rejects_bad_fragments(self):
        self.assertEqual(ca.check(), 0)
        self.frag("x.bogus.md", "- y")
        self.frag("e.added.md", "  \n")
        self.frag("noext.md", "- y")
        self.frag("h.fixed.md", "- y\n## Oops")
        self.frag("ok.fixed.md", "- y\n  ## indented is fine")
        _, errors = ca.load_fragments(ca.FRAGMENT_DIR)
        self.assertEqual(len(errors), 4)
        self.assertEqual(ca.check(), 1)

    def test_unbracketed_heading_style_carries_over(self):
        ca.CHANGELOG.write_text("# Changelog\n\n## Unreleased\n\n## 0.1.0\n\n### Added\n- old\n")
        self.frag("1-a.added.md", "- new\n")
        self.assertEqual(ca.assemble("0.2.0", "2026-02-02"), 0)
        text = ca.CHANGELOG.read_text()
        self.assertIn("## Unreleased\n\n## 0.2.0 - 2026-02-02\n\n### Added\n- new\n\n## 0.1.0\n", text)
        self.assertEqual(ca.assemble("0.2.0", "2026-02-02"), 1)


if __name__ == "__main__":
    unittest.main()
