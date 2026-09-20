"""Public site must serve the exact installer without exporting private files."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from build_pages import build, ROOT


class BuildPagesTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "console").mkdir()
        (self.root / "site").mkdir()
        (self.root / "VERSION").write_text("0.0.1\n")
        (self.root / "console/package.json").write_text(json.dumps({"version": "0.0.1"}))
        (self.root / "console/package-lock.json").write_text(json.dumps({
            "version": "0.0.1", "packages": {"": {"version": "0.0.1"}}}))
        (self.root / "CHANGELOG.md").write_text("## [Unreleased]\n\n## 0.0.1 — pending publication\n\n- Hosting.\n")
        (self.root / "site/index.html").write_bytes((ROOT / "site/index.html").read_bytes())
        (self.root / "install.sh").write_bytes(b"#!/bin/sh\necho exact-installer\n")
        (self.root / "private.txt").write_text("must not be published")
        self.destination = self.root / "public"

    def test_public_payload_is_allowlisted_and_checksum_matches_exact_bytes(self):
        build(self.root, self.destination)
        self.assertEqual({file.name for file in self.destination.iterdir()},
                         {"index.html", "install.sh", "install.sh.sha256", ".nojekyll"})
        source = (self.root / "install.sh").read_bytes()
        self.assertEqual((self.destination / "install.sh").read_bytes(), source)
        self.assertEqual((self.destination / "install.sh.sha256").read_text(),
                         hashlib.sha256(source).hexdigest() + "  install.sh\n")
        page = (self.destination / "index.html").read_text()
        self.assertNotIn("__VERSION__", page)
        self.assertIn("--version 0.0.1", page)
        self.assertIn("https://gp.aland20.com/install.sh", page)

    def test_existing_output_is_preserved_not_republished(self):
        self.destination.mkdir()
        (self.destination / "sentinel").write_text("preserve")
        with self.assertRaises(FileExistsError):
            build(self.root, self.destination)
        self.assertEqual(list(self.destination.iterdir()), [self.destination / "sentinel"])

    def test_metadata_drift_refuses_before_public_output(self):
        (self.root / "VERSION").write_text("0.0.2\n")
        with self.assertRaises(ValueError):
            build(self.root, self.destination)
        self.assertFalse(self.destination.exists())


if __name__ == "__main__":
    unittest.main()
