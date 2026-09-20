"""Release preparation must preserve history and refuse inconsistent metadata."""

import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import bump_version


class BumpVersionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "console").mkdir()
        self.write("VERSION", "0.0.1\n")
        self.write("console/package.json", json.dumps({"version": "0.0.1", "dependencies": {"example": "2.0.0"}}))
        self.write("console/package-lock.json", json.dumps({"version": "0.0.1", "packages": {
            "": {"version": "0.0.1"}, "node_modules/example": {"version": "2.0.0"}}}))
        self.history = "## 0.0.1 — pending publication\n\n- Initial hosting.\n"
        self.write("CHANGELOG.md", "# Changelog\n\n## [Unreleased]\n\n- Fix update retry.\n\n" + self.history)

    def write(self, name, text):
        (self.root / name).write_text(text)

    def snapshot(self):
        return {name: (self.root / name).read_bytes() for name in bump_version.FILES}

    def invoke(self, *args):
        output = io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            result = bump_version.main(list(args), self.root)
        return result, output.getvalue()

    def test_bump_moves_notes_preserves_history_and_dependency_versions(self):
        self.assertEqual(self.invoke("0.0.2")[0], 0)
        self.assertEqual((self.root / "VERSION").read_text(), "0.0.2\n")
        self.assertEqual(bump_version.release_notes(self.root, "0.0.2"), "- Fix update retry.\n")
        changelog = (self.root / "CHANGELOG.md").read_text()
        self.assertIn("## [Unreleased]\n\n## 0.0.2 — pending publication", changelog)
        self.assertTrue(changelog.endswith(self.history))
        lock = json.loads((self.root / "console/package-lock.json").read_text())
        self.assertEqual(lock["packages"]["node_modules/example"]["version"], "2.0.0")

    def test_dry_run_shows_changes_without_writing(self):
        before = self.snapshot()
        status, output = self.invoke("0.1.0", "--dry-run")
        self.assertEqual(status, 0)
        self.assertIn("+0.1.0", output)
        self.assertEqual(before, self.snapshot())

    def test_invalid_same_and_older_versions_cannot_change_files(self):
        before = self.snapshot()
        for value in ("v0.0.2", "01.0.0", "0.0.2-rc.1", "0.0.1", "0.0.0", "latest"):
            with self.subTest(version=value):
                self.assertEqual(self.invoke(value)[0], 1)
                self.assertEqual(before, self.snapshot())

    def test_drift_missing_file_and_duplicate_section_fail_before_writes(self):
        for name, content in (("VERSION", "0.0.9\n"),
                              ("CHANGELOG.md", "## [Unreleased]\n\n" + self.history * 2)):
            original = (self.root / name).read_text()
            self.write(name, content)
            before = self.snapshot()
            self.assertEqual(self.invoke("0.0.2")[0], 1)
            self.assertEqual(before, self.snapshot())
            self.write(name, original)
        (self.root / "console/package-lock.json").unlink()
        self.assertEqual(self.invoke("0.0.2")[0], 1)
        self.assertEqual((self.root / "VERSION").read_text(), "0.0.1\n")

    def test_empty_unreleased_scaffolds_section_but_cannot_publish_empty_notes(self):
        self.write("CHANGELOG.md", "## [Unreleased]\n\n" + self.history)
        self.assertEqual(self.invoke("0.0.2")[0], 0)
        self.assertEqual(self.invoke("--check")[0], 1)

    def test_tag_mismatch_and_notes_extraction(self):
        self.assertEqual(self.invoke("0.0.2", "--check")[0], 1)
        status, notes = self.invoke("0.0.1", "--notes")
        self.assertEqual(status, 0)
        self.assertEqual(notes, "- Initial hosting.\n")
        self.assertNotIn("Fix update retry", notes)

    def test_write_failure_restores_previous_metadata(self):
        before = self.snapshot()
        write = Path.write_text
        failed = False

        def fail_once(path, content, *args, **kwargs):
            nonlocal failed
            if path.name == "package-lock.json" and not failed:
                failed = True
                raise OSError("simulated disk error")
            return write(path, content, *args, **kwargs)

        with mock.patch.object(Path, "write_text", fail_once):
            self.assertEqual(self.invoke("0.0.2")[0], 1)
        self.assertEqual(before, self.snapshot())

    def test_symlinked_metadata_is_not_overwritten(self):
        target = self.root / "version-target"
        target.write_text("0.0.1\n")
        (self.root / "VERSION").unlink()
        (self.root / "VERSION").symlink_to(target)
        self.assertEqual(self.invoke("0.0.2")[0], 1)
        self.assertEqual(target.read_text(), "0.0.1\n")


if __name__ == "__main__":
    unittest.main()
