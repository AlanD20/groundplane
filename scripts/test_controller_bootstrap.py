"""Rationale: partial native installations never fall through to binary overwrite."""
import os
from pathlib import Path
import tempfile
import unittest

import controller_bootstrap as bootstrap


class ControllerBootstrapTest(unittest.TestCase):
    def setUp(self):
        parent = Path(__file__).resolve().parents[1] / ".tmp" / "test-controller-bootstrap"
        parent.mkdir(mode=0o700, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=parent)
        self.base = Path(self.temporary.name)
        self.root = self.base / "updates"
        self.guard = self.base / "controller-recovery"
        self.unit = self.base / "controller.service"
        self.layout = bootstrap.Layout(self.root, self.guard, self.unit, os.geteuid())

    def tearDown(self):
        self.temporary.cleanup()

    def install_guard(self):
        self.guard.write_bytes(b"guard")
        self.guard.chmod(0o500)
        self.unit.write_text(bootstrap.GUARD_LINE + "\n")

    def test_fresh_initialize_then_native_detection(self):
        self.assertEqual(self.layout.mode(), "bootstrap")
        self.layout.initialize()
        with self.assertRaises(ValueError):
            self.layout.mode()
        self.install_guard()
        self.assertEqual(self.layout.mode(), "native")
        with self.assertRaises(FileExistsError):
            self.layout.initialize()

    def test_partial_and_symlink_installations_refuse_bootstrap(self):
        self.install_guard()
        with self.assertRaises(ValueError):
            self.layout.mode()
        self.guard.unlink()
        self.unit.unlink()
        self.root.symlink_to(self.base / "missing")
        with self.assertRaises(OSError):
            self.layout.mode()

    def test_empty_rollback_removes_only_owned_empty_layout(self):
        self.layout.initialize()
        (self.root / "journal.lock").touch(mode=0o600)
        self.layout.remove_empty()
        self.assertFalse(self.root.exists())

    def test_rollback_retains_any_release_or_recovery_evidence(self):
        self.layout.initialize()
        (self.root / "journal.json").write_text("recovery evidence")
        with self.assertRaises(ValueError):
            self.layout.remove_empty()
        self.assertEqual((self.root / "journal.json").read_text(), "recovery evidence")
        (self.root / "journal.json").unlink()
        (self.root / "releases" / "candidate").mkdir()
        with self.assertRaises(ValueError):
            self.layout.remove_empty()
        self.assertTrue((self.root / "releases" / "candidate").is_dir())

    def test_bootstrap_idle_check_requires_every_page_terminal(self):
        class Pages:
            def __init__(self, replies):
                self.replies = iter(replies)
                self.calls = []

            def request(self, method, path):
                self.calls.append((method, path))
                return next(self.replies)

        pages = Pages([(200, {"items": [{"status": "completed"}], "next_cursor": "next"}),
                       (200, {"items": [{"status": "failed"}]})])
        bootstrap.require_idle(pages)
        self.assertEqual(len(pages.calls), 2)
        for response in ((200, {"items": [{"status": "running"}]}),
                         (503, None), (200, {"items": [{"status": "unknown"}]})):
            with self.subTest(response=response):
                with self.assertRaises(ValueError):
                    bootstrap.require_idle(Pages([response]))


if __name__ == "__main__":
    unittest.main()
