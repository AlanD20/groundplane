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

    # QA: HOST-02; local layout detection only, not installation or enrollment.
    # Rationale: partial initialization cannot count as native or be initialized twice.
    def test_fresh_initialize_then_native_detection(self):
        self.assertEqual(self.layout.mode(), "bootstrap")
        self.layout.initialize()
        with self.assertRaises(ValueError):
            self.layout.mode()
        self.install_guard()
        self.assertEqual(self.layout.mode(), "native")
        with self.assertRaises(FileExistsError):
            self.layout.initialize()

    # QA: HOST-02, UP-03; local preflight only, not full deployment.
    # Rationale: an incomplete or substituted installation must not permit overwrite.
    def test_partial_and_symlink_installations_refuse_bootstrap(self):
        self.install_guard()
        with self.assertRaises(ValueError):
            self.layout.mode()
        self.guard.unlink()
        self.unit.unlink()
        self.root.symlink_to(self.base / "missing")
        with self.assertRaises(OSError):
            self.layout.mode()

    # QA: HOST-02; disposable bootstrap layout cleanup, not native update recovery.
    # Rationale: failed fresh initialization may remove its empty owned layout.
    def test_empty_rollback_removes_only_owned_empty_layout(self):
        self.layout.initialize()
        (self.root / "journal.lock").touch(mode=0o600)
        self.layout.remove_empty()
        self.assertFalse(self.root.exists())

    # QA: HOST-02, UP-09; local bootstrap retention, not journal-phase recovery.
    # Rationale: cleanup must refuse as soon as release or recovery evidence exists.
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


if __name__ == "__main__":
    unittest.main()
