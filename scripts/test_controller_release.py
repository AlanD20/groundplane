from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("controller_release.py")
SPEC = importlib.util.spec_from_file_location("controller_release", SCRIPT)
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)


class ControllerReleaseStageTest(unittest.TestCase):
    # Rationale: staging publishes only verified immutable data and a selector,
    # never the installed executable or activation/qualified-selection journal.
    def setUp(self):
        parent = SCRIPT.parents[1] / ".tmp" / "test-controller-release"
        parent.mkdir(mode=0o700, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=parent)
        self.root = Path(self.temporary.name)
        self.bundle = self.root / "bundle"
        self.bundle.mkdir(mode=0o700)
        self.store_path = self.root / "updates"
        self.store_path.mkdir(mode=0o700)
        (self.store_path / "releases").mkdir(mode=0o700)
        self.binary = b"verified candidate; this file must never execute\n"
        (self.bundle / "controller").write_bytes(self.binary)
        (self.bundle / "controller").chmod(0o500)
        self.metadata = {
            "schema": 1, "storage_epoch": 1, "channel_schema": 1,
            "controller_version": "0.1.0-test", "controller_sha256": self.digest(self.binary),
        }
        self.write_metadata()
        self.agent = "localhost:5000/groundplane-agent@sha256:" + "a" * 64
        self.store = release.ReleaseStore(self.store_path, os.geteuid())

    def tearDown(self):
        self.store.close()
        self.temporary.cleanup()

    @staticmethod
    def digest(raw):
        return "sha256:" + hashlib.sha256(raw).hexdigest()

    def write_metadata(self):
        (self.bundle / "controller-release.json").write_text(
            json.dumps(self.metadata, separators=(",", ":"), sort_keys=True), encoding="utf-8",
        )

    def test_stage_is_immutable_and_replayable(self):
        identity = self.store.stage(self.bundle, self.agent)
        expected = json.dumps({**self.metadata, "agent_image": self.agent}, separators=(",", ":"), sort_keys=True).encode()
        self.assertEqual(identity, self.digest(expected))
        leaf = self.store_path / "releases" / identity[7:]
        self.assertEqual((leaf / "manifest.json").read_bytes(), expected)
        self.assertEqual((leaf / "controller").read_bytes(), self.binary)
        self.assertEqual((leaf / "controller").stat().st_mode & 0o777, 0o500)
        self.assertEqual((leaf / "manifest.json").stat().st_mode & 0o777, 0o400)
        self.assertEqual(self.store.stage(self.bundle, self.agent), identity)
        self.assertEqual(json.loads((self.store_path / "candidate.json").read_bytes()), {"release": identity})
        self.assertFalse((self.store_path / "journal.json").exists())
        self.assertFalse((self.store_path / "selected.json").exists())

    def test_changed_binary_preserves_previous_candidate(self):
        identity = self.store.stage(self.bundle, self.agent)
        (self.bundle / "controller").chmod(0o700)
        (self.bundle / "controller").write_bytes(b"different candidate")
        with self.assertRaisesRegex(ValueError, "digest"):
            self.store.stage(self.bundle, self.agent)
        self.assertEqual(json.loads((self.store_path / "candidate.json").read_bytes()), {"release": identity})
        self.assertEqual(len(list((self.store_path / "releases").iterdir())), 1)

    def test_symlink_and_writable_inputs_are_rejected(self):
        for scenario in ("binary-symlink", "binary-writable", "candidate-symlink"):
            with self.subTest(scenario=scenario):
                path = self.bundle / "controller"
                if scenario == "binary-symlink":
                    path.rename(self.bundle / "original")
                    path.symlink_to("original")
                elif scenario == "binary-writable":
                    path.chmod(0o522)
                else:
                    (self.store_path / "candidate.json").symlink_to(self.bundle / "sentinel")
                with self.assertRaises((ValueError, OSError)):
                    self.store.stage(self.bundle, self.agent)
                if path.is_symlink():
                    path.unlink()
                    (self.bundle / "original").rename(path)
                path.chmod(0o500)
                if (self.store_path / "candidate.json").is_symlink():
                    (self.store_path / "candidate.json").unlink()

    def test_invalid_metadata_and_mutated_published_release_fail_closed(self):
        identity = self.store.stage(self.bundle, self.agent)
        manifest = self.store_path / "releases" / identity[7:] / "manifest.json"
        manifest.chmod(0o600)
        manifest.write_bytes(b"{}")
        with self.assertRaises(ValueError):
            self.store.stage(self.bundle, self.agent)
        self.metadata["storage_epoch"] = True
        self.write_metadata()
        with self.assertRaises(ValueError):
            self.store.stage(self.bundle, self.agent)
        self.metadata["storage_epoch"] = 1
        self.write_metadata()
        with self.assertRaises(ValueError):
            self.store.stage(self.bundle, "agent:latest")


if __name__ == "__main__":
    unittest.main()
