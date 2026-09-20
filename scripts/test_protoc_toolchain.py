"""Prevent developer/CI compiler drift before generated files can be rewritten."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent


class ProtocToolchainTests(unittest.TestCase):
    def test_wrong_compiler_fails_before_generators_are_invoked(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "scripts").mkdir()
            shutil.copy2(ROOT / "Makefile", root)
            shutil.copy2(ROOT / ".protoc-version", root)
            shutil.copy2(ROOT / "scripts/repo-env.sh", root / "scripts")
            compiler = root / "wrong-protoc"
            for version in ("3.21.12", "36.2"):
                with self.subTest(version=version):
                    compiler.write_text("#!/bin/sh\nif test \"$1\" = --version; then\n"
                                        f"echo 'libprotoc {version}'\nelse\ntouch generated\nfi\n")
                    compiler.chmod(0o700)
                    environment = os.environ.copy()
                    for key in ("TMPDIR", "GOTMPDIR", "GOCACHE", "GROUNDPLANE_COVERAGE_FILE",
                                "MAKEFLAGS", "MFLAGS", "MAKELEVEL"):
                        environment.pop(key, None)
                    result = subprocess.run(["make", "proto", f"PROTOC={compiler}"], cwd=root,
                                            env=environment, text=True, capture_output=True)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("protoc version mismatch", result.stderr)
                    self.assertFalse((root / "generated").exists())


if __name__ == "__main__":
    unittest.main()
