#!/usr/bin/env python3
"""Stage only the public landing page and exact installer; no repository export."""

import hashlib
from pathlib import Path
import subprocess

from bump_version import ROOT, release_notes


def build(root, destination):
    release_notes(root)  # Require consistent release metadata and meaningful notes.
    installer = (root / "install.sh").read_bytes()
    destination.mkdir()  # Refuse a stale destination, including symlinks.
    subprocess.run(
        ["bash", "scripts/repo-env.sh", "npm", "--prefix", "console", "run",
         "build:site", "--", str(destination.resolve())],
        cwd=root, check=True)
    (destination / "install.sh").write_bytes(installer)
    (destination / "install.sh.sha256").write_text(hashlib.sha256(installer).hexdigest() + "  install.sh\n")
    (destination / ".nojekyll").touch()


if __name__ == "__main__":
    # The existing validator owns ignored repository-local output paths.
    parent = subprocess.check_output(
        ["bash", "-c", "source scripts/repo-env.sh; repo_temp_dir .tmp/pages-build"],
        cwd=ROOT, text=True).strip()
    build(ROOT, Path(parent) / "site")
    print(Path(parent) / "site")
