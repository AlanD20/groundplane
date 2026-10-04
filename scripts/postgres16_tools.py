"""Install authenticated backup clients without modifying a database image."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import tempfile

TOOLS_ROOT = Path("/root/.groundplane/postgres16-tools")
CONTAINER_PATH = "/opt/groundplane/postgres16"
MEMBER = re.compile(r"(?:bin|lib)/[A-Za-z0-9._+-]+|helper|client-gate|release\.json")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")


def verify(directory: Path, manifest: dict) -> None:
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError("PostgreSQL backup tools directory is unsafe")
    info = directory.stat()
    if info.st_uid != 0 or info.st_gid != 0 or info.st_mode & 0o6022:
        raise ValueError("PostgreSQL backup tools directory ownership is unsafe")
    inventory = directory / "SHA256SUMS"
    info = inventory.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_size > 32768 or info.st_uid != 0
            or info.st_gid != 0 or info.st_mode & 0o6022):
        raise ValueError("PostgreSQL backup tools inventory is unsafe")
    expected = {}
    total = 0
    for line in inventory.read_text().splitlines():
        digest, separator, name = line.partition("  ")
        if not separator or not re.fullmatch(r"[0-9a-f]{64}", digest) or not MEMBER.fullmatch(name) or name in expected:
            raise ValueError("PostgreSQL backup tools inventory is invalid")
        path = directory / name
        info = path.lstat()
        total += info.st_size
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_gid != 0
                or info.st_mode & 0o6022 or total > 100 * 1024**2
                or hashlib.sha256(path.read_bytes()).hexdigest() != digest):
            raise ValueError("PostgreSQL backup tool bytes or ownership changed")
        expected[name] = digest
    found = set()
    for path in directory.rglob("*"):
        info = path.lstat()
        if info.st_uid != 0 or info.st_gid != 0 or info.st_mode & 0o6022 or stat.S_ISLNK(info.st_mode):
            raise ValueError("PostgreSQL backup tools tree is unsafe")
        if not stat.S_ISDIR(info.st_mode):
            found.add(str(path.relative_to(directory)))
    if found != {*expected, "SHA256SUMS"}:
        raise ValueError("PostgreSQL backup tools have unexpected files")
    for name, field in (("helper", "helper_sha256"), ("client-gate", "gate_sha256"),
                        ("bin/pg_dump", "pg_dump_sha256"), ("bin/pg_restore", "pg_restore_sha256"),
                        ("bin/psql", "psql_sha256")):
        if expected.get(name) != manifest[field]:
            raise ValueError("PostgreSQL backup tool differs from authenticated release")
    if json.loads((directory / "release.json").read_bytes()) != manifest:
        raise ValueError("PostgreSQL backup tools metadata changed")


def install(catalog: dict, architecture: str) -> Path:
    reference = catalog["image"]
    _, separator, digest = reference.rpartition("@")
    if not separator or not DIGEST.fullmatch(digest) or os.geteuid() != 0:
        raise ValueError("backup tools installation requires root and an authenticated digest")
    native = next(image for image in catalog["images"] if image["manifest"]["architecture"] == architecture)
    inspected = json.loads(subprocess.check_output(
        ["docker", "image", "inspect", reference], text=True, timeout=30))[0]
    if (inspected["Id"] not in {native["image_id"], native["repository_digest"].rsplit("@", 1)[1], digest}
            or inspected["Os"] != "linux" or inspected["Architecture"] != architecture):
        raise ValueError("PostgreSQL backup tools image differs from authenticated release")
    TOOLS_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
    root_info = TOOLS_ROOT.stat()
    if (TOOLS_ROOT.resolve() != TOOLS_ROOT or root_info.st_uid != 0
            or root_info.st_gid != 0 or root_info.st_mode & 0o6022):
        raise ValueError("PostgreSQL backup tools root is unsafe")
    destination = TOOLS_ROOT / digest.removeprefix("sha256:")
    if destination.exists() or destination.is_symlink():
        verify(destination, native["manifest"])
        return destination
    staging_root = Path("/root/.groundplane/.tmp")
    staging_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    staging_info = staging_root.stat()
    if (staging_root.resolve() != staging_root or staging_info.st_uid != 0
            or staging_info.st_gid != 0 or staging_info.st_mode & 0o6022):
        raise ValueError("PostgreSQL backup tools staging root is unsafe")
    staging = Path(tempfile.mkdtemp(prefix="postgres16-tools-", dir=staging_root))
    container = ""
    try:
        container = subprocess.check_output(
            ["docker", "create", "--network", "none", "--entrypoint", "/not-started", reference],
            text=True, timeout=30).strip()
        if not re.fullmatch(r"[0-9a-f]{64}", container):
            raise ValueError("backup tools extraction container identity is invalid")
        subprocess.run(["docker", "cp", f"{container}:{CONTAINER_PATH}/.", str(staging)], check=True, timeout=120)
        verify(staging, native["manifest"])
        staging.chmod(0o755)
        staging.rename(destination)
    finally:
        try:
            if re.fullmatch(r"[0-9a-f]{64}", container):
                subprocess.run(["docker", "rm", container], check=True, timeout=30)
        finally:
            if staging.exists():
                shutil.rmtree(staging)
    return destination
