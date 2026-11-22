#!/usr/bin/env python3
"""Package the built Console as one deterministic, independently selectable release."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import subprocess
import tarfile


ROOT = Path(__file__).resolve().parents[1]
SEMVER = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")
COMMIT = re.compile(r"[0-9a-f]{40}")
MAX_FILE = 16 << 20
MAX_TOTAL = 64 << 20


def canonical(value: dict) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def console_files(directory: Path) -> list[tuple[str, bytes]]:
    if not directory.is_dir() or directory.is_symlink():
        raise ValueError("Console dist directory is unavailable")
    files = []
    total = 0
    for source in sorted(directory.rglob("*")):
        if source.is_dir() and not source.is_symlink():
            continue
        if source.is_symlink() or not source.is_file():
            raise ValueError("Console dist contains an unsafe member")
        name = source.relative_to(directory).as_posix()
        if any(part.startswith((".", "_")) for part in Path(name).parts):
            raise ValueError("Console dist contains a reserved path")
        value = source.read_bytes()
        if not value or len(value) > MAX_FILE:
            raise ValueError("Console dist contains an empty or oversized file")
        total += len(value)
        if total > MAX_TOTAL:
            raise ValueError("Console dist exceeds the release size bound")
        files.append((name, value))
    if not any(name == "index.html" for name, _ in files):
        raise ValueError("Console dist has no index.html")
    return files


def write_archive(destination: Path, manifest: bytes, files: list[tuple[str, bytes]]) -> None:
    with destination.open("xb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode="w") as archive:
                for name, value in [("console.json", manifest), *files]:
                    member = tarfile.TarInfo(name)
                    member.size, member.mtime, member.mode = len(value), 0, 0o600
                    archive.addfile(member, io.BytesIO(value))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--dist", type=Path, default=ROOT / "console" / "dist")
    parser.add_argument("--openapi", type=Path, default=ROOT / "openapi.json")
    parser.add_argument("--output", default=".tmp/releases")
    args = parser.parse_args()
    if not SEMVER.fullmatch(args.version):
        parser.error("version must be X.Y.Z")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    timestamp = int(subprocess.check_output(
        ["git", "show", "-s", "--format=%ct", "HEAD"], cwd=ROOT, text=True).strip())
    if not COMMIT.fullmatch(commit):
        raise ValueError("release source commit is invalid")
    api = args.openapi.read_bytes()
    if not api or len(api) > MAX_FILE or args.openapi.is_symlink():
        raise ValueError("OpenAPI document is unavailable or unsafe")
    files = console_files(args.dist)
    inventory = [{"path": name, "size": len(value),
                  "sha256": "sha256:" + hashlib.sha256(value).hexdigest()} for name, value in files]
    manifest = canonical({
        "api_sha256": "sha256:" + hashlib.sha256(api).hexdigest(),
        "created_at": datetime.fromtimestamp(timestamp, timezone.utc).isoformat().replace("+00:00", "Z"),
        "files": inventory,
        "release": f"console/v{args.version}",
        "schema": 1,
        "source_commit": commit,
    })
    output = subprocess.check_output(
        ["bash", "-c", 'source scripts/repo-env.sh; repo_temp_dir "$1"', "--", args.output],
        cwd=ROOT, text=True).strip()
    destination = Path(output) / f"groundplane-console-{args.version}.tar.gz"
    checksum = destination.with_suffix(destination.suffix + ".sha256")
    if destination.exists() or checksum.exists() or destination.is_symlink() or checksum.is_symlink():
        raise ValueError("Console release output already exists")
    write_archive(destination, manifest, files)
    checksum.write_text(f"{hashlib.sha256(destination.read_bytes()).hexdigest()}  {destination.name}\n")
    print(f"Bundle: {destination}\nSHA256: {hashlib.sha256(destination.read_bytes()).hexdigest()}")


if __name__ == "__main__":
    main()
