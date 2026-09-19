#!/usr/bin/env python3
"""Build a native release archive; never publish it or contact a target host."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path
import platform
import re
import subprocess
import tarfile

import deploy
from controller_release import build_manifest

ROOT = Path(__file__).resolve().parents[1]
VERSION = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+(?:[-.][0-9A-Za-z.-]+)?")
IMAGE = re.compile(r"[a-z0-9][a-z0-9._:/-]*@sha256:[0-9a-f]{64}")
ARCHES = {"x86_64": "amd64", "aarch64": "arm64", "arm64": "arm64"}


def payload(version: str, agent: str, runner: str, arch: str) -> dict[str, bytes]:
    sources = [deploy.CONTROLLER, deploy.CONTROLLER_METADATA, deploy.CLI,
               deploy.RELEASE_STAGER, deploy.BOOTSTRAP_HELPER, deploy.UPDATE_CLIENT,
               deploy.CONTROLLER_UNIT, deploy.TMPFILES, deploy.CONFIG_EXAMPLE,
               ROOT / "scripts" / "install_bundle.py"]
    files = {source.name: source.read_bytes() for source in sources}
    metadata = build_manifest(files["controller-release.json"], agent)
    if metadata["controller_version"] != version:
        raise ValueError("Controller metadata version differs from requested release")
    if metadata["controller_sha256"] != "sha256:" + hashlib.sha256(files["controller"]).hexdigest():
        raise ValueError("Controller bytes differ from build metadata")
    files["install-runtime.sh"] = deploy.REMOTE_INSTALL.encode()
    files["setup-host.sh"] = deploy.REMOTE_SETUP.encode()
    manifest = {"schema": 1, "version": version, "os": "linux", "arch": arch,
                "agent_image": agent, "runner_image": runner,
                "files": {name: hashlib.sha256(data).hexdigest() for name, data in files.items()}}
    files["bundle.json"] = (json.dumps(manifest, sort_keys=True, indent=2) + "\n").encode()
    return files


def write_archive(destination: Path, files: dict[str, bytes]) -> None:
    # Stable archive headers; build once and distribute these exact bytes.
    with destination.open("xb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode="w") as archive:
                for name, data in sorted(files.items()):
                    member = tarfile.TarInfo(name)
                    member.size, member.mtime = len(data), 0
                    member.mode = 0o700 if name in ("controller", "groundplane") else 0o600
                    archive.addfile(member, io.BytesIO(data))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--agent-image", required=True, help="published repository@sha256:digest")
    parser.add_argument("--runner-image", required=True, help="published repository@sha256:digest")
    parser.add_argument("--output", default=".tmp/releases", help="new artifacts below repository .tmp")
    args = parser.parse_args()
    if not VERSION.fullmatch(args.version) or len(args.version) > 100:
        parser.error("version must be an explicit numeric release, not dev/latest")
    if any(not IMAGE.fullmatch(ref) for ref in (args.agent_image, args.runner_image)):
        parser.error("Agent and Runner images must be immutable registry references")
    if platform.system() != "Linux" or platform.machine() not in ARCHES:
        parser.error("build natively on Linux amd64 or arm64; emulation is not qualification")
    # Reuse the canonical path validator, including its symlink rejection.
    output = subprocess.check_output(
        ["bash", "-c", 'source scripts/repo-env.sh; repo_temp_dir "$1"', "--", args.output],
        cwd=ROOT, text=True).strip()
    destination = Path(output) / f"groundplane-{args.version}-linux-{ARCHES[platform.machine()]}.tar.gz"
    checksum_path = destination.with_suffix(destination.suffix + ".sha256")
    if any(path.exists() or path.is_symlink() for path in (destination, checksum_path)):
        parser.error(f"release already exists; choose a new version or output directory: {destination}")
    deploy.require_capacity(ROOT)
    environment = deploy.build_environment() | {
        "GOMAXPROCS": "2", "CGO_ENABLED": "0", "GOFLAGS": "-p=2 -trimpath",
        "GOOS": "linux", "GOARCH": ARCHES[platform.machine()], "GOAMD64": "v1", "GOARM64": "v8.0",
    }
    subprocess.run(["bash", "scripts/repo-env.sh", "make", "controller", "cli", f"VERSION={args.version}"],
                   cwd=ROOT, env=environment, check=True)
    files = payload(args.version, args.agent_image, args.runner_image, ARCHES[platform.machine()])
    write_archive(destination, files)
    checksum = hashlib.sha256(destination.read_bytes()).hexdigest()
    with checksum_path.open("x") as out:
        out.write(f"{checksum}  {destination.name}\n")
    print(f"Bundle: {destination}\nSHA256: {checksum}")
    print("Not published. Qualify these exact artifacts before uploading release assets.")


if __name__ == "__main__":
    main()
