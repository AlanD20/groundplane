#!/usr/bin/env python3
"""Verified-bundle host orchestration; activation stays in the existing updater."""
from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess

from controller_bootstrap import GUARD, UNIT, Layout
from controller_release import RELEASE_ROOT, build_manifest


def validate(bundle: Path, version: str, arch: str) -> dict:
    manifest = json.loads((bundle / "bundle.json").read_text())
    if (manifest.get("schema") != 1 or manifest.get("version") != version
            or manifest.get("os") != "linux" or manifest.get("arch") != arch):
        raise ValueError("bundle version/platform does not match requested installation")
    for name, digest in manifest["files"].items():
        path = bundle / name
        if Path(name).name != name or path.is_symlink() or not path.is_file():
            raise ValueError("unsafe bundle member")
        if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            raise ValueError(f"bundle member checksum mismatch: {name}")
    for role in ("agent_image", "runner_image"):
        if not re.fullmatch(r"[a-z0-9][a-z0-9._:/-]*@sha256:[0-9a-f]{64}", manifest[role]):
            raise ValueError(f"{role} must be a registry digest")
    metadata = build_manifest((bundle / "controller-release.json").read_bytes(), manifest["agent_image"])
    if (metadata["controller_version"] != version or metadata["controller_sha256"] !=
            "sha256:" + hashlib.sha256((bundle / "controller").read_bytes()).hexdigest()):
        raise ValueError("Controller does not match its release descriptor")
    return manifest


def install(bundle: Path, args, manifest: dict, layout: Layout) -> None:
    mode = layout.mode()  # Fail on incomplete recovery authority before any host setup.
    if mode != "native":
        if args.stage_only:
            raise ValueError("--stage-only requires an existing native installation")
        if Path("/usr/local/libexec/groundplane/controller").exists():
            raise ValueError("legacy installation needs the documented explicit maintenance bootstrap")
    elif args.config:
        raise ValueError("--config is initial-install only; normal updates preserve configuration")
    if shutil.disk_usage(bundle).free < 2 * 1024**3:
        raise ValueError("installation requires at least 2 GiB free; nothing was deleted")
    if args.config:
        configuration = Path(args.config)
        if not configuration.is_file() or configuration.is_symlink():
            raise ValueError("--config must name a regular startup YAML file")
        (bundle / "controller.yaml.example").write_bytes(configuration.read_bytes())
    if mode != "native":
        subprocess.run(["sh", str(bundle / "setup-host.sh")], check=True)
    # Native updates do not pull or replace Runner, CLI, config, keys or etcd.
    roles = ("agent_image",) if mode == "native" else ("agent_image", "runner_image")
    for role in roles:
        subprocess.run(["docker", "pull", manifest[role]], check=True)
    os.execvp("sh", ["sh", str(bundle / "install-runtime.sh"), str(bundle), args.version,
                     manifest["agent_image"], manifest["runner_image"], args.listen_ip,
                     "1" if args.stage_only else "0", "0"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--listen-ip", default="127.0.0.1")
    parser.add_argument("--stage-only", action="store_true")
    parser.add_argument("--config")
    args = parser.parse_args()
    address = ipaddress.ip_address(args.listen_ip)
    if address.is_unspecified or address.is_multicast or not (address.is_private or address.is_loopback):
        parser.error("listener must be an explicit trusted private or loopback IP")
    if address.version != 4:
        parser.error("this installer currently accepts an IPv4 listener only")
    if os.geteuid() != 0:
        parser.error("installation requires root")
    arch = {"x86_64": "amd64", "aarch64": "arm64", "arm64": "arm64"}.get(platform.machine())
    bundle = Path(__file__).resolve().parent
    manifest = validate(bundle, args.version, arch)
    install(bundle, args, manifest, Layout(RELEASE_ROOT, GUARD, UNIT, 0))


if __name__ == "__main__":
    main()
