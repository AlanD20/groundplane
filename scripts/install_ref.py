#!/usr/bin/env python3
"""Build an exact source checkout in an invocation-owned Docker builder, then install."""
from __future__ import annotations

import argparse
import ipaddress
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess

import deploy
from controller_bootstrap import GUARD, UNIT, Layout
from controller_release import RELEASE_ROOT

CLIENT_IMAGE = "docker:29.1.3-cli@sha256:4fa0ee1f3a7e4354c4ea34558b6d4ee32859baf4973d4c8ccc8e7fe3dd730c04"
BUILDKIT_IMAGE = "moby/buildkit:buildx-stable-1"
DOCKER_SOCKET = Path("/var/run/docker.sock")


def run(*args: str, **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(args, check=True, timeout=1800, **kwargs)


def present(image: str) -> bool:
    return subprocess.run(["docker", "image", "inspect", image], capture_output=True,
                          timeout=30).returncode == 0


def build(source: Path, output: Path, version: str, identity: str,
          *, include_runner: bool = True) -> tuple[str, str]:
    """Only this builder owns its cache; never prune the host's shared cache."""
    builder = f"groundplane-ref-{identity}"
    client = f"{builder}-client"
    tags = tuple(f"groundplane-{kind}:ref-{identity}" for kind in ("agent", "runner"))
    existing = {image: present(image) for image in (CLIENT_IMAGE, BUILDKIT_IMAGE)}
    socket_group = DOCKER_SOCKET.stat().st_gid
    # Both source and exported binaries are private invocation-owned directories.
    command = ["docker", "run", "--rm", "--name", client,
               "--user", f"{os.getuid()}:{os.getgid()}", "--group-add", str(socket_group),
               "--mount", "type=bind,source=/var/run/docker.sock,target=/var/run/docker.sock",
               "--mount", f"type=bind,source={source},target={source}",
               "--mount", f"type=bind,source={output},target={output}",
               "--workdir", str(source), "--env", f"BUILDX_CONFIG={source}/.tmp/buildx-{identity}",
               "--env", f"DOCKER_CONFIG={source}/.tmp/docker-{identity}",
               "--entrypoint", "docker", CLIENT_IMAGE, "buildx"]
    created = False
    try:
        run(*command, "create", "--name", builder, "--driver", "docker-container",
            "--driver-opt", f"image={BUILDKIT_IMAGE},restart-policy=no")
        created = True
        common = [*command, "build", "--builder", builder, "--provenance=false"]
        run(*common, "--file", "Dockerfile.build", "--build-arg", f"VERSION={version}",
            "--output", f"type=local,dest={output}", ".", cwd=source)
        for kind, tag in zip(("agent", "runner"), tags):
            if kind == "runner" and not include_runner:
                continue
            argument = (f"AGENT_VERSION={version}" if kind == "agent" else
                        f"RUNNER_VERSION={(source / '.runner-version').read_text().strip()}")
            run(*common, "--file", f"Dockerfile.{kind}", "--build-arg", argument,
                "--tag", tag, "--load", ".", cwd=source)
    finally:
        # An interrupted client may leave its container running despite --rm.
        subprocess.run(["docker", "container", "rm", "--force", client],
                       capture_output=True, timeout=30)
        try:
            if created:
                run(*command, "rm", "--force", builder)
        finally:
            for image, existed in existing.items():
                if not existed and present(image):
                    # Never force removal of a shared or now-in-use image.
                    run("docker", "image", "rm", image)
            for prefix in ("buildx", "docker"):
                configuration = source / ".tmp" / f"{prefix}-{identity}"
                if configuration.exists():
                    shutil.rmtree(configuration)
    return tags


def install(source: Path, output: Path, version: str, identity: str,
            listen_ip: str, stage_only: bool, config: str | None) -> None:
    mode = Layout(RELEASE_ROOT, GUARD, UNIT, 0).mode()
    if mode != "native":
        if stage_only:
            raise ValueError("--stage-only requires an existing native installation")
        if Path("/usr/local/libexec/groundplane/controller").exists():
            raise ValueError("legacy installation requires explicit maintenance bootstrap")
    elif config:
        raise ValueError("--config is initial-install only")
    if config and (not Path(config).is_file() or Path(config).is_symlink()):
        raise ValueError("--config must be a regular startup YAML file")
    deploy.require_capacity(source)
    tags = tuple(f"groundplane-{kind}:ref-{identity}" for kind in ("agent", "runner"))
    started_install = False
    try:
        build(source, output, version, identity, include_runner=mode != "native")
        for path in (deploy.RELEASE_STAGER, deploy.BOOTSTRAP_HELPER, deploy.UPDATE_CLIENT,
                     deploy.CONTROLLER_UNIT, deploy.TMPFILES, deploy.CONFIG_EXAMPLE):
            shutil.copyfile(path, output / path.name)
        if config:
            shutil.copyfile(config, output / deploy.CONFIG_EXAMPLE.name)
        (output / "setup-host.sh").write_text(deploy.REMOTE_SETUP)
        (output / "install-runtime.sh").write_text(deploy.REMOTE_INSTALL)
        if mode != "native":
            run("sh", str(output / "setup-host.sh"))
        started_install = True
        run("sh", str(output / "install-runtime.sh"), str(output), version, *tags,
            listen_ip, "1" if stage_only else "0", "0")
    finally:
        # Unique temporary tags only. Registry-pinned installed/recovery images remain.
        for tag in tags:
            if present(tag):
                run("docker", "image", "rm", tag)
        if not started_install and output.exists():
            shutil.rmtree(output)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deploy-dir", type=Path, required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--listen-ip", default="127.0.0.1")
    parser.add_argument("--stage-only", action="store_true")
    parser.add_argument("--config")
    args = parser.parse_args()
    address = ipaddress.ip_address(args.listen_ip)
    if (address.version != 4 or address.is_unspecified or address.is_multicast
            or not (address.is_private or address.is_loopback)):
        parser.error("listener must be an explicit trusted private or loopback IPv4 address")
    if os.geteuid() != 0 or not re.fullmatch(r"[0-9a-f]{40}", args.commit):
        parser.error("root and an exact commit are required")
    output = args.deploy_dir
    if (output.parent != Path(deploy.REMOTE_DEPLOY_ROOT) or output.is_symlink()
            or not re.fullmatch(r"groundplane-deploy-[0-9a-f]{32}", output.name)):
        parser.error("invalid deployment directory")
    source = Path(__file__).resolve().parents[1]
    version = (source / "VERSION").read_text().strip()
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
        parser.error("source VERSION must contain a stable numeric version")
    version += f"-ref.{args.commit}"
    identity = output.name.removeprefix("groundplane-deploy-")
    def interrupted(signum, frame):
        raise SystemExit(128 + signum)
    for sig in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
        signal.signal(sig, interrupted)
    install(source, output, version, identity, args.listen_ip, args.stage_only, args.config)


if __name__ == "__main__":
    main()
