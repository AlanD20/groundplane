#!/usr/bin/env python3
"""Build an exact source checkout in an invocation-owned Docker builder, then install."""
from __future__ import annotations

import argparse
from datetime import datetime
import ipaddress
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess

import deploy
import install_agent
import release_selection
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
          *, source_epoch: int, include_runner: bool = True,
          include_agent: bool = True, include_controller: bool = True) -> tuple[str, str]:
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
        common = [*command, "build", "--builder", builder, "--provenance=false",
                  "--build-arg", f"SOURCE_DATE_EPOCH={source_epoch}"]
        if include_controller:
            run(*common, "--file", "Dockerfile.build", "--build-arg", f"VERSION={version}",
                "--output", f"type=local,dest={output}", ".", cwd=source)
        for kind, tag in zip(("agent", "runner"), tags):
            if kind == "runner" and not include_runner:
                continue
            if kind == "agent" and not include_agent:
                continue
            agent_version = "agent/" + version.split("/")[-1] if re.fullmatch(r"(?:(?:agent|controller)/)?v[0-9]+\.[0-9]+\.[0-9]+", version) else version
            argument = (f"AGENT_VERSION={agent_version}" if kind == "agent" else
                        f"RUNNER_VERSION={(source / '.runner-version').read_text().strip()}")
            run(*common, "--file", f"Dockerfile.{kind}", "--build-arg", argument,
                "--tag", tag, "--output", "type=docker,rewrite-timestamp=true", ".", cwd=source)
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
            listen_ip: str, stage_only: bool, config: str | None, source_epoch: int,
            scope: str = "both") -> None:
    mode = Layout(RELEASE_ROOT, GUARD, UNIT, 0).mode()
    if scope == "agent" and mode != "native":
        raise ValueError("Agent-only installation requires an existing native Controller")
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
        build(source, output, version, identity, source_epoch=source_epoch,
              include_runner=mode != "native" and scope != "agent",
              include_agent=scope != "controller", include_controller=scope != "agent")
        if scope == "agent":
            runtime_tag = f"localhost:5000/groundplane-agent:ref-{identity}"
            run("docker", "tag", tags[0], runtime_tag)
            run("docker", "push", runtime_tag)
            image = subprocess.check_output(["docker", "image", "inspect", runtime_tag,
                                            "--format", "{{index .RepoDigests 0}}"], text=True).strip()
            install_agent.install(image)
            return
        selected_agent = tags[0]
        missing_agent = False
        if scope == "controller":
            current = install_agent.installed_image() if mode == "native" else None
            missing_agent = mode == "native" and current is None
            selected_agent = current or release_selection.agent_image()
            run("docker", "pull", selected_agent)
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
        run("sh", str(output / "install-runtime.sh"), str(output), version.replace("/", "-"), selected_agent, tags[1],
            listen_ip, "1" if stage_only else "0", "0")
        if missing_agent and not stage_only:
            install_agent.install(selected_agent, allow_enroll=True)
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
    parser.add_argument("--ref-name", required=True)
    scope_flags = parser.add_mutually_exclusive_group()
    scope_flags.add_argument("--agent-only", action="store_true")
    scope_flags.add_argument("--controller-only", action="store_true")
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
    # The installer resolved this metadata before downloading the exact source.
    # Never substitute the current clock: repeated builds must keep image identity.
    metadata = json.loads((source.parent / "commit.json").read_text())
    if metadata.get("sha") != args.commit:
        parser.error("source commit metadata does not match the selected commit")
    timestamp = metadata["commit"]["committer"]["date"]
    if not isinstance(timestamp, str) or not re.fullmatch(
            r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z", timestamp):
        parser.error("source commit timestamp must be a UTC date")
    source_epoch = int(datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp())
    if source_epoch < 0:
        parser.error("source commit timestamp must not precede the Unix epoch")
    version = (source / "VERSION").read_text().strip()
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
        parser.error("source VERSION must contain a stable numeric version")
    version += f"-ref.{args.commit}"
    if re.fullmatch(r"(?:(?:agent|controller)/)?v[0-9]+\.[0-9]+\.[0-9]+", args.ref_name):
        version = ("agent/" if args.agent_only else "controller/") + args.ref_name.split("/")[-1]
    identity = output.name.removeprefix("groundplane-deploy-")
    def interrupted(signum, frame):
        raise SystemExit(128 + signum)
    for sig in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
        signal.signal(sig, interrupted)
    install(source, output, version, identity, args.listen_ip, args.stage_only, args.config,
            source_epoch, "agent" if args.agent_only else "controller" if args.controller_only else "both")


if __name__ == "__main__":
    main()
