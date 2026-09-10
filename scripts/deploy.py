#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import io
import ipaddress
import json
import os
import platform
import re
import secrets
import shlex
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
LOCAL_DEPLOY_ROOT = REPOSITORY_ROOT / ".tmp"
REMOTE_DEPLOY_ROOT = "/root/.groundplane/.tmp"
REMOTE_DEPLOY_PREFIX_PATH = f"{REMOTE_DEPLOY_ROOT}/groundplane-deploy-"
CONTROLLER = REPOSITORY_ROOT / "bin" / "controller"
CONTROLLER_METADATA = REPOSITORY_ROOT / "bin" / "controller-release.json"
RELEASE_STAGER = REPOSITORY_ROOT / "scripts" / "controller_release.py"
BOOTSTRAP_HELPER = REPOSITORY_ROOT / "scripts" / "controller_bootstrap.py"
UPDATE_CLIENT = REPOSITORY_ROOT / "scripts" / "controller_update.py"
CLI = REPOSITORY_ROOT / "bin" / "groundplane"
CONTROLLER_UNIT = REPOSITORY_ROOT / "release" / "systemd" / "groundplane-controller.service"
TMPFILES = REPOSITORY_ROOT / "packaging" / "tmpfiles.d" / "groundplane.conf"
CONFIG_EXAMPLE = REPOSITORY_ROOT / "config" / "controller.yaml.example"
REGISTRY_IMAGE = "registry@sha256:a3d8aaa63ed8681a604f1dea0aa03f100d5895b6a58ace528858a7b332415373"
VERSION_PATTERN = re.compile(
    r"(?:dev|[0-9]+\.[0-9]+\.[0-9]+(?:[-.][0-9A-Za-z.-]+)?)",
)

REMOTE_SETUP = rf"""
set -eu

if test "$(id -u)" -ne 0; then
    echo "target provisioning requires root" >&2
    exit 1
fi

. /etc/os-release
if test "${{ID:-}}" != ubuntu || test "${{VERSION_ID:-}}" != 24.04; then
    echo "--setup supports only Ubuntu 24.04" >&2
    exit 1
fi

docker_fresh=0
if command -v docker >/dev/null &&
    docker compose version >/dev/null 2>&1 &&
    systemctl cat docker.service >/dev/null 2>&1; then
    :
elif ! command -v docker >/dev/null &&
    ! systemctl cat docker.service >/dev/null 2>&1 &&
    ! dpkg-query -W -f='${{Status}}' docker.io docker-ce docker-ce-cli \
        docker-compose docker-compose-v2 docker-compose-plugin 2>/dev/null |
        grep -q '^install ok installed$'; then
    docker_fresh=1
else
    echo "existing Docker installation is incomplete or incompatible; refusing package replacement" >&2
    exit 1
fi

packages=""
if ! command -v curl >/dev/null ||
    ! dpkg-query -W -f='${{Status}}' ca-certificates 2>/dev/null |
        grep -q '^install ok installed$'; then
    packages="$packages ca-certificates curl"
fi
if ! command -v age-keygen >/dev/null; then
    packages="$packages age"
fi
if ! command -v python3 >/dev/null; then
    packages="$packages python3"
fi
if ! command -v rootlesskit >/dev/null ||
    ! command -v newuidmap >/dev/null ||
    ! command -v slirp4netns >/dev/null ||
    ! command -v fuse-overlayfs >/dev/null ||
    ! command -v socat >/dev/null ||
    ! command -v nft >/dev/null; then
    packages="$packages rootlesskit uidmap slirp4netns fuse-overlayfs socat nftables"
fi
if test "$docker_fresh" -eq 1; then
    packages="$packages docker.io docker-compose-v2"
fi
if test -n "$packages"; then
    export DEBIAN_FRONTEND=noninteractive
    apt-get update
    apt-get install -y $packages
fi

systemctl reset-failed docker.service || true
systemctl enable docker.service >/dev/null
if ! systemctl is-active --quiet docker.service; then
    systemctl start docker.service
fi
docker version >/dev/null
docker compose version >/dev/null
registry_container_exists=0
if docker container inspect groundplane-registry >/dev/null 2>&1; then
    registry_container_exists=1
    registry_container_image=$(docker container inspect \
        --format '{{{{.Config.Image}}}}' groundplane-registry)
    if test "$registry_container_image" != "{REGISTRY_IMAGE}"; then
        echo "groundplane-registry exists with an unexpected image; refusing replacement" >&2
        exit 1
    fi
fi
if ! curl -fsS http://127.0.0.1:5000/v2/ >/dev/null 2>&1; then
    if test "$registry_container_exists" -eq 1; then
        if ! docker container inspect --format '{{{{.State.Running}}}}' groundplane-registry |
            grep -qx true; then
            docker start groundplane-registry >/dev/null
        fi
    else
        docker run \
            --detach \
            --restart unless-stopped \
            --name groundplane-registry \
            --publish 127.0.0.1:5000:5000 \
            {REGISTRY_IMAGE} >/dev/null
    fi

    ready=0
    attempt=0
    while test "$attempt" -lt 30; do
        if curl -fsS http://127.0.0.1:5000/v2/ >/dev/null 2>&1; then
            ready=1
            break
        fi
        attempt=$((attempt + 1))
        sleep 1
    done
    if test "$ready" -ne 1; then
        echo "loopback OCI registry did not become ready" >&2
        exit 1
    fi
fi

printf 'Docker: '
docker --version
printf 'Compose: '
docker compose version
"""

REMOTE_DEPLOY_GUARD = f"""
deploy_id=${{deploy_dir#{REMOTE_DEPLOY_PREFIX_PATH}}}
if test "$deploy_dir" != "{REMOTE_DEPLOY_PREFIX_PATH}$deploy_id" ||
    test "${{#deploy_id}}" -ne 32; then
    echo "refusing unsafe deployment directory: $deploy_dir" >&2
    exit 1
fi
case "$deploy_id" in
    *[!0-9a-f]*)
        echo "refusing unsafe deployment directory: $deploy_dir" >&2
        exit 1
        ;;
esac
"""

REMOTE_DEPLOY_PREFIX = r"""
set -eu

setup=$1
deploy_dir=$2
shift

""" + REMOTE_DEPLOY_GUARD + r"""

preinstall_finish() {
    status=$?
    trap - EXIT HUP INT TERM
    if test "$status" -ne 0; then
        rm -rf -- "$deploy_dir"
    fi
    exit "$status"
}
trap preinstall_finish EXIT HUP INT TERM

if test "$setup" -eq 1; then
"""

REMOTE_DEPLOY_MIDDLE = r"""
fi

trap - EXIT HUP INT TERM
"""

REMOTE_RECEIVE = r"""
set -eu

setup=$1
deploy_dir=$2
version=$3
source_agent_image=$4
source_runner_image=$5
listen_ip=$6
stage_only=$7
bootstrap=$8

""" + REMOTE_DEPLOY_GUARD + r"""

command -v flock >/dev/null
command -v tar >/dev/null
exec 9>/run/lock/groundplane-deploy.lock
if ! flock -n 9; then
    echo "another Groundplane deployment is active" >&2
    exit 1
fi

receive_finish() {
    status=$?
    trap - EXIT HUP INT TERM
    rm -rf -- "$deploy_dir"
    exit "$status"
}

umask 077
private_root=/root/.groundplane
private_parent=$private_root/.tmp
if test -L "$private_root"; then
    echo "refusing unsafe private deployment parent" >&2
    exit 1
fi
if test -e "$private_root" && ! test -d "$private_root"; then
    echo "refusing unsafe private deployment parent" >&2
    exit 1
fi
if test -L "$private_parent"; then
    echo "refusing unsafe private deployment parent" >&2
    exit 1
fi
if test -e "$private_parent" && ! test -d "$private_parent"; then
    echo "refusing unsafe private deployment parent" >&2
    exit 1
fi
install -d -m 0700 -o root -g root "$private_root" "$private_parent"
if test "$(stat -c '%u:%a' "$private_parent")" != "0:700"; then
    echo "private deployment parent must be root-only" >&2
    exit 1
fi
if test -e "$deploy_dir"; then
    echo "refusing to reuse deployment directory: $deploy_dir" >&2
    exit 1
fi
if test -L "$deploy_dir"; then
    echo "refusing to reuse deployment directory: $deploy_dir" >&2
    exit 1
fi
mkdir -- "$deploy_dir"
trap receive_finish EXIT HUP INT TERM
tar -xf - -C "$deploy_dir"
trap - EXIT HUP INT TERM
exec sh "$deploy_dir/remote-deploy.sh" \
    "$setup" "$deploy_dir" "$version" "$source_agent_image" "$source_runner_image" \
    "$listen_ip" "$stage_only" "$bootstrap"
"""

REMOTE_INSTALL = r"""
set -eu

deploy_dir=$1
version=$2
source_agent_image=$3
source_runner_image=$4
listen_ip=$5
stage_only=$6
bootstrap=$7

""" + REMOTE_DEPLOY_GUARD + (REPOSITORY_ROOT / "scripts" / "deployment" / "bootstrap.sh").read_text(encoding="utf-8") + \
    (REPOSITORY_ROOT / "scripts" / "deployment" / "agent.sh").read_text(encoding="utf-8")


@dataclass(frozen=True)
class Deployment:
    key: Path
    ip: ipaddress.IPv4Address | ipaddress.IPv6Address
    version: str
    setup: bool
    expose_port: int | None
    known_hosts: Path | None
    stage_only: bool
    bootstrap: bool

    @property
    def ssh_target(self) -> str:
        return f"root@{self.ip.compressed}"

    @property
    def ssh_base(self) -> list[str]:
        return [
            "ssh",
            "-i",
            str(self.key),
            *self.ssh_options,
            self.ssh_target,
        ]

    @property
    def ssh_options(self) -> list[str]:
        options = [
            "-F",
            "/dev/null",
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=yes",
            "-o",
            "ConnectTimeout=10",
            "-o",
            "ConnectionAttempts=1",
        ]
        if self.known_hosts is not None:
            options.extend(
                (
                    "-o",
                    f"UserKnownHostsFile={self.known_hosts}",
                    "-o",
                    "GlobalKnownHostsFile=/dev/null",
                ),
            )
        return options


def parse_arguments() -> Deployment:
    parser = argparse.ArgumentParser(
        description="Build and deploy Groundplane to an existing Ubuntu 24 host.",
    )
    parser.add_argument("--key", required=True, type=Path, help="SSH private key for root")
    parser.add_argument(
        "--ip",
        required=True,
        help="target host IP address and explicit Controller LAN listener",
    )
    parser.add_argument(
        "--setup",
        action="store_true",
        help="install Docker Engine, Buildx, Compose, and the loopback OCI registry",
    )
    parser.add_argument(
        "--expose",
        type=int,
        metavar="PORT",
        help="forward local PORT to the Controller and remain attached",
    )
    parser.add_argument(
        "--version",
        default="dev",
        help="Controller and Agent release version; defaults to development identity 'dev'",
    )
    parser.add_argument("--stage-only", action="store_true", help="stage a release on a guarded host without activating it")
    parser.add_argument("--bootstrap", action="store_true", help="explicit first recovery-guard installation on an idle legacy host")
    parser.add_argument(
        "--known-hosts",
        type=Path,
        help="use this pre-populated SSH known-hosts file exclusively",
    )
    arguments = parser.parse_args()

    key = arguments.key.expanduser().resolve()
    if not key.is_file():
        parser.error(f"SSH key does not exist: {key}")
    known_hosts = arguments.known_hosts
    if known_hosts is not None:
        known_hosts = known_hosts.expanduser().resolve()
        if not known_hosts.is_file():
            parser.error(f"known-hosts file does not exist: {known_hosts}")
        metadata = known_hosts.stat()
        if metadata.st_size == 0:
            parser.error(f"known-hosts file is empty: {known_hosts}")
        if metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
            parser.error(f"known-hosts file must not be group- or world-writable: {known_hosts}")
    try:
        address = ipaddress.ip_address(arguments.ip)
    except ValueError as error:
        parser.error(str(error))
    if arguments.expose is not None and not 1 <= arguments.expose <= 65535:
        parser.error("--expose must be between 1 and 65535")
    if VERSION_PATTERN.fullmatch(arguments.version) is None:
        parser.error("--version must be 'dev' or a semantic version")
    if arguments.bootstrap and arguments.stage_only:
        parser.error("--bootstrap and --stage-only are mutually exclusive")

    return Deployment(
        key=key,
        ip=address,
        version=arguments.version,
        setup=arguments.setup,
        expose_port=arguments.expose,
        known_hosts=known_hosts,
        stage_only=arguments.stage_only,
        bootstrap=arguments.bootstrap,
    )


def command_text(command: list[str]) -> str:
    return shlex.join(command)


def ensure_private_directory(path: Path) -> None:
    try:
        path.mkdir(mode=0o700)
    except FileExistsError:
        pass

    try:
        metadata = path.lstat()
    except OSError as error:
        raise RuntimeError(f"could not inspect private temporary directory: {path}") from error
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise RuntimeError(f"private temporary path must be a directory: {path}")

    try:
        os.chmod(path, 0o700, follow_symlinks=False)
        metadata = path.lstat()
    except OSError as error:
        raise RuntimeError(f"could not secure private temporary directory: {path}") from error
    if stat.S_IMODE(metadata.st_mode) != 0o700:
        raise RuntimeError(f"private temporary directory is not mode 0700: {path}")


def run(
    command: list[str],
    *,
    cwd: Path | None = None,
    input_text: str | None = None,
    capture: bool = False,
    environment: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    print(f"+ {command_text(command)}", flush=True)
    return subprocess.run(
        command,
        cwd=cwd,
        check=True,
        text=True,
        input=input_text,
        capture_output=capture,
        env=environment,
    )


def require_local_tools() -> None:
    missing = [name for name in ("make", "docker", "ssh") if shutil.which(name) is None]
    if missing:
        raise RuntimeError(f"missing required local tools: {', '.join(missing)}")


def normalize_architecture(value: str) -> str:
    architecture = value.strip().lower()
    aliases = {
        "x86_64": "amd64",
        "amd64": "amd64",
        "aarch64": "arm64",
        "arm64": "arm64",
    }
    if architecture not in aliases:
        raise RuntimeError(f"unsupported architecture: {architecture}")
    return aliases[architecture]


def verify_architecture(deployment: Deployment) -> None:
    local_arch = normalize_architecture(platform.machine())
    result = run([*deployment.ssh_base, "uname", "-m"], capture=True)
    remote_arch = normalize_architecture(result.stdout)
    if local_arch != remote_arch:
        raise RuntimeError(
            f"local architecture {local_arch} does not match target architecture {remote_arch}",
        )


def build_environment() -> dict[str, str]:
    node_version = (REPOSITORY_ROOT / ".node-version").read_text(encoding="utf-8").strip()
    package = json.loads((REPOSITORY_ROOT / "console" / "package.json").read_text(encoding="utf-8"))
    package_manager = package.get("packageManager", "")
    expected_manager = f"npm@"
    if not package_manager.startswith(expected_manager):
        raise RuntimeError("console packageManager must pin npm")
    npm_version = package_manager.removeprefix(expected_manager)

    candidates: list[Path] = []
    active_node = shutil.which("node")
    if active_node:
        candidates.append(Path(active_node).resolve().parent)

    nvm_roots = [
        Path(os.environ["NVM_DIR"]).expanduser() if os.environ.get("NVM_DIR") else None,
        Path.home() / ".nvm",
        Path.home() / ".local" / "share" / "nvm",
    ]
    for root in nvm_roots:
        if root is not None:
            candidates.append(root / "versions" / "node" / f"v{node_version}" / "bin")

    seen: set[Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        if not (candidate / "node").is_file() or not (candidate / "npm").is_file():
            continue

        environment = os.environ.copy()
        environment["PATH"] = os.pathsep.join((str(candidate), environment.get("PATH", "")))
        node_result = subprocess.run(
            ["node", "--version"],
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )
        npm_result = subprocess.run(
            ["npm", "--version"],
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )
        if node_result.stdout.strip() == f"v{node_version}" and npm_result.stdout.strip() == npm_version:
            print(f"Using Node {node_version} and npm {npm_version} from {candidate}", flush=True)
            return environment

    raise RuntimeError(
        "pinned Console toolchain is unavailable; run "
        f"`nvm install {node_version} && nvm use {node_version} && npm install -g npm@{npm_version}`",
    )


def image_input_digest(kind: str, version: str) -> str:
    if kind == "agent":
        fixed_paths = [
            REPOSITORY_ROOT / ".dockerignore",
            REPOSITORY_ROOT / "Dockerfile.agent",
            REPOSITORY_ROOT / "Makefile",
            REPOSITORY_ROOT / "go.mod",
            REPOSITORY_ROOT / "go.sum",
            REPOSITORY_ROOT / "go.work",
            REPOSITORY_ROOT / "go.work.sum",
            REPOSITORY_ROOT / "component-sdk" / "go.mod",
            REPOSITORY_ROOT / "component-sdk" / "go.sum",
            REPOSITORY_ROOT / "registered-components" / "go.mod",
            REPOSITORY_ROOT / "registered-components" / "go.sum",
        ]
        source_paths = []
        for source_root in (
            "cmd",
            "component-sdk",
            "internal",
            "pkg",
            "proto",
            "registered-components",
        ):
            root = REPOSITORY_ROOT / source_root
            source_paths.extend(root.rglob("*.go"))
            source_paths.extend(root.rglob("*.proto"))
    elif kind == "runner":
        fixed_paths = [
            REPOSITORY_ROOT / ".dockerignore",
            REPOSITORY_ROOT / "Dockerfile.runner",
            REPOSITORY_ROOT / "Makefile",
            REPOSITORY_ROOT / ".runner-version",
            REPOSITORY_ROOT / "release" / "runner" / "entrypoint.sh",
        ]
        source_paths = []
    else:
        raise ValueError(f"unsupported image kind: {kind}")

    digest = hashlib.sha256()
    digest.update(f"{kind}\0{version}\0".encode("utf-8"))
    paths = {path for path in (*fixed_paths, *source_paths) if path.is_file()}
    for path in sorted(paths, key=lambda item: str(item.relative_to(REPOSITORY_ROOT))):
        relative = path.relative_to(REPOSITORY_ROOT).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def local_image_id(image: str) -> str | None:
    result = subprocess.run(
        ["docker", "image", "inspect", "--format", "{{.Id}}", image],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    image_id = result.stdout.strip()
    return image_id or None


def build_artifacts(deployment: Deployment, invocation_id: str) -> tuple[str, str]:
    source_agent_image = f"groundplane-agent:deploy-{invocation_id}"
    source_runner_image = f"groundplane-runner:deploy-{invocation_id}"
    agent_cache_image = f"groundplane-agent:cache-{image_input_digest('agent', deployment.version)}"
    runner_cache_image = f"groundplane-runner:cache-{image_input_digest('runner', deployment.version)}"
    run(
        ["make", "controller", "cli", f"VERSION={deployment.version}"],
        cwd=REPOSITORY_ROOT,
        environment=build_environment(),
    )
    if local_image_id(agent_cache_image) is None:
        run(
            [
                "make",
                "agent-image",
                f"AGENT_VERSION={deployment.version}",
                f"AGENT_IMAGE={agent_cache_image}",
            ],
            cwd=REPOSITORY_ROOT,
        )
    else:
        print(f"Reusing cached Agent image: {agent_cache_image}", flush=True)
    if local_image_id(runner_cache_image) is None:
        run(
            [
                "make",
                "runner-image",
                f"RUNNER_IMAGE={runner_cache_image}",
            ],
            cwd=REPOSITORY_ROOT,
        )
    else:
        print(f"Reusing cached Runner image: {runner_cache_image}", flush=True)
    run(["docker", "tag", agent_cache_image, source_agent_image])
    run(["docker", "tag", runner_cache_image, source_runner_image])
    return source_agent_image, source_runner_image


def create_transfer_archive(transfer_archive: Path) -> None:
    sources = [
        CONTROLLER,
        CONTROLLER_METADATA,
        RELEASE_STAGER,
        BOOTSTRAP_HELPER,
        UPDATE_CLIENT,
        CLI,
        CONTROLLER_UNIT,
        TMPFILES,
        CONFIG_EXAMPLE,
    ]
    remote_script = (REMOTE_DEPLOY_PREFIX + REMOTE_SETUP + REMOTE_DEPLOY_MIDDLE + REMOTE_INSTALL).encode()
    with tarfile.open(transfer_archive, mode="w") as archive:
        for source in sources:
            metadata = archive.gettarinfo(str(source), arcname=source.name)
            metadata.uid = 0
            metadata.gid = 0
            metadata.uname = "root"
            metadata.gname = "root"
            with source.open("rb") as content:
                archive.addfile(metadata, content)
        metadata = tarfile.TarInfo("remote-deploy.sh")
        metadata.size = len(remote_script)
        metadata.mode = 0o600
        metadata.uid = 0
        metadata.gid = 0
        metadata.uname = "root"
        metadata.gname = "root"
        archive.addfile(metadata, io.BytesIO(remote_script))


def transfer_and_deploy(
    deployment: Deployment,
    remote_directory: str,
    source_agent_image: str,
    source_runner_image: str,
    transfer_archive: Path,
) -> None:
    remote_command = shlex.join(
        [
            "sh",
            "-c",
            REMOTE_RECEIVE,
            "--",
            "1" if deployment.setup else "0",
            remote_directory,
            deployment.version,
            source_agent_image,
            source_runner_image,
            deployment.ip.compressed,
            "1" if deployment.stage_only else "0",
            "1" if deployment.bootstrap else "0",
        ],
    )
    command = [*deployment.ssh_base, remote_command]
    print(f"+ {command_text([*deployment.ssh_base, '<deployment bundle>'])}", flush=True)
    with transfer_archive.open("rb") as content:
        subprocess.run(command, check=True, stdin=content)


def prepare_target(deployment: Deployment) -> None:
    if not deployment.setup:
        return
    run([*deployment.ssh_base, "sh", "-s"], input_text=REMOTE_SETUP)


def stream_images(
    deployment: Deployment,
    source_agent_image: str,
    source_runner_image: str,
) -> None:
    save_command = ["docker", "save", source_agent_image, source_runner_image]
    load_command = [*deployment.ssh_base, "docker", "load"]
    print(f"+ {command_text(save_command)} | {command_text(load_command)}", flush=True)
    with subprocess.Popen(save_command, stdout=subprocess.PIPE) as save:
        if save.stdout is None:
            raise RuntimeError("docker save did not expose its output stream")
        try:
            load = subprocess.run(load_command, check=False, stdin=save.stdout)
        finally:
            save.stdout.close()
        save_status = save.wait()
    if save_status != 0:
        raise subprocess.CalledProcessError(save_status, save_command)
    if load.returncode != 0:
        raise subprocess.CalledProcessError(load.returncode, load_command)


def cleanup_temporary_images(images: tuple[str, ...]) -> None:
    temporary_pattern = re.compile(r"^groundplane-(?:agent|runner):deploy-[0-9a-f]{32}$")
    for image in images:
        if temporary_pattern.fullmatch(image) is None:
            raise RuntimeError(f"refusing to remove non-temporary image: {image}")
        result = subprocess.run(
            ["docker", "image", "rm", image],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            print(f"Removed temporary image: {image}", flush=True)


def deploy(deployment: Deployment) -> None:
    require_local_tools()
    verify_architecture(deployment)
    prepare_target(deployment)

    invocation_id = secrets.token_hex(16)
    remote_directory = f"{REMOTE_DEPLOY_PREFIX_PATH}{invocation_id}"
    source_images = (
        f"groundplane-agent:deploy-{invocation_id}",
        f"groundplane-runner:deploy-{invocation_id}",
    )
    try:
        ensure_private_directory(LOCAL_DEPLOY_ROOT)
        with tempfile.TemporaryDirectory(
            prefix="groundplane-deploy-",
            dir=LOCAL_DEPLOY_ROOT,
        ) as temporary_directory:
            transfer_archive = Path(temporary_directory) / "deployment.tar"
            source_agent_image, source_runner_image = build_artifacts(
                deployment,
                invocation_id,
            )
            stream_images(deployment, source_agent_image, source_runner_image)
            create_transfer_archive(transfer_archive)
            transfer_and_deploy(
                Deployment(
                    key=deployment.key,
                    ip=deployment.ip,
                    version=deployment.version,
                    setup=False,
                    expose_port=deployment.expose_port,
                    known_hosts=deployment.known_hosts,
                    stage_only=deployment.stage_only,
                    bootstrap=deployment.bootstrap,
                ),
                remote_directory,
                source_agent_image,
                source_runner_image,
                transfer_archive,
            )
    finally:
        cleanup_temporary_images(source_images)


def console_tunnel_command(deployment: Deployment, local_port: int) -> list[str]:
    return [
        "ssh",
        "-i",
        str(deployment.key),
        *deployment.ssh_options,
        "-o",
        "ExitOnForwardFailure=yes",
        "-o",
        "ServerAliveInterval=30",
        "-o",
        "ServerAliveCountMax=3",
        "-N",
        "-L",
        f"127.0.0.1:{local_port}:127.0.0.1:8080",
        deployment.ssh_target,
    ]


def main() -> int:
    deployment = parse_arguments()
    try:
        deploy(deployment)
    except KeyboardInterrupt:
        print("\ndeployment interrupted", file=sys.stderr)
        return 130
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        if isinstance(error, subprocess.CalledProcessError):
            if error.stdout:
                print(error.stdout, file=sys.stderr, end="")
            if error.stderr:
                print(error.stderr, file=sys.stderr, end="")
        print(f"deployment failed: {error}", file=sys.stderr)
        return 1

    local_port = deployment.expose_port or 8080
    tunnel_command = console_tunnel_command(deployment, local_port)
    if deployment.expose_port is None:
        print(f"Console tunnel: {command_text(tunnel_command)}")
        return 0

    print(f"Console: http://127.0.0.1:{local_port}")
    try:
        run(tunnel_command)
    except KeyboardInterrupt:
        print("\nConsole tunnel closed.")
        return 130
    except (RuntimeError, subprocess.CalledProcessError) as error:
        print(f"console tunnel failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
