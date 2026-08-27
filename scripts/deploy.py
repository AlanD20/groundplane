#!/usr/bin/env python3
from __future__ import annotations

import argparse
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
CONTROLLER = REPOSITORY_ROOT / "bin" / "controller"
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
if ! command -v etcd >/dev/null; then
    packages="$packages etcd-server"
fi
if test "$docker_fresh" -eq 1; then
    packages="$packages docker.io docker-compose-v2"
fi
if test -n "$packages"; then
    export DEBIAN_FRONTEND=noninteractive
    apt-get update
    apt-get install -y $packages
fi

systemctl daemon-reload
systemctl reset-failed docker.service || true
systemctl enable docker.service >/dev/null
systemctl start docker.service
docker version >/dev/null
docker compose version >/dev/null
systemctl enable --now etcd.service >/dev/null
systemctl is-active --quiet etcd.service

if ! curl -fsS http://127.0.0.1:5000/v2/ >/dev/null 2>&1; then
    if docker container inspect groundplane-registry >/dev/null 2>&1; then
        docker start groundplane-registry >/dev/null
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

REMOTE_DEPLOY_PREFIX = r"""
set -eu

setup=$1
deploy_dir=$2
shift

deploy_id=${deploy_dir#/tmp/groundplane-deploy-}
if test "$deploy_dir" != "/tmp/groundplane-deploy-$deploy_id" ||
    test "${#deploy_id}" -ne 32; then
    echo "refusing unsafe deployment directory: $deploy_dir" >&2
    exit 1
fi
case "$deploy_id" in
    *[!0-9a-f]*)
        echo "refusing unsafe deployment directory: $deploy_dir" >&2
        exit 1
        ;;
esac

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
source_image=$4

deploy_id=${deploy_dir#/tmp/groundplane-deploy-}
if test "$deploy_dir" != "/tmp/groundplane-deploy-$deploy_id" ||
    test "${#deploy_id}" -ne 32; then
    echo "refusing unsafe deployment directory: $deploy_dir" >&2
    exit 1
fi
case "$deploy_id" in
    *[!0-9a-f]*)
        echo "refusing unsafe deployment directory: $deploy_dir" >&2
        exit 1
        ;;
esac

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
trap receive_finish EXIT HUP INT TERM

install -d -m 0700 "$deploy_dir"
tar -xf - -C "$deploy_dir"
trap - EXIT HUP INT TERM
exec sh "$deploy_dir/remote-deploy.sh" \
    "$setup" "$deploy_dir" "$version" "$source_image"
"""

REMOTE_INSTALL = r"""
set -eu

deploy_dir=$1
version=$2
source_image=$3

deploy_id=${deploy_dir#/tmp/groundplane-deploy-}
if test "$deploy_dir" != "/tmp/groundplane-deploy-$deploy_id" ||
    test "${#deploy_id}" -ne 32; then
    echo "refusing unsafe deployment directory: $deploy_dir" >&2
    exit 1
fi
case "$deploy_id" in
    *[!0-9a-f]*)
        echo "refusing unsafe deployment directory: $deploy_dir" >&2
        exit 1
        ;;
esac

rollback=0
service_was_active=0
service_was_enabled=0
retain_recovery=0
unresolved_task_id=""
unresolved_task_state=unknown

backup_path() {
    source=$1
    name=$2
    if test -e "$source"; then
        cp -a "$source" "$deploy_dir/backup-$name"
        : > "$deploy_dir/had-$name"
    fi
}

restore_path() {
    destination=$1
    name=$2
    if test -e "$deploy_dir/had-$name"; then
        cp -a "$deploy_dir/backup-$name" "$destination"
    else
        rm -f -- "$destination"
    fi
}

finish() {
    status=$?
    trap - EXIT HUP INT TERM
    if test "$status" -ne 0 && test "$rollback" -eq 1; then
        if test "$retain_recovery" -eq 1; then
            if test -n "$unresolved_task_id"; then
                echo "Agent lifecycle Task $unresolved_task_id state: $unresolved_task_state" >&2
            else
                echo "Agent lifecycle state: $unresolved_task_state" >&2
            fi
            echo "rollback was not attempted; recovery files retained in $deploy_dir" >&2
            echo "wait for the Agent lifecycle to settle, then rerun this deployment" >&2
            exit "$status"
        fi
        echo "deployment failed; restoring the previous Controller installation" >&2
        rollback_failed=0
        if ! restore_path /usr/local/libexec/groundplane/controller controller; then
            rollback_failed=1
        fi
        if ! restore_path /usr/local/bin/groundplane cli; then
            rollback_failed=1
        fi
        if ! restore_path /etc/systemd/system/groundplane-controller.service controller-unit; then
            rollback_failed=1
        fi
        if ! restore_path /usr/lib/tmpfiles.d/groundplane.conf tmpfiles; then
            rollback_failed=1
        fi
        if ! restore_path /etc/groundplane/controller.yaml controller-config; then
            rollback_failed=1
        fi
        if ! restore_path /etc/groundplane/controller.age controller-age; then
            rollback_failed=1
        fi
        if ! systemctl daemon-reload; then
            rollback_failed=1
        fi
        if test "$service_was_enabled" -eq 1; then
            if ! systemctl enable groundplane-controller.service >/dev/null; then
                rollback_failed=1
            fi
        else
            systemctl disable groundplane-controller.service >/dev/null 2>&1 || true
        fi
        if test "$service_was_active" -eq 1; then
            if ! systemctl restart groundplane-controller.service; then
                rollback_failed=1
            fi
        else
            systemctl stop groundplane-controller.service >/dev/null 2>&1 || true
        fi
        if test "$rollback_failed" -ne 0; then
            echo "rollback incomplete; recovery files retained in $deploy_dir" >&2
            exit 1
        fi
    fi
    rm -rf -- "$deploy_dir"
    exit "$status"
}

trap finish EXIT HUP INT TERM

command -v docker >/dev/null
command -v etcd >/dev/null
command -v curl >/dev/null
command -v flock >/dev/null
systemctl is-active --quiet docker.service
systemctl is-active --quiet etcd.service
curl -fsS http://127.0.0.1:5000/v2/ >/dev/null

docker load --input "$deploy_dir/agent-image.tar"
target_image="localhost:5000/groundplane-agent:$version"
docker tag "$source_image" "$target_image"
push_output=$(docker push "$target_image")
printf '%s\n' "$push_output"
agent_digest=$(printf '%s\n' "$push_output" |
    sed -n 's/^.*digest: \(sha256:[0-9a-f]\{64\}\) size:.*$/\1/p' |
    tail -n 1)
if ! printf '%s\n' "$agent_digest" |
    grep -Eq '^sha256:[0-9a-f]{64}$'; then
    echo "target registry did not report the pushed Agent digest" >&2
    exit 1
fi
agent_ref="localhost:5000/groundplane-agent@$agent_digest"
docker pull "$agent_ref" >/dev/null

if systemctl is-active --quiet groundplane-controller.service; then
    service_was_active=1
fi
if systemctl is-enabled --quiet groundplane-controller.service; then
    service_was_enabled=1
fi

backup_path /usr/local/libexec/groundplane/controller controller
backup_path /usr/local/bin/groundplane cli
backup_path /etc/systemd/system/groundplane-controller.service controller-unit
backup_path /usr/lib/tmpfiles.d/groundplane.conf tmpfiles
backup_path /etc/groundplane/controller.yaml controller-config
backup_path /etc/groundplane/controller.age controller-age
rollback=1

install -d -m 0755 /usr/local/libexec/groundplane
install -d -m 0700 /etc/groundplane /var/log/groundplane
install -Dm755 "$deploy_dir/controller" /usr/local/libexec/groundplane/controller.new
mv -f /usr/local/libexec/groundplane/controller.new /usr/local/libexec/groundplane/controller
install -Dm755 "$deploy_dir/groundplane" /usr/local/bin/groundplane.new
mv -f /usr/local/bin/groundplane.new /usr/local/bin/groundplane
install -Dm644 "$deploy_dir/groundplane-controller.service" \
    /etc/systemd/system/groundplane-controller.service
install -Dm644 "$deploy_dir/groundplane.conf" /usr/lib/tmpfiles.d/groundplane.conf

if ! test -f /etc/groundplane/controller.yaml; then
    install -Dm600 "$deploy_dir/controller.yaml.example" /etc/groundplane/controller.yaml
fi

if test -e /etc/groundplane/controller.age; then
    if ! test -f /etc/groundplane/controller.age ||
        test -L /etc/groundplane/controller.age ||
        test "$(stat -c '%u:%a' /etc/groundplane/controller.age)" != "0:600"; then
        echo "/etc/groundplane/controller.age must be a root-owned regular file with mode 0600" >&2
        exit 1
    fi
else
    command -v age-keygen >/dev/null
    generated_key="$deploy_dir/controller.age.generated"
    rm -f -- "$generated_key"
    if ! age-keygen -o "$generated_key" >/dev/null 2>&1; then
        rm -f -- "$generated_key"
        echo "failed to generate the Controller age identity" >&2
        exit 1
    fi
    install -m 0600 -o root -g root "$generated_key" /etc/groundplane/controller.age
    rm -f -- "$generated_key"
fi

rendered_config="$deploy_dir/controller.yaml.rendered"
awk -v image="$agent_ref" '
BEGIN {
    in_agent = 0
    saw_agent = 0
    wrote_image = 0
}
$0 ~ /^agent:[[:space:]]*$/ {
    saw_agent = 1
    in_agent = 1
    print
    next
}
in_agent && $0 ~ /^[^[:space:]#]/ {
    if (!wrote_image) {
        print "  image: " image
        wrote_image = 1
    }
    in_agent = 0
}
in_agent && $0 ~ /^[[:space:]]+image:[[:space:]]*/ {
    print "  image: " image
    wrote_image = 1
    next
}
{
    print
}
END {
    if (in_agent && !wrote_image) {
        print "  image: " image
    } else if (!saw_agent) {
        print ""
        print "agent:"
        print "  image: " image
    }
}
' /etc/groundplane/controller.yaml > "$rendered_config"
install -m 0600 -o root -g root "$rendered_config" /etc/groundplane/controller.yaml

systemd-tmpfiles --create /usr/lib/tmpfiles.d/groundplane.conf
systemctl daemon-reload
systemctl enable groundplane-controller.service >/dev/null
systemctl restart groundplane-controller.service

healthy=0
attempt=0
while test "$attempt" -lt 30; do
    if curl -fsS http://127.0.0.1:8080/api/v1/host >/dev/null 2>&1; then
        healthy=1
        break
    fi
    attempt=$((attempt + 1))
    sleep 1
done
if test "$healthy" -ne 1; then
    journalctl -u groundplane-controller.service -n 100 --no-pager >&2 || true
    echo "Controller did not become healthy within 30 seconds" >&2
    exit 1
fi

load_task() {
    task_id=$1
    if ! task_output=$(/usr/local/bin/groundplane \
        --host http://127.0.0.1:8080 \
        --output json \
        task show "$task_id" 2>&1); then
        printf '%s\n' "$task_output" >&2
        return 1
    fi
    task_status=$(printf '%s\n' "$task_output" |
        sed -n 's/^[[:space:]]*"status":[[:space:]]*"\([^"]*\)".*$/\1/p')
    case "$task_status" in
        completed | pending | running | failed | aborted | timed_out)
            return 0
            ;;
        *)
            echo "Task $task_id returned unknown status: $task_status" >&2
            return 1
            ;;
    esac
}

settle_timed_out_agent_task() {
    task_id=$1
    if abort_output=$(/usr/local/bin/groundplane \
        --host http://127.0.0.1:8080 \
        --output json \
        task abort "$task_id" 2>&1); then
        printf '%s\n' "$abort_output"
    else
        printf '%s\n' "$abort_output" >&2
    fi

    attempt=0
    while test "$attempt" -lt 60; do
        if ! load_task "$task_id"; then
            retain_recovery=1
            unresolved_task_id=$task_id
            unresolved_task_state=unknown
            return 1
        fi
        unresolved_task_state=$task_status
        case "$task_status" in
            completed)
                rollback=0
                retain_recovery=0
                unresolved_task_id=""
                unresolved_task_state=""
                printf 'Agent task: %s completed while abort was requested\n' "$task_id"
                return 0
                ;;
            failed | aborted | timed_out)
                printf '%s\n' "$task_output" >&2
                return 1
                ;;
            pending | running)
                ;;
        esac
        attempt=$((attempt + 1))
        sleep 1
    done

    retain_recovery=1
    unresolved_task_id=$task_id
    unresolved_task_state=unknown
    echo "Agent task did not become terminal after abort: $task_id" >&2
    return 1
}

wait_for_agent_task() {
    task_id=$1
    attempt=0
    while test "$attempt" -lt 330; do
        if ! load_task "$task_id"; then
            retain_recovery=1
            unresolved_task_id=$task_id
            unresolved_task_state=unknown
            return 1
        fi
        unresolved_task_state=$task_status
        case "$task_status" in
            completed)
                rollback=0
                retain_recovery=0
                unresolved_task_id=""
                unresolved_task_state=""
                printf 'Agent task: %s completed\n' "$task_id"
                return 0
                ;;
            pending | running)
                ;;
            failed | aborted | timed_out)
                printf '%s\n' "$task_output" >&2
                return 1
                ;;
            *)
                echo "Agent task returned unknown status: $task_status" >&2
                return 1
                ;;
        esac
        attempt=$((attempt + 1))
        sleep 1
    done
    unresolved_task_state=client_timeout
    echo "Agent task did not complete within 330 seconds: $task_id" >&2
    settle_timed_out_agent_task "$task_id"
}

wait_for_agent_ready() {
    attempt=0
    while test "$attempt" -lt 150; do
        if ! ready_output=$(/usr/local/bin/groundplane \
            --host http://127.0.0.1:8080 \
            --output json \
            agent list 2>&1); then
            printf '%s\n' "$ready_output" >&2
            return 1
        fi
        agent_status=$(printf '%s\n' "$ready_output" |
            sed -n 's/^[[:space:]]*"status":[[:space:]]*"\([^"]*\)".*$/\1/p' |
            head -n 1)
        case "$agent_status" in
            healthy)
                printf 'Agent: Ready\n'
                return 0
                ;;
            pending | degraded | stopped)
                ;;
            *)
                echo "Agent returned unknown status while waiting for Ready: $agent_status" >&2
                return 1
                ;;
        esac
        attempt=$((attempt + 1))
        sleep 1
    done
    echo "Agent did not report Ready within 150 seconds" >&2
    return 1
}

agent_task_id=""
if ! agent_list_output=$(/usr/local/bin/groundplane \
    --host http://127.0.0.1:8080 \
    --output json \
    agent list 2>&1); then
    printf '%s\n' "$agent_list_output" >&2
    exit 1
fi
if printf '%s\n' "$agent_list_output" | grep -Eq '"id"[[:space:]]*:'; then
    update_settled=0
    attempt=0
    while test "$attempt" -lt 330; do
        if update_output=$(/usr/local/bin/groundplane \
            --host http://127.0.0.1:8080 \
            --output json \
            agent update --all 2>&1); then
            retain_recovery=1
            unresolved_task_state=dispatched
            printf '%s\n' "$update_output"
            agent_task_id=$(printf '%s\n' "$update_output" |
                sed -n 's/^[[:space:]]*"task_id":[[:space:]]*"\([^"]*\)".*$/\1/p')
            unresolved_task_id=$agent_task_id
            if test -z "$agent_task_id"; then
                unresolved_task_state=unknown
                echo "Agent update did not return a Task id" >&2
                exit 1
            fi
            update_settled=1
            break
        fi
        case "$update_output" in
            'error: state.conflict: Agent already runs configured agent.image')
                printf 'Agent: already running configured image\n'
                update_settled=1
                break
                ;;
            'error: resource.in_use:'*)
                ;;
            *)
                printf '%s\n' "$update_output" >&2
                retain_recovery=1
                unresolved_task_state=unknown
                echo "Agent update outcome is unknown; refusing to race it with rollback" >&2
                exit 1
                ;;
        esac
        attempt=$((attempt + 1))
        sleep 1
    done
    if test "$update_settled" -ne 1; then
        retain_recovery=1
        unresolved_task_state=in_flight
        echo "Agent remained busy for 330 seconds; its lifecycle may still be in flight" >&2
        exit 1
    fi
else
    if join_output=$(/usr/local/bin/groundplane \
        --host http://127.0.0.1:8080 \
        --output json \
        agent join 2>&1); then
        retain_recovery=1
        unresolved_task_state=dispatched
    else
        printf '%s\n' "$join_output" >&2
        retain_recovery=1
        unresolved_task_state=unknown
        echo "Agent enrollment outcome is unknown; refusing to race it with rollback" >&2
        exit 1
    fi
    printf '%s\n' "$join_output"
    agent_task_id=$(printf '%s\n' "$join_output" |
        sed -n 's/^[[:space:]]*"task_id":[[:space:]]*"\([^"]*\)".*$/\1/p')
    unresolved_task_id=$agent_task_id
    if test -z "$agent_task_id"; then
        unresolved_task_state=unknown
        echo "Agent enrollment did not return a Task id" >&2
        exit 1
    fi
fi
if test -n "$agent_task_id"; then
    wait_for_agent_task "$agent_task_id"
else
    wait_for_agent_ready
    rollback=0
fi

printf 'Controller: active\n'
printf 'Agent image: %s\n' "$agent_ref"
"""


@dataclass(frozen=True)
class Deployment:
    key: Path
    ip: ipaddress.IPv4Address | ipaddress.IPv6Address
    version: str
    setup: bool
    expose_port: int | None
    known_hosts: Path | None

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
    parser.add_argument("--ip", required=True, help="target host IP address")
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
        help="Agent release version; defaults to cacheable development identity 'dev'",
    )
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

    return Deployment(
        key=key,
        ip=address,
        version=arguments.version,
        setup=arguments.setup,
        expose_port=arguments.expose,
        known_hosts=known_hosts,
    )


def command_text(command: list[str]) -> str:
    return shlex.join(command)


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


def build_artifacts(deployment: Deployment, image_archive: Path, invocation_id: str) -> str:
    source_image = f"groundplane-agent:deploy-{invocation_id}"
    run(
        ["make", "controller", "cli"],
        cwd=REPOSITORY_ROOT,
        environment=build_environment(),
    )
    run(
        [
            "make",
            "agent-image",
            f"AGENT_VERSION={deployment.version}",
            f"AGENT_IMAGE={source_image}",
        ],
        cwd=REPOSITORY_ROOT,
    )
    run(["docker", "save", "--output", str(image_archive), source_image])
    return source_image


def create_transfer_archive(image_archive: Path, transfer_archive: Path) -> None:
    sources = [
        CONTROLLER,
        CLI,
        CONTROLLER_UNIT,
        TMPFILES,
        CONFIG_EXAMPLE,
        image_archive,
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
    source_image: str,
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
            source_image,
        ],
    )
    command = [*deployment.ssh_base, remote_command]
    print(f"+ {command_text([*deployment.ssh_base, '<deployment bundle>'])}", flush=True)
    with transfer_archive.open("rb") as content:
        subprocess.run(command, check=True, stdin=content)


def deploy(deployment: Deployment) -> None:
    require_local_tools()
    verify_architecture(deployment)

    invocation_id = secrets.token_hex(16)
    remote_directory = f"/tmp/groundplane-deploy-{invocation_id}"
    with tempfile.TemporaryDirectory(prefix="groundplane-deploy-") as temporary_directory:
        image_archive = Path(temporary_directory) / "agent-image.tar"
        transfer_archive = Path(temporary_directory) / "deployment.tar"
        source_image = build_artifacts(deployment, image_archive, invocation_id)
        create_transfer_archive(image_archive, transfer_archive)
        transfer_and_deploy(deployment, remote_directory, source_image, transfer_archive)


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
