"""Install an independently selected Agent through the existing Controller Task."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import stat
import subprocess
import time

from controller_update import Receipt, TASK_ID, TERMINAL, TASK_ACTIVE, Transport, task_state

REMOTE_NAME = "groundplane-agent"
REMOTE_STATE = Path("/var/lib/groundplane/agent")
REMOTE_CONFIG = Path("/run/groundplane/remote-agent")
REMOTE_DOCKER_SOCKET = "/var/run/docker.sock"
REMOTE_LABEL = "com.groundplane.kind"
IMMUTABLE_IMAGE = r"[a-z0-9][a-z0-9._:/\[\]-]*@sha256:[0-9a-f]{64}"
CONTAINER_ID = r"[a-f0-9]{64}"


def pull_image(image):
    command = ["docker"]
    if image.split("/", 1)[0] in {"localhost:5000", "registry.groundplane.internal:5000"}:
        command += ["--config", "/etc/groundplane/registry/client"]
    subprocess.run([*command, "pull", image], check=True, timeout=600)
    preserve_image_tag(image)


def preserve_image_tag(image, *, replace=False):
    """Retain a verified selection's name without changing its runtime reference."""
    repository, separator, digest = image.rpartition("@")
    if not separator or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise ValueError("image tag preservation requires an immutable image")
    if repository.rfind(":") > repository.rfind("/"):
        target = repository
    else:
        inspected = json.loads(subprocess.check_output(
            ["docker", "image", "inspect", image], text=True, timeout=30))[0]
        labels = (inspected.get("Config") or {}).get("Labels") or {}
        if labels.get("org.opencontainers.image.source") != "https://github.com/AlanD20/groundplane":
            return
        tag = labels.get("com.groundplane.image-tag", "")
        if not tag and labels.get("com.groundplane.source-kind") == "source_ref":
            tag = labels.get("org.opencontainers.image.revision", "")
        if not tag and labels.get("com.groundplane.source-kind") == "release":
            version = labels.get("com.groundplane.version") or labels.get("org.opencontainers.image.version", "")
            version = version.removeprefix("agent/").removeprefix("controller/").removeprefix("v")
            if re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:[-.][0-9A-Za-z.-]+)?", version):
                tag = "v" + version
        if not tag:
            return
        if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", tag):
            raise ValueError("selected image has an invalid build tag")
        target = repository + ":" + tag
    if not replace:
        existing = subprocess.run(["docker", "image", "inspect", target], capture_output=True, text=True, timeout=30)
        if existing.returncode == 0:
            return
        if existing.returncode != 1:
            raise subprocess.CalledProcessError(existing.returncode, existing.args)
    subprocess.run(["docker", "tag", image, target], check=True, timeout=30)


def current_image():
    result = subprocess.run(["docker", "inspect", "groundplane-agent"], capture_output=True, text=True, timeout=30)
    if result.returncode:
        raise ValueError("no installed Agent; enroll it through the Controller first")
    runtime = json.loads(result.stdout)[0]
    labels = runtime["Config"].get("Labels") or {}
    if (not runtime.get("State", {}).get("Running") or labels.get("com.groundplane.managed") != "true"
            or labels.get("com.groundplane.kind") != "agent"):
        raise ValueError("installed Agent runtime is stopped or has foreign ownership")
    image = runtime["Config"]["Image"]
    if not re.fullmatch(r"[a-z0-9][a-z0-9._:/\[\]-]*@sha256:[0-9a-f]{64}", image):
        raise ValueError("installed Agent image is not digest-pinned")
    return image


def installed_image():
    status, document = Transport().request("GET", "/agents")
    if status != 200 or not isinstance(document, dict) or not isinstance(document.get("items"), list):
        raise ValueError("cannot read Agent inventory")
    if len(document["items"]) == 0:
        return None
    if len(document["items"]) != 1:
        raise ValueError("Agent singleton invariant violated")
    return current_image()


def install(image, *, allow_enroll=False):
    if not re.fullmatch(r"[a-z0-9][a-z0-9._:/\[\]-]*@sha256:[0-9a-f]{64}", image):
        raise ValueError("Agent installation requires an immutable image")
    transport = Transport()
    status, agents = transport.request("GET", "/agents")
    if status != 200 or not isinstance(agents, dict) or not isinstance(agents.get("items"), list):
        raise ValueError("Agent inventory unavailable")
    absent = len(agents["items"]) == 0
    if absent and not allow_enroll or len(agents["items"]) > 1:
        raise ValueError("Agent-only installation requires an enrolled local Agent and running Controller")
    agent = agents["items"][0] if not absent else None
    identity = agent["id"] if agent else "enrollment"
    if not absent and not re.fullmatch(r"agt_[0-7][0-9A-HJKMNP-TV-Z]{25}", identity):
        raise ValueError("Controller returned an invalid Agent identity")
    root = Path("/var/lib/groundplane/agent-updates")
    root.mkdir(mode=0o700, exist_ok=True)
    receipt = Receipt(root, 0)
    try:
        active = receipt.read()
        enrollment_release = "sha256:" + hashlib.sha256(("enrollment\n" + image).encode()).hexdigest()
        enrolling = absent or (active is not None and active["status"] not in TERMINAL and
                               active["release"] == enrollment_release)
        if not absent and (active is None or active["status"] in TERMINAL) and current_image() == image and agent["status"] == "healthy":
            preserve_image_tag(image, replace=True)
            print("Agent is already installed and healthy; skipped.")
            return
        pull_image(image)
        release = enrollment_release if enrolling else "sha256:" + hashlib.sha256((identity + "\n" + image).encode()).hexdigest()
        value = receipt.begin(release, "agent-install-" + secrets.token_hex(16))
        if not value["task_id"]:
            status, accepted = transport.request("POST", "/agents" if enrolling else f"/agents/{identity}/update",
                                                 None if enrolling else {"image": image}, value["key"])
            if status != 202 or not isinstance(accepted, dict) or not TASK_ID.fullmatch(accepted.get("task_id", "")):
                raise ValueError(f"Agent update not accepted (HTTP {status}); receipt retained")
            value["task_id"] = accepted["task_id"]
            receipt.write(value)
        print(f"Agent update Task: {value['task_id']}", flush=True)
        deadline = time.monotonic() + 330
        while time.monotonic() < deadline:
            status, task = transport.request("GET", "/tasks/" + value["task_id"])
            if status != 200:
                raise ValueError("Agent Task outcome unavailable; receipt retained")
            _, state = task_state(task, value["task_id"], "agent")
            if state in TERMINAL:
                value["status"] = state
                receipt.write(value)
                if state != "succeeded":
                    raise ValueError(f"Agent update {state}; inspect retained Task and recovery evidence")
                if current_image() != image:
                    raise ValueError("completed Agent update has a different runtime image")
                preserve_image_tag(image, replace=True)
                print("Agent update completed.")
                return
            if state not in TASK_ACTIVE:
                raise ValueError("unknown Agent Task state; receipt retained")
            time.sleep(1)
        raise ValueError("Agent update deadline reached; receipt retained, Task not cancelled")
    finally:
        receipt.close()


def remote_docker(*args, timeout=30):
    # Fix the endpoint to the managed host even when the caller has a Docker
    # context or DOCKER_HOST configured for a different engine.
    command = ["docker", "--host", f"unix://{REMOTE_DOCKER_SOCKET}", *args]
    overrides = {"DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH"}
    environment = {key: value for key, value in os.environ.items() if key not in overrides}
    result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                            timeout=timeout, env=environment)
    return result.stdout


def remote_private_directory(path, *, create=False):
    path = Path(path)
    if not path.is_absolute() or path == Path("/") or path.resolve() != path or "," in str(path) or not str(path).isprintable():
        raise ValueError("Remote Agent paths must be canonical absolute directories")
    for parent in reversed(path.parents):
        if create and not parent.exists():
            parent.mkdir(mode=0o700, exist_ok=True)
        parent_info = parent.stat(follow_symlinks=False)
        if parent_info.st_uid != 0 or not stat.S_ISDIR(parent_info.st_mode) or parent_info.st_mode & 0o022:
            raise ValueError("Remote Agent path parents must be protected root-owned directories")
    if create:
        path.mkdir(mode=0o700, exist_ok=True)
    evidence = path.stat(follow_symlinks=False)
    if evidence.st_uid != 0 or not stat.S_ISDIR(evidence.st_mode) or stat.S_IMODE(evidence.st_mode) != 0o700:
        raise ValueError("Remote Agent directories must be root-owned with mode 0700")
    return path


def remote_expected_identity(host, agent, generation):
    if not re.fullmatch(r"host_[0-7][0-9A-HJKMNP-TV-Z]{25}", host) or not re.fullmatch(r"agt_[0-7][0-9A-HJKMNP-TV-Z]{25}", agent):
        raise ValueError("Remote Agent installer requires canonical Host and Agent IDs")
    if not 0 < generation <= 2**64 - 1:
        raise ValueError("Remote Agent installer requires a valid enrollment generation")
    return {"host_id": host, "agent_id": agent, "generation": generation}


def remote_identity_environment(identity):
    return f"GROUNDPLANE_REMOTE_EXPECTED_IDENTITY={identity['host_id']},{identity['agent_id']},{identity['generation']}"


def remote_mount(source, target, read_only=False):
    return f"type=bind,source={source},target={target}" + (",readonly" if read_only else "")


def remote_validate(image, config_dir, identity):
    result = remote_docker("run", "--rm", "--network", "none", "--read-only", "--user", "0:0", "--cap-drop", "ALL",
                           "--security-opt", "no-new-privileges", "--pull", "never",
                           "--env", f"GROUNDPLANE_AGENT_CONFIG={REMOTE_CONFIG / 'agent.yaml'}",
                           "--env", remote_identity_environment(identity),
                           "--mount", remote_mount(config_dir, REMOTE_CONFIG, True),
                           image, "verify-remote-agent-config")
    if len(result) > 4096:
        raise ValueError("Remote Agent configuration evidence exceeded its output bound")
    proof = json.loads(result)
    if not isinstance(proof, dict) or proof.get("identity") != identity or not isinstance(proof.get("volume_root"), str):
        raise ValueError("Remote Agent configuration does not match the requested enrollment")
    return Path(proof["volume_root"])


def remote_container_ids(*filters):
    arguments = [argument for value in filters for argument in ("--filter", value)]
    identifiers = remote_docker("container", "ls", "--all", "--no-trunc", "--format", "{{.ID}}", *arguments).decode().splitlines()
    if any(not re.fullmatch(CONTAINER_ID, container_id) for container_id in identifiers):
        raise ValueError("Docker returned an invalid Agent inventory")
    return identifiers


def remote_inspect(container_id):
    if not re.fullmatch(CONTAINER_ID, container_id):
        raise ValueError("Docker returned an invalid Agent container id")
    result = json.loads(remote_docker("container", "inspect", container_id))
    if (not isinstance(result, list) or len(result) != 1 or not isinstance(result[0], dict) or
            result[0].get("Id") != container_id):
        raise ValueError("Docker could not establish the exact Agent container")
    return result[0]


def remote_labels(identity):
    return {
        "com.groundplane.managed": "true", REMOTE_LABEL: "remote-agent", "com.groundplane.host-id": identity["host_id"],
        "com.groundplane.agent-id": identity["agent_id"], "com.groundplane.generation": str(identity["generation"]),
    }


def remote_container_matches(info, image, identity, mounts):
    configuration, host = info["Config"], info["HostConfig"]
    labels = configuration.get("Labels") or {}
    actual_mounts = [(m["Source"], m["Destination"], m["RW"]) for m in info["Mounts"] if m["Type"] == "bind"]
    env = configuration.get("Env") or []
    required_env = (remote_identity_environment(identity), f"GROUNDPLANE_AGENT_IMAGE={image}",
                    f"GROUNDPLANE_AGENT_CONFIG={REMOTE_CONFIG / 'agent.yaml'}")
    return (info["Name"] == "/" + REMOTE_NAME and configuration["Image"] == image and
            configuration["User"] == "0:0" and not host["Privileged"] and host["NetworkMode"] == "host" and
            configuration.get("Entrypoint") == ["/usr/local/bin/groundplane-agent"] and not configuration.get("Cmd") and
            host["RestartPolicy"]["Name"] == "unless-stopped" and sorted(actual_mounts) == sorted(mounts) and
            len(info["Mounts"]) == len(mounts) and all(labels.get(key) == value for key, value in remote_labels(identity).items()) and
            all([entry for entry in env if entry.partition("=")[0] == value.partition("=")[0]] == [value]
                for value in required_env))


def remote_create(image, identity, mounts):
    command = ["container", "create", "--pull", "never", "--name", REMOTE_NAME, "--network", "host",
               "--restart", "unless-stopped", "--user", "0:0", "--log-driver", "json-file",
               "--log-opt", "max-size=10m", "--log-opt", "max-file=3",
               "--env", f"GROUNDPLANE_AGENT_CONFIG={REMOTE_CONFIG / 'agent.yaml'}",
               "--env", f"GROUNDPLANE_AGENT_IMAGE={image}", "--env", remote_identity_environment(identity)]
    for key, value in remote_labels(identity).items():
        command.extend(("--label", f"{key}={value}"))
    for source, target, writable in mounts:
        command.extend(("--mount", remote_mount(source, target, not writable)))
    return remote_docker(*command, image).decode().strip()


def remote_select_runtime(image, identity, mounts):
    named = remote_container_ids(f"name=^/{REMOTE_NAME}$")
    known = set(remote_container_ids(f"label={REMOTE_LABEL}=agent"))
    known.update(remote_container_ids(f"label={REMOTE_LABEL}=remote-agent"))
    if len(named) > 1 or known - set(named):
        raise ValueError("Another Agent container is present; existing runtimes were preserved")
    if not named:
        container_id = remote_create(image, identity, mounts)
        created = remote_inspect(container_id)
        if not remote_container_matches(created, image, identity, mounts) or created["State"]["Status"] != "created":
            raise ValueError("New Agent runtime could not be verified; retained for inspection")
        return container_id, "created"
    container_id = named[0]
    existing = remote_inspect(container_id)
    if not remote_container_matches(existing, image, identity, mounts):
        raise ValueError("Existing Agent has different ownership, image, or configuration; retained for inspection")
    state = existing["State"]["Status"]
    if state not in {"created", "running"}:
        raise ValueError("Existing Agent state cannot be resumed safely; retained for inspection")
    return container_id, state


def install_remote(image, config_dir, host, agent, generation):
    if not re.fullmatch(IMMUTABLE_IMAGE, image):
        raise ValueError("Remote Agent installation requires an immutable image")
    identity = remote_expected_identity(host, agent, generation)
    config_dir = remote_private_directory(config_dir)
    for required in ("agent.yaml", "agent.yaml.remote-agent.yaml"):
        if not (config_dir / required).is_file():
            raise ValueError("Remote Agent primary and companion configuration files are required")
    for path in config_dir.iterdir():
        info = path.stat(follow_symlinks=False)
        if info.st_uid != 0 or not stat.S_ISREG(info.st_mode) or info.st_mode & 0o022:
            raise ValueError("Remote Agent configuration and credential files must be root-owned regular files")

    try:
        remote_docker("image", "inspect", image, "--format", "{{.Id}}")
    except subprocess.CalledProcessError:
        remote_docker("image", "pull", image, timeout=600)
    volume_root = remote_validate(image, config_dir, identity)
    directories = (config_dir, REMOTE_STATE, volume_root)
    if any(left.is_relative_to(right) or right.is_relative_to(left)
           for index, left in enumerate(directories) for right in directories[index + 1:]):
        raise ValueError("Remote Agent configuration, state and volume directories must be separate")
    remote_private_directory(REMOTE_STATE, create=True)
    remote_private_directory(volume_root, create=True)
    owner = remote_private_directory(REMOTE_STATE / "execution-owner", create=True)
    mounts = [(REMOTE_DOCKER_SOCKET, REMOTE_DOCKER_SOCKET, True),
              (str(REMOTE_STATE), str(REMOTE_STATE), True), (str(volume_root), str(volume_root), True),
              (str(config_dir), str(REMOTE_CONFIG), False)]

    try:
        lease = os.open(owner, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError as err:
        raise ValueError("Remote Agent execution lease cannot be verified") from err
    try:
        try:
            fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            named = remote_container_ids(f"name=^/{REMOTE_NAME}$")
            if len(named) == 1:
                running = remote_inspect(named[0])
                if remote_container_matches(running, image, identity, mounts) and running["State"]["Status"] == "running":
                    print(json.dumps({**identity, "container_id": named[0], "state": "already_running"}, sort_keys=True))
                    return
            raise ValueError("Agent execution lease is occupied; existing runtimes were preserved")
        container_id, state = remote_select_runtime(image, identity, mounts)
    finally:
        os.close(lease)

    if state == "created":
        # The Agent takes the same host execution lease before it can connect.
        # Keep Docker's named container even when start or verification fails.
        remote_docker("container", "start", container_id)
        time.sleep(2)
        started = remote_inspect(container_id)
        if (started["State"]["Status"] != "running" or started["RestartCount"] != 0 or
                not remote_container_matches(started, image, identity, mounts)):
            raise ValueError("Remote Agent did not stay running; retained for inspection")
    print(json.dumps({**identity, "container_id": container_id, "state": "running"}, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--remote-config-dir", type=Path)
    parser.add_argument("--host-id")
    parser.add_argument("--agent-id")
    parser.add_argument("--generation", type=int)
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error("Agent installation requires root")
    if args.remote_config_dir is None:
        if any(value is not None for value in (args.host_id, args.agent_id, args.generation)):
            parser.error("Remote Agent identity requires --remote-config-dir")
        install(args.image)
        return
    if any(value is None for value in (args.host_id, args.agent_id, args.generation)):
        parser.error("Remote Agent installation requires --host-id, --agent-id and --generation")
    try:
        install_remote(args.image, args.remote_config_dir, args.host_id, args.agent_id, args.generation)
    except (ValueError, OSError, KeyError, TypeError, subprocess.SubprocessError) as err:
        if isinstance(err, subprocess.SubprocessError):
            parser.error("Remote Agent Docker operation failed; inspect its retained runtime and configuration")
        if isinstance(err, (KeyError, TypeError)):
            parser.error("Docker returned incomplete Agent evidence; the existing runtime was preserved")
        parser.error(str(err))


if __name__ == "__main__":
    main()
