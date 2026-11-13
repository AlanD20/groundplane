#!/bin/sh
# Remove this host's Groundplane installation without removing host dependencies.
set -eu
case "${1:-}" in
    --yes|--dry-run) mode=$1; shift ;;
    --help|-h|'')
        printf '%s\n' 'Usage: sudo sh uninstall.sh --yes | --dry-run' \
            'Deletes GP-managed workloads, local data, secrets and installation files.' \
            'Keeps Docker, Compose, host packages and unrelated Docker resources.'
        exit 0 ;;
    *) echo 'Expected --yes or --dry-run. Use --help for details.' >&2; exit 2 ;;
esac
test "$#" -eq 0 || { echo 'Unexpected arguments.' >&2; exit 2; }
test "$(id -u)" -eq 0 || { echo 'Uninstallation requires root.' >&2; exit 1; }
command -v python3 >/dev/null || { echo 'Python 3 is required (installed by the GP installer).' >&2; exit 1; }
exec python3 -c '
import glob
import fcntl
import grp
import json
import os
from pathlib import Path
import pwd
import re
import shutil
import stat
import subprocess
import sys

dry = sys.argv[1] == "--dry-run"
os.umask(0o077)
# Ignore caller Docker contexts: GP bootstrap manages the local root daemon.
docker = ["docker", "--host", "unix:///var/run/docker.sock"]
env = dict(os.environ, DOCKER_CONFIG="/etc/groundplane/registry/client")
for name in ("DOCKER_CONTEXT", "DOCKER_HOST", "DOCKER_TLS", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH"):
    env.pop(name, None)

def run(args, *, change=False):
    if change:
        print(("Would run: " if dry else "Running: ") + " ".join(args), flush=True)
        if dry:
            return ""
    return subprocess.check_output(args, text=True, stderr=subprocess.PIPE, env=env, timeout=180)

def selected(kind, label):
    return run(docker + [kind, "ls", "--quiet", "--filter", "label=" + label] +
               (["--all"] if kind == "container" else [])).split()

def remove_path(path):
    path = Path(path)
    if not os.path.lexists(path):
        return
    # Never traverse an ancestor symlink or a mounted filesystem during a wipe.
    for parent in path.parents:
        if parent.is_symlink():
            raise RuntimeError("Refusing symlink ancestor: " + str(parent))
    if not path.is_symlink():
        for line in Path("/proc/self/mountinfo").read_text().splitlines():
            mount = re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), line.split()[4])
            if mount == str(path) or mount.startswith(str(path) + "/"):
                raise RuntimeError("Unmount before removing: " + mount)
    print(("Would remove: " if dry else "Removing: ") + str(path), flush=True)
    if not dry:
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path)
        else:
            path.unlink()

try:
    print("Groundplane reset: all GP-managed workloads and local data will be deleted.", flush=True)
    if not dry:
        lock = os.open("/run/lock/groundplane-deploy.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    units = set()
    if shutil.which("systemctl"):
        for command in (["list-unit-files", "--no-legend", "--no-pager"],
                        ["list-units", "--all", "--plain", "--no-legend", "--no-pager"]):
            for line in run(["systemctl", *command]).splitlines():
                name = line.split()[0] if line.split() else ""
                if re.fullmatch(r"groundplane-[A-Za-z0-9_.@-]+\.(service|socket|timer)", name):
                    units.add(name)
        # Stop timers/upgrade guards/Runner daemons as well as the Controller.
        for unit in sorted(units):
            run(["systemctl", "stop", unit], change=True)
            if (Path("/etc/systemd/system") / unit).exists():
                run(["systemctl", "disable", unit], change=True)

    # Host-DNS opt-in must be undone before its local resolver is removed.
    resolver = Path("/etc/resolv.conf")
    baseline = Path("/var/lib/groundplane/agent/host-resolution/resolv.conf.previous")
    if resolver.exists() and resolver.read_bytes() == b"nameserver 127.0.0.1\n" and Path("/etc/groundplane").exists():
        with os.fdopen(os.open(baseline, os.O_RDONLY | os.O_NOFOLLOW), "rb") as saved:
            metadata = os.fstat(saved.fileno())
            content = saved.read(65537)
        if metadata.st_uid != 0 or not stat.S_ISREG(metadata.st_mode) or metadata.st_mode & 0o022 or not 0 < len(content) <= 65536:
            raise RuntimeError("Restore host DNS first: its saved baseline is not trusted")
        print("Would restore host DNS baseline" if dry else "Restoring host DNS baseline", flush=True)
        if not dry:
            with resolver.open("wb") as target:
                target.write(content)
                target.flush()
                os.fsync(target.fileno())

    # Only reserved GP Runner accounts with their actual managed home qualify.
    accounts = [u for u in pwd.getpwall() if re.fullmatch(r"gpr_[a-z0-9]+", u.pw_name)
                and re.fullmatch(r"/var/lib/groundplane-runners/slot-[0-9]+/runner", u.pw_dir)
                and u.pw_shell == "/usr/sbin/nologin"]
    for account in accounts:
        if not dry:
            result = subprocess.run(["pkill", "-KILL", "-u", str(account.pw_uid)], check=False)
            if result.returncode not in (0, 1):
                raise RuntimeError("Cannot stop Runner processes: " + account.pw_name)
        run(["userdel", account.pw_name], change=True)
        try:
            group = grp.getgrnam(account.pw_name)
        except KeyError:
            group = None
        if group is not None and group.gr_gid == account.pw_gid:
            run(["groupdel", account.pw_name], change=True)

    if shutil.which("docker"):
        # A missing daemon is not an empty inventory; retain data and report it.
        containers = set(selected("container", "com.groundplane.managed=true"))
        containers.update(selected("container", "groundplane.registry.schema=1"))
        projects = set()
        for identity in containers:
            observed = json.loads(run(docker + ["container", "inspect", identity]))[0]
            project = observed.get("Config", {}).get("Labels", {}).get("com.docker.compose.project", "")
            if project == "groundplane-infra" or re.fullmatch(r"gp-env_[a-z0-9]{26}", project):
                projects.add(project)
        for identity in sorted(containers):
            run(docker + ["container", "rm", "--force", "--volumes", identity], change=True)
        volume_data = set()
        for kind in ("network", "volume"):
            identities = set(selected(kind, "com.groundplane.managed=true"))
            # Compose may create its implicit default network with only its own labels.
            for project in projects:
                identities.update(selected(kind, "com.docker.compose.project=" + project))
            for identity in sorted(identities):
                if kind == "volume":
                    volume = json.loads(run(docker + ["volume", "inspect", identity]))[0]
                    options = volume.get("Options") or {}
                    device = options.get("device", "")
                    if options.get("type") == "none" and "bind" in options.get("o", "").split(","):
                        path = Path(device)
                        if not path.is_absolute() or ".." in path.parts or len(path.parts) < 5:
                            raise RuntimeError("Unsafe managed Volume data path: " + device)
                        volume_data.add(device)
                run(docker + [kind, "rm", identity], change=True)
        for path in sorted(volume_data):
            remove_path(path)
        # Remove GP-built images, never prune shared upstream image caches.
        for identity in set(selected("image", "org.opencontainers.image.source=https://github.com/AlanD20/groundplane")):
            image = json.loads(run(docker + ["image", "inspect", identity]))[0]
            users = run(docker + ["container", "ls", "--all", "--quiet", "--filter", "ancestor=" + identity]).split()
            if any(not any(user.startswith(owner) for owner in containers) for user in users):
                print("Keeping image used by another container: " + identity, flush=True)
                continue
            for reference in image.get("RepoTags") or []:
                run(docker + ["image", "rm", reference], change=True)
            # Removing the final tag can already remove the image.
            if not image.get("RepoTags"):
                run(docker + ["image", "rm", identity], change=True)

    if shutil.which("nft"):
        inventory = json.loads(run(["nft", "--json", "list", "tables"]))
        for item in inventory.get("nftables", []):
            table = item.get("table", {})
            if table.get("family") == "inet" and re.fullmatch(r"gpr_[0-9a-z]{26}", table.get("name", "")):
                run(["nft", "delete", "table", "inet", table["name"]], change=True)

    for unit in sorted(units):
        remove_path(Path("/etc/systemd/system") / unit)
        remove_path(Path("/etc/systemd/system") / (unit + ".d"))
        remove_path(Path("/run/systemd/transient") / unit)
    paths = ["/etc/groundplane", "/var/lib/groundplane", "/var/log/groundplane",
             "/var/lib/groundplane-runners", "/run/groundplane", "/run/groundplane-runners",
             "/run/groundplane-execution", "/usr/local/libexec/groundplane",
             "/usr/local/bin/groundplane", "/etc/tmpfiles.d/groundplane.conf",
             "/usr/lib/tmpfiles.d/groundplane.conf", "/root/.config/groundplane",
             "/root/.groundplane", "/etc/docker/certs.d/localhost:5000",
             "/etc/docker/certs.d/registry.groundplane.internal:5000"]
    # Trust directories belong to GP only when their certificate is GP-generated.
    certificate = Path("/etc/groundplane/registry/tls.crt")
    trusted = certificate.read_bytes() if certificate.is_file() and not certificate.is_symlink() else None
    for path in paths:
        if path.startswith("/etc/docker/certs.d/"):
            ca = Path(path) / "ca.crt"
            if trusted is None or not ca.is_file() or ca.read_bytes() != trusted:
                if Path(path).exists():
                    print("Keeping unverified Docker trust directory: " + path, flush=True)
                continue
            remove_path(ca)
            if Path(path).is_dir() and not any(Path(path).iterdir()) and not dry:
                Path(path).rmdir()
            continue
        remove_path(path)
    for pattern in ("/etc/systemd/system/*.wants/groundplane-*.service",
                    "/etc/systemd/system/*.wants/groundplane-*.timer",
                    "/usr/local/bin/groundplane.groundplane-*", "/usr/local/bin/groundplane.groundplane-rollback-*",
                    "/run/lock/groundplane-*.lock"):
        for path in glob.glob(pattern):
            if path == "/run/lock/groundplane-deploy.lock":
                continue  # Keep the shared lock inode valid for concurrent installers.
            remove_path(path)
    if shutil.which("systemctl"):
        run(["systemctl", "daemon-reload"], change=True)
        for unit in sorted(units):
            # Clears failed transient unit records without resetting unrelated units.
            if not dry:
                subprocess.run(["systemctl", "reset-failed", unit], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print("Dry run complete; nothing changed." if dry else "Groundplane removed. Docker and host dependencies remain installed. You can run install.sh again.")
except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
    print("Uninstall incomplete: " + str(error), file=sys.stderr)
    sys.exit(1)
' "$mode"
