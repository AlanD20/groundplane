"""One persistent, authenticated TLS registry for bootstrap and Runner delivery."""
from __future__ import annotations

import base64
import http.client
import json
import os
from pathlib import Path
import secrets
import shlex
import ssl
import stat
import subprocess
import sys
import tempfile
import time

AUTHORITY = "registry.groundplane.internal:5000"
SETTINGS = Path("/etc/groundplane/registry")
DATA = Path("/var/lib/groundplane/registry")
CONTAINER = "groundplane-registry"
LABEL = "groundplane.registry.schema"


def setup_script(image: str) -> str:
    # Inline the same implementation into release bundles and remote setup.
    return "\npython3 -c " + shlex.quote(Path(__file__).read_text()) + " " + shlex.quote(image) + "\n"


def run(*args: str, **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(args, check=True, timeout=120, **kwargs)


def owned_directory(path: Path, mode: int) -> None:
    try:
        path.mkdir(mode=mode)
        path.chmod(mode)
    except FileExistsError:
        pass
    observed = path.lstat()
    if not stat.S_ISDIR(observed.st_mode) or observed.st_uid != 0 or stat.S_IMODE(observed.st_mode) != mode:
        raise ValueError(f"registry directory ownership differs: {path}")


def prepare_credentials(image: str) -> None:
    owned_directory(SETTINGS.parent, 0o700)
    if SETTINGS.exists() or SETTINGS.is_symlink():
        owned_directory(SETTINGS, 0o700)
        owned_directory(SETTINGS / "client", 0o700)
        for name in ("tls.crt", "tls.key", "htpasswd", "credentials.json", "client/config.json"):
            observed = (SETTINGS / name).lstat()
            if not stat.S_ISREG(observed.st_mode) or observed.st_uid != 0 or stat.S_IMODE(observed.st_mode) != 0o600:
                raise ValueError("registry credentials are incomplete or have unexpected ownership")
        if json.loads((SETTINGS / "credentials.json").read_text()).get("image") != image:
            raise ValueError("registry image differs from its bootstrap identity")
        run("openssl", "x509", "-in", str(SETTINGS / "tls.crt"), "-noout", "-checkend", "0",
            stdout=subprocess.DEVNULL)
        return
    with tempfile.TemporaryDirectory(prefix=".registry-", dir=SETTINGS.parent) as temporary:
        stage = Path(temporary) / "configuration"
        stage.mkdir(mode=0o700)
        username, password = "groundplane", secrets.token_urlsafe(48)
        run("openssl", "req", "-x509", "-newkey", "rsa:3072", "-sha256", "-nodes", "-days", "3650",
            "-subj", "/CN=registry.groundplane.internal",
            "-addext", "subjectAltName=DNS:registry.groundplane.internal,DNS:localhost,IP:127.0.0.1",
            "-addext", "extendedKeyUsage=serverAuth",
            "-keyout", str(stage / "tls.key"), "-out", str(stage / "tls.crt"),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        result = run("htpasswd", "-niB", username, input=password + "\n", text=True, capture_output=True)
        (stage / "htpasswd").write_text(result.stdout)
        (stage / "credentials.json").write_text(json.dumps({"image": image, "username": username, "password": password}))
        client = stage / "client"
        client.mkdir(mode=0o700)
        auth = base64.b64encode(f"{username}:{password}".encode()).decode()
        (client / "config.json").write_text(json.dumps({"auths": {
            "localhost:5000": {"auth": auth}, AUTHORITY: {"auth": auth},
        }}))
        for name in ("tls.crt", "tls.key", "htpasswd", "credentials.json", "client/config.json"):
            (stage / name).chmod(0o600)
            with (stage / name).open("rb") as stream:
                os.fsync(stream.fileno())
        os.rename(stage, SETTINGS)
        descriptor = os.open(SETTINGS.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def install_trust() -> None:
    owned_directory(Path("/etc/docker"), 0o755)
    owned_directory(Path("/etc/docker/certs.d"), 0o755)
    for authority in ("localhost:5000", AUTHORITY):
        directory = Path("/etc/docker/certs.d") / authority
        owned_directory(directory, 0o755)
        target = directory / "ca.crt"
        certificate = (SETTINGS / "tls.crt").read_bytes()
        if target.exists() or target.is_symlink():
            observed = target.lstat()
            if not stat.S_ISREG(observed.st_mode) or observed.st_uid != 0 or target.read_bytes() != certificate:
                raise ValueError("registry trust conflicts with existing host configuration")
        else:
            with target.open("xb") as stream:
                stream.write(certificate)
            target.chmod(0o644)


def inspect_existing(image: str) -> dict | None:
    # A daemon error is not absence. Listing must succeed before name inspection.
    names = run("docker", "container", "ls", "--all", "--format", "{{.Names}}", capture_output=True, text=True)
    if CONTAINER not in names.stdout.splitlines():
        return None
    result = run("docker", "container", "inspect", CONTAINER, capture_output=True, text=True)
    observed = json.loads(result.stdout)[0]
    if observed["Config"]["Image"] != image or observed["Config"].get("Labels", {}).get(LABEL) != "1":
        raise ValueError("existing registry is not the managed TLS registry; refusing replacement")
    mounts = {item["Destination"]: (item["Source"], item["RW"]) for item in observed["Mounts"]}
    if mounts != {
        "/var/lib/registry": (str(DATA), True),
        "/run/groundplane-registry/tls.crt": (str(SETTINGS / "tls.crt"), False),
        "/run/groundplane-registry/tls.key": (str(SETTINGS / "tls.key"), False),
        "/run/groundplane-registry/htpasswd": (str(SETTINGS / "htpasswd"), False),
    }:
        raise ValueError("managed registry storage or credentials mounts changed")
    return observed


def wait_ready() -> None:
    credentials = json.loads((SETTINGS / "credentials.json").read_text())
    auth = base64.b64encode(f"{credentials['username']}:{credentials['password']}".encode()).decode()
    context = ssl.create_default_context(cafile=str(SETTINGS / "tls.crt"))
    for _ in range(30):
        connection = http.client.HTTPSConnection("127.0.0.1", 5000, context=context, timeout=2)
        try:
            connection.request("GET", "/v2/", headers={"Authorization": "Basic " + auth})
            response = connection.getresponse()
            if response.status == 200:
                return
        except (OSError, http.client.HTTPException):
            pass
        finally:
            connection.close()
        time.sleep(1)
    raise RuntimeError("authenticated TLS registry did not become ready")


def setup(image: str) -> None:
    if os.geteuid() != 0:
        raise ValueError("registry setup requires root")
    os.umask(0o077)
    existing = inspect_existing(image)
    prepare_credentials(image)
    install_trust()
    DATA.parent.mkdir(mode=0o700, exist_ok=True)
    parent = DATA.parent.lstat()
    if not stat.S_ISDIR(parent.st_mode) or parent.st_uid != 0 or stat.S_IMODE(parent.st_mode) & 0o022:
        raise ValueError("registry storage parent is not root-controlled")
    owned_directory(DATA, 0o700)
    if existing is None:
        mounts = ["--mount", f"type=bind,src={DATA},dst=/var/lib/registry"]
        for name in ("tls.crt", "tls.key", "htpasswd"):
            mounts += ["--mount", f"type=bind,src={SETTINGS / name},dst=/run/groundplane-registry/{name},readonly"]
        run("docker", "run", "--detach", "--restart", "unless-stopped", "--name", CONTAINER,
            "--label", LABEL + "=1", "--publish", "127.0.0.1:5000:5000", "--read-only",
            "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
            *mounts, "--env", "REGISTRY_HTTP_TLS_CERTIFICATE=/run/groundplane-registry/tls.crt",
            "--env", "REGISTRY_HTTP_TLS_KEY=/run/groundplane-registry/tls.key",
            "--env", "REGISTRY_AUTH=htpasswd", "--env", "REGISTRY_AUTH_HTPASSWD_REALM=groundplane",
            "--env", "REGISTRY_AUTH_HTPASSWD_PATH=/run/groundplane-registry/htpasswd",
            "--env", "REGISTRY_LOG_LEVEL=warn", "--env", "OTEL_TRACES_EXPORTER=none",
            image, stdout=subprocess.DEVNULL)
    elif not existing["State"]["Running"]:
        run("docker", "start", existing["Id"], stdout=subprocess.DEVNULL)
    wait_ready()


if __name__ == "__main__":
    try:
        setup(sys.argv[1])
    except (ValueError, RuntimeError, OSError, subprocess.CalledProcessError) as error:
        # Never include captured subprocess output or credential documents.
        detail = str(error) if isinstance(error, ValueError) else type(error).__name__
        print(f"registry setup failed: {detail}", file=sys.stderr)
        sys.exit(1)
