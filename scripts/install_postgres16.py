"""Build and pin the native managed PostgreSQL artifact for a source installation."""
from __future__ import annotations

import base64
import hashlib
import http.client
import json
from pathlib import Path
import re
import ssl
import subprocess

import private_registry
from release_postgres16 import DIGEST, read_manifest, native_configuration_digest

REPOSITORY = "localhost:5000/groundplane-postgres16"


def output(*args: str) -> str:
    return subprocess.check_output(args, text=True, timeout=120).strip()


def native_manifest(reference: str, image_id: str) -> str:
    """Read exactly the pushed manifest, with the registry's existing TLS identity."""
    digest = reference.removeprefix(REPOSITORY + "@")
    if not DIGEST.fullmatch(digest):
        raise ValueError("managed PostgreSQL registry reference is invalid")
    settings = private_registry.SETTINGS
    credentials = json.loads((settings / "credentials.json").read_bytes())
    auth = base64.b64encode(
        f"{credentials['username']}:{credentials['password']}".encode()).decode()
    connection = http.client.HTTPSConnection(
        "127.0.0.1", 5000, context=ssl.create_default_context(cafile=str(settings / "tls.crt")), timeout=30)
    try:
        connection.request("GET", f"/v2/groundplane-postgres16/manifests/{digest}", headers={
            "Authorization": "Basic " + auth,
            "Accept": "application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json",
        })
        response = connection.getresponse()
        data = response.read((1 << 20) + 1)
        if (response.status != 200 or len(data) > 1 << 20
                or "sha256:" + hashlib.sha256(data).hexdigest() != digest):
            raise ValueError("managed PostgreSQL registry manifest identity changed")
        manifest = json.loads(data)
        return native_configuration_digest(manifest, reference, image_id)
    finally:
        connection.close()


def build_release(source: Path, common: list[str], buildx: list[str], identity: str) -> str:
    # Resolve once; Dockerfile stages and metadata all use these exact base bytes.
    base = json.loads(output(*buildx, "imagetools", "inspect", "postgres:16-alpine",
                             "--format", "{{json .Manifest}}"))["digest"]
    if not DIGEST.fullmatch(base):
        raise ValueError("managed PostgreSQL base image did not resolve to a digest")
    tag = f"groundplane-postgres16:ref-{identity}"
    if not re.fullmatch(r"[0-9a-f]{32}", identity):
        raise ValueError("managed PostgreSQL source build identity is invalid")
    runtime_tag = f"{REPOSITORY}:ref-{identity}"
    subprocess.run([*common, "--file", "Dockerfile.postgres16", "--build-arg",
                    f"POSTGRES16_BASE=postgres:16-alpine@{base}", "--tag", tag,
                    "--output", "type=docker,rewrite-timestamp=true", "."],
                   cwd=source, check=True, timeout=1800)
    inspected = json.loads(output("docker", "image", "inspect", tag))[0]
    image_id, arch = inspected["Id"], inspected["Architecture"]
    if not DIGEST.fullmatch(image_id) or inspected["Os"] != "linux" or arch not in {"amd64", "arm64"}:
        raise ValueError("managed PostgreSQL build did not return a supported native image")
    manifest = read_manifest(image_id, arch)
    subprocess.run(["docker", "tag", image_id, runtime_tag], check=True, timeout=30)
    subprocess.run(["docker", "--config", str(private_registry.SETTINGS / "client"), "push", runtime_tag],
                   check=True, timeout=1200)
    observed = json.loads(output("docker", "image", "inspect", image_id))[0]
    references = [ref for ref in observed.get("RepoDigests", []) if ref.startswith(REPOSITORY + "@")]
    if len(references) != 1:
        raise ValueError("managed PostgreSQL publication did not return one native repository digest")
    reference = references[0]
    configuration_digest = native_manifest(reference, image_id)
    catalog = {"schema": 1, "image": reference, "images": [
        {"repository_digest": reference, "image_id": configuration_digest, "manifest": manifest},
    ]}
    return base64.b64encode(json.dumps(catalog, separators=(",", ":")).encode()).decode()
