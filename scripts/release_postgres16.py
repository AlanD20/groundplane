#!/usr/bin/env python3
"""Publish native backup tools; the database stays on its pinned upstream image."""
from __future__ import annotations

import argparse
import io
import json
from pathlib import Path
import platform
import re
import subprocess
import tarfile

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = "/opt/groundplane/postgres16/release.json"
ARCHES = {"x86_64": "amd64", "aarch64": "arm64", "arm64": "arm64"}
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
MANIFEST_FIELDS = (
    "schema", "os", "architecture", "postgresql_major", "helper_sha256", "gate_sha256",
    "pg_dump_sha256", "pg_restore_sha256", "psql_sha256", "launch_profile_sha256", "gate_seccomp_sha256",
)


def load_catalog(path: Path, *, published: bool = False) -> tuple[bytes, dict]:
    with path.open("rb") as source:
        raw = source.read(16385)
    if not 0 < len(raw) <= 16384:
        raise ValueError("managed PostgreSQL release catalog exceeds its bound")
    catalog = json.loads(raw)
    if (tuple(catalog) != ("schema", "image", "database_image", "images") or catalog["schema"] != 1
            or not re.fullmatch(r"postgres:16-alpine@sha256:[0-9a-f]{64}", catalog["database_image"])
            or not re.fullmatch(r"[a-z0-9][a-z0-9._:/-]*@sha256:[0-9a-f]{64}", catalog["image"])
            or json.dumps(catalog, separators=(",", ":")).encode() != raw.strip()
            or not 1 <= len(catalog["images"]) <= 2):
        raise ValueError("managed PostgreSQL release catalog is invalid")
    images = catalog["images"]
    architectures = []
    for image in images:
        if tuple(image) != ("repository_digest", "image_id", "manifest"):
            raise ValueError("managed PostgreSQL native release identity is invalid")
        manifest = image["manifest"]
        arch = manifest.get("architecture")
        if (tuple(manifest) != MANIFEST_FIELDS or manifest["schema"] != 1
                or manifest["os"] != "linux" or arch not in {"amd64", "arm64"}
                or manifest["postgresql_major"] != 16
                or any(not re.fullmatch(r"[0-9a-f]{64}", manifest[field]) for field in MANIFEST_FIELDS[4:])
                or not DIGEST.fullmatch(image["image_id"])
                or not re.fullmatch(re.escape(catalog["image"].split("@", 1)[0]) +
                                    r"@sha256:[0-9a-f]{64}", image["repository_digest"])):
            raise ValueError("managed PostgreSQL native release evidence is invalid")
        architectures.append(arch)
    if (architectures != sorted(set(architectures))
            or (published or len(images) == 2) and architectures != ["amd64", "arm64"]
            or len({image["image_id"] for image in images}) != len(images)
            or len(images) == 1 and images[0]["repository_digest"] != catalog["image"]
            or len(images) == 2 and any(image["repository_digest"] == catalog["image"] for image in images)):
        raise ValueError("managed PostgreSQL catalog platform identities differ")
    return raw, catalog


def output(*args: str) -> str:
    return subprocess.check_output(args, cwd=ROOT, text=True, timeout=120).strip()


def native_configuration_digest(manifest: dict, reference: str, local_id: str) -> str:
    """Bind engine-local identity to the authenticated native OCI manifest."""
    digest = manifest.get("config", {}).get("digest", "")
    manifest_digest = reference.rsplit("@", 1)[-1]
    if (manifest.get("schemaVersion") != 2 or manifest.get("manifests")
            or not DIGEST.fullmatch(digest) or not DIGEST.fullmatch(manifest_digest)
            or local_id not in {digest, manifest_digest}):
        raise ValueError("native PostgreSQL manifest does not bind the measured image")
    return digest


def read_manifest(image_id: str, arch: str) -> dict:
    # The helper is never started to retrieve metadata. Remove only this
    # temporary stopped container and the anonymous volumes Docker created.
    container = output("docker", "create", "--network", "none", "--entrypoint", "/not-started", image_id)
    if not re.fullmatch(r"[0-9a-f]{64}", container):
        raise ValueError("Docker did not return the temporary container identity")
    try:
        copied = subprocess.check_output(
            ["docker", "cp", f"{container}:{MANIFEST}", "-"], cwd=ROOT, timeout=120)
        if len(copied) > 32768:
            raise ValueError("managed PostgreSQL release metadata exceeds its bound")
        with tarfile.open(fileobj=io.BytesIO(copied), mode="r:") as archive:
            members = archive.getmembers()
            if (len(members) != 1 or not members[0].isfile()
                    or members[0].name != Path(MANIFEST).name or not 0 < members[0].size <= 4096):
                raise ValueError("managed PostgreSQL release metadata is not one bounded file")
            stream = archive.extractfile(members[0])
            if stream is None:
                raise ValueError("managed PostgreSQL release metadata is missing")
            with stream:
                raw = stream.read(4097)
    finally:
        subprocess.run(["docker", "rm", "--volumes", container], cwd=ROOT, check=True, timeout=120)
    manifest = json.loads(raw)
    if (tuple(manifest) != MANIFEST_FIELDS or manifest["schema"] != 1
            or manifest["os"] != "linux" or manifest["architecture"] != arch
            or manifest["postgresql_major"] != 16
            or json.dumps(manifest, separators=(",", ":")).encode() != raw.strip()
            or any(not re.fullmatch(r"[0-9a-f]{64}", manifest[field])
                   for field in MANIFEST_FIELDS[4:])):
        raise ValueError("managed PostgreSQL release metadata is invalid")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-image", required=True)
    parser.add_argument("--image", required=True, help="native release repository:tag")
    parser.add_argument("--output", default=".tmp/release-images")
    args = parser.parse_args()
    if platform.system() != "Linux" or platform.machine() not in ARCHES:
        parser.error("managed PostgreSQL images must be built on native Linux amd64 or arm64")
    if not re.fullmatch(r"[a-z0-9][a-z0-9._:/-]*:[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", args.image):
        parser.error("an explicit native image tag is required")
    arch = ARCHES[platform.machine()]
    destination = Path(output("bash", "-c",
        'source scripts/repo-env.sh; repo_temp_dir "$1"', "--", args.output))
    reference_path = destination / f"postgres16-{arch}"
    metadata_path = destination / f"postgres16-{arch}.json"
    if any(path.exists() or path.is_symlink() for path in (reference_path, metadata_path)):
        parser.error("native release output already exists")
    subprocess.run(["bash", "scripts/build-postgres16-tools.sh", "--base-image", args.base_image,
                    "--image", args.image], cwd=ROOT, check=True)
    inspected = json.loads(output("docker", "image", "inspect", args.image))[0]
    image_id = inspected["Id"]
    if (not DIGEST.fullmatch(image_id) or inspected["Os"] != "linux"
            or inspected["Architecture"] != arch):
        raise ValueError("native PostgreSQL image identity differs from the build platform")
    manifest = read_manifest(image_id, arch)
    subprocess.run(["docker", "push", args.image], cwd=ROOT, check=True, timeout=1200)
    repository = args.image.rsplit(":", 1)[0]
    references = json.loads(output("docker", "image", "inspect", image_id))[0]["RepoDigests"]
    references = [ref for ref in references if ref.startswith(repository + "@")
                  and DIGEST.fullmatch(ref.split("@", 1)[1])]
    if len(references) != 1:
        raise ValueError("native PostgreSQL publication did not return one repository digest")
    reference = references[0]
    remote = json.loads(output("docker", "manifest", "inspect", reference))
    configuration_digest = native_configuration_digest(remote, reference, image_id)
    metadata = {"database_image": args.base_image, "repository_digest": reference,
                "image_id": configuration_digest, "manifest": manifest}
    with metadata_path.open("x") as file:
        file.write(json.dumps(metadata, separators=(",", ":")) + "\n")
    with reference_path.open("x") as file:
        file.write(reference + "\n")


if __name__ == "__main__":
    main()
