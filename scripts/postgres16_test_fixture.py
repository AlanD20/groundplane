"""Deterministic managed PostgreSQL catalog data for host-free script tests."""
from __future__ import annotations

import base64
import json


def postgres16_catalog(*, published: bool = True) -> bytes:
    repository = "ghcr.io/test/groundplane-postgres16-tools"
    architectures = ("amd64", "arm64") if published else ("amd64",)
    index = repository + "@sha256:" + "c" * 64
    images = []
    for offset, architecture in enumerate(architectures):
        manifest_fill = chr(ord("1") + offset)
        image_fill = ("f", "a")[offset]
        images.append({
            "repository_digest": repository + "@sha256:" + chr(ord("d") + offset) * 64,
            "image_id": "sha256:" + image_fill * 64,
            "manifest": {
                "schema": 1,
                "os": "linux",
                "architecture": architecture,
                "postgresql_major": 16,
                "helper_sha256": manifest_fill * 64,
                "gate_sha256": manifest_fill * 64,
                "pg_dump_sha256": manifest_fill * 64,
                "pg_restore_sha256": manifest_fill * 64,
                "psql_sha256": manifest_fill * 64,
                # Fixed production launch profile; all image-file digests are synthetic.
                "launch_profile_sha256": "40161e06b32ea073fcec892434b191598277dd16029866f5b013b9cfbc8da275",
                "gate_seccomp_sha256": manifest_fill * 64,
            },
        })
    catalog = {
        "schema": 1,
        "image": index if published else images[0]["repository_digest"],
        "database_image": "postgres:16-alpine@sha256:" + "b" * 64,
        "images": images,
    }
    return json.dumps(catalog, separators=(",", ":")).encode()


def postgres16_release_base64() -> str:
    return base64.b64encode(postgres16_catalog(published=False)).decode("ascii")
