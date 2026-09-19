#!/usr/bin/env python3
"""Publish native image pairs and verify registry-readback platform identities."""
import json
import os
from pathlib import Path
import re
import subprocess


def checked_index(index: dict, children: dict[str, str]) -> None:
    manifests = index.get("manifests", [])
    if len(manifests) != 2:
        raise ValueError("release index must contain exactly two native children")
    seen = set()
    for child in manifests:
        platform = child.get("platform", {})
        arch = platform.get("architecture")
        if (arch not in children or arch in seen or platform.get("os") != "linux"
                or platform.get("variant") not in ((None, "v8") if arch == "arm64" else (None,))
                or child.get("digest") != children[arch].split("@", 1)[1]):
            raise ValueError("registry index differs from the tested native images")
        seen.add(arch)


def output(*args: str) -> str:
    return subprocess.check_output(args, text=True, timeout=120).strip()


def main() -> None:
    root, version = os.environ["IMAGE_ROOT"], os.environ["VERSION"]
    for kind in ("agent", "runner"):
        repository = f"{root}-{kind}"
        children = {arch: Path(f".tmp/release-images/{kind}-{arch}").read_text().strip()
                    for arch in ("amd64", "arm64")}
        if any(not re.fullmatch(re.escape(repository) + r"@sha256:[0-9a-f]{64}", ref)
               for ref in children.values()) or len(set(children.values())) != 2:
            raise ValueError("native build did not supply two distinct repository digests")
        tag = f"{repository}:{version}"
        subprocess.run(["docker", "manifest", "create", tag, *children.values()],
                       check=True, timeout=120)
        digest = output("docker", "manifest", "push", tag).splitlines()[-1]
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
            raise ValueError("registry did not return an index digest")
        ref = f"{repository}@{digest}"
        checked_index(json.loads(output("docker", "manifest", "inspect", ref)), children)
        with open(os.environ["GITHUB_OUTPUT"], "a") as result:
            result.write(f"{kind}={ref}\n")


if __name__ == "__main__":
    main()
