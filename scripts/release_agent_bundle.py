"""Package the small Agent installer independently of Controller binaries."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

from release_bundle import ROOT, write_archive
from release_selection import IMAGE, SEMVER
import re


def build(version, image, output):
    if not re.fullmatch(SEMVER, version) or not IMAGE.fullmatch(image):
        raise ValueError("Agent bundle requires a stable version and published image digest")
    output.mkdir(parents=True, exist_ok=True)
    tag = f"agent/v{version}"
    destinations = [output / "agent.json"]
    for arch in ("amd64", "arm64"):
        destinations.extend(output / f"groundplane-agent-{version}-linux-{arch}.tar.gz{suffix}"
                            for suffix in ("", ".sha256"))
    if any(path.exists() or path.is_symlink() for path in destinations):
        raise ValueError("Agent artifacts already exist; choose a new output directory")
    with (output / "agent.json").open("x") as manifest:
        manifest.write(json.dumps({"tag": tag, "image": image}, sort_keys=True) + "\n")
    source = Path(__file__).resolve().parent
    for arch in ("amd64", "arm64"):
        files = {name: (source / name).read_bytes() for name in
                 ("install_agent.py", "controller_update.py", "controller_release.py")}
        manifest = {"schema": 1, "version": version, "os": "linux", "arch": arch,
                    "agent_image": image,
                    "files": {name: hashlib.sha256(data).hexdigest() for name, data in files.items()}}
        files["bundle.json"] = (json.dumps(manifest, sort_keys=True, indent=2) + "\n").encode()
        path = output / f"groundplane-agent-{version}-linux-{arch}.tar.gz"
        write_archive(path, files)
        path.with_suffix(path.suffix + ".sha256").write_text(
            hashlib.sha256(path.read_bytes()).hexdigest() + "  " + path.name + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--output", type=Path, default=Path(".tmp/releases"))
    args = parser.parse_args()
    output = subprocess.check_output(
        ["bash", "-c", 'source scripts/repo-env.sh; repo_temp_dir "$1"', "--", str(args.output)],
        cwd=ROOT, text=True).strip()
    build(args.version, args.image, Path(output))
