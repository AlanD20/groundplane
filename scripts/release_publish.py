"""Publish scoped release assets; a combined tag publishes both component tags too."""
import argparse
from pathlib import Path
import re
import subprocess
import urllib.error
import urllib.parse

from release_selection import REPOSITORY, SEMVER, read_json
from bump_version import release_notes


def run(*args):
    return subprocess.check_output(args, text=True, timeout=120).strip()


def publish(tag, root, *, check=False):
    match = re.fullmatch(r"(?:(agent|controller)/)?v(" + SEMVER + r")", tag)
    if not match:
        raise ValueError("release tag must be vX.Y.Z, agent/vX.Y.Z or controller/vX.Y.Z")
    scope, version = match[1], match[2]
    commit = run("git", "rev-parse", "HEAD")
    roles = [scope] if scope else ["agent", "controller", "both"]
    plans = []
    for role in roles:
        selected = f"{role}/v{version}" if role != "both" else f"v{version}"
        agent = sorted(root.glob(f"groundplane-agent-{version}-linux-*.tar.gz*"))
        controller = sorted(root.glob(f"groundplane-{version}-linux-*.tar.gz*"))
        files = (agent + [root / "agent.json"] if role == "agent" else list(controller))
        if role == "both":
            files += agent + [root / "agent.json"]
        if not check and (len(agent) != 4 and role != "controller" or len(controller) != 4 and role != "agent"):
            raise ValueError(f"incomplete {role} release artifacts")
        if not check and any(not file.is_file() for file in files):
            raise ValueError("release artifact is absent")
        refs = run("git", "ls-remote", "--tags", "origin", f"refs/tags/{selected}", f"refs/tags/{selected}^{{}}")
        if refs and refs.splitlines()[-1].split()[0] != commit:
            raise ValueError(f"existing tag {selected} points to another commit; never overwrite it")
        try:
            read_json(f"https://api.github.com/repos/{REPOSITORY}/releases/tags/{urllib.parse.quote(selected, safe='')}")
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise
        else:
            raise ValueError(f"release {selected} already exists; never overwrite assets")
        plans.append((selected, role, files, bool(refs)))
    for selected, role, files, exists in ([] if check else plans):
        notes = (f"Independent Agent release {version}. See CHANGELOG.md for features and limits."
                 if role == "agent" else release_notes(Path(__file__).resolve().parents[1], version))
        notes = notes.replace(f"/blob/v{version}/", f"/blob/{urllib.parse.quote(selected, safe='')}/")
        if not exists:
            # GITHUB_TOKEN-created refs do not recursively trigger push workflows.
            run("gh", "api", "--method", "POST", "repos/{owner}/{repo}/git/refs",
                "-f", f"ref=refs/tags/{selected}", "-f", f"sha={commit}")
        run("gh", "release", "create", selected, "--verify-tag", "--title", f"Groundplane {selected}",
            "--notes", notes,
            "--latest=" + ("true" if role == "both" else "false"), *map(str, files), "install.sh")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tag")
    parser.add_argument("--artifacts", type=Path, default=Path(".tmp/releases"))
    parser.add_argument("--check", action="store_true", help="check tag/release conflicts before building images")
    args = parser.parse_args()
    publish(args.tag, args.artifacts, check=args.check)
