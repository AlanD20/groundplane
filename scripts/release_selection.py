"""Resolve published stable GP releases within one explicit component namespace."""
import json
import re
import urllib.parse
import urllib.request

REPOSITORY = "AlanD20/groundplane"
SEMVER = r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
IMAGE = re.compile(r"ghcr\.io/aland20/groundplane-agent@sha256:[0-9a-f]{64}")


def read_json(url):
    request = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json",
                                                 "User-Agent": "groundplane-installer"})
    with urllib.request.urlopen(request, timeout=30) as response:
        if urllib.parse.urlsplit(response.url).scheme != "https":
            raise ValueError("release download left HTTPS")
        raw = response.read(2 * 1024 * 1024 + 1)
    if len(raw) > 2 * 1024 * 1024:
        raise ValueError("release metadata exceeds size limit")
    return json.loads(raw)


def latest(component):
    prefix = f"{component}/v" if component else "v"
    matches = []
    for page in range(1, 101):
        releases = read_json(f"https://api.github.com/repos/{REPOSITORY}/releases?per_page=100&page={page}")
        if not isinstance(releases, list):
            raise ValueError("invalid release list")
        for release in releases:
            tag = release.get("tag_name", "")
            if not release.get("draft") and not release.get("prerelease") and re.fullmatch(re.escape(prefix) + SEMVER, tag):
                matches.append((tuple(map(int, tag.removeprefix(prefix).split("."))), tag))
        if len(releases) < 100:
            if not matches:
                raise ValueError(f"no published stable {component or 'combined'} release exists")
            return max(matches)[1]
    raise ValueError("release catalog exceeds pagination limit; select an explicit version")


def agent_image(tag=None):
    tag = tag or latest("agent")
    if not re.fullmatch("agent/v" + SEMVER, tag):
        raise ValueError("Agent updates require an agent/vMAJOR.MINOR.PATCH release")
    encoded = urllib.parse.quote(tag, safe="")
    release = read_json(f"https://api.github.com/repos/{REPOSITORY}/releases/tags/{encoded}")
    if release.get("tag_name") != tag or release.get("draft") or release.get("prerelease"):
        raise ValueError("Agent release is not published and stable")
    expected = f"https://github.com/{REPOSITORY}/releases/download/{encoded}/agent.json"
    assets = [asset for asset in release.get("assets", []) if asset.get("name") == "agent.json"]
    if len(assets) != 1:
        raise ValueError("Agent release has no unique image metadata")
    # Construct the URL from trusted repository/tag inputs, not arbitrary asset URLs.
    metadata = read_json(expected)
    if metadata.get("tag") != tag or not IMAGE.fullmatch(metadata.get("image", "")):
        raise ValueError("Agent release image metadata is invalid")
    return metadata["image"]
