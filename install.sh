#!/bin/sh
# Download verified prebuilt artifacts, then use GP's existing bootstrap/updater.
set -eu

usage() {
    printf '%s\n' \
        'Usage: sh install.sh [--version VERSION | --ref REF] [--listen-ip PRIVATE_IPV4]' \
        '       [--bundle FILE --sha256 HEX] [--config FILE] [--stage-only]' \
        'Ubuntu 24.04/26.04 or Debian 13, native amd64/arm64, root.' \
        'No source checkout or compiler needed.' \
        'Defaults to the latest published stable release; --version pins a release.' \
        '--ref builds a branch/tag/commit locally; requires Docker Engine.' \
        'Local --bundle installation requires --version and --sha256.' \
        'Fresh host: provision prerequisites and install. Existing host: guarded update.' \
        '--stage-only stages an existing installation without activating it.' \
        '--config supplies initial startup YAML only; existing configuration is preserved.'
}

resolve_version() {
    if test -n "$version"; then
        return
    fi
    release_url=$(curl --fail --silent --show-error --head --location \
        --proto '=https' --proto-redir '=https' --connect-timeout 15 --max-time 60 \
        --output /dev/null --write-out '%{url_effective}' \
        https://github.com/AlanD20/groundplane/releases/latest) || {
        echo 'Cannot resolve latest stable release; no bundle downloaded.' >&2; return 1;
    }
    case "$release_url" in
        https://github.com/AlanD20/groundplane/releases/tag/v*)
            version=${release_url#https://github.com/AlanD20/groundplane/releases/tag/v} ;;
        *) echo 'No valid latest stable release is available.' >&2; return 1 ;;
    esac
    printf '%s\n' "$version" | grep -Eq '^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$' &&
        test "${#version}" -le 100 || {
        echo 'Latest release does not have a stable vMAJOR.MINOR.PATCH tag.' >&2; return 1;
    }
}

version='' ref='' bundle='' checksum='' listen_ip=127.0.0.1 config='' stage_only=0
while test "$#" -gt 0; do
    case "$1" in
        --version|--ref|--bundle|--sha256|--listen-ip|--config)
            test "$#" -ge 2 && test -n "$2" || { usage >&2; exit 2; }
            case "$1" in
                --version) version=$2 ;;
                --ref) ref=$2 ;;
                --bundle) bundle=$2 ;;
                --sha256) checksum=$2 ;;
                --listen-ip) listen_ip=$2 ;;
                --config) config=$2 ;;
            esac
            shift 2 ;;
        --stage-only) stage_only=1; shift ;;
        --help|-h) usage; exit 0 ;;
        *) usage >&2; exit 2 ;;
    esac
done
if test -n "$ref"; then
    test -z "$version$bundle$checksum" || {
        echo '--ref cannot be combined with --version, --bundle or --sha256.' >&2; exit 2;
    }
    test "${#ref}" -le 255 || { echo 'Git ref is too long.' >&2; exit 2; }
    case "${DOCKER_HOST:-}" in
        ''|unix:///var/run/docker.sock) ;;
        *) echo '--ref requires the local Docker Engine.' >&2; exit 1 ;;
    esac
    test -z "${DOCKER_CONTEXT:-}" || { echo '--ref requires the default local Docker context.' >&2; exit 1; }
    export DOCKER_HOST=unix:///var/run/docker.sock
    if ! command -v docker >/dev/null || ! docker info >/dev/null; then
        echo '--ref requires a working local Docker Engine.' >&2; exit 1;
    fi
fi
if test -n "$version"; then
    printf '%s\n' "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+([-.][0-9A-Za-z.-]+)?$' &&
        test "${#version}" -le 100 || { echo 'Invalid release version.' >&2; exit 2; }
fi
if test -n "$checksum"; then
    printf '%s\n' "$checksum" | grep -Eq '^[0-9a-f]{64}$' || { echo 'Invalid SHA256.' >&2; exit 2; }
fi
if test -n "$bundle"; then
    test -n "$version" || { echo '--bundle requires --version.' >&2; exit 2; }
    test -f "$bundle" && test ! -L "$bundle" && test -n "$checksum" || {
        echo '--bundle requires a regular archive and its trusted --sha256.' >&2; exit 2;
    }
fi
test "$(id -u)" -eq 0 || { echo 'Run this installer as root.' >&2; exit 1; }
# shellcheck source=/dev/null
. /etc/os-release
case "${ID:-}:${VERSION_ID:-}" in
    ubuntu:24.04|ubuntu:26.04|debian:13) ;;
    *)
        echo 'Supported installation hosts: Ubuntu 24.04/26.04 or Debian 13.' >&2
        exit 1
        ;;
esac
case "$(uname -m)" in
    x86_64) arch=amd64 ;;
    aarch64|arm64) arch=arm64 ;;
    *) echo 'Unsupported architecture.' >&2; exit 1 ;;
esac
command -v systemctl >/dev/null
command -v flock >/dev/null
exec 9>/run/lock/groundplane-deploy.lock
flock -n 9 || { echo 'Another Groundplane deployment is active.' >&2; exit 1; }

# Fixed root-controlled work area; never extract or execute in a shared temp dir.
umask 077
for path in /root/.groundplane /root/.groundplane/.tmp; do
    test ! -L "$path" || { echo "Unsafe installation parent: $path" >&2; exit 1; }
    if test -e "$path"; then
        test -d "$path" && test "$(stat -c '%u:%a' "$path")" = 0:700 || {
            echo "Installation parent must be root-owned mode 0700: $path" >&2; exit 1;
        }
    else
        mkdir -m 0700 "$path"
    fi
done
deploy_id=$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')
deploy_dir=/root/.groundplane/.tmp/groundplane-deploy-$deploy_id
build_dir=/root/.groundplane/.tmp/groundplane-build-$deploy_id
mkdir -m 0700 "$deploy_dir"
finish() {
    status=$?
    trap - EXIT HUP INT TERM
    if test -d "$build_dir"; then
        rm -rf -- "$build_dir"
    fi
    if test -n "$ref" && test -d "$deploy_dir"; then
        rmdir -- "$deploy_dir" 2>/dev/null || true
    fi
    if test -d "$deploy_dir"; then
        echo "Installation files retained: $deploy_dir" >&2
    fi
    exit "$status"
}
trap finish EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
available_kib=$(df -Pk "$deploy_dir" | awk 'NR == 2 {print $4}')
test "${available_kib:-0}" -ge 2097152 || {
    echo 'Installation requires at least 2 GiB free before downloading.' >&2; exit 1;
}

# Supported minimal images may omit the downloader or Python. No Go/Node is installed.
packages=''
command -v python3 >/dev/null || packages="$packages python3"
if test -z "$bundle"; then
    command -v curl >/dev/null || packages="$packages curl"
    test -s /etc/ssl/certs/ca-certificates.crt || packages="$packages ca-certificates"
fi
if test -n "$packages"; then
    export DEBIAN_FRONTEND=noninteractive
    apt-get update </dev/null
    # Split only the fixed package names assembled above, never user input.
    # shellcheck disable=SC2086
    apt-get install -y $packages </dev/null
fi
if test -n "$ref"; then
    test "${available_kib:-0}" -ge 10485760 || {
        echo '--ref requires at least 10 GiB free for its disposable build.' >&2; exit 1;
    }
    mkdir -m 0700 "$build_dir"
    encoded_ref=$(python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$ref")
    curl --fail --silent --show-error --location --proto '=https' --proto-redir '=https' \
        --connect-timeout 15 --max-time 60 --max-filesize 2097152 \
        "https://api.github.com/repos/AlanD20/groundplane/commits/$encoded_ref" --output "$build_dir/commit.json"
    commit=$(python3 -c '
import json, re, sys
sha = json.load(open(sys.argv[1])).get("sha", "")
if not re.fullmatch(r"[0-9a-f]{40}", sha):
    raise SystemExit("GitHub did not return an exact commit")
print(sha)
' "$build_dir/commit.json")
    echo "Building Groundplane ref $ref at $commit ($arch)." >&2
    curl --fail --location --proto '=https' --proto-redir '=https' --connect-timeout 15 \
        --max-time 600 --max-filesize 209715200 \
        "https://codeload.github.com/AlanD20/groundplane/tar.gz/$commit" --output "$build_dir/source.tar.gz"
    python3 -c '
import pathlib, sys, tarfile
root = pathlib.Path(sys.argv[1]) / "source"
root.mkdir(mode=0o700)
prefix = "groundplane-" + sys.argv[2]
total = 0
seen = set()
with tarfile.open(root.parent / "source.tar.gz", "r:gz") as archive:
    for member in archive:
        parts = pathlib.PurePosixPath(member.name).parts
        if not parts or parts[0] != prefix or ".." in parts or member.name in seen:
            raise SystemExit("Unsafe source archive path")
        seen.add(member.name)
        target = root.joinpath(*parts[1:])
        if member.isdir():
            target.mkdir(parents=True, exist_ok=True)
        elif member.isfile() and 0 <= member.size <= 67108864:
            total += member.size
            if total > 536870912:
                raise SystemExit("Source archive exceeds unpacked limit")
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("xb") as output:
                import shutil
                shutil.copyfileobj(archive.extractfile(member), output)
            target.chmod(0o700 if member.mode & 0o111 else 0o600)
        else:
            raise SystemExit("Unsupported source archive member")
' "$build_dir" "$commit"
    test -f "$build_dir/source/scripts/install_ref.py" || {
        echo 'Selected commit predates --ref installation support.' >&2; exit 1;
    }
    set -- --deploy-dir "$deploy_dir" --commit "$commit" --listen-ip "$listen_ip"
    test "$stage_only" -eq 0 || set -- "$@" --stage-only
    test -z "$config" || set -- "$@" --config "$config"
    export PYTHONDONTWRITEBYTECODE=1
    python3 "$build_dir/source/scripts/install_ref.py" "$@"
    exit 0
fi
resolve_version
echo "Selected Groundplane $version ($arch)." >&2
asset=groundplane-$version-linux-$arch.tar.gz
if test -n "$bundle"; then
    test "$(stat -c '%s' "$bundle")" -le 536870912 || {
        echo 'Local archive exceeds 512 MiB limit.' >&2; exit 1;
    }
    cp -- "$bundle" "$deploy_dir/bundle.tar.gz"
else
    base=https://github.com/AlanD20/groundplane/releases/download/v$version
    curl --fail --location --proto '=https' --proto-redir '=https' --connect-timeout 15 \
        --max-time 600 --max-filesize 536870912 --output "$deploy_dir/bundle.tar.gz" "$base/$asset"
    if test -z "$checksum"; then
        curl --fail --location --proto '=https' --proto-redir '=https' --connect-timeout 15 \
            --max-time 60 --max-filesize 1024 --output "$deploy_dir/checksum" "$base/$asset.sha256"
        checksum=$(awk -v name="$asset" 'NF == 2 && $2 == name { print $1 }' "$deploy_dir/checksum")
    fi
fi
printf '%s\n' "$checksum" | grep -Eq '^[0-9a-f]{64}$' || { echo 'Missing/invalid release checksum.' >&2; exit 1; }
actual=$(sha256sum "$deploy_dir/bundle.tar.gz" | cut -d ' ' -f 1)
test "$actual" = "$checksum" || { echo 'Archive SHA256 mismatch; nothing executed.' >&2; exit 1; }

# Validate the complete archive before extracting any executable. No links,
# nested paths, duplicate names, devices or unbounded unpacked input are allowed.
python3 -c '
import json, pathlib, sys, tarfile
root = pathlib.Path(sys.argv[1])
allowed = {"controller", "groundplane", "controller-release.json", "controller_release.py",
           "controller_bootstrap.py", "controller_update.py", "groundplane-controller.service",
           "groundplane.conf", "controller.yaml.example", "install_bundle.py",
           "install-runtime.sh", "setup-host.sh", "bundle.json"}
with tarfile.open(root / "bundle.tar.gz", "r:gz") as archive:
    members = []
    names = set()
    total = 0
    for item in archive:
        if item.name not in allowed or item.name in names or not item.isfile():
            raise SystemExit("Unsafe archive member")
        if not 0 < item.size <= 268435456:
            raise SystemExit("Invalid member size")
        total += item.size
        if total > 536870912:
            raise SystemExit("Archive exceeds unpacked limit")
        names.add(item.name)
        members.append(item)
    if names != allowed:
        raise SystemExit("Incomplete release archive")
    manifest = json.load(archive.extractfile("bundle.json"))
    if manifest.get("version") != sys.argv[2] or manifest.get("arch") != sys.argv[3] or manifest.get("os") != "linux":
        raise SystemExit("Wrong release version/platform")
    if set(manifest.get("files", {})) != allowed - {"bundle.json"}:
        raise SystemExit("Incomplete release manifest")
    import hashlib
    for item in members:
        data = archive.extractfile(item).read()
        if item.name != "bundle.json" and hashlib.sha256(data).hexdigest() != manifest["files"][item.name]:
            raise SystemExit("Bundle checksum mismatch")
        with (root / item.name).open("xb") as output:
            output.write(data)
' "$deploy_dir" "$version" "$arch"
chmod 0700 "$deploy_dir/controller" "$deploy_dir/groundplane"
set -- --version "$version" --listen-ip "$listen_ip"
test "$stage_only" -eq 0 || set -- "$@" --stage-only
test -z "$config" || set -- "$@" --config "$config"
export PYTHONDONTWRITEBYTECODE=1
python3 "$deploy_dir/install_bundle.py" "$@"
