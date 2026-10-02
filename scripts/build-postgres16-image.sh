#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
# shellcheck source=scripts/repo-env.sh
source "$script_dir/repo-env.sh"
repo_env_init
cd "$GROUNDPLANE_REPO_ROOT"

usage() {
  printf 'usage: bash scripts/build-postgres16-image.sh --base-image postgres:16-alpine@sha256:DIGEST --image REPOSITORY:TAG\n' >&2
}

base_image=
image=
while (($#)); do
  case "$1" in
    --base-image) [[ $# -ge 2 && -z "$base_image" ]] || { usage; exit 2; }; base_image=$2; shift 2 ;;
    --image) [[ $# -ge 2 && -z "$image" ]] || { usage; exit 2; }; image=$2; shift 2 ;;
    *) usage; exit 2 ;;
  esac
done
[[ "$base_image" =~ ^(docker.io/library/)?postgres:16-alpine@sha256:[0-9a-f]{64}$ ]] || { usage; exit 2; }
[[ "$image" =~ ^[a-z0-9][a-z0-9._:/-]*:[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ && "$image" != *@* ]] || {
  usage; exit 2;
}
case "$(uname -sm)" in
  'Linux x86_64') arch=amd64 ;;
  'Linux aarch64') arch=arm64 ;;
  *) printf 'managed PostgreSQL must be built natively on Linux amd64 or arm64\n' >&2; exit 2 ;;
esac

docker build --platform "linux/$arch" --file Dockerfile.postgres16 \
  --build-arg "BUILDARCH=$arch" --build-arg "TARGETARCH=$arch" \
  --build-arg "POSTGRES16_BASE=$base_image" --tag "$image" .
printf 'Built native linux/%s image %s. Not published or qualified.\n' "$arch" "$image"
printf 'Release metadata is embedded at /usr/local/share/groundplane/postgres16-release.json.\n'
