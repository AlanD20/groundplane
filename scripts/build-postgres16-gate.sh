#!/usr/bin/env bash
set -euo pipefail

# Invoke once inside each native linux/amd64 and linux/arm64 release build.
# The release packager copies and measures the resulting binary; this script
# never selects a runtime release authority or publishes an image.
if [[ $# -ne 0 ]]; then
  echo 'usage: bash scripts/build-postgres16-gate.sh' >&2
  exit 2
fi

case "$(uname -sm)" in
  'Linux x86_64') arch=amd64 ;;
  'Linux aarch64') arch=arm64 ;;
  *) echo 'postgres16 gate requires native Linux amd64 or arm64' >&2; exit 2 ;;
esac

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
output_dir="$repo_root/.tmp/postgres16-gate/$arch"
mkdir -p "$output_dir"

bash "$repo_root/scripts/repo-env.sh" cc -std=c11 -O2 -Wall -Wextra -Werror -D_FORTIFY_SOURCE=2 \
  -fstack-protector-strong -fPIE -pie \
  -o "$output_dir/groundplane-postgres16-client-gate" \
  "$repo_root/cmd/postgres16-gate/main.c" -lcrypto
