#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
temp_root="$(mktemp -d)"
trap 'rm -rf -- "$temp_root"' EXIT

source_file="$temp_root/trusted_known_hosts"
destination_file="$temp_root/evidence-known_hosts"
printf 'test-host ssh-ed25519 AAAA\n' >"$source_file"

# Exercise the same shell function used by the verification journey.
# shellcheck source=/dev/null
source "$script_dir/known_hosts_init.sh"
copy_known_hosts "$source_file" "$destination_file"
cmp -s "$source_file" "$destination_file"
[[ "$(stat -c %a "$destination_file")" == 600 ]]

symlink_source="$temp_root/known_hosts.symlink"
ln -s "$source_file" "$symlink_source"
if copy_known_hosts "$symlink_source" "$temp_root/symlink-output"; then
	printf 'symlink known-host input must fail\n' >&2
	exit 1
fi

fifo_source="$temp_root/known_hosts.fifo"
mkfifo "$fifo_source"
if copy_known_hosts "$fifo_source" "$temp_root/fifo-output"; then
	printf 'FIFO known-host input must fail\n' >&2
	exit 1
fi

oversize_source="$temp_root/known_hosts.oversize"
python3 -c 'from pathlib import Path; import sys; Path(sys.argv[1]).write_bytes(b"x" * 2097153)' \
	"$oversize_source"
if copy_known_hosts "$oversize_source" "$temp_root/oversize-output"; then
	printf 'oversize known-host input must fail\n' >&2
	exit 1
fi

atomic_failure_destination="$temp_root/atomic-destination"
mkdir "$atomic_failure_destination"
if copy_known_hosts "$source_file" "$atomic_failure_destination"; then
	printf 'unwritable atomic destination must fail\n' >&2
	exit 1
fi
[[ ! -e "$atomic_failure_destination.tmp" ]]
