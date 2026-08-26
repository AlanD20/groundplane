#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
temp_root="$(mktemp -d)"
trap 'rm -rf -- "$temp_root"' EXIT

source "$script_dir/supervisor_wait.sh"
proc_root="$temp_root/proc"
mkdir -p "$proc_root/123"
stat_file="$proc_root/123/stat"

printf '123 (supervisor) R\n' >"$stat_file"
if wait_for_supervisor_exit 123 1 "$proc_root"; then
	printf 'running process must hit the polling deadline\n' >&2
	exit 1
else
	poll_status=$?
	[[ "$poll_status" -eq 1 ]]
fi

printf '123 (supervisor) Z\n' >"$stat_file"
wait_for_supervisor_exit 123 1 "$proc_root"
rm -f "$stat_file"
wait_for_supervisor_exit 123 1 "$proc_root"

printf '999 (supervisor) R\n' >"$stat_file"
if wait_for_supervisor_exit 123 1 "$proc_root"; then
	printf 'mismatched proc identity must be ambiguous\n' >&2
	exit 1
else
	ambiguous_status=$?
	[[ "$ambiguous_status" -eq 2 ]]
fi
