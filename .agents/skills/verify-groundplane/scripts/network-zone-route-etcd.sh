#!/usr/bin/env bash
set -euo pipefail

required_commands=(go mktemp date mkdir head rm)
for command_name in "${required_commands[@]}"; do
	command -v "$command_name" >/dev/null 2>&1 || {
		printf 'missing required command: %s\n' "$command_name" >&2
		exit 2
	}
done

: "${GROUNDPLANE_ETCD_ENDPOINT:?GROUNDPLANE_ETCD_ENDPOINT is required}"
: "${GROUNDPLANE_C07_ETCD_PREFIX:?GROUNDPLANE_C07_ETCD_PREFIX is required}"
[[ "$GROUNDPLANE_C07_ETCD_PREFIX" == /groundplane-c07-acceptance/run-*/ ]] || {
	printf 'GROUNDPLANE_C07_ETCD_PREFIX must match /groundplane-c07-acceptance/run-*/\n' >&2
	exit 2
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../../../.." && pwd)"
cd "$repo_root"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
evidence_root="${GROUNDPLANE_EVIDENCE_DIR:-$repo_root/.tmp/verify-groundplane}"
mkdir -p "$evidence_root"
evidence_dir="$(mktemp -d "$evidence_root/network-c07-$timestamp.XXXXXX")"
runtime_dir="$(mktemp -d "${TMPDIR:-/tmp}/groundplane-c07-runtime.XXXXXX")"

cleanup() {
	status=$?
	runtime_cleanup=failed
	if [[ "$runtime_dir" == "${TMPDIR:-/tmp}"/groundplane-c07-runtime.* && -d "$runtime_dir" ]]; then
		rm -rf -- "$runtime_dir"
		[[ ! -e "$runtime_dir" ]] && runtime_cleanup=passed
	fi
	printf 'exit_status=%s\nevidence_dir=%s\netcd_endpoint=%s\netcd_prefix=%s\nruntime_cleanup=%s\nassertions=real-etcd-concurrent-replay,subnet-isolation,restart-durability\nunproven=deployed-api-cli-console,agent-host-effects,c12-c14-component-controls\n' \
		"$status" "$evidence_dir" "$GROUNDPLANE_ETCD_ENDPOINT" "$GROUNDPLANE_C07_ETCD_PREFIX" \
		"$runtime_cleanup" >"$evidence_dir/result.txt"
	trap - EXIT
	exit "$status"
}
trap cleanup EXIT

mkdir -p "$runtime_dir/go-cache" "$runtime_dir/go-tmp"
GOCACHE="$runtime_dir/go-cache" GOTMPDIR="$runtime_dir/go-tmp" \
	go test -tags c07_network_l2 ./internal/infra/etcd/network \
		-run '^TestC07RealEtcdConcurrentReplayAndRestart$' -count=1 -v 2>&1 \
	| head -c 2097152 >"$evidence_dir/go-test.txt"
