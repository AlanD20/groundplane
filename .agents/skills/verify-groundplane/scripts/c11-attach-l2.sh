#!/usr/bin/env bash
set -euo pipefail

required_commands=(go cmp date mkdir mktemp rm)
for command_name in "${required_commands[@]}"; do
	command -v "$command_name" >/dev/null 2>&1 || {
		printf 'missing required command: %s\n' "$command_name" >&2
		exit 2
	}
done

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../../../.." && pwd)"
cd "$repo_root"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
evidence_root="${GROUNDPLANE_EVIDENCE_DIR:-$repo_root/.tmp/verify-groundplane}"
mkdir -p "$evidence_root"
[[ -d "$evidence_root" && ! -L "$evidence_root" ]] || {
	printf 'evidence parent must be a non-symlink directory: %s\n' "$evidence_root" >&2
	exit 2
}
evidence_root="$(cd "$evidence_root" && pwd)"
evidence_dir="$(mktemp -d "$evidence_root/c11-attach-l2-$timestamp.XXXXXX")"
status=1
runtime_dir=
runtime_cleanup=not-created

finish() {
	local exit_status=$?
	trap - EXIT
	if [[ "$exit_status" -ne 0 ]]; then
		status="$exit_status"
	fi
	if [[ -n "$runtime_dir" ]]; then
		runtime_cleanup=passed
		if [[ ! -d "$runtime_dir" || -L "$runtime_dir" ]] || ! rm -rf -- "$runtime_dir"; then
			runtime_cleanup=failed
			status=1
		fi
	fi
	printf 'exit_status=%s\nevidence_dir=%s\nexternal_adapter=hermetic-agent-runner\nruntime_cleanup=%s\nassertions=c11-attach-identity-ownership-facts-grants-retry-detach-isolation-openapi-parity\nunproven=live-external-service,live-docker,remote-controller,browser,console\n' \
		"$status" "$evidence_dir" "$runtime_cleanup" >"$evidence_dir/result.txt"
	printf 'C11 Attach L2 evidence: %s\n' "$evidence_dir"
	exit "$status"
}
trap finish EXIT

chmod 700 "$evidence_dir"
runtime_root="${GROUNDPLANE_VERIFY_RUNTIME_DIR:-${TMPDIR:-/tmp}}"
[[ -d "$runtime_root" && ! -L "$runtime_root" ]] || {
	printf 'runtime parent must be a non-symlink directory: %s\n' "$runtime_root" >&2
	exit 2
}
runtime_root="$(cd "$runtime_root" && pwd)"
runtime_dir="$(mktemp -d "$runtime_root/groundplane-c11-attach-l2-$timestamp.XXXXXX")"
chmod 700 "$runtime_dir"
go_cache="$runtime_dir/go-cache"
go_tmp="$runtime_dir/go-tmp"
mkdir -p "$go_cache" "$go_tmp"

test_packages=(
	./internal/adapters
	./internal/adapters/postgres16
	./internal/adapters/valkey9
	./internal/agent
	./internal/app
	./internal/controller
	./internal/infra/etcd
	./internal/cli
)
test_patterns=(
	'^(TestBuildFactsRendersTypedOwnAndGrantInputs|TestBuildFactsRejectsDuplicateSchemaAndKeepsManualFactless)$'
	'^(TestProvisionStepsCompileResolvedIdentity|TestGrantAndDetachStepsUseCorrectDatabaseContext)$'
	'^(TestProvisionStepsKeepPasswordOutOfArguments)$'
	'^(TestAdapterRuntimeExecutesCompiledPostgresProcedure)$'
	'^(Test.*Attach.*|Test.*Grant.*)$'
	'^(Test.*Attach.*|Test.*Detach.*)$'
	'^(Test.*Attach.*|Test.*Detach.*|TestIdempotencyRepositoryConcurrentClaimsHaveOneWinner|TestEnvironmentMutationFenceCASConflictsPerformNoWrites)$'
	'^(TestServiceAttachDispatchesTaskWithBothTargets)$'
)

: >"$evidence_dir/selected-tests.txt"
for index in "${!test_packages[@]}"; do
	package="${test_packages[$index]}"
	pattern="${test_patterns[$index]}"
	list_file="$evidence_dir/test-list-$index.txt"
	if ! GOCACHE="$go_cache" GOTMPDIR="$go_tmp" go test -list "$pattern" "$package" \
		>"$list_file" 2>&1; then
		status=1
		exit 1
	fi
	match_count=0
	while IFS= read -r test_name; do
		if [[ "$test_name" == Test* ]]; then
			((match_count += 1))
		fi
	done <"$list_file"
	if ((match_count == 0)); then
		printf 'test selection matched zero tests: package=%s pattern=%s\n' \
			"$package" "$pattern" >>"$evidence_dir/selected-tests.txt"
		status=1
		exit 1
	fi
	printf 'package=%s matched=%s pattern=%s\n' \
		"$package" "$match_count" "$pattern" >>"$evidence_dir/selected-tests.txt"
done

: >"$evidence_dir/focused-race.txt"
for index in "${!test_packages[@]}"; do
	if ! GOCACHE="$go_cache" GOTMPDIR="$go_tmp" go test -count=1 -race \
		-run "${test_patterns[$index]}" "${test_packages[$index]}" \
		>>"$evidence_dir/focused-race.txt" 2>&1; then
		status=1
		exit 1
	fi
done

if ! GOCACHE="$go_cache" GOTMPDIR="$go_tmp" go clean -cache \
	>"$evidence_dir/cache-between-phases.txt" 2>&1; then
	status=1
	exit 1
fi

if ! GOCACHE="$go_cache" GOTMPDIR="$go_tmp" go run ./internal/openapigen \
	-output "$evidence_dir/openapi.generated.json" \
	>"$evidence_dir/openapi-generate.txt" 2>&1; then
	status=1
	exit 1
fi
if ! cmp openapi.json "$evidence_dir/openapi.generated.json" \
	>"$evidence_dir/openapi-parity.txt" 2>&1; then
	status=1
	exit 1
fi
printf 'openapi.json matches production route generation byte-for-byte\n' \
	>"$evidence_dir/openapi-parity.txt"

status=0
