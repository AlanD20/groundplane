#!/usr/bin/env bash
# Source for verifier setup, or run: bash scripts/repo-env.sh COMMAND [ARG...].

GROUNDPLANE_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"

repo_temp_dir() {
	local requested=$1 relative component current
	[[ "$requested" == /* ]] || requested="$GROUNDPLANE_REPO_ROOT/$requested"
	case "$requested" in
		"$GROUNDPLANE_REPO_ROOT"/.tmp|"$GROUNDPLANE_REPO_ROOT"/.tmp/*|"$GROUNDPLANE_REPO_ROOT"/.tmp-*) ;;
		*) printf 'temporary path must be inside repository .tmp/ or .tmp-*: %s\n' "$requested" >&2; return 2 ;;
	esac
	if [[ "$requested" == *$'\n'* || "$requested" == *$'\r'* || "$requested" == *//* || "$requested" == */ ]]; then
		printf 'temporary path must be canonical: %s\n' "$requested" >&2
		return 2
	fi
	relative=${requested#"$GROUNDPLANE_REPO_ROOT"/}
	case "/$relative/" in
		*/../*|*/./*) printf 'temporary path cannot contain dot segments: %s\n' "$requested" >&2; return 2 ;;
	esac
	current=$GROUNDPLANE_REPO_ROOT
	while [[ -n "$relative" ]]; do
		component=${relative%%/*}
		current="$current/$component"
		if [[ -L "$current" || ( -e "$current" && ! -d "$current" ) ]]; then
			printf 'temporary path must contain only non-symlink directories: %s\n' "$current" >&2
			return 2
		fi
		if [[ ! -d "$current" ]]; then
			mkdir -m 700 -- "$current" || [[ -d "$current" && ! -L "$current" ]] || return 2
		fi
		[[ "$relative" == */* ]] || break
		relative=${relative#*/}
	done
	printf '%s\n' "$requested"
}

repo_env_init() {
	TMPDIR=$(repo_temp_dir "${TMPDIR:-$GROUNDPLANE_REPO_ROOT/.tmp/tmp}") || return
	GOTMPDIR=$(repo_temp_dir "${GOTMPDIR:-$GROUNDPLANE_REPO_ROOT/.tmp/go-tmp}") || return
	GOCACHE=$(repo_temp_dir "${GOCACHE:-$GROUNDPLANE_REPO_ROOT/.tmp/go-cache}") || return
	repo_temp_dir "$GROUNDPLANE_REPO_ROOT/.tmp" >/dev/null || return
	GROUNDPLANE_COVERAGE_FILE="$GROUNDPLANE_REPO_ROOT/.tmp/coverage.out"
	if [[ -L "$GROUNDPLANE_COVERAGE_FILE" || ( -e "$GROUNDPLANE_COVERAGE_FILE" && ! -f "$GROUNDPLANE_COVERAGE_FILE" ) ]]; then
		printf 'coverage output must be a non-symlink regular file: %s\n' "$GROUNDPLANE_COVERAGE_FILE" >&2
		return 2
	fi
	export TMPDIR GOTMPDIR GOCACHE GROUNDPLANE_COVERAGE_FILE
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
	set -euo pipefail
	repo_env_init
	[[ $# -gt 0 ]] || { printf 'usage: bash scripts/repo-env.sh COMMAND [ARG...]\n' >&2; exit 2; }
	exec "$@"
fi
