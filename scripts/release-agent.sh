#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=scripts/repo-env.sh
source "$SCRIPT_DIR/repo-env.sh"
repo_env_init
cd "$GROUNDPLANE_REPO_ROOT"

usage() {
	printf 'usage: %s --version VERSION --image REPOSITORY:TAG [--push]\n' "${0##*/}" >&2
}

version=
image=
push=false
while (($# > 0)); do
	case "$1" in
	--version)
		[[ $# -ge 2 && -z "$version" ]] || { usage; exit 2; }
		version=$2
		shift 2
		;;
	--image)
		[[ $# -ge 2 && -z "$image" ]] || { usage; exit 2; }
		image=$2
		shift 2
		;;
	--push)
		[[ "$push" == false ]] || { usage; exit 2; }
		push=true
		shift
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		usage
		exit 2
		;;
	esac
done

[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([-.][0-9A-Za-z.-]+)?$ ]] || {
	printf 'release Agent version must be an explicit release version: %s\n' "$version" >&2
	exit 2
}

repository=${image%:*}
tag=${image##*:}
[[ -n "$image" && "$image" != *@* && "$repository" != "$image" && -n "$repository" &&
	"$repository" =~ ^[a-z0-9]+([._-][a-z0-9]+)*(:[0-9]+)?(/[a-z0-9]+([._-][a-z0-9]+)*)*$ &&
	"$tag" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]] || {
	printf 'release Agent image must be an explicit non-digest REPOSITORY:TAG: %s\n' "$image" >&2
	exit 2
}

make agent-image AGENT_VERSION="$version" AGENT_IMAGE="$image" >&2

[[ "$(docker image inspect "$image" --format '{{json .Config.Entrypoint}}')" == '["/usr/local/bin/groundplane-agent"]' ]]
[[ "$(docker image inspect "$image" --format '{{json .Config.Cmd}}')" == 'null' ]]
[[ "$(docker image inspect "$image" --format '{{.Config.User}}')" == '0:0' ]]

docker_version="$(docker run --rm --entrypoint docker "$image" --version)"
[[ "$docker_version" == 'Docker version 29.1.3,'* ]]
compose_version="$(docker run --rm --entrypoint docker "$image" compose version --short)"
[[ "$compose_version" == '2.40.3' ]]

printf 'services:\n  probe:\n    image: scratch\n' |
	docker run --rm --interactive --entrypoint docker "$image" \
		compose --project-name groundplane-release-probe --file - config --quiet --no-interpolate

set +e
helper_output="$(docker run --rm "$image" compose-helper </dev/null 2>&1)"
helper_status=$?
set -e
[[ $helper_status -eq 1 && "$helper_output" == 'agent compose helper:'* ]]

if [[ "$push" == false ]]; then
	exit 0
fi

push_output="$(docker push "$image")"
printf '%s\n' "$push_output" >&2
registry_digest="$(
	printf '%s\n' "$push_output" |
		awk '{
			for (field = 1; field < NF; field++) {
				if ($field == "digest:" && $(field + 1) ~ /^sha256:[0-9a-f]+$/ && length($(field + 1)) == 71) {
					digest = $(field + 1)
				}
			}
		} END { print digest }'
)"
[[ "$registry_digest" =~ ^sha256:[0-9a-f]{64}$ ]] || {
	printf 'registry did not report an immutable digest for %s\n' "$image" >&2
	exit 1
}
published_ref="$repository@$registry_digest"
repo_digests="$(docker image inspect "$image" --format '{{range .RepoDigests}}{{println .}}{{end}}')"
confirmed=false
while IFS= read -r repo_digest; do
	if [[ "$repo_digest" == "$published_ref" ]]; then
		confirmed=true
		break
	fi
done <<<"$repo_digests"
[[ "$confirmed" == true ]] || {
	printf 'Docker did not record the pushed RepoDigest for %s\n' "$image" >&2
	exit 1
}
printf '%s\n' "$published_ref"
