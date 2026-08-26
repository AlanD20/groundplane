#!/usr/bin/env bash
# shellcheck disable=SC2016
set -euo pipefail

required_commands=(
	ssh curl jq cmp go mktemp python3 sha256sum timeout date mkdir chmod head
	dirname rm sleep cp cat tr wc
)
for command_name in "${required_commands[@]}"; do
	command -v "$command_name" >/dev/null 2>&1 || {
		printf 'missing required command: %s\n' "$command_name" >&2
		exit 2
	}
done

: "${GROUNDPLANE_SSH_TARGET:?GROUNDPLANE_SSH_TARGET is required}"
: "${GROUNDPLANE_SSH_KEY:?GROUNDPLANE_SSH_KEY is required}"
: "${GROUNDPLANE_SSH_KNOWN_HOSTS:?GROUNDPLANE_SSH_KNOWN_HOSTS is required}"
: "${GROUNDPLANE_TENANT_ID:?GROUNDPLANE_TENANT_ID is required}"
: "${GROUNDPLANE_PROJECT_ID:?GROUNDPLANE_PROJECT_ID is required}"
: "${GROUNDPLANE_ENVIRONMENT_ID:?GROUNDPLANE_ENVIRONMENT_ID is required}"
: "${GROUNDPLANE_VOLUME_SLUG:?GROUNDPLANE_VOLUME_SLUG is required}"
: "${GROUNDPLANE_VOLUME_RENAMED_SLUG:?GROUNDPLANE_VOLUME_RENAMED_SLUG is required}"

ssh_port="${GROUNDPLANE_SSH_PORT:-22}"
remote_service="groundplane-controller.service"
remote_http_addr="${GROUNDPLANE_REMOTE_HTTP_ADDR:-127.0.0.1:8080}"
volume_key="${GROUNDPLANE_VOLUME_COMPOSE_KEY:-$GROUNDPLANE_VOLUME_SLUG}"

valid_port() {
	[[ "$1" =~ ^[0-9]{1,5}$ ]] && ((10#$1 >= 1 && 10#$1 <= 65535))
}

valid_id() {
	[[ "$1" =~ ^[A-Za-z0-9_-]{1,128}$ ]]
}

valid_slug() {
	[[ "$1" =~ ^[a-z0-9][a-z0-9-]{0,62}$ ]]
}

valid_key() {
	[[ "$1" =~ ^[a-z0-9][a-z0-9_-]{0,62}$ ]]
}

[[ "$GROUNDPLANE_SSH_TARGET" =~ ^[A-Za-z0-9_.@:-]+$ ]] || {
	printf 'GROUNDPLANE_SSH_TARGET contains unsupported characters\n' >&2
	exit 2
}
[[ "$GROUNDPLANE_SSH_TARGET" != -* && "${#GROUNDPLANE_SSH_TARGET}" -le 1024 ]] || {
	printf 'GROUNDPLANE_SSH_TARGET is invalid\n' >&2
	exit 2
}
[[ -f "$GROUNDPLANE_SSH_KEY" && ! -L "$GROUNDPLANE_SSH_KEY" ]] || {
	printf 'SSH key must be a non-symlink regular file\n' >&2
	exit 2
}
[[ -f "$GROUNDPLANE_SSH_KNOWN_HOSTS" && ! -L "$GROUNDPLANE_SSH_KNOWN_HOSTS" ]] || {
	printf 'known-hosts input must be a non-symlink regular file\n' >&2
	exit 2
}
valid_port "$ssh_port" || {
	printf 'GROUNDPLANE_SSH_PORT must be numeric\n' >&2
	exit 2
}
if ! [[ "$remote_http_addr" =~ ^127\.0\.0\.1:[0-9]+$ ]] ||
	! valid_port "${remote_http_addr##*:}"; then
	printf 'GROUNDPLANE_REMOTE_HTTP_ADDR must be a 127.0.0.1 TCP address\n' >&2
	exit 2
fi
for value_name in GROUNDPLANE_TENANT_ID GROUNDPLANE_PROJECT_ID GROUNDPLANE_ENVIRONMENT_ID; do
	value="${!value_name}"
	valid_id "$value" || {
		printf '%s contains unsupported characters\n' "$value_name" >&2
		exit 2
	}
done
valid_slug "$GROUNDPLANE_VOLUME_SLUG" || {
	printf 'GROUNDPLANE_VOLUME_SLUG is invalid\n' >&2
	exit 2
}
valid_slug "$GROUNDPLANE_VOLUME_RENAMED_SLUG" || {
	printf 'GROUNDPLANE_VOLUME_RENAMED_SLUG is invalid\n' >&2
	exit 2
}
valid_key "$volume_key" || {
	printf 'GROUNDPLANE_VOLUME_COMPOSE_KEY is invalid\n' >&2
	exit 2
}
[[ "$GROUNDPLANE_VOLUME_SLUG" != "$GROUNDPLANE_VOLUME_RENAMED_SLUG" ]] || {
	printf 'Volume slugs must differ for the rename assertion\n' >&2
	exit 2
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$script_dir/supervisor_wait.sh"
repo_root="$(cd "$script_dir/../../../.." && pwd)"
cd "$repo_root"

[[ -d /proc && -r /proc && -x /proc && -r "/proc/$$/stat" ]] || {
	printf 'readable Linux procfs is required for supervisor ownership polling\n' >&2
	exit 2
}

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
evidence_root="${GROUNDPLANE_EVIDENCE_DIR:-$repo_root/.tmp/verify-groundplane}"
mkdir -p "$evidence_root"
[[ -d "$evidence_root" && ! -L "$evidence_root" ]] || {
	printf 'evidence parent must be a non-symlink directory: %s\n' "$evidence_root" >&2
	exit 2
}
evidence_root="$(cd "$evidence_root" && pwd)"
evidence_dir="$(mktemp -d "$evidence_root/volume-lifecycle-$timestamp.XXXXXX")"
chmod 700 "$evidence_dir"

runtime_root=""
runtime_dir=""
control_socket=""
supervisor_pid=""
supervisor_started=0
supervisor_launching=0
stop_file=""
ready_file=""
stopped_file=""
failure_file=""
api_base="not-created"
created_volume_id="not-created"
manual_cleanup="not-required"

ssh_options=(
	-F /dev/null
	-p "$ssh_port"
	-i "$GROUNDPLANE_SSH_KEY"
	-o BatchMode=yes
	-o ConnectTimeout=10
	-o IdentitiesOnly=yes
	-o ProxyCommand=none
	-o StrictHostKeyChecking=yes
	-o VerifyHostKeyDNS=no
	-o GlobalKnownHostsFile=/dev/null
	-o "UserKnownHostsFile=$evidence_dir/known_hosts"
)

request_stop() {
	[[ -n "$stop_file" ]] || return 0
	printf 'stop\n' >"$stop_file"
}

handle_signal() {
	request_stop
	exit 130
}

cleanup() {
	local status=$?
	set +e
	retain_runtime=0
	request_stop
	supervisor_cleanup="not-started"
	if [[ "$supervisor_launching" -eq 1 ]]; then
		supervisor_cleanup="launch-race"
		retain_runtime=1
		printf 'supervisor_launching=1\n' >"$evidence_dir/supervisor-stop-status.txt"
	elif [[ "$supervisor_started" -eq 1 ]]; then
		supervisor_cleanup="waiting"
		supervisor_wait_status=125
		wait_for_supervisor_exit "$supervisor_pid" 6300 /proc
		supervisor_poll_status=$?
		if [[ "$supervisor_poll_status" -eq 0 ]]; then
			wait "$supervisor_pid" >"$evidence_dir/supervisor-wait.txt" 2>&1
			supervisor_wait_status=$?
			printf 'pid=%s\nwait_status=%s\n' "$supervisor_pid" "$supervisor_wait_status" \
				>"$evidence_dir/supervisor-stop-status.txt"
		fi
		if [[ "$supervisor_poll_status" -eq 0 && "$supervisor_wait_status" -eq 0 && \
			-f "$stopped_file" ]]; then
			supervisor_cleanup="passed"
		else
			[[ "$supervisor_poll_status" -eq 2 ]] && supervisor_cleanup="ambiguous-proc" ||
				supervisor_cleanup="timeout-or-failed"
			retain_runtime=1
			[[ "$status" -eq 0 ]] && status=1
			if [[ "$supervisor_poll_status" -ne 0 ]]; then
				printf 'supervisor_wait=deadline-or-ambiguous\n' >"$evidence_dir/supervisor-wait.txt"
			fi
		fi
	fi
	runtime_cleanup="not-created"
	if [[ "$retain_runtime" -eq 1 ]]; then
		runtime_cleanup="retained"
	elif [[ -n "$runtime_dir" && -n "$runtime_root" && \
		"$runtime_dir" == "$runtime_root"/groundplane-volume-runtime.* && \
		-d "$runtime_dir" && ! -L "$runtime_dir" ]]; then
		rm -rf -- "$runtime_dir"
		[[ ! -e "$runtime_dir" ]] && runtime_cleanup="passed" || runtime_cleanup="failed"
	fi
	[[ "$runtime_cleanup" == "failed" ]] && [[ "$status" -eq 0 ]] && status=1
	[[ "$created_volume_id" == "not-created" || "$status" -eq 0 ]] || manual_cleanup="required"
	printf 'exit_status=%s\nevidence_dir=%s\ntarget=%s\napi_base=%s\ncreated_volume_id=%s\nmanual_cleanup=%s\nruntime_dir=%s\ncontrol_socket=%s\nsupervisor_pid=%s\nservice_mutation=none\ntunnel_cleanup=%s\nruntime_cleanup=%s\nassertions=volume-api-cli-lifecycle-stable-id-key-path-task-host-cleanup-replay\nunproven=crash-injection,controller-restart,agent-reconnect,normalized-projection-overflow,console,public-exposure,repository-ci\n' \
		"$status" "$evidence_dir" "$GROUNDPLANE_SSH_TARGET" "$api_base" \
		"$created_volume_id" "$manual_cleanup" "${runtime_dir:-not-created}" \
		"${control_socket:-not-created}" "${supervisor_pid:-not-created}" \
		"$supervisor_cleanup" "$runtime_cleanup" >"$evidence_dir/result.txt"
	trap - EXIT
	exit "$status"
}
trap cleanup EXIT
trap handle_signal HUP INT TERM

runtime_root="${TMPDIR:-/tmp}"
[[ -d "$runtime_root" && ! -L "$runtime_root" ]] || {
	printf 'runtime parent must be a non-symlink directory\n' >&2
	exit 2
}
runtime_root="$(cd "$runtime_root" && pwd)"
runtime_dir="$(mktemp -d "$runtime_root/groundplane-volume-runtime.XXXXXX")"
control_socket="$runtime_dir/ssh-control"
mkdir -p "$runtime_dir/go-cache" "$runtime_dir/go-tmp"
# shellcheck disable=SC1091
source "$script_dir/known_hosts_init.sh"
copy_known_hosts "$GROUNDPLANE_SSH_KNOWN_HOSTS" "$evidence_dir/known_hosts"

if [[ -n "${GROUNDPLANE_LOCAL_HTTP_PORT:-}" ]]; then
	local_port="$GROUNDPLANE_LOCAL_HTTP_PORT"
	valid_port "$local_port" || {
		printf 'GROUNDPLANE_LOCAL_HTTP_PORT is not a valid TCP port\n' >&2
		exit 2
	}
else
	local_port="$(python3 - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
)"
	valid_port "$local_port" || {
		printf 'could not allocate a valid local TCP port\n' >&2
		exit 2
	}
fi

if timeout 30 ssh "${ssh_options[@]}" -- "$GROUNDPLANE_SSH_TARGET" \
	systemctl is-active --quiet "$remote_service"; then
	printf 'active\n' >"$evidence_dir/service-before.txt"
else
	service_status=$?
	printf 'not-active-or-query-failed:%s\n' "$service_status" >"$evidence_dir/service-before.txt"
	printf 'Controller service must already be active; query exit status: %s\n' "$service_status" >&2
	exit 1
fi
(
	ulimit -f 4096
	timeout 30 ssh "${ssh_options[@]}" -- "$GROUNDPLANE_SSH_TARGET" \
		systemctl status --no-pager "$remote_service" >"$evidence_dir/service-status.txt" 2>&1
)

GOCACHE="$runtime_dir/go-cache" GOTMPDIR="$runtime_dir/go-tmp" \
	timeout 300 go build -o "$runtime_dir/groundplane" ./cmd/groundplane \
	2>&1 | head -c 2097152 >"$evidence_dir/build-cli.txt"
sha256sum "$runtime_dir/groundplane" >"$evidence_dir/cli.sha256"

stop_file="$runtime_dir/tunnel.stop"
ready_file="$evidence_dir/tunnel.ready"
stopped_file="$evidence_dir/tunnel.stopped"
failure_file="$evidence_dir/tunnel.failure"
control_check_options=()
for ssh_option in "${ssh_options[@]}"; do
	control_check_options+=(--control-check-option="$ssh_option")
done
supervisor_launching=1
python3 "$script_dir/ssh_tunnel_supervisor.py" \
	--stop-file "$stop_file" \
	--ready-file "$ready_file" \
	--stopped-file "$stopped_file" \
	--failure-file "$failure_file" \
	--child-output "$evidence_dir/tunnel.txt" \
	--deadline-seconds 600 \
	--control-socket "$control_socket" \
	--control-target "$GROUNDPLANE_SSH_TARGET" \
	--control-check-timeout 10 \
	--control-ready-timeout 30 \
	"${control_check_options[@]}" \
	-- ssh "${ssh_options[@]}" \
	-o ExitOnForwardFailure=yes -M -S "$control_socket" -N \
	-L "127.0.0.1:$local_port:$remote_http_addr" \
	-- "$GROUNDPLANE_SSH_TARGET" \
	>"$evidence_dir/supervisor.txt" 2>&1 &
supervisor_pid=$!
supervisor_started=1
supervisor_launching=0

supervisor_ready=0
for _ in {1..100}; do
	if [[ -f "$ready_file" ]]; then
		supervisor_ready=1
		break
	fi
	if [[ -f "$failure_file" || -f "$stopped_file" ]]; then
		break
	fi
	sleep 0.1
done
if [[ "$supervisor_ready" -ne 1 ]]; then
	printf 'SSH tunnel supervisor did not become ready\n' >&2
	exit 1
fi
if ! (
	ulimit -f 4096
	timeout 10 ssh "${ssh_options[@]}" -S "$control_socket" -O check \
		-- "$GROUNDPLANE_SSH_TARGET" >"$evidence_dir/tunnel-check.txt" 2>&1
); then
	printf 'could not prove the established SSH tunnel\n' >&2
	exit 1
fi

api_base="http://127.0.0.1:$local_port"
ready=0
for _ in {1..30}; do
	if ! timeout 2 ssh "${ssh_options[@]}" -S "$control_socket" -O check \
		-- "$GROUNDPLANE_SSH_TARGET" >/dev/null 2>&1; then
		printf 'SSH tunnel exited before the Controller became ready\n' >&2
		exit 1
	fi
	if curl --silent --show-error --fail --connect-timeout 1 --max-time 2 \
		"$api_base/api/v1/host" >/dev/null 2>&1; then
		ready=1
		break
	fi
	sleep 1
done
[[ "$ready" -eq 1 ]] || {
	printf 'Controller did not become ready through the SSH tunnel\n' >&2
	exit 1
}

api_request() {
	local method="$1" path="$2" expected="$3" output="$4" idempotency_key="${5:-}" body="${6:-}"
	local status_file="${output%.json}.status" header_file="${output%.json}.headers" status
	local curl_args=(
		--silent --show-error --connect-timeout 5 --max-time 30 --max-filesize 1048576
		--dump-header "$header_file" --output "$output" --write-out '%{http_code}\n'
		-X "$method" -H 'Accept: application/json'
	)
	[[ -n "$idempotency_key" ]] && curl_args+=(-H "Idempotency-Key: $idempotency_key")
	[[ -n "$body" ]] && curl_args+=(-H 'Content-Type: application/json' --data-binary "$body")
	if ! status="$(ulimit -f 4096; timeout 40 curl "${curl_args[@]}" "$api_base$path")"; then
		printf 'API request failed: %s %s\n' "$method" "$path" >&2
		return 1
	fi
	printf '%s\n' "$status" >"$status_file"
	[[ "$status" == "$expected" ]] || {
		printf 'API %s %s returned %s, expected %s\n' "$method" "$path" "$status" "$expected" >&2
		return 1
	}
}

api_assert() {
	local file="$1"
	shift
	(ulimit -f 4096; jq -e "$@" "$file" >/dev/null)
}

canonical_json() {
	local input="$1" output="$2"
	(ulimit -f 4096; jq -S . "$input" >"$output")
}

cli_json() {
	local label="$1" output="$evidence_dir/cli-$1.json" errors="$evidence_dir/cli-$1.stderr"
	shift
	if ! (
		ulimit -f 4096
		timeout 40 "$runtime_dir/groundplane" \
			--host "$api_base" --output json --no-color --id \
			--tenant "$GROUNDPLANE_TENANT_ID" --project "$GROUNDPLANE_PROJECT_ID" \
			--env "$GROUNDPLANE_ENVIRONMENT_ID" "$@" \
			>"$output" 2>"$errors"
	); then
		printf 'CLI command failed: %s\n' "$label" >&2
		return 1
	fi
}

wait_for_task() {
	local task_id="$1" label="$2" task_file="$evidence_dir/api-task-$2.json" state
	for _ in {1..120}; do
		api_request GET "/api/v1/tasks/$task_id" 200 "$task_file"
		state="$(jq -r '.status // empty' "$task_file")"
		case "$state" in
		completed)
			cp "$task_file" "$evidence_dir/api-task-$label-completed.json"
			return 0
			;;
		failed|timed_out|aborted)
			printf 'Task %s reached terminal failure state %s\n' "$task_id" "$state" >&2
			return 1
			;;
		esac
		sleep 1
	done
	printf 'Task %s did not complete within the polling bound\n' "$task_id" >&2
	return 1
}

remote_read() {
	local output="$1"
	shift
	(
		ulimit -f 4096
		timeout 40 ssh "${ssh_options[@]}" -S "$control_socket" \
			-- "$GROUNDPLANE_SSH_TARGET" "$@"
	) >"$output" 2>&1
}

# Validate the pre-provisioned hierarchy and reserve no existing test data.
api_request GET "/api/v1/projects/$GROUNDPLANE_PROJECT_ID" 200 "$evidence_dir/api-project.json"
api_request GET "/api/v1/environments/$GROUNDPLANE_ENVIRONMENT_ID" 200 "$evidence_dir/api-environment.json"
api_assert "$evidence_dir/api-project.json" --arg tenant "$GROUNDPLANE_TENANT_ID" \
	--arg project "$GROUNDPLANE_PROJECT_ID" \
	'.id == $project and .tenant_id == $tenant and .kind == "tenant"'
api_assert "$evidence_dir/api-environment.json" --arg project "$GROUNDPLANE_PROJECT_ID" \
	--arg env "$GROUNDPLANE_ENVIRONMENT_ID" \
	'.id == $env and .project_id == $project and .provisioning_state == "ready" and
	(.volume_dir | type == "string" and startswith("/"))'
api_request GET "/api/v1/volumes?environment=$GROUNDPLANE_ENVIRONMENT_ID&limit=200" 200 \
	"$evidence_dir/api-volumes-before.json"
api_assert "$evidence_dir/api-volumes-before.json" --arg slug "$GROUNDPLANE_VOLUME_SLUG" \
	--arg renamed "$GROUNDPLANE_VOLUME_RENAMED_SLUG" --arg key "$volume_key" \
	'(.next_cursor // "") == "" and ([.items[]? | select(.slug == $slug or .slug == $renamed or .key == $key)] | length) == 0'

volume_body="$(jq -cn --arg env "$GROUNDPLANE_ENVIRONMENT_ID" --arg slug "$GROUNDPLANE_VOLUME_SLUG" \
	--arg key "$volume_key" '{environment_id:$env,slug:$slug,key:$key}')"
add_key="volume-add-proof-$(date -u +%Y%m%dT%H%M%SZ)-$$"
api_request POST /api/v1/volumes 201 "$evidence_dir/api-volume-add.json" "$add_key" "$volume_body"
api_request POST /api/v1/volumes 201 "$evidence_dir/api-volume-add-replay.json" "$add_key" "$volume_body"
canonical_json "$evidence_dir/api-volume-add.json" "$runtime_dir/api-volume-add.canonical.json"
canonical_json "$evidence_dir/api-volume-add-replay.json" "$runtime_dir/api-volume-add-replay.canonical.json"
cmp -s "$runtime_dir/api-volume-add.canonical.json" "$runtime_dir/api-volume-add-replay.canonical.json" || {
	printf 'idempotent Volume add replay changed its response\n' >&2
	exit 1
}
created_volume_id="$(jq -r '.volume.id // empty' "$evidence_dir/api-volume-add.json")"
create_task_id="$(jq -r '.task_id // empty' "$evidence_dir/api-volume-add.json")"
valid_id "$created_volume_id" || { printf 'API add returned an invalid Volume ID\n' >&2; exit 1; }
valid_id "$create_task_id" || { printf 'API add returned an invalid Task ID\n' >&2; exit 1; }
api_assert "$evidence_dir/api-volume-add.json" --arg id "$created_volume_id" \
	--arg env "$GROUNDPLANE_ENVIRONMENT_ID" --arg slug "$GROUNDPLANE_VOLUME_SLUG" \
	--arg key "$volume_key" \
	'.volume | .id == $id and .slug == $slug and .key == $key and .environment_id == $env'
wait_for_task "$create_task_id" create
api_assert "$evidence_dir/api-task-create-completed.json" --arg id "$created_volume_id" \
	--arg task "$create_task_id" '.id == $task and .target == $id and .status == "completed" and .type == "create"'
cli_json task-create task show "$create_task_id"
api_assert "$evidence_dir/cli-task-create.json" --arg id "$created_volume_id" \
	'.target == $id and .status == "completed"'

jq '.volume' "$evidence_dir/api-volume-add.json" >"$runtime_dir/volume-before.json"
volume_path="$(jq -r '.path // empty' "$runtime_dir/volume-before.json")"
api_assert "$runtime_dir/volume-before.json" --arg id "$created_volume_id" \
	--arg env "$GROUNDPLANE_ENVIRONMENT_ID" --arg slug "$GROUNDPLANE_VOLUME_SLUG" --arg key "$volume_key" \
	'.id == $id and .environment_id == $env and .slug == $slug and .key == $key and
	(.path | type == "string" and startswith("/") and endswith("/" + $key) and (contains("/../") | not) and (contains("//") | not))'
[[ "$volume_path" =~ ^/[A-Za-z0-9._/-]+$ ]] || {
	printf 'API add returned an unsafe managed Volume path\n' >&2
	exit 1
}
cli_json list-before volume list
api_assert "$evidence_dir/cli-list-before.json" --arg id "$created_volume_id" \
	--arg slug "$GROUNDPLANE_VOLUME_SLUG" --arg key "$volume_key" \
	'([.items[]? | select(.id == $id and .slug == $slug and .key == $key)] | length) == 1'
cli_json show-before volume show "$created_volume_id"
api_assert "$evidence_dir/cli-show-before.json" --arg id "$created_volume_id" \
	--arg env "$GROUNDPLANE_ENVIRONMENT_ID" --arg slug "$GROUNDPLANE_VOLUME_SLUG" \
	--arg key "$volume_key" --arg path "$volume_path" \
	'.id == $id and .environment_id == $env and .slug == $slug and .key == $key and .path == $path'

cli_json edit volume edit "$created_volume_id" --slug "$GROUNDPLANE_VOLUME_RENAMED_SLUG"
edit_task_id="$(jq -r '.task_id // empty' "$evidence_dir/cli-edit.json")"
valid_id "$edit_task_id" || { printf 'CLI edit returned an invalid Task ID\n' >&2; exit 1; }
api_assert "$evidence_dir/cli-edit.json" --arg id "$created_volume_id" \
	--arg slug "$GROUNDPLANE_VOLUME_RENAMED_SLUG" --arg key "$volume_key" \
	'.volume | .id == $id and .slug == $slug and .key == $key'
wait_for_task "$edit_task_id" edit
api_assert "$evidence_dir/api-task-edit-completed.json" --arg id "$created_volume_id" \
	--arg task "$edit_task_id" '.id == $task and .target == $id and .status == "completed" and .type == "update"'
cli_json task-edit task show "$edit_task_id"
api_assert "$evidence_dir/cli-task-edit.json" --arg id "$created_volume_id" '.target == $id and .status == "completed"'

api_request GET "/api/v1/volumes/$created_volume_id" 200 "$evidence_dir/api-volume-after-edit.json"
api_assert "$evidence_dir/api-volume-after-edit.json" --arg id "$created_volume_id" \
	--arg env "$GROUNDPLANE_ENVIRONMENT_ID" --arg slug "$GROUNDPLANE_VOLUME_RENAMED_SLUG" \
	--arg key "$volume_key" --arg path "$volume_path" \
	'.id == $id and .environment_id == $env and .slug == $slug and .key == $key and .path == $path'
cli_json show-after volume show "$created_volume_id"
api_assert "$evidence_dir/cli-show-after.json" --arg id "$created_volume_id" \
	--arg env "$GROUNDPLANE_ENVIRONMENT_ID" --arg slug "$GROUNDPLANE_VOLUME_RENAMED_SLUG" \
	--arg key "$volume_key" --arg path "$volume_path" \
	'.id == $id and .environment_id == $env and .slug == $slug and .key == $key and .path == $path'
api_request GET "/api/v1/volumes?environment=$GROUNDPLANE_ENVIRONMENT_ID&limit=200" 200 \
	"$evidence_dir/api-volumes-after-edit.json"
api_assert "$evidence_dir/api-volumes-after-edit.json" --arg id "$created_volume_id" \
	--arg slug "$GROUNDPLANE_VOLUME_RENAMED_SLUG" --arg key "$volume_key" \
	'([.items[]? | select(.id == $id and .slug == $slug and .key == $key)] | length) == 1'

docker_name="gp_vol_${created_volume_id,,}"
remote_read "$evidence_dir/remote-volume-stat-before.txt" stat -Lc '%u:%g:%a:%F' -- "$volume_path"
[[ "$(<"$evidence_dir/remote-volume-stat-before.txt")" == "0:0:700:directory" ]] || {
	printf 'managed Volume path is not an exact root-owned 0700 directory\n' >&2
	exit 1
}
remote_read "$evidence_dir/remote-docker-volume-before.txt" docker volume inspect \
	--format '{{.Name}}' "$docker_name"
[[ "$(tr -d '\r\n' <"$evidence_dir/remote-docker-volume-before.txt")" == "$docker_name" ]] || {
	printf 'Docker managed Volume name did not match %s\n' "$docker_name" >&2
	exit 1
}
remote_read "$evidence_dir/remote-volume-stat-after-edit.txt" stat -Lc '%u:%g:%a:%F' -- "$volume_path"
[[ "$(<"$evidence_dir/remote-volume-stat-after-edit.txt")" == "0:0:700:directory" ]] || {
	printf 'managed Volume path changed ownership or type during rename\n' >&2
	exit 1
}

impact_cursor=""
impact_token=""
impact_key=""
impact_count=0
for impact_page in {1..1024}; do
	impact_path="/api/v1/volumes/$created_volume_id/deletion-impact?limit=40"
	[[ -z "$impact_cursor" ]] || impact_path="$impact_path&cursor=$impact_cursor"
	impact_file="$evidence_dir/api-volume-impact-$impact_page.json"
	api_request GET "$impact_path" 200 "$impact_file"
	impact_bytes="$(wc -c <"$impact_file")"
	(( impact_bytes <= 786432 )) || {
		printf 'Volume impact response exceeded 768 KiB\n' >&2
		exit 1
	}
	api_assert "$impact_file" --arg id "$created_volume_id" --arg env "$GROUNDPLANE_ENVIRONMENT_ID" \
		' .volume_id == $id and .environment_id == $env and (.key | type == "string" and length > 0) and
		  (.rolling_digest | test("^[a-f0-9]{64}$")) and (.items | type == "array" and length <= 40)'
	page_items="$(jq '.items | length' "$impact_file")"
	impact_count=$((impact_count + page_items))
	api_assert "$impact_file" --argjson count "$impact_count" '.item_count == $count'
	if [[ "$impact_page" -eq 1 ]]; then
		impact_key="$(jq -r '.key' "$impact_file")"
		impact_revision="$(jq -r '.revision' "$impact_file")"
		impact_head="$(jq -r '.environment_head' "$impact_file")"
	else
		api_assert "$impact_file" --arg key "$impact_key" --arg revision "$impact_revision" \
			--arg head "$impact_head" \
			'.key == $key and (.revision | tostring) == $revision and .environment_head == $head'
	fi
	if [[ "$(jq -r '.complete' "$impact_file")" == "true" ]]; then
		impact_token="$(jq -r '.impact_token // empty' "$impact_file")"
		[[ "$impact_token" =~ ^[a-f0-9]{64}$ ]] || {
			printf 'final Volume impact page did not return a valid token\n' >&2
			exit 1
		}
		[[ -z "$(jq -r '.next_cursor // empty' "$impact_file")" ]] || {
			printf 'complete Volume impact page returned a cursor\n' >&2
			exit 1
		}
		break
	fi
	impact_cursor="$(jq -r '.next_cursor // empty' "$impact_file")"
	[[ -n "$impact_cursor" ]] || {
		printf 'incomplete Volume impact page omitted its cursor\n' >&2
		exit 1
	}
done
[[ -n "$impact_token" && -n "$impact_key" ]] || {
	printf 'Volume impact sequence exceeded its page bound\n' >&2
	exit 1
}
[[ "$impact_key" == "$volume_key" ]] || {
	printf 'Volume impact key did not match immutable Compose key\n' >&2
	exit 1
}

cli_json remove volume remove "$created_volume_id" --impact-token "$impact_token" --confirm-key "$impact_key"
remove_task_id="$(jq -r '.task_id // empty' "$evidence_dir/cli-remove.json")"
valid_id "$remove_task_id" || { printf 'CLI remove returned an invalid Task ID\n' >&2; exit 1; }
wait_for_task "$remove_task_id" remove
api_assert "$evidence_dir/api-task-remove-completed.json" --arg id "$created_volume_id" \
	--arg task "$remove_task_id" '.id == $task and .target == $id and .status == "completed" and .type == "remove"'
cli_json task-remove task show "$remove_task_id"
api_assert "$evidence_dir/cli-task-remove.json" --arg id "$created_volume_id" \
	'.target == $id and .status == "completed"'

api_request GET "/api/v1/volumes/$created_volume_id" 404 "$evidence_dir/api-volume-after-remove.json"
api_assert "$evidence_dir/api-volume-after-remove.json" '.status == 404 and (.code | type == "string")'
api_request GET "/api/v1/volumes?environment=$GROUNDPLANE_ENVIRONMENT_ID&limit=200" 200 \
	"$evidence_dir/api-volumes-after-remove.json"
api_assert "$evidence_dir/api-volumes-after-remove.json" --arg id "$created_volume_id" \
	'([.items[]? | select(.id == $id)] | length) == 0'
if remote_read "$evidence_dir/remote-docker-volume-after.txt" docker volume inspect \
	--format '{{.Name}}' "$docker_name"; then
	printf 'Docker Volume %s still exists after completed removal\n' "$docker_name" >&2
	exit 1
fi
if remote_read "$evidence_dir/remote-volume-stat-after.txt" stat -Lc '%u:%g:%F' -- "$volume_path"; then
	printf 'managed Volume path still exists after completed removal\n' >&2
	exit 1
fi

printf 'Volume lifecycle verification passed; evidence retained at %s\n' "$evidence_dir"
