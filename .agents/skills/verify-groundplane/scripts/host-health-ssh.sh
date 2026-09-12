#!/usr/bin/env bash
set -euo pipefail

required_commands=(
	ssh curl jq cmp go mktemp python3 sha256sum timeout date mkdir chmod
	head dirname rm sleep
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

ssh_port="${GROUNDPLANE_SSH_PORT:-22}"
remote_service="groundplane-controller.service"
remote_http_addr="${GROUNDPLANE_REMOTE_HTTP_ADDR:-127.0.0.1:8080}"

[[ "$GROUNDPLANE_SSH_TARGET" =~ ^[A-Za-z0-9_.@:-]+$ ]] || {
	printf 'GROUNDPLANE_SSH_TARGET contains unsupported characters\n' >&2
	exit 2
}
[[ "$GROUNDPLANE_SSH_TARGET" != -* ]] || {
	printf 'GROUNDPLANE_SSH_TARGET must not begin with a dash\n' >&2
	exit 2
}
[[ "${#GROUNDPLANE_SSH_TARGET}" -le 1024 ]] || {
	printf 'GROUNDPLANE_SSH_TARGET exceeds the 1024-byte bound\n' >&2
	exit 2
}
[[ -f "$GROUNDPLANE_SSH_KEY" ]] || {
	printf 'SSH key is not a regular file: %s\n' "$GROUNDPLANE_SSH_KEY" >&2
	exit 2
}
valid_port() {
	[[ "$1" =~ ^[0-9]{1,5}$ ]] && ((10#$1 >= 1 && 10#$1 <= 65535))
}
valid_port "$ssh_port" || {
	printf 'GROUNDPLANE_SSH_PORT must be numeric\n' >&2
	exit 2
}
[[ "$remote_http_addr" =~ ^127\.0\.0\.1:[0-9]+$ ]] || {
	printf 'GROUNDPLANE_REMOTE_HTTP_ADDR must be a 127.0.0.1 TCP address\n' >&2
	exit 2
}
valid_port "${remote_http_addr##*:}" || {
	printf 'GROUNDPLANE_REMOTE_HTTP_ADDR contains an invalid TCP port\n' >&2
	exit 2
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/supervisor_wait.sh"
repo_root="$(cd "$script_dir/../../../.." && pwd)"
cd "$repo_root"
source "$repo_root/scripts/repo-env.sh"
repo_env_init

[[ -d /proc && -r /proc && -x /proc && -r "/proc/$$/stat" ]] || {
	printf 'readable Linux procfs is required for supervisor ownership polling\n' >&2
	exit 2
}

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
evidence_root=$(repo_temp_dir "${GROUNDPLANE_EVIDENCE_DIR:-$repo_root/.tmp/verify-groundplane}")
[[ -d "$evidence_root" && ! -L "$evidence_root" ]] || {
	printf 'evidence parent must be a non-symlink directory: %s\n' "$evidence_root" >&2
	exit 2
}
evidence_root="$(cd "$evidence_root" && pwd)"
evidence_dir="$(mktemp -d "$evidence_root/host-health-$timestamp.XXXXXX")"
known_hosts="$evidence_dir/known_hosts"
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
	-o "UserKnownHostsFile=$known_hosts"
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
	cleanup_result="not-needed"
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
			if [[ "$supervisor_poll_status" -eq 2 ]]; then
				supervisor_cleanup="ambiguous-proc"
			else
				supervisor_cleanup="timeout-or-failed"
			fi
			retain_runtime=1
			if [[ "$supervisor_poll_status" -ne 0 ]]; then
				printf 'supervisor_wait=deadline-exceeded\n' \
					>"$evidence_dir/supervisor-wait.txt"
				if [[ "$supervisor_poll_status" -eq 2 ]]; then
					printf 'supervisor_proc=ambiguous\n' \
						>"$evidence_dir/supervisor-stop-status.txt"
				else
					printf 'supervisor_wait=deadline-exceeded\n' \
						>"$evidence_dir/supervisor-stop-status.txt"
				fi
			fi
			[[ "$status" -ne 0 ]] || status=1
		fi
	fi
	runtime_cleanup="not-created"
	if [[ "$retain_runtime" -eq 1 ]]; then
		runtime_cleanup="retained"
	elif [[ -n "$runtime_dir" && -n "$runtime_root" && \
		"$runtime_dir" == "$runtime_root"/groundplane-verify-runtime.* && \
		-d "$runtime_dir" && ! -L "$runtime_dir" ]]; then
		runtime_cleanup="failed"
		rm -rf -- "$runtime_dir"
		[[ ! -e "$runtime_dir" ]] && runtime_cleanup="passed"
	fi
	if [[ "$runtime_cleanup" == "failed" ]]; then
		printf 'failed to remove journey runtime directory\n' >&2
		[[ "$status" -ne 0 ]] || status=1
	fi
	printf 'exit_status=%s\nevidence_dir=%s\ntarget=%s\napi_base=%s\nruntime_dir=%s\ncontrol_socket=%s\nsupervisor_pid=%s\nservice_mutation=none\ntunnel_cleanup=%s\nruntime_cleanup=%s\nassertions=canonical-host-and-api-cli-semantic-parity\nunproven=bootstrap,public-exposure,mutations,console,repository-ci\n' \
		"$status" "$evidence_dir" "$GROUNDPLANE_SSH_TARGET" "$api_base" \
		"${runtime_dir:-not-created}" "${control_socket:-not-created}" \
		"${supervisor_pid:-not-created}" "$supervisor_cleanup" "$runtime_cleanup" \
		>"$evidence_dir/result.txt"
	trap - EXIT
	exit "$status"
}
trap cleanup EXIT
trap handle_signal HUP INT TERM

chmod 700 "$evidence_dir"
runtime_root=$(repo_temp_dir "${GROUNDPLANE_VERIFY_RUNTIME_DIR:-$TMPDIR}")
[[ -d "$runtime_root" && ! -L "$runtime_root" ]] || {
	printf 'runtime parent must be a non-symlink directory: %s\n' "$runtime_root" >&2
	exit 2
}
runtime_root="$(cd "$runtime_root" && pwd)"
runtime_dir="$(mktemp -d "$runtime_root/groundplane-verify-runtime.XXXXXX")"
control_socket="$runtime_dir/ssh-control"
mkdir -p "$runtime_dir/go-cache" "$runtime_dir/go-tmp"
source "$script_dir/known_hosts_init.sh"
if ! copy_known_hosts "$GROUNDPLANE_SSH_KNOWN_HOSTS" "$known_hosts"; then
	exit 2
fi

if [[ -n "${GROUNDPLANE_LOCAL_HTTP_PORT:-}" ]]; then
	local_port="$GROUNDPLANE_LOCAL_HTTP_PORT"
	valid_port "$local_port" || {
		printf 'GROUNDPLANE_LOCAL_HTTP_PORT is not a valid TCP port\n' >&2
		exit 2
	}
else
	local_port="$(python3 -c 'import socket
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])')"
	valid_port "$local_port" || {
		printf 'could not allocate a valid local TCP port\n' >&2
		exit 2
	}
fi

if timeout 30 ssh "${ssh_options[@]}" -- "$GROUNDPLANE_SSH_TARGET" \
	systemctl is-active --quiet "$remote_service"; then
	printf 'active\n' >"$evidence_dir/service-before.txt"
else
	status=$?
	printf 'not-active-or-query-failed:%s\n' "$status" >"$evidence_dir/service-before.txt"
	printf 'Controller service must already be active; query exit status: %s\n' "$status" >&2
	exit 1
fi

(
	ulimit -f 2048
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
	-o ExitOnForwardFailure=yes \
	-M \
	-S "$control_socket" \
	-N \
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
	ulimit -f 2048
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
	if curl --silent --show-error --fail \
		--connect-timeout 1 --max-time 2 \
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

(
	ulimit -f 1024
	curl --silent --show-error \
		--connect-timeout 5 --max-time 30 \
		--max-filesize 1048576 \
		--dump-header "$evidence_dir/api-headers.txt" \
		--output "$evidence_dir/api.json" \
		--write-out '%{http_code}\n' \
		"$api_base/api/v1/host"
) >"$evidence_dir/api-status.txt"
IFS= read -r api_status <"$evidence_dir/api-status.txt"
[[ "$api_status" == "200" ]] || {
	printf 'Host API returned HTTP status %s; expected 200\n' "$api_status" >&2
	exit 1
}

(
	ulimit -f 2048
	timeout 30 "$runtime_dir/groundplane" \
		--host "$api_base" \
		--output json \
		host show >"$evidence_dir/cli.json"
)

host_contract='
def integer:
  type == "number" and floor == .;
def exact_keys($expected):
  type == "object" and keys == $expected;
def host_resource:
  exact_keys(["total", "used", "used_pct"]) and
  (.total | type == "string") and
  (.used | type == "string") and
  (.used_pct | integer and . >= 0 and . <= 100);
def etcd_status:
  . == "healthy" or . == "degraded" or . == "failed";
def agent_status:
  . == "healthy" or . == "degraded" or . == "stopped" or . == "pending";
def release_digest:
  type == "string" and test("^sha256:[0-9a-f]{64}$");
def native_candidate:
  if . == null then true else
    exact_keys(["agent_image", "channel_schema", "controller_sha256", "controller_version", "release", "storage_epoch"]) and
    (.release | release_digest) and (.controller_sha256 | release_digest) and
    (.controller_version | type == "string") and
    (.agent_image | type == "string" and test("@sha256:[0-9a-f]{64}$")) and
    (.storage_epoch | integer and . > 0) and (.channel_schema | integer and . > 0)
  end;
def native_history:
  if . == null then true else
    exact_keys(["created_at", "phase", "release", "status", "task_id"]) and
    (.task_id | type == "string" and test("^task_[0-7][0-9A-HJKMNP-TV-Z]{25}$")) and
    (.release | release_digest) and (.phase | type == "string") and
    (.created_at | type == "string") and
    (.status | . == "pending" or . == "running" or . == "completed" or . == "failed" or . == "aborted" or . == "timed_out")
  end;
def native_update:
  exact_keys(["available", "candidate", "error", "last_update", "running_sha256"]) and
  (.available | type == "boolean") and (.error | type == "string") and
  (.running_sha256 | release_digest) and
  (.candidate | native_candidate) and (.last_update | native_history);
def canonical_host:
  exact_keys([
    "agent", "arch", "controller", "cpu", "disk", "docker", "etcd",
    "hostname", "memory", "os", "swap", "uptime"
  ]) and
  (.hostname | type == "string") and
  (.arch | type == "string") and
  (.os | type == "string") and
  (.uptime | type == "string") and
  (.docker | type == "string") and
  (.cpu |
    exact_keys(["cores", "load", "model"]) and
    (.model | type == "string") and
    (.cores | integer and . > 0) and
    (.load | integer and . >= 0 and . <= 100)) and
  (.memory | host_resource) and
  (.disk | host_resource) and
  (.swap | host_resource) and
  (.etcd |
    exact_keys(["db_size", "node", "status"]) and
    .node == "single-node" and
    (.status | type == "string" and etcd_status) and
    (.db_size | type == "string")) and
  (.controller |
    exact_keys(["service", "status", "update", "version"]) and
    .service == "groundplane-controller.service" and
    .status == "healthy" and
    (.version | type == "string") and (.update | native_update)) and
  (.agent |
    exact_keys(["labels", "max_concurrent", "pull_interval", "status"]) and
    (.status | type == "string" and agent_status) and
    (.pull_interval | type == "string") and
    (.max_concurrent | integer) and
    (.labels | type == "array") and
    (all(.labels[]; type == "string")) and
    (.labels == (.labels | sort)));
'

jq -e "$host_contract
  (((has(\"\$schema\") | not) or (.[\"\$schema\"] | type == \"string\")) and
  (del(.[\"\$schema\"]) | canonical_host))
" "$evidence_dir/api.json" >/dev/null
jq -e "$host_contract
  ((has(\"\$schema\") | not) and canonical_host)
" "$evidence_dir/cli.json" >/dev/null

(
	ulimit -f 2048
	jq -S 'del(.["$schema"])' \
		"$evidence_dir/api.json" >"$evidence_dir/api.normalized.json"
)
(
	ulimit -f 2048
	jq -S . "$evidence_dir/cli.json" >"$evidence_dir/cli.normalized.json"
)

path_type_map='
def path_type_map:
  def walk($path):
    [{path: $path, type: type}] +
    if type == "object" then
      [keys[] as $key | .[$key] | walk($path + [$key])] | add
    else
      []
    end;
  walk([]);
path_type_map
'
stable_projection='{
  hostname,
  arch,
  os,
  cpu: {model: .cpu.model, cores: .cpu.cores},
  controller: {
    status: .controller.status,
    service: .controller.service,
    version: .controller.version
  }
}'

for document in api cli; do
	(
		ulimit -f 2048
		jq -S "$path_type_map" \
			"$evidence_dir/$document.normalized.json" \
			>"$evidence_dir/$document.path-types.json"
	)
	(
		ulimit -f 2048
		jq -S "$stable_projection" \
			"$evidence_dir/$document.normalized.json" \
			>"$evidence_dir/$document.stable.json"
	)
done

cmp -s \
	"$evidence_dir/api.path-types.json" \
	"$evidence_dir/cli.path-types.json" || {
	printf 'API and CLI Host path/type maps differ\n' >&2
	exit 1
}
cmp -s \
	"$evidence_dir/api.stable.json" \
	"$evidence_dir/cli.stable.json" || {
	printf 'API and CLI stable Host values differ\n' >&2
	exit 1
}

if timeout 30 ssh "${ssh_options[@]}" -- "$GROUNDPLANE_SSH_TARGET" \
	systemctl is-active --quiet "$remote_service"; then
	printf 'active\n' >"$evidence_dir/service-after.txt"
else
	printf 'inactive\n' >"$evidence_dir/service-after.txt"
	printf 'Controller service is not active after verification\n' >&2
	exit 1
fi
(
	ulimit -f 2048
	timeout 30 ssh "${ssh_options[@]}" -- "$GROUNDPLANE_SSH_TARGET" \
		systemctl status --no-pager "$remote_service" >"$evidence_dir/service-status-after.txt" 2>&1
)

printf 'Host health verification passed; evidence: %s\n' "$evidence_dir"
