#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/../../../../scripts/repo-env.sh"
repo_env_init

action=${1:-}

usage() {
  cat <<'USAGE'
usage: foundation-host-acceptance.sh ACTION [TARGET]

Actions:
  trust                   record two identical host-key scans and fingerprints
  observe                 verify both Controller listeners and Agent ownership
  join                    enroll the singleton local Agent and wait for its Task
  config AGENT_ID         set the accepted runtime config on the exact Agent
  update-exact AGENT_ID   update the exact Agent and wait for its Task
  update-all              update the singleton Agent through --all
  restart AGENT_ID        restart Controller and prove Agent/container continuity
  task TASK_ID            print platform list, detail, and bounded events
  abort TASK_ID           abort an in-flight Task and wait for its terminal state
  retry TASK_ID           retry a terminal Task and wait for the new Task
  remove AGENT_ID         remove the exact Agent and prove credential cleanup

All SSH calls use the isolated GP_ACCEPTANCE_KNOWN_HOSTS file with strict
checking. Mutating actions additionally require GP_ACCEPTANCE_MUTATE=1 and the
remote ownership marker. The script never reads or prints an Agent token.
USAGE
}

die() {
  printf 'foundation acceptance: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "missing command: $1"
}

assert_id() {
  [[ $1 =~ ^(agt|task)_[0-9A-Z]{26}$ ]] || die "invalid Groundplane id: $1"
}

if [[ -z $action ]]; then
  usage
  exit 2
fi

: "${GP_ACCEPTANCE_HOST:?GP_ACCEPTANCE_HOST is required}"
: "${GP_ACCEPTANCE_SSH_KEY:?GP_ACCEPTANCE_SSH_KEY is required}"
: "${GP_ACCEPTANCE_KNOWN_HOSTS:?GP_ACCEPTANCE_KNOWN_HOSTS is required}"
: "${GP_ACCEPTANCE_OWNERSHIP_MARKER:?GP_ACCEPTANCE_OWNERSHIP_MARKER is required}"
host=$GP_ACCEPTANCE_HOST
key=$GP_ACCEPTANCE_SSH_KEY
known_hosts=$GP_ACCEPTANCE_KNOWN_HOSTS
marker=$GP_ACCEPTANCE_OWNERSHIP_MARKER
trust_tmpdir=

[[ $host =~ ^[A-Za-z0-9_.@-]+$ && $host != -* && $host != *@*@* ]] ||
  die 'GP_ACCEPTANCE_HOST contains unsupported characters'
[[ $marker =~ ^/[A-Za-z0-9._/-]+$ ]] ||
  die 'GP_ACCEPTANCE_OWNERSHIP_MARKER must be a simple absolute path'

ssh_remote() {
  timeout 120 ssh -F /dev/null -i "$key" \
    -o UserKnownHostsFile="$known_hosts" \
    -o GlobalKnownHostsFile=/dev/null \
    -o StrictHostKeyChecking=yes \
    -o BatchMode=yes \
    -o IdentitiesOnly=yes \
    -o VerifyHostKeyDNS=no \
    -o ConnectTimeout=10 \
    -o ServerAliveInterval=5 \
    -o ServerAliveCountMax=2 \
    -- "$host" "$@"
}

require_mutation() {
  [[ ${GP_ACCEPTANCE_MUTATE:-0} == 1 ]] || die 'set GP_ACCEPTANCE_MUTATE=1'
  ssh_remote bash -s -- "$marker" <<'REMOTE'
set -euo pipefail
marker=$1
[[ -f "$marker" ]]
REMOTE
}

wait_task() {
  local task_id=$1
  assert_id "$task_id"
  ssh_remote bash -s -- "$task_id" <<'REMOTE'
set -euo pipefail
task_id=$1
for _ in $(seq 1 90); do
  receipt=$(/usr/local/bin/groundplane task show "$task_id" --output json)
  status=$(printf '%s' "$receipt" | jq -er '.status | strings')
  case "$status" in
    completed|failed|timed_out|aborted)
      printf '%s\n' "$receipt"
      exit 0
      ;;
  esac
  sleep 1
done
printf 'Task did not become terminal: %s\n' "$task_id" >&2
exit 1
REMOTE
}

record_task() {
  local result task_id receipt status
  result=$1
  task_id=$(printf '%s' "$result" | jq -er .task_id)
  printf '%s\n' "$result"
  receipt=$(wait_task "$task_id")
  printf '%s\n' "$receipt"
  status=$(printf '%s' "$receipt" | jq -er '.status | strings')
  [[ $status == completed ]] || die "Task $task_id ended with status $status"
}

trust() {
  local scan_host tmpdir first second
  require_command ssh-keyscan
  require_command ssh-keygen
  require_command timeout
  scan_host=${host#*@}
  [[ $scan_host != *:* && $scan_host != -* ]] || die 'trust capture requires a hostname or IPv4 address'
  known_dir=${known_hosts%/*}
  [[ $known_dir != "$known_hosts" ]] || known_dir=.
  [[ -d $known_dir && ! -L $known_dir ]] || die "known_hosts parent is not a directory: $known_dir"
  [[ ! -e $known_hosts && ! -L $known_hosts ]] || die "refusing to overwrite known_hosts: $known_hosts"
  tmpdir=$(mktemp -d)
  trust_tmpdir=$tmpdir
  trap 'rm -rf -- "$trust_tmpdir"' EXIT
  first=$tmpdir/scan-1
  second=$tmpdir/scan-2
  ssh-keyscan -T 5 -t rsa,ecdsa,ed25519 "$scan_host" 2>/dev/null | LC_ALL=C sort -u >"$first"
  sleep 1
  ssh-keyscan -T 5 -t rsa,ecdsa,ed25519 "$scan_host" 2>/dev/null | LC_ALL=C sort -u >"$second"
  [[ -s $first ]] || die 'host-key scan was empty'
  cmp -s "$first" "$second" || die 'host-key scans differ; refusing TOFU'
  install -m 0600 "$first" "$known_hosts"
  sha256sum "$first" "$second"
  ssh-keygen -lf "$known_hosts"
}

observe() {
  ssh_remote bash -s -- "http://${host#*@}:8080" <<'REMOTE'
set -euo pipefail
host_url=$1
controller_active=$(systemctl is-active groundplane-controller.service)
[[ $controller_active == active ]]
runtime_preserve=$(systemctl show groundplane-controller.service -p RuntimeDirectoryPreserve --value)
[[ $runtime_preserve == restart ]]
agent_unit=$(systemctl show groundplane-agent.service -p LoadState --value)
[[ $agent_unit == not-found ]]
loopback=$(curl -fsS http://127.0.0.1:8080/api/v1/host)
host_interface=$(curl -fsS "$host_url/api/v1/host")
printf '%s' "$loopback" | jq -e '.controller.status == "healthy"' >/dev/null
printf '%s' "$host_interface" | jq -e '.controller.status == "healthy"' >/dev/null
agents=$(/usr/local/bin/groundplane agent list --output json)
printf '%s' "$agents" | jq -e '.items | length == 1 and .[0].status == "healthy" and (.[0].labels | type == "object")' >/dev/null
agent_id=$(printf '%s' "$agents" | jq -er '.items[0].id')
container=$(docker inspect groundplane-agent --format '{{.Id}}|{{.State.Running}}|{{.State.StartedAt}}|{{.Config.Image}}|{{index .Config.Labels "com.groundplane.managed"}}|{{index .Config.Labels "com.groundplane.agent-id"}}|{{index .Config.Labels "com.groundplane.agent-generation"}}')
IFS='|' read -r container_id running started_at image managed container_agent_id generation <<<"$container"
[[ -n $container_id && $running == true && $managed == true && $container_agent_id == "$agent_id" ]]
printf 'controller_active=%s\nruntime_preserve=%s\nagent_unit=%s\nloopback_host=%s\nhost_interface=%s\nagent=%s\ncontainer=%s\n' \
  "$controller_active" "$runtime_preserve" "$agent_unit" \
  "$(printf '%s' "$loopback" | jq -c '{controller:.controller.status,version:.controller.version}')" \
  "$(printf '%s' "$host_interface" | jq -c '{controller:.controller.status,version:.controller.version}')" \
  "$(printf '%s' "$agents" | jq -c '.items[0] | {id,enrollment_task_id,host,status,version,labels,ready_at,in_flight}')" \
  "$container"
REMOTE
}

restart_controller() {
  local agent_id=$1
  assert_id "$agent_id"
  ssh_remote bash -s -- "$agent_id" <<'REMOTE'
set -euo pipefail
agent_id=$1
[[ $(systemctl show groundplane-controller.service -p RuntimeDirectoryPreserve --value) == restart ]]
[[ $(/usr/local/bin/groundplane agent list --output json | jq -r '.items | length') == 1 ]]
[[ $(/usr/local/bin/groundplane agent list --output json | jq -r '.items[0].id') == "$agent_id" ]]
before_pid=$(systemctl show groundplane-controller.service -p MainPID --value)
before_inode=$(stat -c %i /run/groundplane/controller)
before_container=$(docker inspect groundplane-agent --format '{{.Id}}|{{.State.StartedAt}}|{{index .Config.Labels "com.groundplane.agent-generation"}}')
systemctl restart groundplane-controller.service
ready=0
for _ in $(seq 1 60); do
  if [[ $(systemctl is-active groundplane-controller.service) == active ]] &&
     [[ $(/usr/local/bin/groundplane agent list --output json | jq -r --arg id "$agent_id" '.items[] | select(.id == $id) | .status') == healthy ]]; then
    ready=1
    break
  fi
  sleep 1
done
[[ $ready == 1 ]] || die 'Controller/Agent did not become healthy after restart'
after_pid=$(systemctl show groundplane-controller.service -p MainPID --value)
after_inode=$(stat -c %i /run/groundplane/controller)
after_container=$(docker inspect groundplane-agent --format '{{.Id}}|{{.State.StartedAt}}|{{index .Config.Labels "com.groundplane.agent-generation"}}')
[[ $before_pid != "$after_pid" ]]
[[ $before_inode == "$after_inode" ]]
[[ $before_container == "$after_container" ]]
printf 'before_pid=%s\nafter_pid=%s\nruntime_dir_inode=%s\ncontainer=%s\nagent_status=healthy\n' \
  "$before_pid" "$after_pid" "$after_inode" "$after_container"
REMOTE
}

require_command ssh
require_command jq
require_command timeout

if [[ $action == trust ]]; then
  trust
  exit 0
fi

[[ -f $key && ! -L $key && -r $key ]] || die "SSH key is not a regular readable file: $key"
[[ -f $known_hosts && ! -L $known_hosts && -s $known_hosts ]] ||
  die "isolated known_hosts is not a regular non-empty file: $known_hosts"

case "$action" in
  observe)
    observe
    ;;
  join)
    require_mutation
    [[ $(ssh_remote /usr/local/bin/groundplane agent list --output json | jq -r '.items | length') == 0 ]] || die 'join requires no enrolled Agent'
    record_task "$(ssh_remote /usr/local/bin/groundplane agent join --output json)"
    ;;
  config)
    require_mutation
    agent_id=${2:-}; assert_id "$agent_id"
    ssh_remote /usr/local/bin/groundplane agent config set "$agent_id" \
      --pull-interval 1 --max-concurrent 2 --label acceptance=true --output json
    ;;
  update-exact)
    require_mutation
    agent_id=${2:-}; assert_id "$agent_id"
    record_task "$(ssh_remote /usr/local/bin/groundplane agent update "$agent_id" --output json)"
    ;;
  update-all)
    require_mutation
    record_task "$(ssh_remote /usr/local/bin/groundplane agent update --all --output json)"
    ;;
  restart)
    require_mutation
    agent_id=${2:-}; assert_id "$agent_id"
    restart_controller "$agent_id"
    ;;
  task)
    task_id=${2:-}; assert_id "$task_id"
    ssh_remote /usr/local/bin/groundplane task list --workspace platform --output json
    ssh_remote /usr/local/bin/groundplane task show "$task_id" --output json
    ssh_remote /usr/local/bin/groundplane task events "$task_id" --output json
    ;;
  abort)
    require_mutation
    task_id=${2:-}; assert_id "$task_id"
    result=$(ssh_remote /usr/local/bin/groundplane task abort "$task_id" --output json)
    [[ $(printf '%s' "$result" | jq -er .task_id) == "$task_id" ]] || die "abort returned a different Task id"
    printf '%s\n' "$result"
    receipt=$(wait_task "$task_id")
    printf '%s\n' "$receipt"
    [[ $(printf '%s' "$receipt" | jq -er '.status | strings') == aborted ]] ||
      die "Task $task_id did not abort"
    ;;
  retry)
    require_mutation
    task_id=${2:-}; assert_id "$task_id"
    record_task "$(ssh_remote /usr/local/bin/groundplane task retry "$task_id" --output json)"
    ;;
  remove)
    require_mutation
    agent_id=${2:-}; assert_id "$agent_id"
    record_task "$(ssh_remote /usr/local/bin/groundplane agent remove "$agent_id" --output json)"
    ssh_remote bash -s -- "$agent_id" <<'REMOTE'
set -euo pipefail
agent_id=$1
[[ $(/usr/local/bin/groundplane agent list --output json | jq -r '.items | length') == 0 ]]
! docker inspect groundplane-agent >/dev/null 2>&1
[[ ! -e /run/groundplane/agents/$agent_id ]]
[[ $(systemctl show groundplane-agent.service -p LoadState --value) == not-found ]]
REMOTE
    ;;
  *)
    usage
    exit 2
    ;;
esac
