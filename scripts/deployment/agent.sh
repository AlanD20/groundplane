load_task() {
    task_id=$1
    if ! task_output=$(/usr/local/bin/groundplane \
        --host http://127.0.0.1:8080 \
        --output json \
        task show "$task_id" 2>&1); then
        printf '%s\n' "$task_output" >&2
        return 1
    fi
    task_status=$(printf '%s\n' "$task_output" |
        sed -n 's/^[[:space:]]*"status":[[:space:]]*"\([^"]*\)".*$/\1/p')
    case "$task_status" in
        completed | pending | running | failed | aborted | timed_out)
            return 0
            ;;
        *)
            echo "Task $task_id returned unknown status: $task_status" >&2
            return 1
            ;;
    esac
}

settle_timed_out_agent_task() {
    task_id=$1
    if abort_output=$(/usr/local/bin/groundplane \
        --host http://127.0.0.1:8080 \
        --output json \
        task abort "$task_id" 2>&1); then
        printf '%s\n' "$abort_output"
    else
        printf '%s\n' "$abort_output" >&2
    fi

    attempt=0
    while test "$attempt" -lt 60; do
        if ! load_task "$task_id"; then
            retain_recovery=1
            unresolved_task_id=$task_id
            unresolved_task_state=unknown
            return 1
        fi
        unresolved_task_state=$task_status
        case "$task_status" in
            completed)
                rollback=0
                retain_recovery=0
                unresolved_task_id=""
                unresolved_task_state=""
                printf 'Agent task: %s completed while abort was requested\n' "$task_id"
                return 0
                ;;
            failed | aborted | timed_out)
                printf '%s\n' "$task_output" >&2
                return 1
                ;;
            pending | running)
                ;;
        esac
        attempt=$((attempt + 1))
        sleep 1
    done

    retain_recovery=1
    unresolved_task_id=$task_id
    unresolved_task_state=unknown
    echo "Agent task did not become terminal after abort: $task_id" >&2
    return 1
}

wait_for_agent_task() {
    task_id=$1
    attempt=0
    while test "$attempt" -lt 330; do
        if ! load_task "$task_id"; then
            retain_recovery=1
            unresolved_task_id=$task_id
            unresolved_task_state=unknown
            return 1
        fi
        unresolved_task_state=$task_status
        case "$task_status" in
            completed)
                rollback=0
                retain_recovery=0
                unresolved_task_id=""
                unresolved_task_state=""
                printf 'Agent task: %s completed\n' "$task_id"
                return 0
                ;;
            pending | running)
                ;;
            failed | aborted | timed_out)
                printf '%s\n' "$task_output" >&2
                return 1
                ;;
            *)
                echo "Agent task returned unknown status: $task_status" >&2
                return 1
                ;;
        esac
        attempt=$((attempt + 1))
        sleep 1
    done
    unresolved_task_state=client_timeout
    echo "Agent task did not complete within 330 seconds: $task_id" >&2
    settle_timed_out_agent_task "$task_id"
}

load_agent_list() {
    if ! agent_list_output=$(/usr/local/bin/groundplane \
        --host http://127.0.0.1:8080 \
        --output json \
        agent list 2>&1); then
        printf '%s\n' "$agent_list_output" >&2
        echo "Agent list could not be read; refusing Agent update" >&2
        return 1
    fi
    if ! agent_list_state_output=$(
        printf '%s\n' "$agent_list_output" |
            python3 -c '
import json
import sys

try:
    document = json.load(sys.stdin)
except (TypeError, ValueError):
    raise SystemExit(1)

if not isinstance(document, dict):
    raise SystemExit(1)
items = document.get("items")
if not isinstance(items, list):
    raise SystemExit(1)
if len(items) == 0:
    print("absent")
    raise SystemExit(0)
if len(items) != 1 or not isinstance(items[0], dict):
    raise SystemExit(1)
agent = items[0]
agent_id = agent.get("id")
in_flight = agent.get("in_flight")
if not isinstance(agent_id, str) or not agent_id or "\n" in agent_id or "\r" in agent_id:
    raise SystemExit(1)
if type(in_flight) is not int or in_flight < 0:
    raise SystemExit(1)
print("present")
print(agent_id)
print(in_flight)
'
    ); then
        printf '%s\n' "$agent_list_output" >&2
        echo "Agent list response was missing or malformed; refusing Agent update" >&2
        return 1
    fi
    agent_list_state=$(printf '%s\n' "$agent_list_state_output" | sed -n '1p')
    case "$agent_list_state" in
        absent)
            agent_id=""
            agent_in_flight=""
            ;;
        present)
            agent_id=$(printf '%s\n' "$agent_list_state_output" | sed -n '2p')
            agent_in_flight=$(printf '%s\n' "$agent_list_state_output" | sed -n '3p')
            ;;
        *)
            echo "Agent list parser returned unknown state: $agent_list_state" >&2
            return 1
            ;;
    esac
}

dispatch_agent_update() {
    update_settled=0
    attempt=0
    while test "$attempt" -lt 330; do
        if test "$agent_list_state" != present ||
            test "$agent_id" != "$selected_agent_id"; then
            echo "Agent singleton changed while waiting for idle; refusing Agent update" >&2
            exit 1
        fi
        if test "$agent_in_flight" -eq 0; then
            if update_output=$(/usr/local/bin/groundplane \
                --host http://127.0.0.1:8080 \
                --output json \
                agent update --all 2>&1); then
                retain_recovery=1
                unresolved_task_state=dispatched
                printf '%s\n' "$update_output"
                agent_task_id=$(printf '%s\n' "$update_output" |
                    sed -n 's/^[[:space:]]*"task_id":[[:space:]]*"\([^"]*\)".*$/\1/p')
                unresolved_task_id=$agent_task_id
                if test -z "$agent_task_id"; then
                    unresolved_task_state=unknown
                    echo "Agent update did not return a Task id" >&2
                    exit 1
                fi
                update_settled=1
                break
            fi
            case "$update_output" in
                'error: state.conflict: Agent already runs configured agent.image')
                    printf 'Agent: already running configured image\n'
                    update_settled=1
                    break
                    ;;
                'error: resource.in_use:'*)
                    ;;
                *)
                    printf '%s\n' "$update_output" >&2
                    retain_recovery=1
                    unresolved_task_state=unknown
                    echo "Agent update outcome is unknown; refusing to race it with rollback" >&2
                    exit 1
                    ;;
            esac
        fi
        attempt=$((attempt + 1))
        if test "$attempt" -ge 330; then
            break
        fi
        sleep 1
        if ! load_agent_list; then
            exit 1
        fi
    done
    if test "$update_settled" -ne 1; then
        echo "Agent remained busy for 330 seconds; its lifecycle may still be in flight" >&2
        exit 1
    fi
}

wait_for_agent_ready() {
    attempt=0
    while test "$attempt" -lt 150; do
        if ! ready_output=$(/usr/local/bin/groundplane \
            --host http://127.0.0.1:8080 \
            --output json \
            agent list 2>&1); then
            printf '%s\n' "$ready_output" >&2
            return 1
        fi
        agent_status=$(printf '%s\n' "$ready_output" |
            sed -n 's/^[[:space:]]*"status":[[:space:]]*"\([^"]*\)".*$/\1/p' |
            head -n 1)
        case "$agent_status" in
            healthy)
                printf 'Agent: Ready\n'
                return 0
                ;;
            pending | degraded | stopped)
                ;;
            *)
                echo "Agent returned unknown status while waiting for Ready: $agent_status" >&2
                return 1
                ;;
        esac
        attempt=$((attempt + 1))
        sleep 1
    done
    echo "Agent did not report Ready within 150 seconds" >&2
    return 1
}

agent_task_id=""
if ! load_agent_list; then
    exit 1
fi
case "$agent_list_state" in
present)
    selected_agent_id=$agent_id
    dispatch_agent_update
    ;;
absent)
    if join_output=$(/usr/local/bin/groundplane \
        --host http://127.0.0.1:8080 \
        --output json \
        agent join 2>&1); then
        retain_recovery=1
        unresolved_task_state=dispatched
    else
        printf '%s\n' "$join_output" >&2
        retain_recovery=1
        unresolved_task_state=unknown
        echo "Agent enrollment outcome is unknown; refusing to race it with rollback" >&2
        exit 1
    fi
    printf '%s\n' "$join_output"
    agent_task_id=$(printf '%s\n' "$join_output" |
        sed -n 's/^[[:space:]]*"task_id":[[:space:]]*"\([^"]*\)".*$/\1/p')
    unresolved_task_id=$agent_task_id
    if test -z "$agent_task_id"; then
        unresolved_task_state=unknown
        echo "Agent enrollment did not return a Task id" >&2
        exit 1
    fi
    ;;
*)
    echo "Agent list returned unknown state: $agent_list_state" >&2
    exit 1
    ;;
esac
if test -n "$agent_task_id"; then
    wait_for_agent_task "$agent_task_id"
else
    wait_for_agent_ready
    rollback=0
fi

# Publish the first selectable release only after bootstrap rollback is disarmed.
# No native update can be accepted while the shell owns bootstrap restoration.
rollback=0
release=$(python3 "$deploy_dir/controller_release.py" "$deploy_dir" "$agent_ref")
printf 'Staged Controller release: %s\n' "$release"
printf 'Controller: active\n'
printf 'Agent image: %s\n' "$agent_ref"
printf 'Runner image: %s\n' "$runner_ref"
