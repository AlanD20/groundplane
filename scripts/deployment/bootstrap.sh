rollback=0
service_was_active=0
service_was_enabled=0
retain_recovery=0
unresolved_task_id=""
unresolved_task_state=unknown
native_initialized=0
cli_stage=""
cli_activation_pending=0
export PYTHONDONTWRITEBYTECODE=1

backup_path() {
    source=$1
    name=$2
    if test -L "$source"; then
        echo "$source must be a regular file or absent" >&2
        return 1
    fi
    if test -e "$source"; then
        if ! test -f "$source"; then
            echo "$source must be a regular file or absent" >&2
            return 1
        fi
        cp -a -- "$source" "$deploy_dir/backup-$name"
        : > "$deploy_dir/had-$name"
    fi
}

restore_path() {
    destination=$1
    name=$2
    backup="$deploy_dir/backup-$name"
    marker="$deploy_dir/had-$name"
    staged="${destination}.groundplane-rollback-$deploy_id"
    if test -L "$destination"; then
        echo "$destination is not a regular rollback destination" >&2
        return 1
    fi
    if test -e "$destination" && ! test -f "$destination"; then
        echo "$destination is not a regular rollback destination" >&2
        return 1
    fi
    if ! rm -f -- "$staged"; then
        return 1
    fi
    if test -e "$marker" || test -L "$marker"; then
        if test -L "$marker" || ! test -f "$marker";
            then
            echo "invalid rollback marker for $destination" >&2
            return 1
        fi
        if test -L "$backup" || ! test -f "$backup"; then
            echo "invalid rollback backup for $destination" >&2
            return 1
        fi
        if cp -a -- "$backup" "$staged"; then
            :
        else
            restore_status=$?
            rm -f -- "$staged" || true
            return "$restore_status"
        fi
        if mv -fT -- "$staged" "$destination"; then
            :
        else
            restore_status=$?
            rm -f -- "$staged" || true
            return "$restore_status"
        fi
    else
        if test -e "$backup" || test -L "$backup"; then
            echo "rollback backup exists without marker for $destination" >&2
            return 1
        fi
        rm -f -- "$destination"
    fi
}

finish() {
    status=$?
    trap - EXIT HUP INT TERM
    if test -n "$cli_stage"; then
        if ! rm -f -- "$cli_stage"; then
            echo "could not remove owned CLI staging file: $cli_stage" >&2
            if test "$status" -eq 0; then
                status=1
            fi
        fi
    fi
    if test "$status" -ne 0 && test "$cli_activation_pending" -eq 1; then
        echo "Controller update completed, but CLI activation did not; rerun this installer" >&2
        echo "CLI candidate retained in $deploy_dir" >&2
        exit "$status"
    fi
    if test "$status" -ne 0 && test "$retain_recovery" -eq 1 && test "$rollback" -eq 0; then
        echo "native update failed or remains unresolved; no SSH rollback attempted" >&2
        echo "deployment receipt is retained under /var/lib/groundplane/controller-updates" >&2
        echo "recovery helper retained: python3 $deploy_dir/controller_update.py --resume" >&2
        exit "$status"
    fi
    if test "$status" -ne 0 && test "$rollback" -eq 1; then
        if test "$retain_recovery" -eq 1; then
            if test -n "$unresolved_task_id"; then
                echo "Agent lifecycle Task $unresolved_task_id state: $unresolved_task_state" >&2
            else
                echo "Agent lifecycle state: $unresolved_task_state" >&2
            fi
            echo "rollback was not attempted; recovery files retained in $deploy_dir" >&2
            echo "wait for the Agent lifecycle to settle, then rerun this deployment" >&2
            exit "$status"
        fi
        echo "deployment failed; restoring the previous Controller installation" >&2
        rollback_failed=0
        if ! systemctl stop groundplane-controller.service; then
            echo "Controller stop failed; retaining installation and recovery files in $deploy_dir" >&2
            exit 1
        fi
        if test "$native_initialized" -eq 1; then
            if ! python3 "$deploy_dir/controller_bootstrap.py" remove-empty; then
                echo "native evidence exists; retaining installation and recovery files in $deploy_dir" >&2
                exit 1
            fi
        fi
        if ! restore_path /usr/local/libexec/groundplane/controller controller; then
            rollback_failed=1
        fi
        if ! restore_path /usr/local/libexec/groundplane/controller-recovery controller-recovery; then
            rollback_failed=1
        fi
        if ! restore_path /usr/local/bin/groundplane cli; then
            rollback_failed=1
        fi
        if ! restore_path /etc/systemd/system/groundplane-controller.service controller-unit; then
            rollback_failed=1
        fi
        if ! restore_path /usr/lib/tmpfiles.d/groundplane.conf tmpfiles; then
            rollback_failed=1
        fi
        if ! restore_path /etc/groundplane/controller.yaml controller-config; then
            rollback_failed=1
        fi
        if ! restore_path /etc/groundplane/controller.age controller-age; then
            rollback_failed=1
        fi
        if ! systemctl daemon-reload; then
            rollback_failed=1
        fi
        if test -e "$deploy_dir/had-controller-unit"; then
            if test "$service_was_enabled" -eq 1; then
                if ! systemctl enable groundplane-controller.service >/dev/null; then
                    rollback_failed=1
                fi
            else
                if ! systemctl disable groundplane-controller.service >/dev/null; then
                    rollback_failed=1
                fi
            fi
            if test "$service_was_active" -eq 1; then
                if ! systemctl restart groundplane-controller.service; then
                    rollback_failed=1
                fi
            else
                if ! systemctl stop groundplane-controller.service >/dev/null; then
                    rollback_failed=1
                fi
            fi
        fi
        if test "$rollback_failed" -ne 0; then
            echo "rollback incomplete; recovery files retained in $deploy_dir" >&2
            exit 1
        fi
    fi
    rm -rf -- "$deploy_dir"
    exit "$status"
}

trap finish EXIT HUP INT TERM

command -v docker >/dev/null
command -v curl >/dev/null
command -v flock >/dev/null
command -v python3 >/dev/null
systemctl is-active --quiet docker.service
curl -fsS http://127.0.0.1:5000/v2/ >/dev/null

resolve_repo_digest() {
    image=$1
    repository=$2
    repo_digest=$(
        docker image inspect "$image" \
            --format '{{range .RepoDigests}}{{println .}}{{end}}' |
            while IFS= read -r reference; do
                case "$reference" in
                    "$repository"@sha256:*)
                        digest=${reference#"$repository"@}
                        if printf '%s\n' "$digest" | grep -Eq '^sha256:[0-9a-f]{64}$'; then
                            printf '%s\n' "$digest"
                        fi
                        ;;
                esac
            done |
            tail -n 1
    )
    if ! printf '%s\n' "$repo_digest" | grep -Eq '^sha256:[0-9a-f]{64}$'; then
        echo "image has no immutable RepoDigest for $repository: $image" >&2
        return 1
    fi
    printf '%s@%s\n' "$repository" "$repo_digest"
}

publish_image() {
    repository_name=$1
    source_image=$2
    role=$3
    repository="localhost:5000/$repository_name"
    target_image="$repository:$version"
    source_id=$(docker image inspect "$source_image" --format '{{.Id}}')
    target_id=""
    target_ref=""
    if docker pull "$target_image" >/dev/null 2>&1; then
        target_id=$(docker image inspect "$target_image" --format '{{.Id}}')
        if target_ref=$(resolve_repo_digest "$target_image" "$repository" 2>/dev/null); then
            :
        else
            target_ref=""
        fi
    fi

    if test "$source_id" = "$target_id" && test -n "$target_ref"; then
        printf 'Reusing target %s image %s\n' "$role" "$target_ref" >&2
        printf '%s\n' "$target_ref"
        return 0
    fi

    docker tag "$source_image" "$target_image"
    push_output=$(docker push "$target_image")
    printf '%s\n' "$push_output" >&2
    docker pull "$target_image" >/dev/null
    resolve_repo_digest "$target_image" "$repository"
}

deployment_mode=$(python3 "$deploy_dir/controller_bootstrap.py" mode)
if test "$deployment_mode" = native; then
    if test "$bootstrap" -eq 1; then
        echo "native recovery is already installed; omit --bootstrap and use a normal update" >&2
        exit 1
    fi
else
    if test "$stage_only" -eq 1; then
        echo "--stage-only requires an existing native recovery installation" >&2
        exit 1
    fi
    if test -e /usr/local/libexec/groundplane/controller && test "$bootstrap" -ne 1; then
        echo "legacy Controller requires an explicit --bootstrap maintenance invocation" >&2
        exit 1
    fi
    if systemctl is-active --quiet groundplane-controller.service; then
        python3 "$deploy_dir/controller_bootstrap.py" require-idle
    fi
fi

agent_ref=$(publish_image groundplane-agent "$source_agent_image" Agent)
if test "$deployment_mode" = native; then
    release=$(python3 "$deploy_dir/controller_release.py" "$deploy_dir" "$agent_ref")
    printf 'Staged Controller release: %s\n' "$release"
    if test "$stage_only" -eq 1; then
        exit 0
    fi
    cli_target=/usr/local/bin/groundplane
    if test ! -f "$cli_target" || test -L "$cli_target" ||
        test "$(stat -c '%u:%h:%a' "$cli_target")" != '0:1:755'; then
        echo "installed CLI is not a root-owned regular executable" >&2
        exit 1
    fi
    if test ! -f "$deploy_dir/groundplane" || test -L "$deploy_dir/groundplane"; then
        echo "candidate CLI is not a regular file" >&2
        exit 1
    fi
    if ! cmp -s "$deploy_dir/groundplane" "$cli_target"; then
        cli_stage="/usr/local/bin/groundplane.groundplane-$deploy_id"
        if test -e "$cli_stage" || test -L "$cli_stage"; then
            echo "CLI staging path already exists" >&2
            exit 1
        fi
        install -m 0755 -o root -g root "$deploy_dir/groundplane" "$cli_stage"
        cmp -s "$deploy_dir/groundplane" "$cli_stage"
        sync -f "$cli_stage"
    fi
    retain_recovery=1
    python3 "$deploy_dir/controller_update.py" "$release" "groundplane-deploy-$deploy_id" --ensure
    retain_recovery=0
    cli_activation_pending=1
    if test -n "$cli_stage"; then
        mv -fT -- "$cli_stage" "$cli_target"
        cli_stage=""
        sync -f /usr/local/bin
    fi
    cmp -s "$deploy_dir/groundplane" "$cli_target"
    cli_activation_pending=0
    printf 'CLI: current\n'
    exit 0
fi
runner_ref=$(publish_image groundplane-runner "$source_runner_image" Runner)

if systemctl is-active --quiet groundplane-controller.service; then
    service_was_active=1
fi
if systemctl is-enabled --quiet groundplane-controller.service; then
    service_was_enabled=1
fi

backup_path /usr/local/libexec/groundplane/controller controller
backup_path /usr/local/libexec/groundplane/controller-recovery controller-recovery
backup_path /usr/local/bin/groundplane cli
backup_path /etc/systemd/system/groundplane-controller.service controller-unit
backup_path /usr/lib/tmpfiles.d/groundplane.conf tmpfiles
backup_path /etc/groundplane/controller.yaml controller-config
backup_path /etc/groundplane/controller.age controller-age
rollback=1

# Legacy binaries cannot fence new work: --bootstrap is an explicit maintenance
# boundary. Recheck immediately before stopping; never abort existing Tasks.
if test "$service_was_active" -eq 1; then
    python3 "$deploy_dir/controller_bootstrap.py" require-idle
    systemctl stop groundplane-controller.service
fi
python3 "$deploy_dir/controller_bootstrap.py" initialize
native_initialized=1

runtime_changed=0
if ! cmp -s "$deploy_dir/controller" /usr/local/libexec/groundplane/controller ||
    ! cmp -s "$deploy_dir/groundplane" /usr/local/bin/groundplane ||
    ! cmp -s "$deploy_dir/groundplane-controller.service" \
        /etc/systemd/system/groundplane-controller.service ||
    ! cmp -s "$deploy_dir/groundplane.conf" /usr/lib/tmpfiles.d/groundplane.conf; then
    runtime_changed=1
fi

install -d -m 0755 /usr/local/libexec/groundplane
install -d -m 0700 /etc/groundplane /var/log/groundplane
install -Dm500 "$deploy_dir/controller" /usr/local/libexec/groundplane/controller.new
mv -f /usr/local/libexec/groundplane/controller.new /usr/local/libexec/groundplane/controller
install -Dm500 "$deploy_dir/controller" /usr/local/libexec/groundplane/controller-recovery.new
mv -f /usr/local/libexec/groundplane/controller-recovery.new /usr/local/libexec/groundplane/controller-recovery
install -Dm755 "$deploy_dir/groundplane" /usr/local/bin/groundplane.new
mv -f /usr/local/bin/groundplane.new /usr/local/bin/groundplane
install -Dm644 "$deploy_dir/groundplane-controller.service" \
    /etc/systemd/system/groundplane-controller.service
install -Dm644 "$deploy_dir/groundplane.conf" /usr/lib/tmpfiles.d/groundplane.conf

if ! test -f /etc/groundplane/controller.yaml; then
    install -Dm600 "$deploy_dir/controller.yaml.example" /etc/groundplane/controller.yaml
fi

if test -e /etc/groundplane/controller.age; then
    if ! test -f /etc/groundplane/controller.age ||
        test -L /etc/groundplane/controller.age ||
        test "$(stat -c '%u:%a' /etc/groundplane/controller.age)" != "0:600"; then
        echo "/etc/groundplane/controller.age must be a root-owned regular file with mode 0600" >&2
        exit 1
    fi
else
    command -v age-keygen >/dev/null
    generated_key="$deploy_dir/controller.age.generated"
    rm -f -- "$generated_key"
    if ! age-keygen -o "$generated_key" >/dev/null 2>&1; then
        rm -f -- "$generated_key"
        echo "failed to generate the Controller age identity" >&2
        exit 1
    fi
    install -m 0600 -o root -g root "$generated_key" /etc/groundplane/controller.age
    rm -f -- "$generated_key"
fi

rendered_config="$deploy_dir/controller.yaml.rendered"
awk -v image="$agent_ref" '
BEGIN {
    in_agent = 0
    saw_agent = 0
    wrote_image = 0
}
$0 ~ /^agent:[[:space:]]*$/ {
    saw_agent = 1
    in_agent = 1
    print
    next
}
in_agent && $0 ~ /^[^[:space:]#]/ {
    if (!wrote_image) {
        print "  image: " image
        wrote_image = 1
    }
    in_agent = 0
}
in_agent && $0 ~ /^[[:space:]]+image:[[:space:]]*/ {
    print "  image: " image
    wrote_image = 1
    next
}
{
    print
}
END {
    if (in_agent && !wrote_image) {
        print "  image: " image
    } else if (!saw_agent) {
        print ""
        print "agent:"
        print "  image: " image
    }
}
' /etc/groundplane/controller.yaml > "$rendered_config"

rendered_runner_config="$deploy_dir/controller.runner.yaml.rendered"
awk -v image="$runner_ref" '
BEGIN {
    in_runner = 0
    saw_runner = 0
    wrote_image = 0
}
$0 ~ /^runner:[[:space:]]*$/ {
    saw_runner = 1
    in_runner = 1
    print
    next
}
in_runner && $0 ~ /^[^[:space:]#]/ {
    if (!wrote_image) {
        print "  image: " image
        wrote_image = 1
    }
    in_runner = 0
}
in_runner && $0 ~ /^[[:space:]]+image:[[:space:]]*/ {
    print "  image: " image
    wrote_image = 1
    next
}
{
    print
}
END {
    if (in_runner && !wrote_image) {
        print "  image: " image
    } else if (!saw_runner) {
        print ""
        print "runner:"
        print "  image: " image
    }
}
' "$rendered_config" > "$rendered_runner_config"

rendered_listen_config="$deploy_dir/controller.listen.yaml.rendered"
awk -v endpoint="$listen_ip:8080" '
BEGIN {
    in_http = 0
    saw_endpoint = 0
    inserted = 0
}
$0 == "  http:" {
    in_http = 1
}
in_http && $0 == "    - " endpoint {
    saw_endpoint = 1
}
in_http && $0 != "  http:" && $0 !~ /^    - / && !inserted {
    if (!saw_endpoint) {
        print "    - " endpoint
    }
    inserted = 1
    in_http = 0
}
{
    print
}
END {
    if (in_http && !saw_endpoint) {
        print "    - " endpoint
    }
}
' "$rendered_runner_config" > "$rendered_listen_config"
if ! cmp -s "$rendered_listen_config" /etc/groundplane/controller.yaml; then
    runtime_changed=1
fi
install -m 0600 -o root -g root "$rendered_listen_config" /etc/groundplane/controller.yaml

systemd-tmpfiles --create /usr/lib/tmpfiles.d/groundplane.conf
if test "$runtime_changed" -eq 1; then
    systemctl daemon-reload
fi
systemctl enable groundplane-controller.service >/dev/null
if test "$runtime_changed" -eq 1 || test "$service_was_active" -ne 1; then
    systemctl restart groundplane-controller.service
fi

healthy=0
attempt=0
while test "$attempt" -lt 30; do
    if curl -fsS http://127.0.0.1:8080/api/v1/host >/dev/null 2>&1; then
        healthy=1
        break
    fi
    attempt=$((attempt + 1))
    sleep 1
done
if test "$healthy" -ne 1; then
    journalctl -u groundplane-controller.service -n 100 --no-pager >&2 || true
    echo "Controller did not become healthy within 30 seconds" >&2
    exit 1
fi
