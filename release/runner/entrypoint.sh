#!/bin/sh
set -eu

umask 077

# The home contains independently maintained upstream software. The container
# image identity is verified by GP; a newer upstream package must survive restart.
if ! test -f .groundplane-runner-version; then
    cp -R /opt/actions-runner/. .
fi

if ! test -f .runner; then
    IFS= read -r github_url || exit 64
    IFS= read -r runner_name || exit 64
    IFS= read -r labels || exit 64
    IFS= read -r runner_group || exit 64
    IFS= read -r registration_token || exit 64
    if test -z "$github_url" || test -z "$runner_name" || test -z "$runner_group" || test -z "$registration_token"; then
        registration_token=
        exit 64
    fi
    set -- ./config.sh \
        --unattended \
        --url "$github_url" \
        --name "$runner_name" \
        --runnergroup "$runner_group" \
        --work _work \
        --disableupdate
    if test -n "$labels"; then
        set -- "$@" --labels "$labels"
    fi
    ACTIONS_RUNNER_INPUT_TOKEN="$registration_token" "$@" </dev/null
fi

registration_token=
exec 0</dev/null
exec ./run.sh
