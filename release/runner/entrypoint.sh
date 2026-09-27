#!/bin/sh
set -eu

umask 077

image_version=$(cat /opt/actions-runner/.groundplane-runner-version)
if test -f .groundplane-runner-version; then
    installed_version=$(cat .groundplane-runner-version)
    if test "$installed_version" != "$image_version"; then
        registration_token=
        echo "Runner home belongs to a different immutable image" >&2
        exit 78
    fi
else
    cp -R /opt/actions-runner/. .
fi

if ! test -f .runner; then
    IFS= read -r github_url || exit 64
    IFS= read -r runner_name || exit 64
    IFS= read -r labels || exit 64
    IFS= read -r registration_token || exit 64
    if test -z "$github_url" || test -z "$runner_name" || test -z "$registration_token"; then
        registration_token=
        exit 64
    fi
    set -- ./config.sh \
        --unattended \
        --url "$github_url" \
        --name "$runner_name" \
        --runnergroup Default \
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
