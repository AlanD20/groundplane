#!/usr/bin/env bash

supervisor_proc_state() {
	local supervisor_pid="$1"
	local proc_root="${2:-/proc}"
	local stat_path="$proc_root/$supervisor_pid/stat"
	local proc_pid proc_comm proc_state proc_rest
	[[ "$supervisor_pid" =~ ^[0-9]+$ ]] || return 2
	if [[ ! -e "$stat_path" ]]; then
		return 1
	fi
	[[ -r "$stat_path" ]] || return 2
	if ! IFS=' ' read -r proc_pid proc_comm proc_state proc_rest <"$stat_path"; then
		[[ ! -e "$stat_path" ]] && return 1
		return 2
	fi
	[[ "$proc_pid" == "$supervisor_pid" ]] || return 2
	printf '%s\n' "$proc_state"
}

wait_for_supervisor_exit() {
	local supervisor_pid="$1"
	local iterations="$2"
	local proc_root="${3:-/proc}"
	local supervisor_state proc_status
	for ((iteration = 0; iteration < iterations; iteration++)); do
        if supervisor_state="$(supervisor_proc_state "$supervisor_pid" "$proc_root")"; then
            proc_status=0
        else
            proc_status=$?
        fi
		if [[ "$proc_status" -eq 1 ]]; then
			return 0
		fi
		if [[ "$proc_status" -ne 0 ]]; then
			return 2
		fi
		if [[ "$supervisor_state" == Z* ]]; then
			return 0
		fi
		sleep 0.1
	done
	return 1
}
