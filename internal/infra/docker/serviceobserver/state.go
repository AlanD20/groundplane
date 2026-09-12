package serviceobserver

import (
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/container"
)

// countState projects no Docker diagnostics, environment or healthcheck output.
// Unknown or contradictory evidence invalidates the entire Service row.
func countState(counts *agentpb.ServiceReplicaCounts, state *container.State) bool {
	if state == nil {
		return false
	}
	switch state.Status {
	case container.StateRunning:
		if !state.Running || state.Paused || state.Restarting || state.Dead {
			return false
		}
		if state.Health == nil {
			counts.Running++
			return true
		}
		switch state.Health.Status {
		case container.Healthy:
			counts.Healthy++
		case container.Starting:
			counts.Starting++
		case container.Unhealthy:
			counts.Unhealthy++
		default:
			return false
		}
	case container.StateCreated, container.StateRemoving:
		if state.Running || state.Paused || state.Restarting || state.Dead {
			return false
		}
		counts.Transitional++
	case container.StatePaused:
		if !state.Running || !state.Paused || state.Restarting || state.Dead {
			return false
		}
		counts.Transitional++
	case container.StateRestarting:
		if !state.Running || !state.Restarting || state.Paused || state.Dead {
			return false
		}
		counts.Transitional++
	case container.StateExited:
		if state.Running || state.Paused || state.Restarting || state.Dead {
			return false
		}
		if state.ExitCode == 0 {
			counts.Stopped++
		} else {
			counts.Failed++
		}
	case container.StateDead:
		if !state.Dead || state.Running || state.Paused || state.Restarting {
			return false
		}
		counts.Failed++
	default:
		return false
	}
	return true
}
