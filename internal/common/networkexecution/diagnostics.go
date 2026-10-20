package networkexecution

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// DiagnosticStages describes an acknowledged boundary, not an inferred Docker
// mutation. An already matching bridge is a successful reconciliation too.
func DiagnosticStages(attempt workexecution.Attempt, input *agentpb.NetworkWorkInput,
	begin, progress Progress,
) (workexecution.Stage, workexecution.Stage, error) {
	if input == nil || begin.Validate(attempt, input.Action, input.NetworkId) != nil ||
		progress.Validate(attempt, input.Action, input.NetworkId) != nil ||
		begin.Sequence != 1 || !begin.InFlight || progress.Sequence < begin.Sequence ||
		progress.UpdatedAt.Before(begin.UpdatedAt) {
		return workexecution.Stage{}, workexecution.Stage{}, invalidProgress()
	}
	stage := workexecution.Stage{Key: string(begin.Stage), State: workexecution.StageRunning,
		StartedAt: begin.BeganAt, Observed: "Agent authorized to inspect or reconcile the selected Zone network"}
	completed := "The selected bridge exists and matches the captured Zone configuration"
	switch input.Action {
	case agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_ENSURE:
		stage.Name = "Create or verify Zone network"
		stage.Expected = "The selected bridge must match the captured subnet, ownership and isolation settings"
	case agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_REMOVE:
		stage.Name = "Remove Zone network"
		stage.Expected = "Disconnect only the captured workloads and verify that the selected bridge is absent"
		completed = "The selected Zone bridge is absent"
	default:
		return workexecution.Stage{}, workexecution.Stage{}, invalidProgress()
	}
	if err := stage.Validate(); err != nil {
		return workexecution.Stage{}, workexecution.Stage{}, err
	}
	result := stage
	if !progress.InFlight {
		result.State, result.FinishedAt, result.Observed = workexecution.StageSucceeded, &progress.UpdatedAt, completed
	}
	return stage, result, result.Validate()
}
