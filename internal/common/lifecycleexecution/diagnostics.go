package lifecycleexecution

import (
	"strings"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// DiagnosticStages uses acknowledged stage timing and receipts, never command
// text or an inference that a running container means a hook succeeded.
func DiagnosticStages(attempt workexecution.Attempt, input *agentpb.ServiceLifecycleWorkInput,
	begin, progress Progress,
) (workexecution.Stage, workexecution.Stage, error) {
	if begin.Validate(attempt, input) != nil || progress.Validate(attempt, input) != nil || begin.BeganAt == nil ||
		progress.UpdatedAt.Before(begin.UpdatedAt) {
		return workexecution.Stage{}, workexecution.Stage{}, invalid()
	}
	stages, err := Stages(input)
	if err != nil {
		return workexecution.Stage{}, workexecution.Stage{}, err
	}
	current := stages[len(begin.Completed)]
	stage := workexecution.Stage{Key: strings.ToLower(current.ID), StartedAt: begin.BeganAt,
		State: workexecution.StageRunning, Observed: "The Agent is inspecting or applying the captured action"}
	observation := ""
	switch current.Expected {
	case Running:
		stage.Name = "Start " + current.Member
		stage.Expected = "The captured container must be running and pass its configured healthcheck, if any"
		observation = "The captured container is running and its configured readiness checks passed"
	case Stopped:
		stage.Name = "Stop " + current.Member
		stage.Expected = "The captured container must be stopped without deleting its data"
		observation = "The captured container is stopped"
	case Absent:
		stage.Name = "Remove runtime for " + current.Member
		stage.Expected = "The captured container must be absent; persistent Volume data is retained"
		observation = "The captured container is absent"
	case HookSucceeded:
		stage.Name = "Run backing Service lifecycle hook"
		stage.Expected = "The captured hook must finish successfully; container state alone is not proof"
		observation = "The captured hook completed successfully"
	}
	result := stage
	if progress.BeganAt == nil {
		if progress.Sequence != begin.Sequence+1 || len(progress.Completed) != len(begin.Completed)+1 {
			return workexecution.Stage{}, workexecution.Stage{}, invalid()
		}
		receipt := progress.Completed[len(begin.Completed)]
		result.State, result.FinishedAt, result.Observed = workexecution.StageSucceeded, &receipt.ObservedAt, observation
	} else if progress.Sequence != begin.Sequence {
		return workexecution.Stage{}, workexecution.Stage{}, invalid()
	}
	if err := stage.Validate(); err != nil {
		return workexecution.Stage{}, workexecution.Stage{}, err
	}
	return stage, result, result.Validate()
}
