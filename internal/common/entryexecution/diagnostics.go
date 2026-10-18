package entryexecution

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func DiagnosticStage(attempt workexecution.Attempt, input *agentpb.EntryConfigurationWorkInput,
	begin, progress Progress,
) (workexecution.Stage, error) {
	if begin.Validate(attempt, input) != nil || !begin.InFlight {
		return workexecution.Stage{}, invalidLayout()
	}
	if progress.InFlight {
		if !EqualProgress(begin, progress) {
			return workexecution.Stage{}, invalidLayout()
		}
	} else if ValidateTransition(attempt, input, &begin, progress) != nil {
		return workexecution.Stage{}, invalidLayout()
	}
	stage := workexecution.Stage{Key: string(begin.Stage), StartedAt: begin.BeganAt,
		State: workexecution.StageRunning, Observed: "Authorized; waiting for the Agent's observation"}
	var completed string
	switch begin.Stage {
	case WriteConfiguration:
		stage.Name = "Update Entry files"
		stage.Expected = "Write or remove the captured configuration files without changing unrelated paths"
		completed = "Captured file operations completed"
	case VerifyConfiguration:
		stage.Name = "Verify Entry files"
		stage.Expected = "Check content, ownership, permissions and required file removals against the captured input"
		completed = "Configuration files match the captured input"
		if input.Removal {
			completed += "; running Services keep their loaded configuration until their next Deploy"
		}
	case ReconcileConsumers:
		stage.Name = "Update affected workloads"
		stage.Expected = "Apply configuration only to the selected running workloads; do not start stopped Services or dependencies"
		completed = "Selected workloads were reconciled"
	case VerifyConsumers:
		stage.Name = "Verify affected workloads"
		stage.Expected = "Verify the exact captured workload instances after their configuration update"
		completed = "Selected workload identities and configuration were verified"
	default:
		return workexecution.Stage{}, invalidLayout()
	}
	if !progress.InFlight {
		stage.State, stage.FinishedAt, stage.Observed = workexecution.StageSucceeded, &progress.UpdatedAt, completed
	}
	return stage, stage.Validate()
}
