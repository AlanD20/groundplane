package componentexecution

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// DiagnosticStage describes a recorded boundary, not inferred provider output.
// Confirm has already moved the cursor, so its saved Begin supplies the action
// and real start time. Configuration contents and commands are never exposed.
func DiagnosticStage(
	attempt workexecution.Attempt,
	input *agentpb.ComponentWorkInput,
	begin, progress Progress,
) (workexecution.Stage, error) {
	if begin.Validate(attempt, input) != nil || progress.Validate(attempt, input) != nil || !begin.InFlight ||
		(progress.InFlight && progress.Sequence != begin.Sequence) ||
		(!progress.InFlight && (progress.Sequence != begin.Sequence+1 || progress.Observation == nil ||
			!progress.Observation.Satisfies(begin.Stage, input.Remove))) {
		return workexecution.Stage{}, invalidProgress()
	}
	stage := workexecution.Stage{Key: string(begin.Stage), State: workexecution.StageRunning,
		StartedAt: begin.BeganAt, Observed: "Action authorized; waiting for the Agent's observation"}
	var completed string
	switch begin.Stage {
	case WriteConfiguration:
		stage.Name, stage.Expected = "Write Component configuration", "Write the captured files with their required ownership and permissions"
		completed = "Captured files were written and verified"
	case ReconcileRuntime:
		stage.Name, stage.Expected = "Apply Component runtime", "Run the captured Component container and remove only its captured predecessor when required"
		completed = "Captured Component runtime was reconciled"
	case ActivateConfiguration:
		stage.Name, stage.Expected = "Activate Component configuration", "Activate the captured provider configuration and verify the provider is serving it"
		completed = "Provider activation was observed and verified"
	case RemoveRuntime:
		stage.Name, stage.Expected = "Remove Component runtime", "Remove the captured Component containers without deleting application data"
		completed = "Captured Component containers are absent"
	case VerifyRuntime:
		stage.Name, stage.Expected = "Verify Component runtime", "Verify the expected container and its captured readiness requirements"
		completed = "Component runtime satisfies the captured readiness requirements"
		if input.Remove {
			stage.Expected, completed = "Verify that no captured Component container remains", "Component runtime removal was verified"
		}
	default:
		return workexecution.Stage{}, invalidProgress()
	}
	if !progress.InFlight {
		stage.State, stage.FinishedAt, stage.Observed = workexecution.StageSucceeded, &progress.UpdatedAt, completed
	}
	return stage, stage.Validate()
}
