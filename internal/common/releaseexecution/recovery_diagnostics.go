package releaseexecution

import "github.com/AlanD20/groundplane/internal/common/workexecution"

// RecoveryStage describes only acknowledged action boundaries. Times come from
// those boundaries, not reconstructed wall-clock durations or current resources.
func RecoveryStage(input RecoveryInput, before *RecoveryReport, report RecoveryReport) (workexecution.Stage, error) {
	if report.Validate(input) != nil {
		return workexecution.Stage{}, invalidProgress()
	}
	boundary := report
	if !report.Progress.InFlight {
		if before == nil || before.Validate(input) != nil || !before.Progress.InFlight ||
			before.StageID != report.StageID || ValidateRecoveryTransition(input, &before.Progress, report.Progress) != nil {
			return workexecution.Stage{}, invalidProgress()
		}
		boundary = *before
	}
	stage := workexecution.Stage{Key: string(boundary.Progress.Phase), State: workexecution.StageRunning,
		StartedAt: &boundary.Progress.UpdatedAt, Observed: "Action authorized; waiting for its result"}
	var completed string
	switch boundary.Progress.Phase {
	case QuiesceForward:
		stage.Name, stage.Expected = "Stop the previous executor", "Confirm exclusive execution ownership and absence of the previous deployment helper"
		completed = "Previous deployment helper is absent and execution ownership is held"
	case InspectFailedRuntime:
		stage.Name, stage.Expected = "Inspect deployment containers", "Identify candidate and previous containers using their captured ownership"
		completed = "Container ownership was inspected and the inventory was recorded"
	case RestoreConfiguration:
		stage.Name, stage.Expected = "Restore captured configuration", "Restore the selected prior files and remove files that did not previously exist"
		completed = "Selected files match the captured configuration"
	case RestoreServingRuntime:
		stage.Name, stage.Expected = "Restore the previous service", "Restore captured workloads and proxy routing, or preserve absence for a first deployment"
		completed = "The captured previous runtime was restored"
	case RemoveFailedCandidate:
		stage.Name, stage.Expected = "Remove failed deployment containers", "Remove only owned candidate containers while preserving application data"
		completed = "Candidate removal and retained runtime restoration were verified"
	case VerifyRestoration:
		stage.Name, stage.Expected = "Verify restored service", "Verify files, workloads, proxy routing and candidate absence without changing them"
		completed = "Configuration and runtime restoration proofs were recorded"
	default:
		return workexecution.Stage{}, invalidProgress()
	}
	if !report.Progress.InFlight {
		stage.State, stage.FinishedAt, stage.Observed = workexecution.StageSucceeded, &report.Progress.UpdatedAt, completed
	}
	return stage, stage.Validate()
}
