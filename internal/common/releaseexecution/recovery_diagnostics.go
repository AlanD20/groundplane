package releaseexecution

import (
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

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
		if input.PreservesServing() {
			stage.Name, stage.Expected = "Verify retained configuration", "Keep the serving deployment's files and verify their captured contents without rewriting them"
		}
	case RestoreServingRuntime:
		stage.Name, stage.Expected = "Restore the previous service", "Restore captured workloads and proxy routing, or preserve absence for a first deployment"
		completed = "The captured previous runtime was restored"
		if input.PreservesServing() {
			stage.Name, stage.Expected = "Keep the serving deployment", "Inspect the completed switch's containers and routing without restarting them"
			completed = "Serving deployment ownership and routing were verified without runtime changes"
		}
	case RemoveFailedCandidate:
		stage.Name, stage.Expected = "Remove failed deployment containers", "Remove only owned candidate containers while preserving application data"
		completed = "Candidate removal and retained runtime restoration were verified"
		if input.PreservesServing() {
			stage.Name, stage.Expected = "Preserve deployment containers", "Leave the completed switch in place under the captured leave_active policy"
			completed = "The serving deployment remains in place; no containers were removed"
		}
	case VerifyRestoration:
		stage.Name, stage.Expected = "Verify restored service", "Verify files, workloads, proxy routing and candidate absence without changing them"
		completed = "Configuration and runtime restoration proofs were recorded"
		if input.PreservesServing() {
			stage.Name, stage.Expected = "Verify retained deployment", "Verify captured files, container ownership and routing without claiming application health"
			completed = "Retained configuration and deployment ownership proofs were recorded"
		}
	case FailureHooks:
		position := strconv.FormatUint(uint64(boundary.Progress.Hook)+1, 10)
		stage.Key += "_" + position
		stage.Name, stage.Expected = "Run failure hook "+position, "Run the captured failure hook after the selected recovery action and prove its cleanup"
		completed = "Failure hook outcome and cleanup were recorded; the original deployment remains failed"
	default:
		return workexecution.Stage{}, invalidProgress()
	}
	if !report.Progress.InFlight {
		stage.State, stage.FinishedAt, stage.Observed = workexecution.StageSucceeded, &report.Progress.UpdatedAt, completed
	}
	return stage, stage.Validate()
}
