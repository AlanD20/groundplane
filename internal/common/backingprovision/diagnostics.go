package backingprovision

import "github.com/AlanD20/groundplane/internal/common/workexecution"

// DiagnosticStage describes a durable boundary without exposing configuration,
// commands, credentials or provider output. Confirm retains its actual Begin.
func DiagnosticStage(
	attempt workexecution.Attempt,
	layout Layout,
	begin, progress Progress,
) (workexecution.Stage, error) {
	if begin.Validate(attempt, layout) != nil || !begin.InFlight ||
		progress.InFlight && !EqualProgress(begin, progress) ||
		!progress.InFlight && ValidateTransition(attempt, layout, &begin, progress) != nil {
		return workexecution.Stage{}, invalid("backing creation diagnostic has no matching recorded boundary")
	}
	stage := workexecution.Stage{Key: string(begin.Stage), StartedAt: begin.BeganAt,
		State: workexecution.StageRunning, Observed: "Authorized; waiting for the Agent's observation"}
	var completed string
	switch begin.Stage {
	case PrepareDirectories:
		stage.Name, stage.Expected = "Prepare Backing storage", "Create the captured directories and verify their ownership without deleting data"
		completed = "Captured directories and storage ownership were verified"
	case WriteConfiguration:
		stage.Name, stage.Expected = "Write Backing configuration", "Write the captured configuration with its required ownership, permissions and content"
		completed = "Captured configuration was written and verified"
	case ReconcileRuntime:
		stage.Name, stage.Expected = "Start Backing container", "Start the captured image and Compose service without pulling another image or starting dependencies"
		completed = "The captured Backing container is running"
	case WaitReady:
		stage.Name, stage.Expected = "Wait for Backing readiness", "Observe the captured container running and its configured healthcheck passing, when present"
		completed = "The captured container meets its readiness requirements"
	case AfterStart:
		stage.Name, stage.Expected = "Run Backing initialization hook", "Execute the captured hook once against the captured container and record its validated result"
		completed = "The initialization hook exited successfully and its result was validated"
	case VerifyRuntime:
		stage.Name, stage.Expected = "Verify Backing runtime", "Verify the container image, ownership and readiness before publishing applied state"
		completed = "Container image, ownership and readiness match the captured input"
	default:
		return workexecution.Stage{}, invalid("backing creation diagnostic names no executable stage")
	}
	if !progress.InFlight {
		stage.State, stage.FinishedAt, stage.Observed = workexecution.StageSucceeded, &progress.UpdatedAt, completed
	}
	return stage, stage.Validate()
}
