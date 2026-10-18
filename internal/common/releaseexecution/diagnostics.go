package releaseexecution

import (
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// DeploymentStage describes the action that crossed the recorded boundary.
// Confirm advances the cursor, so its description must use the prior cursor.
func DeploymentStage(input Input, before *Progress, progress Progress) (workexecution.Stage, error) {
	if err := ValidateTransition(input, before, progress); err != nil {
		return workexecution.Stage{}, err
	}
	boundary := progress
	if !progress.InFlight {
		if before == nil || !before.InFlight {
			return workexecution.Stage{}, invalidProgress()
		}
		boundary = *before
	}
	stage := workexecution.Stage{Key: string(boundary.Phase), State: workexecution.StageRunning,
		StartedAt: boundary.BeganAt, Observed: "Action authorized; waiting for its result"}
	var completed string
	switch boundary.Phase {
	case Configuration:
		stage.Name, stage.Expected = "Write deployment configuration", "Materialize the captured files with their required ownership and permissions"
		completed = "Captured configuration files were written and verified"
	case PreHooks, PostHooks:
		position := strconv.FormatUint(uint64(boundary.Hook)+1, 10)
		stage.Key += "_" + position
		stage.Name = "Run pre-deploy hook " + position
		if boundary.Phase == PostHooks {
			stage.Name = "Run post-deploy hook " + position
		}
		stage.Expected = "Execute the captured hook and record its successful completion"
		completed = "Hook completion was recorded"
	case Rollout:
		if boundary.Rolling != nil {
			stage.Key += "_" + strconv.FormatUint(uint64(boundary.Rolling.Change), 10) + "_" + string(boundary.Rolling.Phase)
			switch boundary.Rolling.Phase {
			case rollingrelease.StartCandidate:
				stage.Name, stage.Expected, completed = "Start candidate replica", "Create the captured replacement replica", "Candidate replica was started"
			case rollingrelease.WaitCandidateHealthy:
				stage.Name, stage.Expected, completed = "Check candidate replica health", "Wait for the configured healthcheck within the replica deadline", "Candidate replica passed its healthcheck"
			case rollingrelease.SwitchTraffic:
				stage.Name, stage.Expected, completed = "Switch replica traffic", "Apply the captured proxy routing for this replacement", "Replica routing was applied and verified"
			case rollingrelease.DrainPredecessor:
				stage.Name, stage.Expected, completed = "Drain previous replica", "Allow the captured connection-drain period before removal", "Connection-drain period completed"
			case rollingrelease.RemovePredecessor, rollingrelease.RemoveRetained:
				stage.Name, stage.Expected, completed = "Remove previous replica", "Remove only the captured obsolete replica, preserving data", "Obsolete replica removal was verified"
			default:
				return workexecution.Stage{}, invalidProgress()
			}
		} else {
			stage.Key += "_" + string(boundary.Action)
			switch boundary.Action {
			case RemovePrevious, RetireSwitchedPrevious:
				stage.Name, stage.Expected, completed = "Remove previous workload", "Remove the captured previous containers without deleting application data", "Previous workload removal was verified"
			case StartCandidate:
				stage.Name, stage.Expected, completed = "Start replacement workload", "Create the workload from the captured image and service configuration", "Replacement workload was started"
			case WaitCandidate:
				stage.Name, stage.Expected, completed = "Check replacement readiness", "Verify the captured readiness requirements before serving traffic", "Replacement readiness requirements were satisfied"
			case SwitchTraffic:
				stage.Name, stage.Expected, completed = "Switch service traffic", "Apply and verify proxy routing to the replacement workload", "Service routing was applied and verified"
			case VerifyServing:
				stage.Name, stage.Expected, completed = "Verify serving workload", "Verify the final containers and routing against the captured deployment", "Final serving runtime was verified"
			default:
				return workexecution.Stage{}, invalidProgress()
			}
		}
	default:
		return workexecution.Stage{}, invalidProgress()
	}
	if !progress.InFlight {
		stage.State, stage.FinishedAt, stage.Observed = workexecution.StageSucceeded, &progress.UpdatedAt, completed
	}
	return stage, stage.Validate()
}
