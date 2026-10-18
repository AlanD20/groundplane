package taskinspection

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
)

// Runner outcomes use accepted typed evidence, not logs or current desired state.
// Timings describe the attempt as a whole; per-step timings are not invented.
func (service *Service) runnerOutcome(ctx context.Context, attempt workexecution.Attempt, result *Stages) error {
	if attempt.FinishedAt == nil {
		return nil
	}
	view, err := service.queue.ReadWork(ctx, attempt.TaskID, attempt.WorkItemID, result.Revision)
	if err != nil {
		return err
	}
	if view.Item.Item.Operation != workexecution.Runner {
		return nil
	}
	stored, err := service.runners.LoadResult(ctx, attempt, result.Revision)
	if err != nil {
		return err
	}
	value := stored.Record
	stage := workexecution.Stage{Key: "runner_result", Name: "Runner execution outcome",
		Expected:  "Complete the captured Runner operation and account for its host resources",
		StartedAt: attempt.StartedAt, FinishedAt: attempt.FinishedAt, State: workexecution.StageFailed}
	if attempt.StartedAt == nil {
		stage.State = workexecution.StageSkipped
		stage.Observed = "The attempt ended before execution started. The existing Runner allocation was preserved."
	} else {
		switch value.Effects {
		case runnerwork.RuntimeReady:
			stage.State = workexecution.StageSucceeded
			stage.Observed = "Runner container and dedicated Docker daemon are ready; exact runtime ownership was recorded."
		case runnerwork.RuntimeRemoved:
			stage.State = workexecution.StageSucceeded
			stage.Name = "Runner cleanup outcome"
			stage.Observed = "Removal receipts confirm the Runner container, Docker daemon, network, egress rules and host identity were removed."
			if attempt.Direction == workexecution.Recovery {
				stage.Observed += " Cleanup does not change the original operation's failure."
			}
		case runnerwork.NoEffects:
			stage.FailureCode = value.Failure
			stage.Observed = runnerFailureObservation(value.Failure) + " No host effect was authorized; the allocation remains reserved."
		case runnerwork.EffectsUnknown:
			stage.FailureCode = value.Failure
			stage.Observed = runnerFailureObservation(value.Failure) + " Host effects remain unresolved. Resources stay protected until captured cleanup completes."
		}
	}
	return appendComponentOutcome(result, stage)
}

func runnerFailureObservation(code string) string {
	switch code {
	case "registration_token_required":
		return "The registration token is missing or expired. Retry Runner registration with a fresh token."
	case "deadline":
		return "The execution deadline expired."
	case "cancelled":
		return "Execution was cancelled."
	case "interrupted":
		return "The Controller was interrupted before the operation finished."
	case "ownership_changed":
		return "Runtime ownership changed; this attempt cannot continue."
	default:
		return "Runner runtime execution failed."
	}
}
