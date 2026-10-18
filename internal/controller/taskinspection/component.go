package taskinspection

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// Terminal observations come from the accepted operation result, not log text
// or a best-effort diagnostic write racing settlement. Recovery is a separate
// attempt and never replaces the original forward failure.
func (service *Service) componentOutcome(ctx context.Context, attempt workexecution.Attempt, result *Stages) error {
	if attempt.FinishedAt == nil || attempt.StartedAt == nil {
		return nil
	}
	work, err := service.queue.ReadWork(ctx, attempt.TaskID, attempt.WorkItemID, result.Revision)
	if err != nil {
		return err
	}
	if work.Item.Item.Operation != workexecution.Component || work.Item.Item.Placement.Executor != workexecution.Agent {
		return nil
	}
	view := workqueue.ResumeView{Work: work, Attempt: workqueue.AttemptSnapshot{Attempt: attempt}}
	stage := workexecution.Stage{Key: "execution_result", Name: "Component execution result",
		Expected: "Complete the captured Component action", StartedAt: attempt.StartedAt, FinishedAt: attempt.FinishedAt,
		State: workexecution.StageFailed}
	if attempt.Direction == workexecution.Recovery {
		stage.Key, stage.Name = "restoration_result", "Restore previous Component state"
		stage.Expected = "Stop the interrupted helper and restore only the captured runtime and configuration files"
		if attempt.Outcome == workexecution.Succeeded {
			raw, err := service.components.LoadRecoveryResult(ctx, view)
			if err != nil {
				return err
			}
			clear(raw)
			stage.State = workexecution.StageSucceeded
			stage.Observed = "Previous runtime and configuration were restored and verified. The original action remains failed."
			return appendComponentOutcome(result, stage)
		}
	} else if attempt.Outcome == workexecution.Succeeded {
		return nil // Forward named stages already carry their verified observations.
	}
	raw, err := service.components.LoadFailure(ctx, view)
	if err != nil {
		return err
	}
	defer clear(raw)
	input, _, _, err := service.components.Load(ctx, work)
	if err != nil {
		return err
	}
	failure, err := componentexecution.DecodeFailure(attempt, input, raw, *attempt.Result)
	if err != nil {
		return err
	}
	stage.FailureCode = string(failure.Reason)
	switch failure.Reason {
	case componentexecution.DeadlineExpired:
		stage.Observed = "Execution exceeded its captured deadline."
	case componentexecution.ExecutionInterrupted:
		stage.Observed = "The executing Agent was interrupted before a successful result was recorded."
	default:
		stage.Observed = "The Agent reported an execution failure. No provider error detail was captured."
	}
	if attempt.Direction == workexecution.Recovery || failure.Checkpoint != nil {
		stage.Observed += " This result does not prove that effects were undone; inspect the work item's cleanup status."
	} else {
		stage.Observed += " No host-effect stage was authorized."
	}
	return appendComponentOutcome(result, stage)
}

func appendComponentOutcome(result *Stages, stage workexecution.Stage) error {
	if err := stage.Validate(); err != nil {
		return err
	}
	result.Items = append(result.Items, stage)
	result.Available = true
	if result.UpdatedAt == nil || result.UpdatedAt.Before(*stage.FinishedAt) {
		result.UpdatedAt = stage.FinishedAt
	}
	return nil
}
