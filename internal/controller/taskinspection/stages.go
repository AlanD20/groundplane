package taskinspection

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

type Stages struct {
	Incomplete bool
	Items      []workexecution.Stage
	Available  bool
	Sequence   uint64
	UpdatedAt  *time.Time
	Revision   int64
}

func (service *Service) Stages(ctx context.Context, taskID, workID, attemptID string, revision int64) (Stages, error) {
	snapshot, err := service.diagnostics.Read(ctx, taskID, workID, attemptID, revision)
	if err != nil {
		return Stages{}, err
	}
	result := Stages{Items: []workexecution.Stage{}, Revision: snapshot.ReadRevision, Incomplete: snapshot.Incomplete}
	if record := snapshot.Record; record != nil {
		result.Items, result.Available, result.Sequence = append([]workexecution.Stage(nil), record.Stages...), true, record.Sequence
		result.UpdatedAt = &record.UpdatedAt
		if snapshot.FinishedAt != nil {
			for index, stage := range result.Items {
				if stage.State != workexecution.StageRunning {
					continue
				}
				// An attempt outcome does not prove the last action's outcome.
				// Keep its real start time, but never leave it looking active or
				// invent a successful/failed stage or completion timestamp.
				stage.State = workexecution.StageUnknown
				stage.Observed = unfinishedStageObservation(snapshot.Outcome)
				result.Items[index], result.Incomplete = stage, true
			}
		}
	}
	if err := service.componentOutcome(ctx, snapshot.Attempt, &result); err != nil {
		return Stages{}, err
	}
	if err := service.runnerOutcome(ctx, snapshot.Attempt, &result); err != nil {
		return Stages{}, err
	}
	return result, nil
}

func unfinishedStageObservation(outcome workexecution.Outcome) string {
	switch outcome {
	case workexecution.Failed:
		return "The attempt failed before this action's result was recorded. Its effects may require cleanup; see the work item's cleanup status."
	case workexecution.Cancelled:
		return "The attempt was cancelled without a recorded result for this action. Cancellation alone does not prove its effects were undone."
	case workexecution.Superseded:
		return "A newer action superseded this attempt. This action's final observation is missing; see the work item's cleanup status."
	case workexecution.Succeeded:
		return "The attempt completed successfully, but this action's final diagnostic observation is missing."
	default:
		return "The attempt ended without a recorded result for this action."
	}
}
