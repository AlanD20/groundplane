package taskcheckpoint

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// Embedded Scripts append to this same observation journal. Its next sequence
// comes from the journal, never from the independent Release progress cursor.
func (service *ReleaseCheckpointService) reportDiagnosticStage(ctx context.Context,
	attempt workexecution.Attempt, stage workexecution.Stage, started *workexecution.Stage, at time.Time,
) error {
	snapshot, err := service.diagnostics.Read(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, 0)
	if err != nil || snapshot.Expired || snapshot.FinishedAt != nil {
		return err
	}
	sequence := uint64(0)
	var saved *workexecution.Stage
	if snapshot.Record != nil {
		sequence = snapshot.Record.Sequence
		for index := range snapshot.Record.Stages {
			if snapshot.Record.Stages[index].Key == stage.Key {
				saved = &snapshot.Record.Stages[index]
				break
			}
		}
	}
	if saved != nil && (sameDiagnosticStage(*saved, stage) ||
		stage.State == workexecution.StageRunning && saved.FinishedAt != nil &&
			saved.Name == stage.Name && saved.Expected == stage.Expected &&
			sameDiagnosticTime(saved.StartedAt, stage.StartedAt)) {
		return nil // Exact replay must not append duplicates or reopen a completed stage.
	}
	if saved == nil && started != nil {
		sequence++
		// The retained boundary supplies the true start time. Record its
		// repaired observation now, after any intervening Script observations.
		if err := service.diagnostics.Report(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID,
			attempt.Executor, sequence, *started, at); err != nil {
			return err
		}
	}
	return service.diagnostics.Report(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID,
		attempt.Executor, sequence+1, stage, at)
}

func sameDiagnosticStage(left, right workexecution.Stage) bool {
	return left.Key == right.Key && left.Name == right.Name && left.State == right.State &&
		left.Expected == right.Expected && left.Observed == right.Observed && left.FailureCode == right.FailureCode &&
		sameDiagnosticTime(left.StartedAt, right.StartedAt) && sameDiagnosticTime(left.FinishedAt, right.FinishedAt)
}

func sameDiagnosticTime(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}
