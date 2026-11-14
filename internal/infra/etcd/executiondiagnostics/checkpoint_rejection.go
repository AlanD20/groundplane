package executiondiagnostics

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// CheckpointRejected records a Controller refusal, not an effect failure. It
// changes no execution ownership and never implies an unacknowledged action ran.
func (repository *Repository) CheckpointRejected(ctx context.Context, attempt workexecution.Attempt,
	detail string,
) error {
	snapshot, err := repository.Read(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, 0)
	if err != nil || snapshot.Expired {
		return err
	}
	sequence := uint64(1)
	if snapshot.Record != nil {
		sequence = snapshot.Record.Sequence + 1
	}
	at := time.Now().UTC()
	stage := workexecution.Stage{Key: "checkpoint_acknowledgement", Name: "Save execution checkpoint",
		State: workexecution.StageFailed, FailureCode: "state.conflict", StartedAt: &at, FinishedAt: &at,
		Expected: "The Controller saves this report before authorizing the next host action",
		Observed: detail + " No acknowledgement was issued. Inspect the work item's recovery status before Retry."}
	return repository.Report(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor,
		sequence, stage, at)
}
