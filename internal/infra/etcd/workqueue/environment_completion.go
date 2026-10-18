package workqueue

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// The operation owner must retain unknown helper effects. Executor replacement
// accounts for ownership, not filesystem readiness or helper termination.
func (repository *Repository) ReadInterruptedEnvironmentAt(ctx context.Context, view ResumeView) (Completion, error) {
	if !view.ActiveAttempt || view.CurrentExecutor || view.ExecutorRevision <= 0 || view.Work.ReadRevision <= 0 ||
		view.Work.Task.Revision <= 0 || view.Work.Item.Revision <= 0 || view.Attempt.Revision <= 0 ||
		view.Work.Item.Item.Operation != workexecution.Environment || view.Attempt.Attempt.FinishedAt != nil ||
		view.Checkpoint.Record != nil || view.Checkpoint.Revision != 0 || workexecution.ValidateDelivery(view.Work.Item.Item, view.Attempt.Attempt) != nil {
		return Completion{}, errs.New(errs.KindStateConflict, "environment interruption requires its replaced execution and retained ownership")
	}
	return repository.readCompletionOwnership(ctx, view)
}
