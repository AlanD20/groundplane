package workqueue

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReadInterruptedAt captures replaced-executor ownership only. The operation
// owner supplies its own captured failure/cleanup proof and authenticates the
// replacement process. This read never declares external effects terminated.
func (repository *Repository) ReadInterruptedAt(ctx context.Context, view ResumeView, operation workexecution.Operation) (Completion, error) {
	if !workexecution.ValidOperation(operation) || !view.ActiveAttempt || view.CurrentExecutor || view.ExecutorRevision <= 0 ||
		view.Work.ReadRevision <= 0 || view.Work.Task.Revision <= 0 || view.Work.Item.Revision <= 0 || view.Attempt.Revision <= 0 ||
		view.Work.Item.Item.Operation != operation || view.Attempt.Attempt.FinishedAt != nil ||
		(view.Checkpoint.Record == nil) != (view.Checkpoint.Revision == 0) ||
		workexecution.ValidateDelivery(view.Work.Item.Item, view.Attempt.Attempt) != nil {
		return Completion{}, errs.New(errs.KindStateConflict, "interruption requires the replaced attempt and its retained resource claims")
	}
	return repository.readCompletionOwnership(ctx, view)
}
