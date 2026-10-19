package workqueue

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReadInterruptedAttachAt captures retained ownership, not proof that any
// provisioning command has stopped or that its database effects were undone.
func (repository *Repository) ReadInterruptedAttachAt(ctx context.Context, view ResumeView) (Completion, error) {
	if !view.ActiveAttempt || view.CurrentExecutor || view.ExecutorRevision <= 0 || view.Work.ReadRevision <= 0 ||
		view.Work.Task.Revision <= 0 || view.Work.Item.Revision <= 0 || view.Attempt.Revision <= 0 ||
		view.Work.Item.Item.Operation != workexecution.Attach || view.Attempt.Attempt.FinishedAt != nil ||
		(view.Checkpoint.Record == nil) != (view.Checkpoint.Revision == 0) ||
		workexecution.ValidateDelivery(view.Work.Item.Item, view.Attempt.Attempt) != nil {
		return Completion{}, errs.New(
			errs.KindStateConflict,
			"Attach reconciliation requires its replaced attempt and retained claims",
		)
	}
	return repository.readCompletionOwnership(ctx, view)
}
