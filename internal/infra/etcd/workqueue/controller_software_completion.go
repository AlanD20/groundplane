package workqueue

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Native qualification belongs to the replacement process. This captures the
// original slot/claims for accounting, never for another forward activation.
func (repository *Repository) ReadInterruptedControllerSoftwareAt(
	ctx context.Context,
	view ResumeView,
) (Completion, error) {
	if !view.ActiveAttempt || view.CurrentExecutor || view.ExecutorRevision <= 0 || view.Work.ReadRevision <= 0 ||
		view.Work.Task.Revision <= 0 || view.Work.Item.Revision <= 0 || view.Attempt.Revision <= 0 ||
		view.Work.Item.Item.Operation != workexecution.ControllerSoftware || view.Attempt.Attempt.FinishedAt != nil ||
		view.Checkpoint.Record != nil || view.Checkpoint.Revision != 0 || workexecution.ValidateDelivery(view.Work.Item.Item, view.Attempt.Attempt) != nil {
		return Completion{}, errs.New(
			errs.KindStateConflict,
			"Controller handoff reconciliation requires its retained native attempt",
		)
	}
	return repository.readCompletionOwnership(ctx, view)
}
