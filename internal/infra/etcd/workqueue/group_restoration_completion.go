package workqueue

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReadInterruptedGroupRestorationAt reads retained slot/claim ownership only.
// Its operation owner must prove all restoration effects, or preserve the claims.
func (repository *Repository) ReadInterruptedGroupRestorationAt(ctx context.Context, view ResumeView) (Completion, error) {
	if !view.ActiveAttempt || view.CurrentExecutor || view.ExecutorRevision <= 0 || view.Work.ReadRevision <= 0 ||
		view.Work.Task.Revision <= 0 || view.Work.Item.Revision <= 0 || view.Attempt.Revision <= 0 ||
		view.Work.Item.Item.Operation != workexecution.ReleaseRestoration || view.Attempt.Attempt.FinishedAt != nil ||
		(view.Checkpoint.Record == nil) != (view.Checkpoint.Revision == 0) ||
		workexecution.ValidateDelivery(view.Work.Item.Item, view.Attempt.Attempt) != nil {
		return Completion{}, errs.New(errs.KindStateConflict, "Group restoration reconciliation requires its replaced attempt and retained claims")
	}
	return repository.readCompletionOwnership(ctx, view)
}
