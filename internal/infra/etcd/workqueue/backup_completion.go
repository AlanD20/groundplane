package workqueue

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReadInterruptedBackupAt captures retained ownership, not permission to
// release it. Replacement of an Agent does not prove external effects stopped.
func (repository *Repository) ReadInterruptedBackupAt(ctx context.Context, view ResumeView) (Completion, error) {
	operation := view.Work.Item.Item.Operation
	if !view.ActiveAttempt || view.CurrentExecutor || view.ExecutorRevision <= 0 || view.Work.ReadRevision <= 0 ||
		view.Work.Task.Revision <= 0 || view.Work.Item.Revision <= 0 || view.Attempt.Revision <= 0 ||
		(operation != workexecution.BackupPrune && operation != workexecution.Backup && operation != workexecution.Restore) || view.Attempt.Attempt.FinishedAt != nil ||
		(view.Checkpoint.Record == nil) != (view.Checkpoint.Revision == 0) ||
		workexecution.ValidateDelivery(view.Work.Item.Item, view.Attempt.Attempt) != nil {
		return Completion{}, errs.New(errs.KindStateConflict, "Backup reconciliation requires its replaced attempt and retained claims")
	}
	return repository.readCompletionOwnership(ctx, view)
}
