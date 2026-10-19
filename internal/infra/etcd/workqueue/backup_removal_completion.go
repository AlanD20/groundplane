package workqueue

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReadInterruptedBackupRemovalAt captures the replaced Controller's slot and
// claims. Its operation owner supplies exact absence or retains uncertain effects;
// replacing a process is never proof that a remote request was cancelled.
func (repository *Repository) ReadInterruptedBackupRemovalAt(ctx context.Context, view ResumeView) (Completion, error) {
	if !view.ActiveAttempt || view.CurrentExecutor || view.ExecutorRevision <= 0 || view.Work.ReadRevision <= 0 ||
		view.Work.Task.Revision <= 0 || view.Work.Item.Revision <= 0 || view.Attempt.Revision <= 0 ||
		view.Work.Item.Item.Operation != workexecution.RemovalBackupObject ||
		view.Attempt.Attempt.Executor.Executor != workexecution.Controller ||
		view.Attempt.Attempt.FinishedAt != nil ||
		view.Checkpoint.Record != nil || view.Checkpoint.Revision != 0 ||
		workexecution.ValidateDelivery(view.Work.Item.Item, view.Attempt.Attempt) != nil {
		return Completion{}, errs.New(
			errs.KindStateConflict,
			"backup removal reconciliation requires its replaced Controller attempt",
		)
	}
	return repository.readCompletionOwnership(ctx, view)
}
