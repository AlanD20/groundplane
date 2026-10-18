package workqueue

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Repository) ReadInterruptedSoftwareStagingAt(ctx context.Context, view ResumeView) (Completion, error) {
	if !view.ActiveAttempt || view.CurrentExecutor || view.ExecutorRevision <= 0 || view.Work.ReadRevision <= 0 ||
		view.Work.Task.Revision <= 0 || view.Work.Item.Revision <= 0 || view.Attempt.Revision <= 0 || view.Work.Item.Item.Operation != workexecution.SoftwareStaging ||
		view.Attempt.Attempt.FinishedAt != nil || view.Attempt.Attempt.Executor.Executor != workexecution.Controller || view.Checkpoint.Record != nil || view.Checkpoint.Revision != 0 ||
		workexecution.ValidateDelivery(view.Work.Item.Item, view.Attempt.Attempt) != nil {
		return Completion{}, errs.New(errs.KindStateConflict, "software staging reconciliation requires its retained attempt")
	}
	return repository.readCompletionOwnership(ctx, view)
}
