package workqueue

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A replacement Controller can account for the captured lifecycle's effects;
// this method grants no new enrollment, image replacement or deletion.
func (repository *Repository) ReadInterruptedAgentManagementAt(
	ctx context.Context,
	view ResumeView,
) (Completion, error) {
	if !view.ActiveAttempt || view.CurrentExecutor || view.ExecutorRevision <= 0 || view.Work.ReadRevision <= 0 ||
		view.Work.Task.Revision <= 0 || view.Work.Item.Revision <= 0 || view.Attempt.Revision <= 0 ||
		view.Work.Item.Item.Operation != workexecution.AgentLifecycle &&
			view.Work.Item.Item.Operation != workexecution.AgentSoftware ||
		view.Attempt.Attempt.Executor.Executor != workexecution.Controller || view.Attempt.Attempt.FinishedAt != nil ||
		view.Checkpoint.Record != nil || view.Checkpoint.Revision != 0 || workexecution.ValidateDelivery(view.Work.Item.Item, view.Attempt.Attempt) != nil {
		return Completion{}, errs.New(
			errs.KindStateConflict,
			"Agent lifecycle reconciliation requires its retained native attempt",
		)
	}
	return repository.readCompletionOwnership(ctx, view)
}
