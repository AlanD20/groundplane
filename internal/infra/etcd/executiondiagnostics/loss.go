package executiondiagnostics

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func LossKey(attemptID string) string { return "/v1/diagnostics/execution-loss/" + attemptID }

// MarkIncomplete records diagnostic loss without touching execution outcome.
// It remains best effort when storage itself is unavailable; absent diagnostics
// must never be interpreted as successful or untouched execution.
func (repository *Repository) MarkIncomplete(
	ctx context.Context,
	taskID, workID, attemptID string,
	identity workexecution.Identity,
) error {
	view, err := repository.queue.ReadResume(ctx, taskID, workID, attemptID, identity)
	if err != nil {
		return err
	}
	if !view.ActiveAttempt || !view.CurrentExecutor || view.Work.Task.Task.CollectionStartedAt != nil {
		return invalid()
	}
	result, err := repository.store.Transact(ctx, []keyvalue.Condition{
		{Key: workqueue.TaskKey(taskID), ModRevision: view.Work.Task.Revision},
		{Key: workqueue.AttemptKey(attemptID), ModRevision: view.Attempt.Revision},
		{Key: workqueue.ItemKey(workID), ModRevision: view.Work.Item.Revision},
		{Key: workqueue.ExecutorKey(identity), ModRevision: view.ExecutorRevision},
	}, []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: LossKey(attemptID), Value: []byte("incomplete")}})
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "execution changed before diagnostic loss could be recorded")
	}
	return nil
}
