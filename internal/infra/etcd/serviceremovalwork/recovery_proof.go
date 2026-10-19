package serviceremovalwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareRecoveryProof supplies the final acknowledged restoration receipt, not
// a terminal state mutation. The owner must atomically preserve the Service,
// settle its restored runtime and release its deletion fence alongside cleanup
// completion. The original forward result is never replaced by this receipt.
func (repository *Repository) PrepareRecoveryProof(ctx context.Context, view workqueue.ResumeView,
	delivery *agentpb.ServiceLifecycleWorkInput, at time.Time,
) (Input, workexecution.Checkpoint, workqueue.Change, error) {
	task, item, attempt := view.Work.Task.Task, view.Work.Item.Item, view.Attempt.Attempt
	if !view.ActiveAttempt || view.ExecutorRevision <= 0 || item.Operation != workexecution.ServiceRemoval ||
		item.Cleanup != workexecution.CleanupRunning || attempt.Direction != workexecution.Recovery ||
		attempt.StartedAt == nil || attempt.FinishedAt != nil || workexecution.ValidateDelivery(item, attempt) != nil ||
		!task.PublicationComplete || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		view.Checkpoint.Record == nil || view.Checkpoint.Revision <= 0 || at.IsZero() || at.Location() != time.UTC {
		return Input{}, workexecution.Checkpoint{}, workqueue.Change{}, conflict(
			"Service removal recovery has no active acknowledged restoration",
		)
	}
	input, captureGuard, err := repository.Load(ctx, view.Work)
	if err != nil {
		return Input{}, workexecution.Checkpoint{}, workqueue.Change{}, err
	}
	saved, err := repository.LoadRecovery(ctx, view.Work, delivery)
	if err != nil {
		return Input{}, workexecution.Checkpoint{}, workqueue.Change{}, err
	}
	progress, progressGuard, err := repository.LoadRecoveryProgress(ctx, view, delivery, saved.Capture)
	if err != nil {
		return Input{}, workexecution.Checkpoint{}, workqueue.Change{}, err
	}
	checkpoint := *view.Checkpoint.Record
	if progress.Phase != removalexecution.Complete ||
		progress.InFlight || progress.UpdatedAt.After(at) {
		return Input{}, workexecution.Checkpoint{}, workqueue.Change{}, conflict(
			"Service removal restoration has not reached its final verification step",
		)
	}
	conditions, err := repository.RuntimeGuards(ctx, input, view.Work.ReadRevision)
	if err != nil {
		return Input{}, workexecution.Checkpoint{}, workqueue.Change{}, err
	}
	conditions = append(conditions, captureGuard, progressGuard,
		keyvalue.Condition{Key: RecoveryKey(item.ID), ModRevision: saved.Revision},
		keyvalue.Condition{Key: workqueue.CheckpointKey(attempt.ID), ModRevision: view.Checkpoint.Revision},
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Work.Item.Revision},
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision})
	return input, checkpoint, workqueue.Change{Conditions: conditions}, nil
}
