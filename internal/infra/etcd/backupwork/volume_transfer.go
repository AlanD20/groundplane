package backupwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupvolumemanifest"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// VolumeTransferGuard binds each durable credit to its live native execution.
// Stored bindings identify an owner; they never substitute for current start,
// deadline, revocation, Agent generation or Environment ownership checks.
func (repository *InputRepository) VolumeTransferGuard(queue *workqueue.Repository) backupvolumemanifest.Guard {
	return func(ctx context.Context, owner backupvolumemanifest.Owner, revision int64) ([]keyvalue.Condition, error) {
		if queue == nil || owner.Validate() != nil || revision <= 0 {
			return nil, conflict("Volume transfer requires native execution ownership")
		}
		binding := owner.Binding
		attempt := binding.Attempt
		view, err := queue.ReadResumeAt(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor, revision)
		if err != nil {
			return nil, err
		}
		live := view.Attempt.Attempt
		task, item := view.Work.Task.Task, view.Work.Item.Item
		at := time.Now().UTC()
		if !workexecution.SameAssignment(live, attempt) || live.StartedAt == nil || live.FinishedAt != nil ||
			!view.ActiveAttempt || !view.CurrentExecutor || task.Stop != nil || item.ForwardRevoked ||
			!at.Before(live.Deadline) || !at.Before(item.Deadline) || !at.Before(task.Deadline) {
			return nil, conflict("Volume transfer attempt is stopped, expired or replaced")
		}
		assignment, guarded, err := repository.LoadAssignment(ctx, view)
		if err != nil {
			return nil, err
		}
		expected, err := backupvolumetransfer.Bind(assignment, live.Executor, binding.SourceOrdinal, binding.Direction)
		if err != nil {
			return nil, err
		}
		if expected != binding {
			return nil, conflict("Volume transfer differs from captured source authority")
		}
		return append(guarded.Conditions,
			keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Work.Task.Revision},
			keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Work.Item.Revision},
			keyvalue.Condition{Key: workqueue.AttemptKey(live.ID), ModRevision: view.Attempt.Revision},
			keyvalue.Condition{Key: workqueue.ExecutorKey(live.Executor), ModRevision: view.ExecutorRevision}), nil
	}
}
