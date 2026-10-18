package backupwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// LoadConfigTransfer joins the captured source, current attempt, executor and
// operation locks at one MVCC revision. Returned guards must accompany writes;
// callers must not turn the read result into an enduring execution grant.
func (repository *InputRepository) LoadConfigTransfer(ctx context.Context, queue *workqueue.Repository,
	binding backupconfigtransfer.Binding, revision int64,
) (*agentpb.BackupWorkAssignment, []keyvalue.Condition, error) {
	if queue == nil || binding.Validate() != nil || revision <= 0 {
		return nil, nil, conflict("Config transfer requires a native source binding and snapshot")
	}
	attempt := binding.Attempt
	view, err := queue.ReadResumeAt(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor, revision)
	if err != nil {
		return nil, nil, err
	}
	live, task, item := view.Attempt.Attempt, view.Work.Task.Task, view.Work.Item.Item
	now := time.Now().UTC()
	if !workexecution.SameAssignment(live, attempt) || !view.CurrentExecutor || !view.ActiveAttempt ||
		live.Direction != workexecution.Forward || live.StartedAt == nil || live.FinishedAt != nil ||
		task.Stop != nil || item.ForwardRevoked || !now.Before(live.Deadline) ||
		!now.Before(task.Deadline) || !now.Before(item.Deadline) {
		return nil, nil, conflict("Config transfer attempt is no longer running on this host")
	}
	assignment, change, err := repository.LoadAssignment(ctx, view)
	if err != nil {
		return nil, nil, err
	}
	expected, err := backupconfigtransfer.Bind(assignment, attempt.Executor, binding.SourceOrdinal, binding.Direction)
	if err != nil {
		return nil, nil, err
	}
	if expected != binding || assignment.Input.Steps[binding.SourceOrdinal].StepDeadlineUnixNano <= uint64(now.UnixNano()) {
		return nil, nil, conflict("Config transfer differs from its captured source or its deadline has elapsed")
	}
	conditions := append(change.Conditions,
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Work.Item.Revision},
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision})
	return assignment, conditions, nil
}
