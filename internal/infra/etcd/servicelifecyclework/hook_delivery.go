package servicelifecyclework

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareHookDelivery binds private input delivery to the acknowledged hook
// boundary. Re-delivery grants no replay: the Agent's recorded exec owns that.
func (repository *Repository) PrepareHookDelivery(ctx context.Context, view workqueue.ResumeView,
	input *agentpb.ServiceLifecycleWorkInput, stageID string, at time.Time,
) (workqueue.Change, error) {
	attempt, checkpoint := view.Attempt.Attempt, view.Checkpoint.Record
	if input == nil || input.Hook == nil || input.Hook.StageId != stageID || !view.ActiveAttempt || !view.CurrentExecutor ||
		attempt.Direction != workexecution.Forward || attempt.StartedAt == nil || attempt.FinishedAt != nil ||
		!at.Before(attempt.Deadline) || view.Work.Task.Task.Stop != nil || view.Work.Task.Task.FinishedAt != nil ||
		view.Work.Item.Item.ForwardRevoked || checkpoint == nil || checkpoint.StageID != stageID {
		return workqueue.Change{}, conflict("Service lifecycle hook no longer owns an active execution stage")
	}
	key := ReportKey(attempt.ID, checkpoint.Sequence)
	read, err := repository.read(ctx, []string{key}, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return workqueue.Change{}, conflict("Service lifecycle hook has no acknowledged start boundary")
	}
	progress, err := lifecycleexecution.DecodeProgress(attempt, input, read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	stages, err := lifecycleexecution.Stages(input)
	if err != nil {
		return workqueue.Change{}, err
	}
	if progress.BeganAt == nil || len(progress.Completed) >= len(stages) || stages[len(progress.Completed)].ID != stageID ||
		stages[len(progress.Completed)].Expected != lifecycleexecution.HookSucceeded {
		return workqueue.Change{}, conflict("Service lifecycle hook is not the current in-flight stage")
	}
	change, err := PrepareCheckpoint(ctx, repository.store, view, input, progress, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions,
		keyvalue.Condition{Key: workqueue.TaskKey(attempt.TaskID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(attempt.WorkItemID), ModRevision: view.Work.Item.Revision},
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision})
	return change, nil
}
