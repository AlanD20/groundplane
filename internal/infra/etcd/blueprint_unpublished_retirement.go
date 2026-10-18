package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareUnpublishedReleaseRetirement handles work stopped before it prepared
// any runtime. Staged captures need their own abandonment proof; do not drop
// their owner merely because the executable input was never published.
func (ledger *ReleaseLedger) PrepareUnpublishedReleaseRetirement(ctx context.Context, view workqueue.View, at time.Time) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if ctx == nil || ledger == nil || ledger.store == nil || task.Validate() != nil || item.Validate() != nil ||
		task.CollectionStartedAt == nil || task.FinishedAt == nil || task.Progress.Unsettled() != 0 ||
		at.Before(task.UpdatedAt) || item.Operation != workexecution.Release || item.TaskID != task.ID ||
		item.Input != nil || item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.Phase != workexecution.Settled || item.Result == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Unpublished Release retirement requires settled unstarted work")
	}
	keys := []string{ReleaseWorkInputKey(item.ID), blueprintReleaseHookStageKey(item.ID),
		releaserender.ServiceRolloutKey(task.Scope.EnvironmentID, task.ID, item.ID)}
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: view.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != len(keys) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Unpublished Release retirement read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	guards := []keyvalue.Condition{{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision}}
	for index, key := range keys {
		if read.Values[index] != nil {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Release preparation must be retired before its work history")
		}
		guards = append(guards, keyvalue.Condition{Key: key})
	}
	return workqueue.Change{Conditions: guards}, nil
}
