package volumework

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareRetention preserves captured inputs until every attempt is retired.
// AppliedVolume is resource state, not execution history, and is never deleted
// here. The collecting Task fence prevents Retry from acquiring these inputs.
func (repository *InputRepository) PrepareRetention(ctx context.Context, view workqueue.View, at time.Time, final bool) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Validate() != nil || item.Validate() != nil || view.Task.Revision <= 0 || view.Item.Revision <= 0 ||
		task.CollectionStartedAt == nil || task.FinishedAt == nil || task.RetainUntil == nil ||
		at.Before(*task.CollectionStartedAt) || at.Before(*task.RetainUntil) ||
		item.Operation != workexecution.Volume || item.TaskID != task.ID ||
		item.Phase != workexecution.Settled || item.ActiveAttemptID != "" ||
		item.Cleanup != workexecution.CleanupNone && item.Cleanup != workexecution.CleanupComplete {
		return false, workqueue.Change{}, conflict("Volume history requires expired, fully settled execution")
	}
	var guards []keyvalue.Condition
	if item.Input == nil {
		// A stopped Apply may have staged chunks without ever making this
		// item executable. Task/item CAS fences the staging writer; there can
		// be no published root or attempt to preserve in this case.
		if task.Action != workexecution.ActionApply || item.AttemptNumber != 0 || !final {
			return false, workqueue.Change{}, conflict("Volume without input cannot have execution history")
		}
		guards = append(guards, keyvalue.Condition{Key: InputKey(item.ID)})
	} else {
		input, captured, err := repository.Load(ctx, view)
		if err != nil {
			return false, workqueue.Change{}, err
		}
		if input.VolumeId != item.TargetID || input.EnvironmentId != task.Scope.EnvironmentID {
			return false, workqueue.Change{}, conflict("Volume history input belongs to different resources")
		}
		guards = captured
	}
	guards = append(guards,
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision})
	change := workqueue.Change{Conditions: guards}
	if final {
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: workqueue.AttemptIndexPrefix(item.ID), Prefix: true})
		change.Mutations = []keyvalue.Mutation{
			{Type: keyvalue.MutationDelete, Key: InputPrefix(item.ID), Prefix: true},
			{Type: keyvalue.MutationDelete, Key: UnstartedKey(item.ID)},
			{Type: keyvalue.MutationDelete, Key: RecoveryKey(item.ID)},
			{Type: keyvalue.MutationDelete, Key: unstartedEvidencePrefix(item.ID), Prefix: true},
		}
	}
	return true, change, nil
}

// PrepareAttemptRetirement joins the shared attempt deletion transaction. No
// report can be appended after collection has fenced the Task.
func PrepareAttemptRetirement(view workqueue.View, snapshot workqueue.AttemptSnapshot) (workqueue.Change, error) {
	item, attempt := view.Item.Item, snapshot.Attempt
	if item.Operation != workexecution.Volume || item.Input == nil ||
		attempt.Input != *item.Input || attempt.Executor.Executor != workexecution.Agent ||
		attempt.Executor.HostID != item.Placement.HostID {
		return workqueue.Change{}, conflict("Volume attempt history differs from its captured host or input")
	}
	prepared, err := workqueue.PrepareAttemptCollection(view.Task, view.Item, snapshot)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: prepared.Conditions, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationDelete, Key: ReportPrefix(attempt.ID), Prefix: true},
		{Type: keyvalue.MutationDelete, Key: FailureKey(attempt.ID)},
		{Type: keyvalue.MutationDelete, Key: RecoveryResultKey(attempt.ID)},
	}}, nil
}
