package blueprintunits

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareVolumeCompletionReceipt records the effect of the exact native
// attempt. Its caller must join the Volume owner's final host proof, applied
// storage record and work completion in the same transaction. This method
// grants no host effects and creates no parallel execution record.
func (repository *Repository) PrepareVolumeCompletionReceipt(ctx context.Context, view workqueue.ResumeView) (workqueue.Change, error) {
	task, item, attempt := view.Work.Task.Task, view.Work.Item.Item, view.Attempt.Attempt
	checkpoint := view.Checkpoint.Record
	if task.Validate() != nil || item.Validate() != nil || attempt.Validate() != nil ||
		task.Action != workexecution.ActionApply || task.Scope.EnvironmentID == "" ||
		item.Operation != workexecution.Volume || item.TaskID != task.ID || item.Input == nil ||
		!view.ActiveAttempt || attempt.FinishedAt != nil || attempt.Direction != workexecution.Forward ||
		attempt.TaskID != task.ID || attempt.WorkItemID != item.ID || attempt.Input != *item.Input ||
		attempt.Executor.Executor != workexecution.Agent || attempt.Executor.HostID != item.Placement.HostID ||
		checkpoint == nil || checkpoint.AttemptID != attempt.ID || checkpoint.Evidence.Validate() != nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Volume receipt requires its active proven attempt")
	}
	graph, err := repository.LoadCapturedGraph(ctx, task, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	var unit *Unit
	for index, captured := range graph.Record.Items {
		if captured.ID != item.ID {
			continue
		}
		if captured.TaskID != item.TaskID || captured.Operation != item.Operation || captured.TargetID != item.TargetID ||
			captured.Placement != item.Placement || !slices.Equal(captured.Dependencies, item.Dependencies) ||
			!slices.Equal(captured.Claims, item.Claims) || !captured.CreatedAt.Equal(item.CreatedAt) ||
			!captured.Deadline.Equal(item.Deadline) {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Volume receipt differs from its captured work")
		}
		unit = &graph.Record.Units[index]
		break
	}
	if unit == nil || unit.Target.Kind != ids.KindVolume || unit.Removal ||
		len(unit.Writes) != 1 || unit.Writes[0] != unit.Target {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Volume receipt has no captured storage effect")
	}
	snapshot, err := repository.LoadAtRevision(ctx, task.Scope.EnvironmentID, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if snapshot.Epoch.Sequence == ^uint64(0) {
		return workqueue.Change{}, invalidRecord()
	}
	var appliedRevision int64
	found := false
	for _, applied := range snapshot.Applied {
		if applied.Record.Target == unit.Target {
			if found {
				return workqueue.Change{}, corruptRecord()
			}
			found, appliedRevision = true, applied.Revision
		}
	}
	if !found {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Volume receipt has no prior resource state")
	}
	receipt := AppliedRecord{EnvironmentID: task.Scope.EnvironmentID, Target: unit.Target,
		State: Applied, Fingerprint: unit.Fingerprint, ParentTaskID: task.ID,
		Execution: &AppliedExecution{TaskID: task.ID, WorkItemID: item.ID, AttemptID: attempt.ID,
			Executor: attempt.Executor, Input: attempt.Input, Result: checkpoint.Evidence}}
	value, err := EncodeApplied(receipt)
	if err != nil {
		return workqueue.Change{}, err
	}
	epoch, err := EncodeEpoch(EpochRecord{EnvironmentID: snapshot.EnvironmentID, Sequence: snapshot.Epoch.Sequence + 1})
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	// A newer desired head does not erase an already authorized host effect.
	// Its fingerprint remains the original one; newer reconciliation must
	// compare it, not silently adopt it as the newer intent's result.
	return workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: CapturedGraphKey(snapshot.EnvironmentID, task.ID), ModRevision: graph.Revision},
		{Key: EpochKey(snapshot.EnvironmentID), ModRevision: snapshot.EpochRevision},
		{Key: AppliedKey(snapshot.EnvironmentID, unit.Target), ModRevision: appliedRevision},
		{Key: workqueue.TaskKey(task.ID), ModRevision: view.Work.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Work.Item.Revision},
		{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
	}, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: AppliedKey(snapshot.EnvironmentID, unit.Target), Value: value},
		{Type: keyvalue.MutationPut, Key: EpochKey(snapshot.EnvironmentID), Value: epoch},
	}}, nil
}
