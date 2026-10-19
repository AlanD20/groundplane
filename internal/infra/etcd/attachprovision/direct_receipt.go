package attachprovision

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareDirectCompletionReceipt retains the exact applied effect for future
// Blueprint reconciliation, without inventing an Apply graph for a direct action.
func (repository *InputRepository) PrepareDirectCompletionReceipt(ctx context.Context, view workqueue.ResumeView,
	result workexecution.Input,
) (workqueue.Change, error) {
	attempt, item := view.Attempt.Attempt, view.Work.Item.Item
	if !view.ActiveAttempt || attempt.Validate() != nil || attempt.Direction != workexecution.Forward ||
		attempt.FinishedAt != nil || attempt.StartedAt == nil || item.Input == nil ||
		attempt.TaskID != view.Work.Task.Task.ID || attempt.WorkItemID != item.ID || attempt.Input != *item.Input ||
		attempt.Executor.Executor != workexecution.Agent || attempt.Executor.HostID != item.Placement.HostID || result.Validate() != nil {
		return workqueue.Change{}, checkpointConflict(
			"Attach receipt requires its active completed provisioning attempt",
		)
	}
	source, err := repository.LoadCaptured(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer source.Clear()
	receipt := directReceipt(view.Work)
	receipt.Execution = &blueprintunits.AppliedExecution{TaskID: attempt.TaskID, WorkItemID: attempt.WorkItemID,
		AttemptID: attempt.ID, Executor: attempt.Executor, Input: attempt.Input, Result: result}
	change, err := repository.prepareDirectReceipt(ctx, view.Work, receipt)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, source.Conditions...)
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
	)
	return change, nil
}

func directReceipt(view workqueue.View) blueprintunits.AppliedRecord {
	task, item := view.Task.Task, view.Item.Item
	return blueprintunits.AppliedRecord{EnvironmentID: task.Scope.EnvironmentID,
		Target: blueprintunits.ResourceKey{Kind: ids.KindAttach, ID: item.TargetID}, State: blueprintunits.Applied,
		Fingerprint: item.Input.SHA256, ParentTaskID: task.ID}
}

func (repository *InputRepository) prepareDirectReceipt(ctx context.Context, view workqueue.View,
	receipt blueprintunits.AppliedRecord,
) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Action != workexecution.ActionAttach || task.TargetID != item.TargetID ||
		item.Operation != workexecution.Attach ||
		item.TaskID != task.ID ||
		item.Input == nil ||
		view.ReadRevision <= 0 ||
		view.Task.Revision <= 0 ||
		view.Item.Revision <= 0 ||
		receipt.ParentTaskID != task.ID ||
		receipt.EnvironmentID != task.Scope.EnvironmentID ||
		receipt.Target.ID != task.TargetID {
		return workqueue.Change{}, checkpointConflict("Attach applied receipt has no matching direct action")
	}
	keys := []string{
		blueprintunits.EpochKey(receipt.EnvironmentID),
		blueprintunits.AppliedKey(receipt.EnvironmentID, receipt.Target),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: view.ReadRevision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != len(keys) {
		return workqueue.Change{}, checkpointConflict("Attach applied receipt read is incomplete")
	}
	guards := []keyvalue.Condition{{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision}}
	for index, value := range read.Values {
		if value != nil &&
			(value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > view.ReadRevision) {
			return workqueue.Change{}, checkpointConflict("Attach applied receipt has an invalid revision")
		}
		guards = append(guards, keyvalue.Condition{Key: keys[index], ModRevision: keyvalue.RevisionOf(value)})
	}
	epoch := blueprintunits.EpochRecord{EnvironmentID: receipt.EnvironmentID}
	if read.Values[0] != nil {
		epoch, err = blueprintunits.DecodeEpoch(read.Values[0].Value)
		if err != nil || epoch.EnvironmentID != receipt.EnvironmentID {
			return workqueue.Change{}, checkpointConflict("Attach applied receipt epoch is invalid")
		}
	}
	if epoch.Sequence == ^uint64(0) {
		return workqueue.Change{}, checkpointConflict("Attach applied receipt epoch is exhausted")
	}
	if read.Values[1] != nil {
		prior, err := blueprintunits.DecodeApplied(read.Values[1].Value)
		if err != nil || prior.EnvironmentID != receipt.EnvironmentID || prior.Target != receipt.Target ||
			read.Values[0] == nil || read.Values[1].ModRevision > read.Values[0].ModRevision {
			return workqueue.Change{}, checkpointConflict("Attach prior applied receipt is invalid")
		}
		if prior.State == blueprintunits.Applied && prior.ParentTaskID != task.ID {
			return workqueue.Change{}, checkpointConflict("Attach cannot overwrite another action's applied binding")
		}
	}
	value, err := blueprintunits.EncodeApplied(receipt)
	if err != nil {
		return workqueue.Change{}, err
	}
	epoch.Sequence++
	epochValue, err := blueprintunits.EncodeEpoch(epoch)
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: guards, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: keys[0], Value: epochValue},
		{Type: keyvalue.MutationPut, Key: keys[1], Value: value},
	}}, nil
}
