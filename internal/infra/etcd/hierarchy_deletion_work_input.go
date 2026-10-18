package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type HierarchyWorkInput struct {
	Operation  hierarchydeletion.HierarchyDeletionOperation
	Action     hierarchydeletion.HierarchyDeletionAction
	Conditions []keyvalue.Condition
}

func (repository *HierarchyDeletionRepository) LoadHierarchyWork(ctx context.Context, view workqueue.View) (HierarchyWorkInput, error) {
	if view.ReadRevision <= 0 || view.Task.Revision <= 0 || view.Item.Revision <= 0 {
		return HierarchyWorkInput{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	work := view.Item.Item
	keys := []string{hierarchydeletion.TaskBindingKey(view.Task.Task.ID), hierarchydeletion.WorkBindingKey(work.ID)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: view.ReadRevision})
	if err != nil {
		return HierarchyWorkInput{}, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 2 || read.Values[0] == nil || read.Values[1] == nil {
		return HierarchyWorkInput{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer keyvalue.ClearValues(read.Values)
	taskBinding, err := hierarchydeletion.DecodeTaskBinding(read.Values[0].Value)
	if err != nil {
		return HierarchyWorkInput{}, err
	}
	binding, err := hierarchydeletion.DecodeWorkBinding(read.Values[1].Value)
	if err != nil {
		return HierarchyWorkInput{}, err
	}
	if binding.TaskID != view.Task.Task.ID || taskBinding.TaskID != binding.TaskID || taskBinding.OperationID != binding.OperationID {
		return HierarchyWorkInput{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	operation, err := repository.OperationByTaskAtRevision(ctx, binding.TaskID, view.ReadRevision)
	if err != nil {
		return HierarchyWorkInput{}, err
	}
	if operation.Tombstone.PlanCount == nil || binding.Ordinal >= *operation.Tombstone.PlanCount || operation.Tombstone.PlanDigest == nil {
		return HierarchyWorkInput{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	actionKey, err := hierarchydeletion.HierarchyDeletionActionKey(binding.OperationID, binding.Ordinal)
	if err != nil {
		return HierarchyWorkInput{}, err
	}
	captured, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{actionKey}, Revision: view.ReadRevision})
	if err != nil {
		return HierarchyWorkInput{}, err
	}
	if captured == nil || captured.ReadRevision != view.ReadRevision || len(captured.Values) != 1 || captured.Values[0] == nil {
		return HierarchyWorkInput{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer keyvalue.ClearValues(captured.Values)
	action, err := hierarchydeletion.DecodeHierarchyDeletionAction(captured.Values[0].Value)
	if err != nil {
		return HierarchyWorkInput{}, err
	}
	expected, raw, err := hierarchydeletion.ActionWork(view.Task.Task, action, taskBinding.HostID)
	clear(raw)
	if err != nil {
		return HierarchyWorkInput{}, err
	}
	if action.ActionKind == hierarchydeletion.HierarchyDeletionAttachDeprovision {
		source, err := repository.loadHierarchyAttachSnapshot(ctx, operation, action, view.ReadRevision)
		if err != nil {
			return HierarchyWorkInput{}, err
		}
		expected = hierarchyAttachClaims(expected, source.Input)
		attachremoval.Clear(&source.Input)
	}
	if action.Ordinal != binding.Ordinal || action.ParentOperationID != binding.OperationID || expected.ID != work.ID || expected.TaskID != work.TaskID ||
		expected.Operation != work.Operation || expected.TargetID != work.TargetID || expected.Placement != work.Placement || work.Input == nil || *expected.Input != *work.Input ||
		!slices.Equal(expected.Claims, work.Claims) || !slices.Equal(expected.Dependencies, work.Dependencies) {
		return HierarchyWorkInput{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	fenceKey, err := hierarchydeletion.HierarchyDeletionCleanupFenceKey(binding.OperationID)
	if err != nil {
		return HierarchyWorkInput{}, err
	}
	return HierarchyWorkInput{Operation: operation, Action: action, Conditions: []keyvalue.Condition{
		{Key: keys[0], ModRevision: read.Values[0].ModRevision}, {Key: keys[1], ModRevision: read.Values[1].ModRevision},
		{Key: actionKey, ModRevision: captured.Values[0].ModRevision}, {Key: fenceKey, ModRevision: operation.FenceRevision},
		{Key: hierarchydeletion.HierarchyDeletionTombstoneKey(string(operation.Tombstone.TargetKind), operation.Tombstone.TargetID), ModRevision: operation.TombstoneRevision},
	}}, nil
}
