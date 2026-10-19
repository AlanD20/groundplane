package etcd

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareHierarchyGraphCollection removes the aggregate only after all work and
// action evidence is absent. Current resource state and coordination epochs are
// not Task history and are never removed by this operation.
func PrepareHierarchyGraphCollection(task workqueue.TaskSnapshot, guard HierarchyCollection) (workqueue.Change, error) {
	if !guard.Present || !guard.Eligible || guard.Operation.Tombstone.CurrentTaskID != task.Task.ID ||
		len(guard.Authority.Conditions) == 0 {
		return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	change, err := workqueue.PrepareTaskCollection(task)
	if err != nil {
		return workqueue.Change{}, err
	}
	if guard.PlanningRejection {
		return prepareRejectedHierarchyCollection(task, guard, change)
	}
	operation := guard.Operation
	id := operation.Tombstone.OperationID
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Prefix: true,
		Key: hierarchydeletionplanning.MembershipCapturePrefix(id)})
	change.Conditions = append(change.Conditions, guard.Authority.Conditions...)
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: hierarchydeletion.EnvironmentInputPrefix(id), Prefix: true},
	)
	for _, prefixFor := range []func(string) (string, error){hierarchydeletion.HierarchyDeletionActionPrefix, hierarchydeletion.HierarchyDeletionCompletionPrefix} {
		prefix, err := prefixFor(id)
		if err != nil {
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: prefix, Prefix: true})
	}
	for _, keyFor := range []func(string) (string, error){hierarchydeletion.HierarchyDeletionIntentKey, hierarchydeletion.HierarchyDeletionCleanupFenceKey, hierarchydeletion.HierarchyDeletionReplayTargetKey} {
		key, err := keyFor(id)
		if err != nil {
			return workqueue.Change{}, err
		}
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
	}
	for _, key := range []string{hierarchydeletion.TaskBindingKey(task.Task.ID), hierarchydeletion.WorkSummaryKey(task.Task.ID),
		hierarchydeletion.HierarchyDeletionTombstoneKey(string(operation.Tombstone.TargetKind), operation.Tombstone.TargetID)} {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
	}
	return change, nil
}

func prepareRejectedHierarchyCollection(
	task workqueue.TaskSnapshot,
	guard HierarchyCollection,
	change workqueue.Change,
) (workqueue.Change, error) {
	id := guard.Operation.Tombstone.OperationID
	prefixes, err := planningArtifactPrefixes(id)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guard.Authority.Conditions...)
	for _, prefix := range prefixes {
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: prefix, Prefix: true})
	}
	for _, keyFor := range []func(string) (string, error){hierarchydeletion.HierarchyDeletionIntentKey,
		hierarchydeletion.HierarchyDeletionCleanupFenceKey, hierarchydeletion.HierarchyDeletionReplayTargetKey} {
		key, err := keyFor(id)
		if err != nil {
			return workqueue.Change{}, err
		}
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
	}
	for _, key := range []string{hierarchydeletion.TaskBindingKey(task.Task.ID), hierarchydeletion.PlanningRejectionKey(task.Task.ID)} {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
	}
	return change, nil
}
