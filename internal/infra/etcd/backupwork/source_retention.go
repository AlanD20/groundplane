package backupwork

import (
	"bytes"
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Hierarchy removal may still consume captured authority after execution ends.
func (repository *InputRepository) retentionHierarchyGuards(
	ctx context.Context,
	view workqueue.View,
) (bool, []keyvalue.Condition, error) {
	var conditions []keyvalue.Condition
	keys := []string{hierarchydeletion.HierarchyDeletionTombstoneKey("environment", view.Task.Task.Scope.EnvironmentID)}
	if view.Task.Task.Scope.ProjectID != "" {
		keys = append(keys, hierarchydeletion.HierarchyDeletionTombstoneKey("project", view.Task.Task.Scope.ProjectID),
			hierarchydeletion.HierarchyDeletionTombstoneKey("backing-service", view.Task.Task.Scope.ProjectID))
	}
	if view.Task.Task.Scope.TenantID != "" {
		keys = append(keys, hierarchydeletion.HierarchyDeletionTombstoneKey("tenant", view.Task.Task.Scope.TenantID))
	}
	read, err := repository.readSettlementKeys(ctx, keys, view.ReadRevision)
	if err != nil {
		return false, nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	for index, value := range read.Values {
		if value != nil {
			return false, nil, nil
		}
		conditions = append(conditions, keyvalue.Condition{Key: keys[index]})
	}

	return true, conditions, nil
}

// Delete only immutable indexes that still identify this exact captured input.
func (repository *InputRepository) prepareStagingIndexRetirement(
	ctx context.Context,
	view workqueue.View,
	input *agentpb.BackupWorkInput,
) (workqueue.Change, error) {
	var change workqueue.Change
	indexes, err := prepareSourceStagingIndexes(view.Item.Item, input)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearMutationValues(indexes.Mutations)
	for _, expected := range indexes.Mutations {
		stored, err := repository.readSettlementKeys(ctx, []string{expected.Key}, view.ReadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
		value := stored.Values[0]
		valid := value != nil && value.Version == 1 && bytes.Equal(value.Value, expected.Value)
		if valid {
			change.Conditions = append(
				change.Conditions,
				keyvalue.Condition{Key: expected.Key, ModRevision: value.ModRevision},
			)
		}
		keyvalue.ClearValues(stored.Values)
		if !valid {
			return workqueue.Change{}, conflict("Backup staging retention index changed")
		}
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: expected.Key})
	}

	return change, nil
}
