package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (repository *HierarchyDeletionRepository) hierarchyPlanningCollectionGuard(ctx context.Context, task workqueue.TaskSnapshot,
	revision int64, at time.Time, bindingValue, summaryValue *keyvalue.KeyValue,
) (HierarchyCollection, error) {
	guard := HierarchyCollection{Present: true, PlanningRejection: true}
	if task.Task.RetainUntil == nil || at.Before(*task.Task.RetainUntil) {
		return guard, nil
	}
	if summaryValue != nil || bindingValue == nil {
		return guard, hierarchydeletion.CorruptHierarchyDeletion()
	}
	binding, err := hierarchydeletion.DecodeTaskBinding(bindingValue.Value)
	if err != nil {
		return guard, err
	}
	key := hierarchydeletion.PlanningRejectionKey(task.Task.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return guard, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return guard, hierarchydeletion.CorruptHierarchyDeletion()
	}
	failure, err := hierarchydeletion.DecodePlanningRejection(read.Values[0].Value, task.Task)
	if err != nil {
		return guard, err
	}
	if binding.TaskID != task.Task.ID || binding.OperationID != failure.Operation.Tombstone.OperationID ||
		binding.Marker != failure.Operation.MarkerLocator {
		return guard, hierarchydeletion.CorruptHierarchyDeletion()
	}
	guard.Eligible, guard.Operation = true, failure.Operation
	guard.Authority.Conditions = []keyvalue.Condition{
		{Key: workqueue.TaskKey(task.Task.ID), ModRevision: task.Revision},
		{Key: hierarchydeletion.TaskBindingKey(task.Task.ID), ModRevision: bindingValue.ModRevision},
		{Key: key, ModRevision: read.Values[0].ModRevision},
		{Key: hierarchydeletion.WorkSummaryKey(task.Task.ID)},
		{Key: workqueue.TaskItemsPrefix(task.Task.ID), Prefix: true},
	}
	return guard, nil
}

func planningArtifactPrefixes(operationID string) ([]string, error) {
	actions, err := hierarchydeletion.HierarchyDeletionActionPrefix(operationID)
	if err != nil {
		return nil, err
	}
	return []string{actions, attachremoval.FrozenPrefix(operationID), hierarchydeletion.EnvironmentInputPrefix(operationID),
		hierarchydeletionplanning.MembershipCapturePrefix(operationID)}, nil
}

// One bounded page is removed per sweep. Only the archived operation's private
// inputs/actions are eligible; target-scoped keys may belong to a newer removal.
func (repository *HierarchyDeletionRepository) PreparePlanningArtifactCollection(ctx context.Context, task workqueue.TaskSnapshot,
	guard HierarchyCollection, revision int64,
) (bool, workqueue.Change, error) {
	if !guard.Eligible || !guard.PlanningRejection || task.Task.CollectionStartedAt == nil || task.Task.ExpectedItems != 0 {
		return false, workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	id := guard.Operation.Tombstone.OperationID
	prefixes, err := planningArtifactPrefixes(id)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	change := workqueue.Change{Conditions: append([]keyvalue.Condition(nil), guard.Authority.Conditions...)}
	for index, prefix := range prefixes {
		page, err := repository.store.Range(ctx, keyvalue.RangeRequest{Prefix: prefix, Limit: 8, Revision: revision})
		if err != nil {
			return false, workqueue.Change{}, err
		}
		if page == nil || page.ReadRevision != revision || len(page.Values) > 8 {
			return false, workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		if len(page.Values) == 0 {
			if page.More {
				return false, workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
			}
			change.Conditions = append(change.Conditions, keyvalue.Condition{Key: prefix, Prefix: true})
			continue
		}
		for _, value := range page.Values {
			change.Conditions = append(change.Conditions, keyvalue.Condition{Key: value.Key, ModRevision: value.ModRevision})
			change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: value.Key})
			if index == 0 {
				action, err := hierarchydeletion.DecodeHierarchyDeletionAction(value.Value)
				if err != nil || action.ParentOperationID != id {
					keyvalue.ClearRangeValues(page.Values)
					return false, workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
				}
				workID := hierarchydeletion.ActionWorkID(task.Task.ID, task.Task.CreatedAt, id, action.Ordinal)
				change.Conditions = append(change.Conditions, keyvalue.Condition{Key: workqueue.ItemKey(workID)},
					keyvalue.Condition{Key: workqueue.AttemptIndexPrefix(workID), Prefix: true})
				change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: hierarchydeletion.WorkBindingKey(workID)})
			}
		}
		keyvalue.ClearRangeValues(page.Values)
		return false, change, nil
	}
	return true, change, nil
}
