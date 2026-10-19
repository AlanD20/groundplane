package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) LoadWorkGraph(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	revision int64,
) ([]workexecution.Item, workqueue.Change, error) {
	if task.Revision <= 0 || revision < task.Revision {
		return nil, workqueue.Change{}, errs.New(errs.KindStateConflict, "hierarchy publication revision is invalid")
	}
	operation, err := repository.OperationByTaskAtRevision(ctx, task.Task.ID, revision)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	count := operation.Tombstone.PlanCount
	if count == nil || *count <= 0 || *count > int64(workexecution.MaximumTaskItems) {
		return nil, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"hierarchy plan is not sealed or exceeds the work limit",
		)
	}
	bindingRead, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{hierarchydeletion.TaskBindingKey(task.Task.ID)}, Revision: revision},
	)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	if bindingRead == nil || bindingRead.ReadRevision != revision || len(bindingRead.Values) != 1 ||
		bindingRead.Values[0] == nil {
		return nil, workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer keyvalue.ClearValues(bindingRead.Values)
	binding, err := hierarchydeletion.DecodeTaskBinding(bindingRead.Values[0].Value)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	if binding.TaskID != task.Task.ID || binding.OperationID != operation.Tombstone.OperationID {
		return nil, workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	actions := make([]hierarchydeletion.HierarchyDeletionAction, 0, int(*count))
	for offset := int64(0); offset < *count; {
		end := min(offset+32, *count)
		keys := make([]string, 0, int(end-offset))
		for ordinal := offset; ordinal < end; ordinal++ {
			key, err := hierarchydeletion.HierarchyDeletionActionKey(binding.OperationID, ordinal)
			if err != nil {
				return nil, workqueue.Change{}, err
			}
			keys = append(keys, key)
		}
		read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
		if err != nil {
			return nil, workqueue.Change{}, err
		}
		if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
			return nil, workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		for index, value := range read.Values {
			if value == nil || value.Key != keys[index] {
				keyvalue.ClearValues(read.Values)
				return nil, workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
			}
			action, err := hierarchydeletion.DecodeHierarchyDeletionAction(value.Value)
			if err != nil {
				keyvalue.ClearValues(read.Values)
				return nil, workqueue.Change{}, err
			}
			actions = append(actions, action)
		}
		keyvalue.ClearValues(read.Values)
		offset = end
	}
	captured := task.Task
	if captured.ExpectedItems == 0 {
		captured.ExpectedItems = uint32(*count)
	}
	graph, conditions, err := hierarchydeletion.WorkGraph(captured, operation, actions, binding.HostID)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	for index, action := range actions {
		if action.ActionKind != hierarchydeletion.HierarchyDeletionAttachDeprovision {
			continue
		}
		source, err := repository.loadHierarchyAttachSnapshot(ctx, operation, action, revision)
		if err != nil {
			return nil, workqueue.Change{}, err
		}
		graph[index] = hierarchyAttachClaims(graph[index], source.Input)
		attachremoval.Clear(&source.Input)
	}
	if err := workexecution.ValidateGraph(captured, graph); err != nil {
		return nil, workqueue.Change{}, err
	}
	conditions = append(
		conditions,
		keyvalue.Condition{
			Key:         hierarchydeletion.TaskBindingKey(task.Task.ID),
			ModRevision: bindingRead.Values[0].ModRevision,
		},
	)
	return graph, workqueue.Change{Conditions: conditions}, nil
}
