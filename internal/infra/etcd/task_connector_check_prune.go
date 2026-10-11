package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) pruneConnectorCheckInput(ctx context.Context, task TaskRecord,
	taskRevision int64, retention keyvalue.KeyValue, revision int64,
) (bool, error) {
	if !connectorCheckTask(task) {
		return false, nil
	}
	keys := []string{connectorchecks.Key(task.ID), connectorchecks.ReferenceKey(task.Target, task.ID),
		connectorchecks.OperationKey(task.OperationID), taskjournal.TaskActiveOperationKey(task.OperationID),
		taskjournal.TaskAssignmentIndexKey(task.ID), connectorchecks.LatestKey(task.Target),
		connectorchecks.EnvironmentReferenceKey(task.Owner.EnvironmentID, task.ID)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return true, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return true, errs.New(errs.KindInternal, "connector check retention read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[1] != nil || read.Values[2] != nil || read.Values[3] != nil || read.Values[4] != nil ||
		read.Values[6] != nil {
		return true, nil
	}
	if read.Values[0] == nil {
		return false, nil
	}
	record, err := connectorchecks.Decode(read.Values[0].Value)
	if err != nil {
		return true, err
	}
	defer record.Clear()
	if record.Owner.TaskID != task.ID || record.Owner.PlanID != task.PlanID ||
		record.Owner.OperationID != task.OperationID ||
		record.Owner.EnvironmentID != task.Owner.EnvironmentID ||
		record.Connector.Connector.ID != task.Target ||
		(record.Phase != connectorchecks.Finished && record.Phase != connectorchecks.Transferred) {
		return true, errs.New(errs.KindStateConflict, "connector check still owns unresolved execution or cleanup")
	}
	conditions := []keyvalue.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskRevision},
		{Key: retention.Key, ModRevision: retention.ModRevision},
	}
	for index, key := range keys {
		conditions = append(
			conditions,
			keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])},
		)
	}
	mutations := []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: keys[0]}}
	if read.Values[5] != nil && string(read.Values[5].Value) == task.ID {
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: keys[5]})
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return true, err
	}
	if !result.Succeeded {
		return true, errs.New(errs.KindStateConflict, "connector check retention raced its Task or cleanup owner")
	}
	return true, nil
}

func (repository *TaskRepository) connectorCheckRetentionHeld(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (bool, error) {
	if !connectorCheckTask(task) {
		return false, nil
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{
		connectorchecks.ReferenceKey(task.Target, task.ID), connectorchecks.OperationKey(task.OperationID),
		connectorchecks.EnvironmentReferenceKey(task.Owner.EnvironmentID, task.ID),
	}, Revision: revision})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 3 {
		return false, errs.New(errs.KindInternal, "connector check retention ownership read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	return read.Values[0] != nil || read.Values[1] != nil || read.Values[2] != nil, nil
}
