package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) edgeCheckRetentionHeld(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (bool, error) {
	if !edgeCheckTask(task) {
		return false, nil
	}
	keys := []string{edgechecks.Key(task.ID), edgechecks.OperationKey(task.OperationID)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return false, errs.New(errs.KindInternal, "Tunnel check retention read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return read.Values[1] != nil, nil
	}
	record, err := edgechecks.Decode(read.Values[0].Value)
	if err != nil {
		return false, err
	}
	if !edgeCheckMatchesTask(record, task) {
		return false, errs.New(errs.KindStateConflict, "Tunnel check retention input differs from its Task")
	}
	// Every historical transfer waits for the operation's final cleanup owner.
	// Otherwise an expired original Task could disappear during a live Retry.
	return read.Values[1] != nil || record.Phase != edgechecks.Finished && record.Phase != edgechecks.Transferred, nil
}

func (repository *TaskRepository) pruneEdgeCheckInput(
	ctx context.Context,
	task TaskRecord,
	taskRevision int64,
	retention keyvalue.KeyValue,
	revision int64,
) (bool, error) {
	if !edgeCheckTask(task) {
		return false, nil
	}
	input, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{edgechecks.Key(task.ID)}, Revision: revision},
	)
	if err != nil {
		return true, err
	}
	if input == nil || input.ReadRevision != revision || len(input.Values) != 1 {
		if input != nil {
			keyvalue.ClearValues(input.Values)
		}
		return true, errs.New(errs.KindInternal, "Tunnel check pruning input is incomplete")
	}
	defer keyvalue.ClearValues(input.Values)
	if input.Values[0] == nil {
		return false, nil
	}
	record, _, err := repository.loadEdgeCheckAt(ctx, task, revision)
	if err != nil {
		return true, err
	}
	if record.Phase != edgechecks.Finished && record.Phase != edgechecks.Transferred {
		return true, nil
	}
	keys := []string{taskjournal.TaskActiveOperationKey(task.OperationID), taskjournal.ControllerTaskClaimKey(task.ID),
		taskjournal.TaskAssignmentIndexKey(task.ID), edgechecks.OperationKey(task.OperationID)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return true, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return true, errs.New(errs.KindInternal, "Tunnel check pruning ownership is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	for _, value := range read.Values {
		if value != nil {
			return true, nil
		}
	}
	records, err := edgechecks.NewRepository(repository.store)
	if err != nil {
		return true, err
	}
	conditions, mutations, err := records.PreparePrune(ctx, task.ID, revision)
	if err != nil {
		return true, err
	}
	// The child owns record/claim/reference/latest comparisons; native retention
	// joins the exact terminal Task, expiry index and active-operation absence.
	conditions = append(
		conditions,
		keyvalue.Condition{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskRevision},
		keyvalue.Condition{Key: retention.Key, ModRevision: retention.ModRevision},
		keyvalue.Condition{Key: keys[0]},
	)
	result, err := repository.store.Transact(ctx, conditions, mutations)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return true, err
	}
	if !result.Succeeded {
		return true, errs.New(errs.KindStateConflict, "Tunnel check pruning raced its Task or cleanup owner")
	}
	return true, nil
}
