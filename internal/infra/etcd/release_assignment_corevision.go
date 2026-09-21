package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) incrementAssignmentEpoch(
	ctx context.Context,
	current TaskAssignment,
	evidenceConditions []etcdstore.Condition,
) error {
	taskValue, err := encodeTaskRecord(current.Task.Record)
	if err != nil {
		return err
	}
	defer clear(taskValue)
	record := current.Assignment.Record
	record.ExecutionEpoch++
	encoded, err := encodeTaskAssignment(record)
	if err != nil {
		return err
	}
	defer clear(encoded)
	old, err := encodeTaskAssignment(current.Assignment.Record)
	if err != nil {
		return err
	}
	defer clear(old)
	lifecycleKey, lifecycleValue, proofRequired, err := repository.assignmentLifecycleIndexAtRevision(
		ctx, current.Assignment.Record,
		&etcdstore.KeyValue{Value: old, ModRevision: current.Assignment.Revision}, current.Task.ReadRevision,
	)
	if err != nil || proofRequired != current.RecoveryProofRequired {
		if err != nil {
			return err
		}
		return errs.New(errs.KindStateConflict, "Agent reconnect lifecycle authority changed")
	}
	conditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(record.TaskID), ModRevision: current.Task.Revision},
		{
			Key:         taskjournal.TaskExecutionClaimKey(record.Executor, record.AgentID, record.TaskID),
			ModRevision: current.Assignment.Revision,
		},
		{Key: taskjournal.TaskAssignmentIndexKey(record.TaskID), ModRevision: current.Assignment.Revision},
		{Key: lifecycleKey, ModRevision: lifecycleValue.ModRevision},
	}
	conditions = append(conditions, evidenceConditions...)
	if record.ExecutionMode == TaskExecutionModeRecoveryOnly {
		recoveryRead, readErr := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{releaseRecoveryKey(record.TaskID)}, Revision: current.Task.ReadRevision,
		})
		if readErr != nil || recoveryRead == nil || len(recoveryRead.Values) != 1 || recoveryRead.Values[0] == nil {
			if readErr != nil {
				return readErr
			}
			return corruptTaskAssignment()
		}
		conditions = append(conditions, etcdstore.Condition{
			Key: releaseRecoveryKey(record.TaskID), ModRevision: recoveryRead.Values[0].ModRevision,
		})
	} else {
		conditions = append(conditions, etcdstore.Condition{Key: releaseRecoveryKey(record.TaskID)})
	}
	transaction, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(record.TaskID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskExecutionClaimKey(record.Executor, record.AgentID, record.TaskID), Value: encoded},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskAssignmentIndexKey(record.TaskID), Value: encoded},
		{Type: etcdstore.MutationPut, Key: lifecycleKey, Value: encoded},
	})
	if err != nil {
		return err
	}
	if !transaction.Succeeded {
		return errs.New(errs.KindStateConflict, "Agent reconnect raced durable execution evidence")
	}
	return nil
}
