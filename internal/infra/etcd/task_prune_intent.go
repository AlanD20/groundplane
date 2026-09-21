package etcd

import (
	"context"
	attachrender "github.com/AlanD20/groundplane/internal/infra/etcd/attachrender"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	maximumTaskPruneBatchRecords = (etcdstore.MaximumOperations - 2) / 2
	maximumTaskPruneCASAttempts  = 8
)

func (repository *TaskRepository) advanceTaskPruneIntent(
	ctx context.Context,
	current etcdstore.Versioned[taskjournal.PruneIntent],
	next taskjournal.PruneIntent,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) (etcdstore.Versioned[taskjournal.PruneIntent], error) {
	intentValue, err := taskjournal.EncodePruneIntent(next)
	if err != nil {
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, err
	}
	defer clear(intentValue)
	conditions = append([]etcdstore.Condition{{
		Key: taskjournal.TaskPruneIntentKey(current.Record.TaskID), ModRevision: current.Revision,
	}}, conditions...)
	mutations = append(mutations, etcdstore.Mutation{
		Type: etcdstore.MutationPut, Key: taskjournal.TaskPruneIntentKey(current.Record.TaskID), Value: intentValue,
	})
	if len(conditions)+len(mutations) > etcdstore.MaximumOperations {
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, errs.New(
			errs.KindInternal,
			"task prune batch exceeds transaction limit",
		)
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, err
	}
	etcdstore.ClearValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, errs.New(
			errs.KindStateConflict,
			"task prune batch changed",
		)
	}
	return etcdstore.Versioned[taskjournal.PruneIntent]{
		Record: next, Revision: transaction.Revision, ReadRevision: transaction.Revision,
	}, nil
}

func (repository *TaskRepository) finishTaskPruneIntent(
	ctx context.Context,
	current etcdstore.Versioned[taskjournal.PruneIntent],
) error {
	conditions := []etcdstore.Condition{{
		Key: taskjournal.TaskPruneIntentKey(current.Record.TaskID), ModRevision: current.Revision,
	}}
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationDelete, Key: taskjournal.TaskPruneIntentKey(current.Record.TaskID)}}
	if current.Record.AttachPlanID != "" {
		references, err := repository.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: attachrender.AttachTaskPlanReferenceScopePrefix(current.Record.AttachPlanID), Limit: 1,
		})
		if err != nil {
			return err
		}
		if references == nil || references.ReadRevision <= 0 || len(references.Values) > 1 {
			return taskjournal.CorruptPruneIntent()
		}
		defer clearKeyValueSlice(references.Values)
		if len(references.Values) != 0 {
			if _, err := attachrender.DecodeAttachTaskPlanReference(
				references.Values[0].Key,
				references.Values[0].Value,
				current.Record.AttachPlanID,
			); err != nil {
				return taskjournal.CorruptPruneIntent()
			}
		} else {
			inputKey := attachrender.AttachTaskRenderInputKey(current.Record.AttachPlanID)
			inputResult, err := repository.store.GetMany(
				ctx,
				etcdstore.GetManyRequest{Keys: []string{inputKey}},
			)
			if err != nil {
				return err
			}
			if inputResult == nil || len(inputResult.Values) != 1 || inputResult.Values[0] == nil {
				return taskjournal.CorruptPruneIntent()
			}
			defer etcdstore.ClearValues(inputResult.Values)
			input, err := attachrender.DecodeAttachTaskRenderInput(inputResult.Values[0].Value)
			if err != nil || input.PlanID != current.Record.AttachPlanID {
				return taskjournal.CorruptPruneIntent()
			}
			conditions = append(conditions, etcdstore.Condition{
				Key: inputKey, ModRevision: inputResult.Values[0].ModRevision,
			})
			mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: inputKey})
		}
	}
	conditions, mutations = appendBackupTerminalReceiptPruneFinalization(
		current.Record,
		conditions,
		mutations,
	)
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return err
	}
	etcdstore.ClearValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return errs.New(errs.KindStateConflict, "task prune completion changed")
	}
	return nil
}
