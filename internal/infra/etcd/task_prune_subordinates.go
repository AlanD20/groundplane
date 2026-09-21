package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) pruneTaskSubordinateBatch(
	ctx context.Context,
	current etcdstore.Versioned[taskjournal.PruneIntent],
	events bool,
) (etcdstore.Versioned[taskjournal.PruneIntent], error) {
	remaining := current.Record.RemainingDeduplications
	prefix := taskjournal.TaskEventDedupScopePrefix(current.Record.TaskID)
	if events {
		remaining = current.Record.RemainingEvents
		prefix = taskjournal.TaskEventScopePrefix(current.Record.TaskID)
	}
	limit := int64(maximumTaskPruneBatchRecords + 1)
	if remaining < maximumTaskPruneBatchRecords {
		limit = int64(remaining) + 1
	}
	page, err := repository.store.Range(ctx, etcdstore.RangeRequest{Prefix: prefix, Limit: limit})
	if err != nil {
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, err
	}
	if page == nil || page.ReadRevision <= 0 || len(page.Values) == 0 {
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, taskjournal.CorruptPruneIntent()
	}
	defer clearKeyValueSlice(page.Values)
	if uint32(len(page.Values)) > remaining && remaining <= maximumTaskPruneBatchRecords {
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, taskjournal.CorruptPruneIntent()
	}
	count := len(page.Values)
	if count > maximumTaskPruneBatchRecords {
		count = maximumTaskPruneBatchRecords
	}
	if uint32(count) > remaining {
		count = int(remaining)
	}
	if !page.More && uint32(len(page.Values)) < remaining {
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, taskjournal.CorruptPruneIntent()
	}
	conditions := make([]etcdstore.Condition, 1, count+1)
	conditions[0] = etcdstore.Condition{
		Key:         taskjournal.TaskPruneIntentKey(current.Record.TaskID),
		ModRevision: current.Revision,
	}
	mutations := make([]etcdstore.Mutation, 0, count+1)
	for index := 0; index < count; index++ {
		entry := page.Values[index]
		if err := validateTaskPruneSubordinate(current.Record.TaskID, entry, events); err != nil {
			return etcdstore.Versioned[taskjournal.PruneIntent]{}, err
		}
		conditions = append(conditions, etcdstore.Condition{Key: entry.Key, ModRevision: entry.ModRevision})
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: entry.Key})
	}
	next := current.Record
	if events {
		next.RemainingEvents -= uint32(count)
	} else {
		next.RemainingDeduplications -= uint32(count)
	}
	intentValue, err := taskjournal.EncodePruneIntent(next)
	if err != nil {
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, err
	}
	defer clear(intentValue)
	mutations = append(mutations, etcdstore.Mutation{
		Type: etcdstore.MutationPut, Key: taskjournal.TaskPruneIntentKey(next.TaskID), Value: intentValue,
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

func validateTaskPruneSubordinate(taskID string, entry etcdstore.KeyValue, events bool) error {
	if entry.ModRevision <= 0 {
		return taskjournal.CorruptPruneIntent()
	}
	if events {
		sequence, err := taskjournal.TaskEventSequenceFromKey(taskID, entry.Key)
		if err != nil {
			return taskjournal.CorruptPruneIntent()
		}
		event, err := taskjournal.DecodeTaskEventRecord(entry.Value)
		if err != nil || event.Identity.TaskID != taskID || event.Sequence != sequence {
			return taskjournal.CorruptPruneIntent()
		}
		return nil
	}
	record, err := taskjournal.DecodeTaskEventDedupRecord(entry.Value)
	if err != nil || record.Identity.TaskID != taskID ||
		taskjournal.TaskEventDedupKey(record.Identity) != entry.Key {
		return taskjournal.CorruptPruneIntent()
	}
	return nil
}

func (repository *TaskRepository) verifyTaskPrunePrefixEmpty(
	ctx context.Context,
	taskID string,
	events bool,
) error {
	prefix := taskjournal.TaskEventDedupScopePrefix(taskID)
	if events {
		prefix = taskjournal.TaskEventScopePrefix(taskID)
	}
	page, err := repository.store.Range(ctx, etcdstore.RangeRequest{Prefix: prefix, Limit: 1})
	if err != nil {
		return err
	}
	if page == nil || page.ReadRevision <= 0 || len(page.Values) != 0 {
		if page != nil {
			clearKeyValueSlice(page.Values)
		}
		return taskjournal.CorruptPruneIntent()
	}
	return nil
}
