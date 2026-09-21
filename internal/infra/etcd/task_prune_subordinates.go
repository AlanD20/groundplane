package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) pruneTaskSubordinateBatch(
	ctx context.Context,
	current etcdstore.Versioned[taskPruneIntent],
	events bool,
) (etcdstore.Versioned[taskPruneIntent], error) {
	remaining := current.Record.RemainingDeduplications
	prefix := taskEventDedupScopePrefix(current.Record.TaskID)
	if events {
		remaining = current.Record.RemainingEvents
		prefix = taskEventScopePrefix(current.Record.TaskID)
	}
	limit := int64(maximumTaskPruneBatchRecords + 1)
	if remaining < maximumTaskPruneBatchRecords {
		limit = int64(remaining) + 1
	}
	page, err := repository.store.Range(ctx, etcdstore.RangeRequest{Prefix: prefix, Limit: limit})
	if err != nil {
		return etcdstore.Versioned[taskPruneIntent]{}, err
	}
	if page == nil || page.ReadRevision <= 0 || len(page.Values) == 0 {
		return etcdstore.Versioned[taskPruneIntent]{}, corruptTaskPruneIntent()
	}
	defer clearKeyValueSlice(page.Values)
	if uint32(len(page.Values)) > remaining && remaining <= maximumTaskPruneBatchRecords {
		return etcdstore.Versioned[taskPruneIntent]{}, corruptTaskPruneIntent()
	}
	count := len(page.Values)
	if count > maximumTaskPruneBatchRecords {
		count = maximumTaskPruneBatchRecords
	}
	if uint32(count) > remaining {
		count = int(remaining)
	}
	if !page.More && uint32(len(page.Values)) < remaining {
		return etcdstore.Versioned[taskPruneIntent]{}, corruptTaskPruneIntent()
	}
	conditions := make([]etcdstore.Condition, 1, count+1)
	conditions[0] = etcdstore.Condition{
		Key:         taskPruneIntentKey(current.Record.TaskID),
		ModRevision: current.Revision,
	}
	mutations := make([]etcdstore.Mutation, 0, count+1)
	for index := 0; index < count; index++ {
		entry := page.Values[index]
		if err := validateTaskPruneSubordinate(current.Record.TaskID, entry, events); err != nil {
			return etcdstore.Versioned[taskPruneIntent]{}, err
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
	intentValue, err := encodeTaskPruneIntent(next)
	if err != nil {
		return etcdstore.Versioned[taskPruneIntent]{}, err
	}
	defer clear(intentValue)
	mutations = append(mutations, etcdstore.Mutation{
		Type: etcdstore.MutationPut, Key: taskPruneIntentKey(next.TaskID), Value: intentValue,
	})
	if len(conditions)+len(mutations) > etcdstore.MaximumOperations {
		return etcdstore.Versioned[taskPruneIntent]{}, errs.New(
			errs.KindInternal,
			"task prune batch exceeds transaction limit",
		)
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return etcdstore.Versioned[taskPruneIntent]{}, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return etcdstore.Versioned[taskPruneIntent]{}, errs.New(
			errs.KindStateConflict,
			"task prune batch changed",
		)
	}
	return etcdstore.Versioned[taskPruneIntent]{
		Record: next, Revision: transaction.Revision, ReadRevision: transaction.Revision,
	}, nil
}

func validateTaskPruneSubordinate(taskID string, entry etcdstore.KeyValue, events bool) error {
	if entry.ModRevision <= 0 {
		return corruptTaskPruneIntent()
	}
	if events {
		sequence, err := taskEventSequenceFromKey(taskID, entry.Key)
		if err != nil {
			return corruptTaskPruneIntent()
		}
		event, err := taskjournal.DecodeTaskEventRecord(entry.Value)
		if err != nil || event.Identity.TaskID != taskID || event.Sequence != sequence {
			return corruptTaskPruneIntent()
		}
		return nil
	}
	record, err := taskjournal.DecodeTaskEventDedupRecord(entry.Value)
	if err != nil || record.Identity.TaskID != taskID ||
		taskEventDedupKey(record.Identity) != entry.Key {
		return corruptTaskPruneIntent()
	}
	return nil
}

func (repository *TaskRepository) verifyTaskPrunePrefixEmpty(
	ctx context.Context,
	taskID string,
	events bool,
) error {
	prefix := taskEventDedupScopePrefix(taskID)
	if events {
		prefix = taskEventScopePrefix(taskID)
	}
	page, err := repository.store.Range(ctx, etcdstore.RangeRequest{Prefix: prefix, Limit: 1})
	if err != nil {
		return err
	}
	if page == nil || page.ReadRevision <= 0 || len(page.Values) != 0 {
		if page != nil {
			clearKeyValueSlice(page.Values)
		}
		return corruptTaskPruneIntent()
	}
	return nil
}
