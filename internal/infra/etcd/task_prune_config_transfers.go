package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func (repository *TaskRepository) pruneTaskConfigTransferBatch(
	ctx context.Context,
	current etcdstore.Versioned[taskjournal.PruneIntent],
) (etcdstore.Versioned[taskjournal.PruneIntent], error) {
	prefix := backupconfiguration.ConfigTransferTaskPrefix(current.Record.TaskID)
	page, err := repository.store.Range(
		ctx,
		etcdstore.RangeRequest{Prefix: prefix, Limit: int64(maximumTaskPruneBatchRecords + 1)},
	)
	if err != nil {
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, err
	}
	if page == nil || page.ReadRevision <= 0 {
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, taskjournal.CorruptPruneIntent()
	}
	defer clearKeyValueSlice(page.Values)
	next := current.Record
	if len(page.Values) == 0 {
		next.BackupConfigTransfersComplete = true
		return repository.advanceTaskPruneIntent(
			ctx,
			current,
			next,
			[]etcdstore.Condition{{Key: prefix, Prefix: true}},
			nil,
		)
	}
	count := min(len(page.Values), maximumTaskPruneBatchRecords)
	conditions := make([]etcdstore.Condition, 0, count)
	mutations := make([]etcdstore.Mutation, 0, count)
	for _, value := range page.Values[:count] {
		if value.ModRevision <= 0 ||
			backupconfiguration.ValidateConfigTransferPruneEntry(current.Record.TaskID, value.Key, value.Value) != nil {
			return etcdstore.Versioned[taskjournal.PruneIntent]{}, taskjournal.CorruptPruneIntent()
		}
		conditions = append(conditions, etcdstore.Condition{Key: value.Key, ModRevision: value.ModRevision})
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: value.Key})
	}
	return repository.advanceTaskPruneIntent(ctx, current, next, conditions, mutations)
}
