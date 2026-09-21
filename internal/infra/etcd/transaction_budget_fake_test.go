package etcd

import (
	"context"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func (store *connectorDeletionLockedStore) MeasureTransaction(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionBudget, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return store.store.MeasureTransaction(ctx, conditions, mutations)
}
