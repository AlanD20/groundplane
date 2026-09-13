package etcd

import "context"

func (store *connectorDeletionLockedStore) MeasureTransaction(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionBudget, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return store.store.MeasureTransaction(ctx, conditions, mutations)
}
