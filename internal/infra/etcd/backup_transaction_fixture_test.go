package etcd

import (
	"context"

	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type backupTerminalTransactionFailureStore struct {
	hierarchyStore
	failNext error
}

func (store *backupTerminalTransactionFailureStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	if store.failNext != nil {
		failure := store.failNext
		store.failNext = nil
		return testkeyvalue.TransactionResult{}, failure
	}
	return store.hierarchyStore.Transact(ctx, conditions, mutations)
}
