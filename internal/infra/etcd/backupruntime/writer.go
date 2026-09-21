package backupruntime

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

const maximumBackupRuntimeTransactionBytes = 768 << 10

type writeStore interface {
	readStore
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

// Writer owns bounded runtime transactions and Controller-owned orphan repair.
type Writer struct {
	*Reader
	store writeStore
}

func NewWriter(store writeStore) *Writer {
	return &Writer{Reader: NewReader(store), store: store}
}
