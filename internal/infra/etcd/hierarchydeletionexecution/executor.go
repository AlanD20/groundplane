package hierarchydeletionexecution

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type executionStore interface {
	Get(context.Context, string) (*keyvalue.GetResult, error)
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
}

type operationReader interface {
	OperationByTask(context.Context, string) (hierarchydeletion.HierarchyDeletionOperation, error)
}

type Executor struct {
	store      executionStore
	operations operationReader
}

func NewExecutor(store executionStore, operations operationReader) *Executor {
	return &Executor{store: store, operations: operations}
}
