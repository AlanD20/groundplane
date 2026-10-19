package etcd

import (
	"context"

	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	groupstore "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type taskRepositoryStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	MeasureTransaction(
		context.Context,
		[]etcdstore.Condition,
		[]etcdstore.Mutation,
	) (etcdstore.TransactionBudget, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

// TaskRepository composes resource publication with native execution admission.
// Work scheduling, attempts and diagnostics have their own persistence owners.
type TaskRepository struct {
	*groupstore.Preparer
	store taskRepositoryStore
}

func newTaskRepository(store taskRepositoryStore) (*TaskRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "task store is required")
	}
	return &TaskRepository{Preparer: groupstore.NewPreparer(store), store: store}, nil
}

func NewTaskRepository(store taskRepositoryStore) (*TaskRepository, error) {
	return newTaskRepository(store)
}

// TaskRetryScope domain-separates a human retry intent from the same
// idempotency key under another resource owner.
type TaskRetryScope struct {
	Kind idempotencyrecord.IdempotencyScopeKind
	ID   string
}
