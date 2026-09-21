package routepersistence

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type store interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

// Repository owns Route reads and direct, target-fenced record writes.
type Repository struct {
	store store
}

// NewRepository composes an already configured persistence store.
func NewRepository(store store) *Repository {
	return &Repository{store: store}
}
