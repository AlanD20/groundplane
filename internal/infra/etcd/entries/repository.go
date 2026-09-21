package entries

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type persistenceStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

// Repository owns Entry snapshots and fenced metadata/value-generation writes.
type Repository struct{ store persistenceStore }

// NewRepository binds Entry persistence to an already configured store.
func NewRepository(store persistenceStore) *Repository { return &Repository{store: store} }
