package secretmutations

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
)

type persistenceStore interface {
	readStore
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

// Repository owns Secret snapshots and direct encrypted-value persistence.
type Repository struct {
	*secretrecord.Reader
	store persistenceStore
}

// NewRepository binds Secret persistence to an already configured store.
func NewRepository(store persistenceStore) *Repository {
	return &Repository{Reader: secretrecord.NewReader(store), store: store}
}

type readStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
}
