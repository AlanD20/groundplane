package platformcomponents

import (
	"context"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type persistenceStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

// Persistence owns platform singleton bootstrap, indexes and observations.
type Persistence struct {
	store      persistenceStore
	components *componentrecord.Repository
}

// NewPersistence binds platform records to an already configured store.
func NewPersistence(store persistenceStore) *Persistence {
	return &Persistence{store: store, components: componentrecord.NewRepository(store)}
}
