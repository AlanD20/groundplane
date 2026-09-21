package connectormutations

import (
	"context"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type persistenceStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

// Repository owns Connector creation and its hierarchy/credential fences.
type Repository struct {
	*connectorrecord.Reader
	store persistenceStore
}

// NewRepository binds Connector mutations to an already configured store.
func NewRepository(store persistenceStore) *Repository {
	return &Repository{Reader: connectorrecord.NewReader(store), store: store}
}
