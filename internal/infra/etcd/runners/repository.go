package runners

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type persistenceStore interface {
	readStore
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

// Repository owns Runner queries, bootstrap reservations and observations.
// Task publication remains responsible for coordinated lifecycle changes.
type Repository struct {
	*Reader
	store persistenceStore
}

// NewRepository binds Runner persistence to an already configured store.
func NewRepository(store persistenceStore) *Repository {
	return &Repository{Reader: NewReader(store), store: store}
}
