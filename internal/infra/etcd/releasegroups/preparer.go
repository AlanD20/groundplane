package releasegroups

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type preparationStore interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
}

// Preparer owns Release Group membership evidence and opaque mutation fragments.
// It cannot publish a Task or write the prepared changes.
type Preparer struct {
	store preparationStore
}

// NewPreparer binds preparation to an already configured persistence store.
func NewPreparer(store preparationStore) *Preparer {
	return &Preparer{store: store}
}
