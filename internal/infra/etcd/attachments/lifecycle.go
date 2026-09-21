package attachments

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type lifecycleStore interface {
	attachRemovalStore
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

// Lifecycle persists Attach status changes and removes detached bindings.
type Lifecycle struct{ store lifecycleStore }

// NewLifecycle binds Attach changes to an already configured persistence store.
func NewLifecycle(store lifecycleStore) *Lifecycle { return &Lifecycle{store: store} }
