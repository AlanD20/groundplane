package backuppolicymutations

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"time"
)

type persistenceStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

// Repository owns Backup Policy snapshots and replacement preparation.
type Repository struct {
	store persistenceStore
	now   func() time.Time
}

// NewRepository binds preparation to the configured persistence store and clock.
func NewRepository(store persistenceStore, now func() time.Time) *Repository {
	return &Repository{store: store, now: now}
}
