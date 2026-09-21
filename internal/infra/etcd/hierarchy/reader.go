package hierarchy

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type readStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
}

// Reader owns Tenant, Project and Environment lookup, scoped resolution and pages.
type Reader struct {
	store readStore
}

// NewReader binds reads to an already configured persistence store.
func NewReader(store readStore) *Reader {
	return &Reader{store: store}
}
