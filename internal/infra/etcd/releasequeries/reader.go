package releasequeries

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type readStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
}

// Reader owns Release history, serving state, rollback selection and log targets.
type Reader struct {
	store readStore
}

// NewReader binds Release reads to an already configured persistence store.
func NewReader(store readStore) *Reader {
	return &Reader{store: store}
}
