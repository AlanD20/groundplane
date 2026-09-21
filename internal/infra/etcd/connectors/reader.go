package connectors

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type readStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
}

// Reader owns Connector reads and credential Secret selection evidence.
type Reader struct {
	store readStore
}

// NewReader binds Connector reads to an already configured persistence store.
func NewReader(store readStore) *Reader {
	return &Reader{store: store}
}
