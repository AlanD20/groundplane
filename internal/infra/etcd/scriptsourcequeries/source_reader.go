package scriptsourcequeries

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type readStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
}

// SourceReader resolves Script inputs, Release authority and Attach networks
// together at the execution's fixed persistence revision.
type SourceReader struct {
	store readStore
}

func NewSourceReader(store readStore) *SourceReader {
	return &SourceReader{store: store}
}
