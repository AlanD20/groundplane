package environmentqueries

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type projectionStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
}

// ProjectionReader reads desired and applied Environment state without mutation authority.
type ProjectionReader struct {
	store projectionStore
}

// NewProjectionReader binds the read side of an already configured persistence store.
func NewProjectionReader(store projectionStore) *ProjectionReader {
	return &ProjectionReader{store: store}
}
