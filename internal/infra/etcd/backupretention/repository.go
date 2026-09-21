package backupretention

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type retentionStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

// Repository owns fixed-revision retention selection and its fenced writes.
type Repository struct {
	store   retentionStore
	runtime *backupruntime.Writer
}

func NewRepository(store retentionStore) *Repository {
	return &Repository{store: store, runtime: backupruntime.NewWriter(store)}
}
