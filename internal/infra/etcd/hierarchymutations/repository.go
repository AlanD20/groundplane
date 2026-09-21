package hierarchymutations

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type mutationStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

// Repository owns direct hierarchy creation and label-index transactions.
// Task publication and idempotency claims are composed separately.
type Repository struct {
	store  mutationStore
	reader *hierarchy.Reader
}

// NewRepository binds direct mutations to an already configured persistence store.
func NewRepository(store mutationStore) *Repository {
	return &Repository{store: store, reader: hierarchy.NewReader(store)}
}
