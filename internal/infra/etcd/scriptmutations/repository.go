package scriptmutations

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
)

type mutationStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

// Repository owns Script record preparation, direct creation and locator cleanup.
type Repository struct {
	*scripts.Reader
	store mutationStore
}

func NewRepository(store mutationStore) *Repository {
	return &Repository{Reader: scripts.NewReader(store), store: store}
}
