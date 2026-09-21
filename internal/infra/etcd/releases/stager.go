package releases

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type stagingStore interface {
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

// Stager persists immutable Release inputs before atomic publication.
type Stager struct{ store stagingStore }

// NewStager binds Release staging to an already configured persistence store.
func NewStager(store stagingStore) *Stager { return &Stager{store: store} }
