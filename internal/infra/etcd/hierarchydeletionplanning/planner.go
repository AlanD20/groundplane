package hierarchydeletionplanning

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type membershipStore interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error)
}

type Planner struct {
	store membershipStore
}

func NewPlanner(store membershipStore) *Planner {
	return &Planner{store: store}
}
