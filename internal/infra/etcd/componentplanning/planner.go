package componentplanning

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type planningStore interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
}

type Planner struct{ store planningStore }

func NewPlanner(store planningStore) *Planner { return &Planner{store: store} }
