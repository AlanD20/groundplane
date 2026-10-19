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

// These slices retain the prepared Route observation buffers until publication.
func (change RouteObservationChange) Conditions() []keyvalue.Condition { return change.conditions }
func (change RouteObservationChange) Mutations() []keyvalue.Mutation   { return change.mutations }
func (change RouteObservationChange) Values() [][]byte                 { return change.values }
