package componentplanning

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"
)

type planningStore interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
}

type Planner struct{ store planningStore }

func NewPlanner(store planningStore) *Planner { return &Planner{store: store} }

// TaskIdentity carries the existing Task fields that bind a Component candidate.
type TaskIdentity struct {
	ID        string
	Target    string
	Executor  taskjournal.TaskExecutor
	Type      taskjournal.TaskType
	CreatedAt time.Time
}

// Conditions and Mutations borrow the prepared transaction buffers.
func (publication Publication) Conditions() []keyvalue.Condition { return publication.conditions }
func (publication Publication) Mutations() []keyvalue.Mutation   { return publication.mutations }

// These slices retain the prepared Route observation buffers until publication.
func (change RouteObservationChange) Conditions() []keyvalue.Condition { return change.conditions }
func (change RouteObservationChange) Mutations() []keyvalue.Mutation   { return change.mutations }
func (change RouteObservationChange) Values() [][]byte                 { return change.values }
