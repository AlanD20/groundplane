package networkreservations

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type poolReadStore interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
}

type Planner struct{ store poolReadStore }

func NewPlanner(store poolReadStore) *Planner { return &Planner{store: store} }

func (change EnvironmentPoolChange) Environment() keyvalue.Versioned[hierarchyrecord.EnvironmentRecord] {
	return change.environment
}
func (change EnvironmentPoolChange) RegistryRevision() int64 { return change.registryRevision }

// Values borrow the prepared bytes through publication and clearing.
func (change EnvironmentPoolChange) EnvironmentValue() []byte { return change.environmentValue }
func (change EnvironmentPoolChange) RegistryValue() []byte    { return change.registryValue }
func (change ZonePoolChange) CurrentRevision() int64          { return change.currentRevision }
func (change ZonePoolChange) Value() []byte                   { return change.value }
