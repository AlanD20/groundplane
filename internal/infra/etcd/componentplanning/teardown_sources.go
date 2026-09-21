package componentplanning

import (
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
)

// ManagedComponentTeardownSources returns the immutable prior runtime source
// authority captured by Component candidate preparation.
func (preparation ComponentTaskPreparation) ManagedComponentTeardownSources() []projectionrecord.ManagedComponentRuntimeSource {
	return projectionrecord.CloneManagedComponentRuntimeSources(preparation.managedRuntimeSources)
}
