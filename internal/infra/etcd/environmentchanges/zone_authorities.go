package environmentchanges

import (
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// EnvironmentZoneRemovalAuthorities binds the desired revision being edited
// to the independently mutable projection last acknowledged by the runtime.
type EnvironmentZoneRemovalAuthorities struct {
	Desired etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]
	Applied etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]
}
