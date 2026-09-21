package backingservices

import (
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"

	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"

	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
)

func buildBackingServiceCreationProjection(
	environmentID string,
	revisionID string,
	identities composeidentity.Snapshot,
	volumeSlugs map[string]string,
	volumeMounts []projectionrecord.EnvironmentServiceVolumeMount,
	artifact []byte,
	normalizedCompose []byte,
	zone zonerecord.Record,
	service servicerecord.ServiceRecord,
	entries []entryrecord.Record,
) projectionrecord.EnvironmentComposeProjection {
	projection := desiredrevision.ComposeProjection(
		environmentID, revisionID, 1, identities, volumeSlugs, volumeMounts,
		artifact, normalizedCompose, nil, nil, nil, nil, entries,
	)
	return desiredrevision.WithDesiredTopology(
		projection,
		[]projectionrecord.EnvironmentZoneProjection{{EnvironmentID: zone.EnvironmentID, Desired: zone.Desired}},
		[]servicerecord.EnvironmentServiceProjection{{
			EnvironmentID: service.EnvironmentID, BackingNetworkID: service.BackingNetworkID, Desired: service.Desired,
		}},
		nil,
	)
}
