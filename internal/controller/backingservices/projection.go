package backingservices

import (
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"

	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
)

func buildBackingServiceCreationProjection(
	environmentID string,
	revisionID string,
	identities composeidentity.Snapshot,
	volumeSlugs map[string]string,
	volumeMounts []etcd.EnvironmentServiceVolumeMount,
	artifact []byte,
	normalizedCompose []byte,
	zone zonerecord.Record,
	service etcd.ServiceRecord,
	entries []entryrecord.Record,
) etcd.EnvironmentComposeProjection {
	projection := desiredrevision.ComposeProjection(
		environmentID, revisionID, 1, identities, volumeSlugs, volumeMounts,
		artifact, normalizedCompose, nil, nil, nil, nil, entries,
	)
	return desiredrevision.WithDesiredTopology(
		projection,
		[]etcd.EnvironmentZoneProjection{{EnvironmentID: zone.EnvironmentID, Desired: zone.Desired}},
		[]etcd.EnvironmentServiceProjection{{
			EnvironmentID: service.EnvironmentID, BackingNetworkID: service.BackingNetworkID, Desired: service.Desired,
		}},
		nil,
	)
}
