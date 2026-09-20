package backingservices

import (
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
)

func buildBackingServiceCreationProjection(
	environmentID string,
	revisionID string,
	identities controller.ComposeIdentitySnapshot,
	volumeSlugs map[string]string,
	volumeMounts []etcd.EnvironmentServiceVolumeMount,
	artifact []byte,
	normalizedCompose []byte,
	zone etcd.ZoneRecord,
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
