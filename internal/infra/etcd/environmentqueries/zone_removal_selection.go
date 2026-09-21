package environmentqueries

import (
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func SelectZoneForDeletion(
	projection etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	supplied etcdstore.Versioned[zonerecord.Record],
	zoneID string,
) (etcdstore.Versioned[zonerecord.Record], error) {
	if projection.Revision <= 0 || projection.ReadRevision < projection.Revision ||
		supplied.Revision != projection.Revision || supplied.ReadRevision < supplied.Revision {
		return etcdstore.Versioned[zonerecord.Record]{}, errs.New(
			errs.KindValidationFailed,
			"Zone deletion projection is invalid",
		)
	}
	var selected *etcdstore.Versioned[zonerecord.Record]
	for _, desired := range projection.Record.DesiredZones {
		if desired.Desired.ID != zoneID {
			continue
		}
		if selected != nil {
			return etcdstore.Versioned[zonerecord.Record]{}, projectionrecord.CorruptEnvironmentComposeProjection()
		}
		joined, err := JoinZone(projection, desired)
		if err != nil {
			return etcdstore.Versioned[zonerecord.Record]{}, err
		}
		selected = &joined
	}
	if selected == nil || selected.Record != supplied.Record {
		return etcdstore.Versioned[zonerecord.Record]{}, errs.New(
			errs.KindValidationFailed,
			"Zone does not match the selected projection",
		)
	}
	return *selected, nil
}
