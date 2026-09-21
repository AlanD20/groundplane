package environmentqueries

import (
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
)

func JoinZone(
	projection etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	desired projectionrecord.EnvironmentZoneProjection,
) (etcdstore.Versioned[zonerecord.Record], error) {
	if desired.EnvironmentID != projection.Record.EnvironmentID {
		return etcdstore.Versioned[zonerecord.Record]{}, projectionrecord.CorruptEnvironmentComposeProjection()
	}
	record, err := zonerecord.NewRecord(desired.EnvironmentID, desired.Desired)
	if err != nil {
		return etcdstore.Versioned[zonerecord.Record]{}, projectionrecord.CorruptEnvironmentComposeProjection()
	}
	return etcdstore.Versioned[zonerecord.Record]{
		Record: record, Revision: projection.Revision, ReadRevision: projection.ReadRevision,
	}, nil
}
