package etcd

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
)

func joinEnvironmentZone(
	projection etcdstore.Versioned[EnvironmentComposeProjection],
	desired EnvironmentZoneProjection,
) (etcdstore.Versioned[zonerecord.Record], error) {
	if desired.EnvironmentID != projection.Record.EnvironmentID {
		return etcdstore.Versioned[zonerecord.Record]{}, corruptEnvironmentComposeProjection()
	}
	record, err := zonerecord.NewRecord(desired.EnvironmentID, desired.Desired)
	if err != nil {
		return etcdstore.Versioned[zonerecord.Record]{}, corruptEnvironmentComposeProjection()
	}
	return etcdstore.Versioned[zonerecord.Record]{
		Record: record, Revision: projection.Revision, ReadRevision: projection.ReadRevision,
	}, nil
}
