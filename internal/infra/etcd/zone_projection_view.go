package etcd

import zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"

func joinEnvironmentZone(
	projection Versioned[EnvironmentComposeProjection],
	desired EnvironmentZoneProjection,
) (Versioned[zonerecord.Record], error) {
	if desired.EnvironmentID != projection.Record.EnvironmentID {
		return Versioned[zonerecord.Record]{}, corruptEnvironmentComposeProjection()
	}
	record, err := zonerecord.NewRecord(desired.EnvironmentID, desired.Desired)
	if err != nil {
		return Versioned[zonerecord.Record]{}, corruptEnvironmentComposeProjection()
	}
	return Versioned[zonerecord.Record]{
		Record: record, Revision: projection.Revision, ReadRevision: projection.ReadRevision,
	}, nil
}
