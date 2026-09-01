package etcd

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
)

func TestZoneRecordRejectsNonCanonicalSubnet(t *testing.T) {
	// Rationale: immutable Docker bridge IPAM must have one canonical subnet
	// representation before it enters durable desired state.
	t.Parallel()
	desired := zoneRecordTestRecord(t, "backend", 901).Desired
	desired.Subnet = "10.200.20.9/24"
	if _, err := NewZoneRecord(ids.NewAt(ids.KindEnvironment, serviceRecordTestTime(), 900), desired); err == nil {
		t.Fatal("NewZoneRecord() accepted a non-canonical subnet")
	}
}

func TestZoneRecordJoinsSelectedProjection(t *testing.T) {
	// Rationale: public Zone records are joined views of immutable desired
	// projection fields, not persisted flat desired-state records.
	t.Parallel()
	environmentID := ids.NewAt(ids.KindEnvironment, serviceRecordTestTime(), 902)
	desired := core.Zone{
		ID: ids.NewAt(ids.KindNetwork, serviceRecordTestTime(), 903), Name: "frontend",
		Subnet: "10.200.20.0/24", Internal: true,
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
	}
	joined, err := joinEnvironmentZone(Versioned[EnvironmentComposeProjection]{
		Record: EnvironmentComposeProjection{EnvironmentID: environmentID}, Revision: 41, ReadRevision: 42,
	}, EnvironmentZoneProjection{EnvironmentID: environmentID, Desired: desired})
	if err != nil {
		t.Fatalf("joinEnvironmentZone() error = %v", err)
	}
	if joined.Record.EnvironmentID != environmentID || joined.Record.Desired != desired ||
		joined.Revision != 41 || joined.ReadRevision != 42 {
		t.Fatalf("joinEnvironmentZone() = %#v, want desired projection join", joined)
	}
}

func zoneRecordTestRecord(t *testing.T, name string, offset int64) ZoneRecord {
	t.Helper()
	record, err := NewZoneRecord(
		ids.NewAt(ids.KindEnvironment, serviceRecordTestTime(), 900),
		core.Zone{
			ID: ids.NewAt(ids.KindNetwork, serviceRecordTestTime(), offset), Name: name,
			Subnet: "10.200.20.0/24", Internal: true,
			OwnerKind: core.ZoneOwnerEnvironment,
			OwnerID:   ids.NewAt(ids.KindEnvironment, serviceRecordTestTime(), 900),
		},
	)
	if err != nil {
		t.Fatalf("NewZoneRecord() error = %v", err)
	}
	return record
}
