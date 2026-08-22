package etcd

import (
	"bytes"
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

func TestZoneRecordEnvelopeRoundTripsStrictly(t *testing.T) {
	// Rationale: corrupt or shape-shifted Zone records must fail closed rather
	// than silently changing network identity or ownership.
	t.Parallel()
	record := zoneRecordTestRecord(t, "frontend", 902)
	encoded, err := encodeZoneRecord(record)
	if err != nil {
		t.Fatalf("encodeZoneRecord() error = %v", err)
	}
	decoded, err := decodeZoneRecord(encoded)
	if err != nil || decoded != record {
		t.Fatalf("decodeZoneRecord() = %#v, %v, want %#v", decoded, err, record)
	}
	for name, corrupt := range map[string][]byte{
		"unknown":   bytes.Replace(encoded, []byte(`"desired":`), []byte(`"unknown":0,"desired":`), 1),
		"duplicate": bytes.Replace(encoded, []byte(`"desired":`), []byte(`"environment_id":"x","desired":`), 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeZoneRecord(corrupt); err == nil {
				t.Fatal("decodeZoneRecord() accepted corrupt record")
			}
		})
	}
}

func TestZoneRecordKeysUseStableEnvironmentScope(t *testing.T) {
	// Rationale: the primary, membership, and scoped-name keys are the atomic
	// contract shared by direct mutations and Blueprint reconciliation.
	t.Parallel()
	record := zoneRecordTestRecord(t, "api.backend", 903)
	if got := zoneKey(record.Desired.ID); got != "/v1/records/zones/"+record.Desired.ID {
		t.Fatalf("zoneKey() = %q", got)
	}
	if got := zoneOwnerKey(record.EnvironmentID, record.Desired.ID); got !=
		"/v1/indexes/zones/by-owner/environment/"+record.EnvironmentID+"/"+record.Desired.ID {
		t.Fatalf("zoneOwnerKey() = %q", got)
	}
	if got := zoneNameKey(record.EnvironmentID, record.Desired.Name); got !=
		"/v1/indexes/zones/by-name/environment/"+record.EnvironmentID+"/~YXBpLmJhY2tlbmQ" {
		t.Fatalf("zoneNameKey() = %q", got)
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
