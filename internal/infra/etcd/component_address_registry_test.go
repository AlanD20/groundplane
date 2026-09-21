package etcd

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testnetworkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testzones "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
)

// Rationale: the durable Zone registry must retain one Component's address,
// exclude it from later allocations, and make it reusable only after release.
func TestComponentAddressRegistryReserveAndRelease(t *testing.T) {
	at := serviceRecordTestTime()
	zone, err := testzones.NewRecord(ids.NewAt(ids.KindEnvironment, at, 1), core.Zone{
		ID: ids.NewAt(ids.KindNetwork, at, 2), Name: "frontend", Subnet: "10.40.10.0/29",
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: ids.NewAt(ids.KindEnvironment, at, 1),
	})
	if err != nil {
		t.Fatalf("NewZoneRecord() error = %v", err)
	}
	firstID := ids.NewAt(ids.KindComponent, at, 3)
	secondID := ids.NewAt(ids.KindComponent, at, 4)
	thirdID := ids.NewAt(ids.KindComponent, at, 5)
	registry := testnetworkreservations.ComponentAddressRegistry{Reservations: map[string]string{}}

	registry, first, err := registry.Reserve(zone, firstID)
	if err != nil || first != "10.40.10.6" {
		t.Fatalf("first reserve = %q, %v", first, err)
	}
	stable, repeated, err := registry.Reserve(zone, firstID)
	if err != nil || repeated != first || stable.Reservations[firstID] != first {
		t.Fatalf("repeat reserve = %#v, %q, %v", stable, repeated, err)
	}
	registry, second, err := registry.Reserve(zone, secondID)
	if err != nil || second != "10.40.10.5" {
		t.Fatalf("second reserve = %q, %v", second, err)
	}

	encoded, err := testnetworkreservations.EncodeComponentAddressRegistry(zone, registry)
	if err != nil {
		t.Fatalf("encodeComponentAddressRegistry() error = %v", err)
	}
	decoded, err := testrecordcodec.Decode[testnetworkreservations.ComponentAddressRegistry](
		encoded,
		"component_address_registry",
	)
	if err != nil || testnetworkreservations.ValidateComponentAddressRegistry(zone, decoded) != nil {
		t.Fatalf("decoded registry = %#v, %v", decoded, err)
	}
	registry, released, found, err := decoded.Release(zone, firstID)
	if err != nil || !found || released != first {
		t.Fatalf("release = %#v, %q, %t, %v", registry, released, found, err)
	}
	registry, reused, err := registry.Reserve(zone, thirdID)
	if err != nil || reused != first || registry.Reservations[secondID] != second {
		t.Fatalf("reused reserve = %#v, %q, %v", registry, reused, err)
	}
}
