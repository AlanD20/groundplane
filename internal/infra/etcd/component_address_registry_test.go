package etcd

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
)

// Rationale: the durable Zone registry must retain one Component's address,
// exclude it from later allocations, and make it reusable only after release.
func TestComponentAddressRegistryReserveAndRelease(t *testing.T) {
	at := serviceRecordTestTime()
	zone, err := NewZoneRecord(ids.NewAt(ids.KindEnvironment, at, 1), core.Zone{
		ID: ids.NewAt(ids.KindNetwork, at, 2), Name: "frontend", Subnet: "10.40.10.0/29",
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: ids.NewAt(ids.KindEnvironment, at, 1),
	})
	if err != nil {
		t.Fatalf("NewZoneRecord() error = %v", err)
	}
	firstID := ids.NewAt(ids.KindComponent, at, 3)
	secondID := ids.NewAt(ids.KindComponent, at, 4)
	thirdID := ids.NewAt(ids.KindComponent, at, 5)
	registry := componentAddressRegistry{Reservations: map[string]string{}}

	registry, first, err := registry.reserve(zone, firstID)
	if err != nil || first != "10.40.10.2" {
		t.Fatalf("first reserve = %q, %v", first, err)
	}
	stable, repeated, err := registry.reserve(zone, firstID)
	if err != nil || repeated != first || stable.Reservations[firstID] != first {
		t.Fatalf("repeat reserve = %#v, %q, %v", stable, repeated, err)
	}
	registry, second, err := registry.reserve(zone, secondID)
	if err != nil || second != "10.40.10.3" {
		t.Fatalf("second reserve = %q, %v", second, err)
	}

	encoded, err := encodeComponentAddressRegistry(zone, registry)
	if err != nil {
		t.Fatalf("encodeComponentAddressRegistry() error = %v", err)
	}
	decoded, err := decodeEnvelope[componentAddressRegistry](encoded, "component_address_registry")
	if err != nil || validateComponentAddressRegistry(zone, decoded) != nil {
		t.Fatalf("decoded registry = %#v, %v", decoded, err)
	}
	registry, released, found, err := decoded.release(zone, firstID)
	if err != nil || !found || released != first {
		t.Fatalf("release = %#v, %q, %t, %v", registry, released, found, err)
	}
	registry, reused, err := registry.reserve(zone, thirdID)
	if err != nil || reused != first || registry.Reservations[secondID] != second {
		t.Fatalf("reused reserve = %#v, %q, %v", registry, reused, err)
	}
}
