package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testnetworkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	testzones "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
)

// Rationale: concurrent deployments cannot claim the same proxy address, and
// later Deploys or removing a different owner cannot change a DNS target.
func TestServiceProxyAddressPublicationRetainsOwnersAndRejectsRace(t *testing.T) {
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	at := serviceRecordTestTime()
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	zone, err := testzones.NewRecord(
		environmentID,
		core.Zone{
			ID:        ids.NewAt(ids.KindNetwork, at, 2),
			Name:      "frontend",
			Subnet:    "10.40.10.0/29",
			OwnerKind: core.ZoneOwnerEnvironment,
			OwnerID:   environmentID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	componentID := ids.NewAt(ids.KindComponent, at, 3)
	registry := testnetworkreservations.ComponentAddressRegistry{
		Reservations: map[string]string{componentID: "10.40.10.6"},
	}
	value, err := testnetworkreservations.EncodeComponentAddressRegistry(zone, registry)
	if err != nil {
		t.Fatal(err)
	}
	key := testnetworkreservations.ComponentAddressRegistryKey(zone.Desired.ID)
	if _, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: key, Value: value}}); err != nil {
		t.Fatal(err)
	}
	first := core.Service{ID: ids.NewAt(ids.KindService, at, 4), Expose: []string{"80"}, Zones: []string{"frontend"}}
	second := core.Service{ID: ids.NewAt(ids.KindService, at, 5), Expose: []string{"80"}, Zones: []string{"frontend"}}
	planner := testnetworkreservations.NewPlanner(store)
	prepare := func(services ...core.Service) testnetworkreservations.ProxyAddresses {
		t.Helper()
		result, err := planner.PrepareProxyAddresses(ctx, []testzones.Record{zone}, services)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(result.Clear)
		return result
	}
	firstCandidate, concurrent := prepare(first), prepare(second)
	if firstCandidate.ForService(first.ID)["frontend"] != "10.40.10.5" {
		t.Fatal("proxy collided with Component reservation")
	}
	if committed, err := store.Transact(ctx, firstCandidate.Conditions(), firstCandidate.Mutations()); err != nil ||
		!committed.Succeeded {
		t.Fatalf("commit first = %#v, %v", committed, err)
	}
	if raced, err := store.Transact(ctx, concurrent.Conditions(), concurrent.Mutations()); err != nil ||
		raced.Succeeded {
		t.Fatalf("stale allocation committed = %#v, %v", raced, err)
	}
	combined := prepare(first, second)
	if combined.ForService(first.ID)["frontend"] != "10.40.10.5" ||
		combined.ForService(second.ID)["frontend"] != "10.40.10.4" {
		t.Fatal("repeat allocation moved or duplicated address")
	}
	if result, err := store.Transact(ctx, combined.Conditions(), combined.Mutations()); err != nil ||
		!result.Succeeded {
		t.Fatalf("commit combined = %#v, %v", result, err)
	}
	release, err := testnetworkreservations.PrepareProxyAddressRelease(ctx, store, first.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer release.Clear()
	if result, err := store.Transact(ctx, release.Conditions(), release.Mutations()); err != nil || !result.Succeeded {
		t.Fatalf("release = %#v, %v", result, err)
	}
	retained, err := testnetworkreservations.GetComponentAddressRegistry(ctx, store, zone)
	if err != nil || retained.Record.ServiceReservations[first.ID] != "" ||
		retained.Record.ServiceReservations[second.ID] != "10.40.10.4" ||
		retained.Record.Reservations[componentID] != "10.40.10.6" {
		t.Fatalf("release changed another owner: %#v, %v", retained, err)
	}
}
