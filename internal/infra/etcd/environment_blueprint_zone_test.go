package etcd

import (
	"context"
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testnetworkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testzones "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
)

func TestZonePoolPreparationRetainsReservationsUntilProtectedRemoval(t *testing.T) {
	// Rationale: a new desired input cannot release an old native bridge's
	// reservation before protected removal proves its absence.
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	_, environment := createEnvironmentBlueprintOwners(t, repository)
	staleID := ids.NewAt(ids.KindNetwork, serviceRecordTestTime(), 800)
	staleValue, err := testrecordcodec.Encode(
		"zone_pool_registry",
		testnetworkreservations.ZonePoolRegistry{Reservations: map[string]string{
			staleID: "10.40.20.0/24",
		}},
	)
	if err != nil {
		t.Fatalf("encodeEnvelope() error = %v", err)
	}
	if _, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testnetworkreservations.ZonePoolRegistryKey(environment.Record.ID), Value: staleValue,
	}}); err != nil {
		t.Fatalf("seed Zone pool registry: %v", err)
	}

	desired, err := testzones.NewRecord(environment.Record.ID, core.Zone{
		ID: ids.New(ids.KindNetwork), Name: "new-zone", Subnet: "10.40.30.0/24",
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environment.Record.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := testnetworkreservations.NewPlanner(store).PrepareZonePoolAtRevision(ctx,
		environment.Record, []testzones.Record{desired}, store.revision)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(prepared.Value())
	key := testnetworkreservations.ZonePoolRegistryKey(environment.Record.ID)
	result, err := store.Transact(ctx,
		[]testkeyvalue.Condition{{Key: key, ModRevision: prepared.CurrentRevision()}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: key, Value: prepared.Value()}})
	if err != nil || !result.Succeeded {
		t.Fatalf("Zone pool reservation = %t, %v", result.Succeeded, err)
	}
	current, err := store.Get(ctx, key)
	if err != nil || current.Entry == nil {
		t.Fatalf("Zone pool = %#v, %v", current, err)
	}
	registry, err := testrecordcodec.Decode[testnetworkreservations.ZonePoolRegistry](
		current.Entry.Value,
		"zone_pool_registry",
	)
	want := map[string]string{staleID: "10.40.20.0/24", desired.Desired.ID: "10.40.30.0/24"}
	if err != nil || !reflect.DeepEqual(registry.Reservations, want) {
		t.Fatalf("Zone reservation changed before protected removal: %#v, %v", registry.Reservations, err)
	}

}

func TestZonePoolPreparationRejectsOverlappingSubnets(t *testing.T) {
	// Rationale: a Blueprint transaction must reject sibling overlap before it
	// can publish any partial Zone, Service, Route, Task, or head state.
	t.Parallel()
	repository, err := newHierarchyRepository(newMemoryHierarchyStore())
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	_, environment := createEnvironmentBlueprintOwners(t, repository)
	projection := testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environment.Record.ID,
		DesiredZones: []testenvironmentprojection.EnvironmentZoneProjection{
			{
				EnvironmentID: environment.Record.ID,
				Desired: core.Zone{
					ID: ids.NewAt(ids.KindNetwork, serviceRecordTestTime(), 801), Name: "one",
					Subnet: "10.40.10.0/24", OwnerKind: core.ZoneOwnerEnvironment,
					OwnerID: environment.Record.ID,
				},
			},
			{
				EnvironmentID: environment.Record.ID,
				Desired: core.Zone{
					ID: ids.NewAt(ids.KindNetwork, serviceRecordTestTime(), 802), Name: "two",
					Subnet: "10.40.10.128/25", OwnerKind: core.ZoneOwnerEnvironment,
					OwnerID: environment.Record.ID,
				},
			},
		},
	}
	desired := make([]testzones.Record, len(projection.DesiredZones))
	for index, projection := range projection.DesiredZones {
		desired[index] = testzones.Record(projection)
	}
	if _, err := testnetworkreservations.NewPlanner(repository.store).PrepareZonePoolAtRevision(
		context.Background(), environment.Record, desired, environment.ReadRevision,
	); err == nil {
		t.Fatal("PrepareZonePoolAtRevision() accepted overlapping subnets")
	}
}
