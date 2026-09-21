package etcd

import (
	"context"
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testcomponentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testnetworkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testroutes "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	testzones "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func environmentBlueprintTestZoneChanges(
	t *testing.T,
	repository *HierarchyRepository,
	projection testenvironmentprojection.EnvironmentComposeProjection,
) []testblueprints.EnvironmentBlueprintZoneChange {
	t.Helper()
	zones, err := newZoneRepository(repository.store)
	if err != nil {
		t.Fatalf("newZoneRepository() error = %v", err)
	}
	desired := projection.DesiredZones[0].Desired
	current, err := zones.GetZone(context.Background(), desired.ID)
	if err == nil {
		currentCopy := current
		return []testblueprints.EnvironmentBlueprintZoneChange{{Current: &currentCopy, Record: current.Record}}
	}
	if !isKind(err, errs.KindZoneNotFound) {
		t.Fatalf("GetZone() error = %v", err)
	}
	record, recordErr := testzones.NewRecord(projection.EnvironmentID, desired)
	if recordErr != nil {
		t.Fatalf("NewZoneRecord() error = %v", recordErr)
	}
	return []testblueprints.EnvironmentBlueprintZoneChange{{Record: record}}
}

func assertEnvironmentBlueprintTopologyAuthority(
	t *testing.T,
	store *memoryHierarchyStore,
	projection testenvironmentprojection.EnvironmentComposeProjection,
) {
	t.Helper()
	want := make(map[string]string, len(projection.DesiredZones))
	for _, zone := range projection.DesiredZones {
		want[zone.Desired.ID] = zone.Desired.Subnet
	}
	result, err := store.Get(
		context.Background(),
		testnetworkreservations.ZonePoolRegistryKey(projection.EnvironmentID),
	)
	if err != nil || result.Entry == nil {
		t.Fatalf("Zone pool registry = %#v, %v", result, err)
	}
	registry, err := testrecordcodec.Decode[testnetworkreservations.ZonePoolRegistry](
		result.Entry.Value,
		"zone_pool_registry",
	)
	if err != nil || !reflect.DeepEqual(registry.Reservations, want) {
		t.Fatalf("Zone pool reservations = %#v, %v; want %#v", registry.Reservations, err, want)
	}

	keys := make([]string, 0, len(projection.DesiredZones)+len(projection.DesiredRoutes)*4)
	for _, zone := range projection.DesiredZones {
		keys = append(keys, testdeletions.TombstoneKey("zone", zone.Desired.ID))
	}
	for _, route := range projection.DesiredRoutes {
		keys = append(
			keys,
			testroutes.RecordKey(route.Desired.ID),
			testroutes.OwnerKey(projection.EnvironmentID, route.Desired.ID),
			testroutes.MatchKey(projection.EnvironmentID, route.Desired.Host, route.Desired.Path),
			testdeletions.TombstoneKey("route", route.Desired.ID),
		)
	}
	for _, key := range keys {
		result, err := store.Get(context.Background(), key)
		if err != nil || result.Entry != nil {
			t.Fatalf("topology key %q = %#v, %v", key, result, err)
		}
	}
}

func TestEnvironmentBlueprintZonePoolPublicationReplacesExactCandidateSet(t *testing.T) {
	// Rationale: the candidate desired projection is the complete Zone subnet
	// authority; reservations absent from it must not survive publication.
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, repository)
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

	task := environmentBlueprintTestTask(t, project.Record, environment.Record, 805)
	projection := environmentBlueprintTestProjection(environment.Record.ID, task, 1)
	result := publishEnvironmentBlueprintTestRevision(
		t,
		repository,
		project,
		environment,
		0,
		environmentBlueprintTestRevision(environment.Record.ID, task, "services: {}\n"),
		projection,
		environmentBlueprintTestZoneChanges(t, repository, projection),
		environmentBlueprintTestServiceChanges(t, repository, projection),
		environmentBlueprintTestRouteChanges(
			t,
			repository,
			projection,
		),
		testcomponentplanning.ComponentTaskPreparation{},
		task,
		environmentBlueprintTestMarker(task, environment.Record.ID),
	)
	if outcome, _, conflict, classifyErr := result.Classify(); classifyErr != nil || conflict != nil ||
		outcome != IdempotencyKnownApplied {
		t.Fatalf("publication outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}
	assertEnvironmentBlueprintTopologyAuthority(t, store, projection)
}

func TestEnvironmentBlueprintZonePreparationRejectsOverlappingSubnets(t *testing.T) {
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
