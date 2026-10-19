package etcd

import (
	"context"
	"net/http"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testroutes "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestRouteRepositoryCreatesReadsAndPagesScopedRecords(t *testing.T) {
	// Rationale: one atomic write must publish the Route primary and Environment
	// membership while fencing the exact live target Service.
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, _, target := routeRepositoryTestHierarchy(t)
	records := []testroutes.Record{
		routeRepositoryTestRecord(t, environment.Record.ID, target.Record.Desired.ID, 1010, "/api/*"),
		routeRepositoryTestRecord(t, environment.Record.ID, target.Record.Desired.ID, 1011, "/admin/*"),
	}
	projection := routeRepositoryTestProjection(
		t,
		environment.Record.ID,
		target.Record.Desired.ID,
		ids.NewAt(ids.KindTask, environment.Record.CreatedAt, 1005),
	)
	for _, record := range records {
		var err error
		projection, err = testenvironmentprojection.ApplyEnvironmentRoute(projection, record)
		if err != nil {
			t.Fatalf("ApplyEnvironmentRoute(%s) error = %v", record.Desired.Path, err)
		}
	}
	routeRepositoryTestSelectDesiredHead(t, store, projection)
	duplicateMatch := routeRepositoryTestRecord(t, environment.Record.ID, target.Record.Desired.ID, 1012, "/api/*")
	if _, err := testenvironmentprojection.ApplyEnvironmentRoute(projection, duplicateMatch); !isKind(
		err,
		errs.KindValidationFailed,
	) {
		t.Fatalf("ApplyEnvironmentRoute(duplicate match) error = %v", err)
	}
	stored, err := repository.GetRoute(ctx, records[0].Desired.ID)
	if err != nil || stored.Record != records[0] {
		t.Fatalf("GetRoute() = %#v, %v", stored, err)
	}
	first, err := repository.ListRoutes(ctx, environment.Record.ID, testkeyvalue.PageRequest{Limit: 1})
	if err != nil || len(first.Items) != 1 || first.NextCursor == "" {
		t.Fatalf("ListRoutes(first) = %#v, %v", first, err)
	}
	second, err := repository.ListRoutes(
		ctx,
		environment.Record.ID, testkeyvalue.PageRequest{Limit: 1, Cursor: first.NextCursor},
	)
	if err != nil || len(second.Items) != 1 || second.NextCursor != "" || second.Revision != first.Revision {
		t.Fatalf("ListRoutes(second) = %#v, %v", second, err)
	}
}

func TestRouteRepositoryRejectsMissingOrDeletingTargetService(t *testing.T) {
	// Rationale: required Route validation must stay true across the commit, not
	// merely observe a target Service before a concurrent deletion.
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, project, target := routeRepositoryTestHierarchy(t)
	wrongTarget := target
	wrongTarget.Record.Desired.ID = ids.NewAt(ids.KindService, serviceRecordTestTime(), 1021)
	record := routeRepositoryTestRecord(t, environment.Record.ID, target.Record.Desired.ID, 1020, "/api/*")
	if _, err := repository.CreateRoute(ctx, environment, project, wrongTarget, record); err == nil {
		t.Fatal("CreateRoute() accepted the wrong target Service")
	}
	if _, err := store.Transact(
		ctx,
		[]testkeyvalue.Condition{{Key: testdeletions.TombstoneKey("service", target.Record.Desired.ID)}},
		[]testkeyvalue.Mutation{{
			Type: testkeyvalue.MutationPut, Key: testdeletions.TombstoneKey("service", target.Record.Desired.ID), Value: []byte("fenced"),
		}},
	); err != nil {
		t.Fatalf("install Service fence: %v", err)
	}
	if _, err := repository.CreateRoute(
		ctx,
		environment,
		project,
		target,
		record,
	); !isKind(
		err,
		errs.KindResourceInUse,
	) {
		t.Fatalf("CreateRoute(deleting target) error = %v", err)
	}
}

func TestRouteRepositoryUpdatesExposureByCAS(t *testing.T) {
	// Rationale: direct and Blueprint exposure edits must serialize without
	// changing the typed match or target Service.
	t.Parallel()
	ctx := context.Background()
	repository, _, environment, project, target := routeRepositoryTestHierarchy(t)
	record := routeRepositoryTestRecord(t, environment.Record.ID, target.Record.Desired.ID, 1030, "/api/*")
	current, err := repository.CreateRoute(ctx, environment, project, target, record)
	if err != nil {
		t.Fatalf("CreateRoute() error = %v", err)
	}
	desired := current.Record.Desired
	desired.Exposure = "internal"
	updated, err := repository.ReplaceDesired(ctx, environment, project, target, current, desired)
	if err != nil || updated.Record.Desired.Exposure != "internal" {
		t.Fatalf("ReplaceDesired() = %#v, %v", updated, err)
	}
	desired = updated.Record.Desired
	desired.Exposure = "public"
	updatedAgain, err := repository.ReplaceDesired(ctx, environment, project, target, updated, desired)
	if err != nil || updatedAgain.Record.Desired.Exposure != "public" {
		t.Fatalf("ReplaceDesired(second) = %#v, %v", updatedAgain, err)
	}
	if _, err := repository.ReplaceDesired(
		ctx,
		environment,
		project,
		target,
		current,
		desired,
	); !isKind(
		err,
		errs.KindStateConflict,
	) {
		t.Fatalf("ReplaceDesired(stale) error = %v", err)
	}
}

func routeRepositoryTestHierarchy(
	t *testing.T,
) (
	*RouteRepository,
	*memoryHierarchyStore, testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], testkeyvalue.Versioned[testhierarchy.ProjectRecord], testkeyvalue.Versioned[testservices.ServiceRecord],
) {
	t.Helper()
	store := newMemoryHierarchyStore()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, hierarchy)
	targetID := ids.NewAt(ids.KindService, environment.Record.CreatedAt, 1004)
	targetDesired := core.Service{
		ID: targetID, Name: "api", Image: "app:latest", Strategy: core.StrategyRecreate, Replicas: 1,
	}
	targetRecord, err := testservices.NewServiceRecord(environment.Record.ID, targetDesired, "")
	if err != nil {
		t.Fatalf("NewServiceRecord(target) error = %v", err)
	}
	revisionID := ids.NewAt(ids.KindTask, environment.Record.CreatedAt, 1005)
	projection := routeRepositoryTestProjection(t, environment.Record.ID, targetID, revisionID)
	headValue := routeRepositoryTestStageProjection(t, hierarchy, projection)
	defer clear(headValue)
	configurationValue := stageTestRuntimeConfiguration(t, store, environment.Record.ID, projection.RenderGeneration)
	defer clear(configurationValue)
	runtimeValue, err := testservices.EncodeServiceRuntimeRecord(testservices.NewServiceRuntimeRecord(targetRecord))
	if err != nil {
		t.Fatalf("encodeServiceRuntimeRecord() error = %v", err)
	}
	defer clear(runtimeValue)
	seed, err := store.Transact(context.Background(), []testkeyvalue.Condition{
		{Key: testblueprints.EnvironmentBlueprintHeadKey(environment.Record.ID)},
		{Key: testservices.ServiceRuntimeKey(targetID)},
		{Key: runtimeconfiguration.HeadKey(environment.Record.ID)},
	}, []testkeyvalue.Mutation{
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testblueprints.EnvironmentBlueprintHeadKey(environment.Record.ID),
			Value: headValue,
		},
		{Type: testkeyvalue.MutationPut, Key: testservices.ServiceRuntimeKey(targetID), Value: runtimeValue},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   runtimeconfiguration.HeadKey(environment.Record.ID),
			Value: configurationValue,
		},
	})
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed desired Service projection = %#v, %v", seed, err)
	}
	serviceRepository, err := newServiceRepository(store)
	if err != nil {
		t.Fatalf("newServiceRepository() error = %v", err)
	}
	target, err := serviceRepository.GetService(context.Background(), targetID)
	if err != nil {
		t.Fatalf("GetService(target) error = %v", err)
	}
	repository, err := newRouteRepository(store)
	if err != nil {
		t.Fatalf("newRouteRepository() error = %v", err)
	}
	return repository, store, environment, project, target
}

func routeRepositoryTestProjection(
	t *testing.T,
	environmentID string,
	serviceID string,
	revisionID string,
) testenvironmentprojection.EnvironmentComposeProjection {
	t.Helper()
	service := core.Service{
		ID: serviceID, Name: "api", Image: "app:latest", Strategy: core.StrategyRecreate, Replicas: 1,
	}
	return withTestEnvironmentComposeArtifact(testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: revisionID, RenderGeneration: 1,
		DesiredServices: []testservices.EnvironmentServiceProjection{{
			EnvironmentID: environmentID, Desired: service,
		}},
	})
}

func routeRepositoryTestSelectDesiredHead(
	t *testing.T,
	store *memoryHierarchyStore,
	projection testenvironmentprojection.EnvironmentComposeProjection,
) {
	t.Helper()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	projection.RevisionID = ids.New(ids.KindTask)
	headValue := routeRepositoryTestStageProjection(t, hierarchy, projection)
	defer clear(headValue)
	conditions := make([]testkeyvalue.Condition, 0, len(projection.DesiredRoutes))
	mutations := []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testblueprints.EnvironmentBlueprintHeadKey(projection.EnvironmentID), Value: headValue,
	}}
	for _, desired := range projection.DesiredRoutes {
		observation, observationErr := testroutes.NewObservationRecord(
			desired.EnvironmentID,
			desired.Desired.ID,
			desired.DesiredGeneration, testroutes.Observation{
				Status: testroutes.ObservedUnserved, DesiredGeneration: desired.DesiredGeneration,
			},
		)
		if observationErr != nil {
			t.Fatalf("NewRouteObservationRecord() error = %v", observationErr)
		}
		observationValue, observationErr := testroutes.EncodeObservation(observation)
		if observationErr != nil {
			t.Fatalf("encodeRouteObservation() error = %v", observationErr)
		}
		defer clear(observationValue)
		conditions = append(conditions, testkeyvalue.Condition{Key: testroutes.ObservationKey(desired.Desired.ID)})
		mutations = append(mutations, testkeyvalue.Mutation{
			Type: testkeyvalue.MutationPut, Key: testroutes.ObservationKey(desired.Desired.ID), Value: observationValue,
		})
	}
	result, err := store.Transact(context.Background(), conditions, mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("select desired Environment head = %#v, %v", result, err)
	}
}

// Seed only current desired-state storage. A revision identity is not an
// execution Task, and these repository checks do not simulate execution.
func routeRepositoryTestStageProjection(
	t *testing.T,
	hierarchy *HierarchyRepository,
	projection testenvironmentprojection.EnvironmentComposeProjection,
) []byte {
	t.Helper()
	marker := testDirectMarker()
	marker.TaskID = projection.RevisionID
	marker.Locator = testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeEnvironment, ScopeID: projection.EnvironmentID,
		Method: http.MethodPut, Route: "/environments/{id}/blueprint", Key: projection.RevisionID,
	}
	revision := testblueprints.EnvironmentBlueprintRevision{
		EnvironmentID: projection.EnvironmentID, RevisionID: projection.RevisionID,
		RootPath: "blueprint.yaml", ComposeSources: []string{"blueprint.yaml"},
		Files: []testblueprints.EnvironmentBlueprintFile{
			{Path: "blueprint.yaml", Content: []byte("services: {api: {}}\n")},
		},
		CreatedAt: marker.CreatedAt,
	}
	stageEnvironmentBlueprintForPublicationTest(t, hierarchy, 0, revision, projection, marker)
	seedEffectiveBlueprintFixture(t, hierarchy.store, projection)
	value, err := testidempotency.EncodeTaskReference(projection.RevisionID)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func routeRepositoryTestRecord(
	t *testing.T,
	environmentID string,
	serviceID string,
	offset int64,
	path string,
) testroutes.Record {
	t.Helper()
	record, err := testroutes.NewRecord(environmentID, core.Route{
		ID: ids.NewAt(ids.KindRoute, serviceRecordTestTime(), offset), Host: "app.example.com",
		Path: path, TargetServiceID: serviceID, TargetPort: 8080, Exposure: "public",
	})
	if err != nil {
		t.Fatalf("NewRouteRecord() error = %v", err)
	}
	return record
}
