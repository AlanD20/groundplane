package app

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
)

func routeRepositoryTestHierarchy(
	t *testing.T,
) (
	*etcd.RouteRepository,
	*memoryHierarchyStore, testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], testkeyvalue.Versioned[testhierarchy.ProjectRecord], testkeyvalue.Versioned[testservices.ServiceRecord],
) {
	t.Helper()
	store := newMemoryHierarchyStore()
	hierarchy, err := etcd.NewHierarchyRepository(store)
	if err != nil {
		t.Fatalf("etcd.NewHierarchyRepository() error = %v", err)
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
	task := environmentBlueprintTestTask(t, project.Record, environment.Record, 1005)
	projection := routeRepositoryTestProjection(t, environment.Record.ID, targetID, task.ID)
	projection.RevisionID = task.ID
	revision := environmentBlueprintTestRevision(environment.Record.ID, task, "services: {api: {}}\n")
	marker := environmentBlueprintTestMarker(task, environment.Record.ID)
	stageEnvironmentBlueprintForPublicationTest(t, store, 0, revision, projection, marker)
	headValue, err := testidempotency.EncodeTaskReference(task.ID)
	if err != nil {
		t.Fatalf("encodeTaskReference() error = %v", err)
	}
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
		{Key: "/v1/runtime/environment-configurations/" + environment.Record.ID},
	}, []testkeyvalue.Mutation{
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testblueprints.EnvironmentBlueprintHeadKey(environment.Record.ID),
			Value: headValue,
		},
		{Type: testkeyvalue.MutationPut, Key: testservices.ServiceRuntimeKey(targetID), Value: runtimeValue},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   "/v1/runtime/environment-configurations/" + environment.Record.ID,
			Value: configurationValue,
		},
	})
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed desired Service projection = %#v, %v", seed, err)
	}
	serviceRepository, err := etcd.NewServiceRepository(store)
	if err != nil {
		t.Fatalf("etcd.NewServiceRepository() error = %v", err)
	}
	target, err := serviceRepository.GetService(context.Background(), targetID)
	if err != nil {
		t.Fatalf("GetService(target) error = %v", err)
	}
	repository, err := etcd.NewRouteRepository(store)
	if err != nil {
		t.Fatalf("etcd.NewRouteRepository() error = %v", err)
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
