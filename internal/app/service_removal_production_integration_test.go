package app

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/serviceremoval"
	serviceoperations "github.com/AlanD20/groundplane/internal/controller/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalwork"
	api "github.com/AlanD20/groundplane/pkg/api"
)

// A Service created but never deployed must be removable through the actual
// mutation and storage path, without inventing an acknowledged runtime.
func TestServiceRemovalBeforeFirstDeploy(t *testing.T) {
	fixture := newNativeVolumeFixture(t)
	records, err := etcd.NewServiceRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	zones, err := etcd.NewZoneRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := etcd.NewTaskRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	releases, err := etcd.NewReleaseLedger(fixture.store, tasks)
	if err != nil {
		t.Fatal(err)
	}
	desired, err := desiredrevision.NewRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := serviceoperations.NewMutationRepository(
		fixture.blueprint.HierarchyRepository,
		records,
		zones,
		desired,
		releases,
	)
	if err != nil {
		t.Fatal(err)
	}
	protector, ciphertext := manualJourneyEncryptedValue(t, "service-removal-intent")
	clear(ciphertext)
	coordinator, err := requestidempotency.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	replays, err := etcd.NewIdempotencyRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	mutations, err := newControllerServiceMutations(repository, nil, nil, coordinator, replays)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	created, err := mutations.CreateService(
		ctx,
		api.ServiceCreate{
			EnvironmentID: fixture.environmentID,
			Name:          "routing",
			Image:         "nginx:latest",
			Strategy:      "recreate",
			OnFailure:     "switch_back",
			Replicas:      1,
		},
		"018f3111-0000-7000-8000-000000000021",
	)
	if err != nil || created.Status != http.StatusCreated {
		t.Fatalf("create Service: status=%d error=%v", created.Status, err)
	}
	var service api.Service
	if err := json.Unmarshal(created.Body, &service); err != nil {
		t.Fatal(err)
	}
	// Direct dependency edits must survive the complete storage publication,
	// not just the mutation service's in-memory projection.
	prerequisite, err := mutations.CreateService(ctx, api.ServiceCreate{
		EnvironmentID: fixture.environmentID, Name: "prerequisite", Image: "nginx:latest",
		Strategy: "recreate", OnFailure: "switch_back", Replicas: 1,
	}, "018f3111-0000-7000-8000-000000000023")
	if err != nil || prerequisite.Status != http.StatusCreated {
		t.Fatalf("create prerequisite: %v", err)
	}
	dependencies := map[string]api.ServiceDependency{
		"prerequisite": {Condition: "service_started", Phases: []string{"deploy"}},
	}
	edited, err := mutations.EditService(ctx, service.ID, api.ServiceEdit{
		Image: service.Image, Strategy: service.Strategy, OnFailure: service.OnFailure,
		Replicas: service.Replicas, DependsOn: &dependencies,
	}, "018f3111-0000-7000-8000-000000000024")
	if err != nil || edited.Status != http.StatusOK {
		t.Fatalf("edit dependency before first Deploy: %v", err)
	}
	if err := json.Unmarshal(edited.Body, &service); err != nil {
		t.Fatal(err)
	}
	if dependency := service.DependsOn["prerequisite"]; dependency.Condition != "service_started" ||
		len(dependency.Phases) != 1 || dependency.Phases[0] != "deploy" {
		t.Fatalf("dependency response lost authored settings: %#v", service.DependsOn)
	}
	dependencies = map[string]api.ServiceDependency{}
	cleared, err := mutations.EditService(ctx, service.ID, api.ServiceEdit{
		Image: service.Image, Strategy: service.Strategy, OnFailure: service.OnFailure,
		Replicas: service.Replicas, DependsOn: &dependencies,
	}, "018f3111-0000-7000-8000-000000000025")
	if err != nil || cleared.Status != http.StatusOK {
		t.Fatalf("clear dependency before first Deploy: %v", err)
	}
	service = api.Service{}
	if err := json.Unmarshal(cleared.Body, &service); err != nil || len(service.DependsOn) != 0 {
		t.Fatalf("dependency reset did not clear desired state: %v %#v", err, service.DependsOn)
	}
	removed, err := mutations.RemoveService(ctx, service.ID, "018f3111-0000-7000-8000-000000000022")
	if err != nil || removed.Status != http.StatusAccepted {
		t.Fatalf("remove undeployed Service: status=%d error=%v", removed.Status, err)
	}
	var accepted api.TaskAccepted
	if err := json.Unmarshal(removed.Body, &accepted); err != nil {
		t.Fatal(err)
	}
	page, err := fixture.queue.ListWork(ctx, accepted.TaskID, "", 10, 0)
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("Service removal work: %v", err)
	}
	view, err := fixture.queue.ReadWork(ctx, accepted.TaskID, page.Items[0].Item.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if view.Item.Item.Placement.Executor != workexecution.Controller || view.Item.Item.AttemptNumber != 0 {
		t.Fatal("never-deployed Service removal invented host execution")
	}
	change, err := records.PrepareServiceRemovalDecision(ctx, view, time.Now().UTC())
	if err != nil {
		t.Fatalf("prepare native Service removal: %v", err)
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	fixture.commit(t, change)
	inputs, err := serviceremovalwork.NewRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := serviceremoval.NewAgentWork(inputs, fixture.queue, records)
	if err != nil {
		t.Fatal(err)
	}
	finished, err := fixture.queue.ReadFinishedWork(ctx, accepted.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	proof, err := owner.PrepareFinish(ctx, finished, at)
	if err != nil {
		t.Fatal(err)
	}
	finish, err := fixture.queue.PrepareTaskFinish(ctx, finished.Task, at, at.Add(workexecution.TaskHistoryRetention))
	if err != nil {
		t.Fatal(err)
	}
	defer keyvalue.ClearMutationValues(finish.Mutations)
	fixture.commit(t, proof, finish)
	if _, err := records.GetService(ctx, service.ID); err == nil {
		t.Fatal("removed Service still visible")
	}
	if _, found, err := fixture.blueprint.GetEnvironmentAppliedComposeProjection(ctx, fixture.environmentID); err != nil ||
		found {
		t.Fatalf("removal invented applied runtime: found=%t error=%v", found, err)
	}
	replay, err := mutations.RemoveService(ctx, service.ID, "018f3111-0000-7000-8000-000000000022")
	if err != nil || string(replay.Body) != string(removed.Body) {
		t.Fatalf("removal replay changed: %v", err)
	}
}
