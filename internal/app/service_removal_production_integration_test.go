package app

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"

	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	serviceoperations "github.com/AlanD20/groundplane/internal/controller/services"
	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	api "github.com/AlanD20/groundplane/pkg/api"
)

// A Service created but never deployed must be removable through the actual
// mutation and storage path, without inventing an acknowledged runtime.
func TestServiceRemovalBeforeFirstDeploy(t *testing.T) {
	fixture := newVolumeRemovalProductionFixture(t)
	records, err := etcd.NewServiceRepository(fixture.Store)
	if err != nil {
		t.Fatal(err)
	}
	zones, err := etcd.NewZoneRepository(fixture.Store)
	if err != nil {
		t.Fatal(err)
	}
	releases, err := etcd.NewReleaseLedger(fixture.Store, fixture.Tasks)
	if err != nil {
		t.Fatal(err)
	}
	desired, err := desiredrevision.NewRepository(fixture.Store)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := serviceoperations.NewMutationRepository(
		fixture.Blueprint.HierarchyRepository,
		records,
		zones,
		desired,
		releases,
	)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := testtaskplanning.NewTaskPlanResolverWithBlueprints(
		"/var/lib/groundplane/vol",
		fixture.Blueprint,
		nil,
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
	mutations, err := newControllerServiceMutations(repository, resolver, nil, coordinator, fixture.Idempotency)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	created, err := mutations.CreateService(
		ctx,
		api.ServiceCreate{
			EnvironmentID: fixture.EnvironmentID,
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
	removed, err := mutations.RemoveService(ctx, service.ID, "018f3111-0000-7000-8000-000000000022")
	if err != nil || removed.Status != http.StatusAccepted {
		t.Fatalf("remove undeployed Service: status=%d error=%v", removed.Status, err)
	}
	var accepted api.TaskAccepted
	if err := json.Unmarshal(removed.Body, &accepted); err != nil {
		t.Fatal(err)
	}
	task, err := fixture.Tasks.GetTask(ctx, accepted.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	agentID := ids.New(ids.KindAgent)
	claim, found, err := fixture.Tasks.ClaimNextTask(ctx, agentID, 1, task.Record.CreatedAt.Add(time.Millisecond))
	if err != nil || !found || claim.Task.Record.ID != task.Record.ID {
		t.Fatalf("claim removal: %v", err)
	}
	_, err = fixture.Tasks.AcknowledgeTask(ctx, agentID, 1, task.Record.ID, claim.Assignment.Record.AssignmentID,
		testtaskjournal.TaskStatusCompleted, testtaskjournal.TaskResultRecord{Kind: testtaskjournal.TaskResultCompose,
			ExecutionEpoch: 1, Diagnostic: testtaskjournal.TaskResultDiagnosticNone}, task.Record.CreatedAt.Add(time.Second))
	if err != nil {
		t.Fatalf("acknowledge Service removal: %v", err)
	}
	if _, err := records.GetService(ctx, service.ID); err == nil {
		t.Fatal("removed Service still visible")
	}
	if _, found, err := fixture.Blueprint.GetEnvironmentAppliedComposeProjection(ctx, fixture.EnvironmentID); err != nil ||
		found {
		t.Fatalf("removal invented applied runtime: found=%t error=%v", found, err)
	}
	replay, err := mutations.RemoveService(ctx, service.ID, "018f3111-0000-7000-8000-000000000022")
	if err != nil || string(replay.Body) != string(removed.Body) {
		t.Fatalf("removal replay changed: %v", err)
	}
}
