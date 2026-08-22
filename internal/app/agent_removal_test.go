package app

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

const testAgentRemovalID = "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV"

func TestAgentRemovalServiceCreatesBoundedControllerTask(t *testing.T) {
	t.Parallel()

	targets := &fakeAgentRemovalTargets{health: localagent.Health{
		Agent: localagent.Agent{ID: testAgentRemovalID},
	}}
	tasks := &fakeAgentEnrollmentTasks{}
	idempotency := &fakeAgentRemovalIdempotency{resolution: idempotentintent.Resolution{
		Kind: idempotentintent.ResolutionApplied,
	}}
	service, err := newAgentRemovalService(targets, tasks, idempotency)
	if err != nil {
		t.Fatalf("newAgentRemovalService() error = %v", err)
	}
	now := time.Date(2026, 8, 22, 15, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }
	response, err := service.RemoveAgent(context.Background(), testAgentRemovalID, "agent-remove-key-0001")
	if err != nil {
		t.Fatalf("RemoveAgent() error = %v", err)
	}
	var accepted apiTypes.TaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil || accepted.TaskID != tasks.task.ID {
		t.Fatalf("RemoveAgent() body = %s, %v", response.Body, err)
	}
	if response.Status != http.StatusAccepted || tasks.task.Executor != etcd.TaskExecutorController ||
		tasks.task.Type != etcd.TaskRemove || tasks.task.Target != testAgentRemovalID ||
		tasks.task.TimeoutSeconds != agentRemovalTimeoutSeconds || tasks.task.PlanHash == "" ||
		tasks.task.Params[etcd.TaskResourceKindParam] != etcd.TaskResourceAgent {
		t.Fatalf("created Agent removal Task = %#v, response = %#v", tasks.task, response)
	}
	if targets.healthCalls != 1 || tasks.marker.Locator.Route != agentRemovalRoute ||
		tasks.marker.Locator.Method != http.MethodDelete {
		t.Fatalf("removal target/marker = %d/%#v", targets.healthCalls, tasks.marker)
	}
}

func TestAgentRemovalServiceReplaysBeforeTargetLookup(t *testing.T) {
	t.Parallel()

	want := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: []byte(`{"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`),
	}
	targets := &fakeAgentRemovalTargets{}
	service, err := newAgentRemovalService(
		targets,
		&fakeAgentEnrollmentTasks{},
		&fakeAgentRemovalIdempotency{
			existing: true,
			resolution: idempotentintent.Resolution{
				Kind: idempotentintent.ResolutionReplay, Response: want,
			},
		},
	)
	if err != nil {
		t.Fatalf("newAgentRemovalService() error = %v", err)
	}
	got, err := service.RemoveAgent(context.Background(), testAgentRemovalID, "agent-remove-key-0002")
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("RemoveAgent(replay) = %#v, %v", got, err)
	}
	if targets.healthCalls != 0 {
		t.Fatalf("Health() calls = %d during replay", targets.healthCalls)
	}
}

type fakeAgentRemovalTargets struct {
	health      localagent.Health
	healthErr   error
	healthCalls int
}

func (targets *fakeAgentRemovalTargets) Health(
	context.Context,
	string,
) (localagent.Health, error) {
	targets.healthCalls++
	return targets.health, targets.healthErr
}

type fakeAgentRemovalIdempotency struct {
	existing   bool
	resolution idempotentintent.Resolution
}

func (idempotency *fakeAgentRemovalIdempotency) Prepare(
	context.Context,
	string,
) (agentRemovalEvidence, error) {
	return agentRemovalEvidence{}, nil
}

func (idempotency *fakeAgentRemovalIdempotency) ResolveExisting(
	context.Context,
	etcd.IdempotencyLocator,
	agentRemovalEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotency.resolution, idempotency.existing, nil
}

func (idempotency *fakeAgentRemovalIdempotency) ResolveKnown(
	context.Context,
	agentRemovalEvidence,
	etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}

func (idempotency *fakeAgentRemovalIdempotency) ResolveUnknown(
	context.Context,
	etcd.IdempotencyLocator,
	agentRemovalEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}
