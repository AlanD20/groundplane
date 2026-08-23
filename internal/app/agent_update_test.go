package app

import (
	"context"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

const (
	testAgentUpdateID            = "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testAgentUpdatePreviousImage = "ghcr.io/aland20/groundplane-agent@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testAgentUpdateDesiredImage  = "ghcr.io/aland20/groundplane-agent@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func TestAgentUpdateServiceCreatesPinnedControllerTask(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 23, 10, 0, 0, 0, time.UTC)
	targets := &fakeAgentUpdateTargets{health: localagent.Health{Agent: localagent.Agent{
		ID: testAgentUpdateID, Image: testAgentUpdatePreviousImage,
		Generation: 7, Phase: localagent.PhaseReady,
	}}}
	tasks := &fakeAgentUpdateTasks{}
	idempotency := &fakeAgentUpdateIdempotency{
		resolution: idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied},
	}
	service, err := newAgentUpdateService(testAgentUpdateDesiredImage, targets, tasks, idempotency)
	if err != nil {
		t.Fatalf("newAgentUpdateService() error = %v", err)
	}
	service.now = func() time.Time { return now }

	response, err := service.UpdateAgent(context.Background(), testAgentUpdateID, "agent-update-key-0001")
	if err != nil {
		t.Fatalf("UpdateAgent() error = %v", err)
	}
	if response.Status != http.StatusAccepted || response.ContentKind != "application/json" {
		t.Fatalf("UpdateAgent() response = %#v", response)
	}
	if tasks.task.Executor != etcd.TaskExecutorController || tasks.task.Type != etcd.TaskUpdate ||
		tasks.task.Target != testAgentUpdateID || tasks.task.TimeoutSeconds != 300 ||
		tasks.task.Params[etcd.TaskResourceKindParam] != etcd.TaskResourceAgent ||
		tasks.task.Params[agentTaskPreviousImageKey] != testAgentUpdatePreviousImage ||
		tasks.task.Params[agentTaskImageKey] != testAgentUpdateDesiredImage ||
		tasks.task.Params[agentTaskStartingGenerationKey] != "7" {
		t.Fatalf("Agent update Task = %#v", tasks.task)
	}
	if tasks.marker.Locator.Method != http.MethodPost ||
		tasks.marker.Locator.Route != agentUpdateRoute ||
		tasks.marker.Locator.Key != "agent-update-key-0001" {
		t.Fatalf("Agent update marker = %#v", tasks.marker)
	}
}

func TestAgentUpdateServiceReplaysBeforeTargetLookup(t *testing.T) {
	t.Parallel()

	want := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: []byte(`{"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`),
	}
	targets := &fakeAgentUpdateTargets{}
	service, err := newAgentUpdateService(
		testAgentUpdateDesiredImage,
		targets,
		&fakeAgentUpdateTasks{},
		&fakeAgentUpdateIdempotency{
			existing: true,
			resolution: idempotentintent.Resolution{
				Kind: idempotentintent.ResolutionReplay, Response: want,
			},
		},
	)
	if err != nil {
		t.Fatalf("newAgentUpdateService() error = %v", err)
	}
	got, err := service.UpdateAgent(context.Background(), testAgentUpdateID, "agent-update-key-0002")
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("UpdateAgent(replay) = %#v, %v", got, err)
	}
	if targets.calls != 0 {
		t.Fatalf("Health() calls = %d during replay", targets.calls)
	}
}

type fakeAgentUpdateTargets struct {
	health localagent.Health
	err    error
	calls  int
}

func (targets *fakeAgentUpdateTargets) Health(context.Context, string) (localagent.Health, error) {
	targets.calls++
	return targets.health, targets.err
}

type fakeAgentUpdateTasks struct {
	task   etcd.TaskRecord
	marker etcd.IdempotencyMarker
}

func (tasks *fakeAgentUpdateTasks) CreateTask(
	_ context.Context,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	tasks.task = task
	tasks.marker = marker
	return etcd.IdempotencyTransactionResult{}, nil
}

type fakeAgentUpdateIdempotency struct {
	existing   bool
	resolution idempotentintent.Resolution
}

func (idempotency *fakeAgentUpdateIdempotency) Prepare(
	context.Context,
	string,
) (agentUpdateEvidence, error) {
	return agentUpdateEvidence{}, nil
}

func (idempotency *fakeAgentUpdateIdempotency) ResolveExisting(
	context.Context,
	etcd.IdempotencyLocator,
	agentUpdateEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotency.resolution, idempotency.existing, nil
}

func (idempotency *fakeAgentUpdateIdempotency) ResolveKnown(
	context.Context,
	agentUpdateEvidence,
	etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}

func (idempotency *fakeAgentUpdateIdempotency) ResolveUnknown(
	context.Context,
	etcd.IdempotencyLocator,
	agentUpdateEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}
