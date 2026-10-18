package agentmanagement

import (
	"context"
	"net/http"
	"reflect"
	"testing"
	"time"

	idempotentintent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
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
	service, err := NewUpdateService(targets, tasks, idempotency)
	if err != nil {
		t.Fatalf("NewUpdateService() error = %v", err)
	}
	service.now = func() time.Time { return now }

	response, err := service.UpdateAgent(
		context.Background(),
		testAgentUpdateID,
		testAgentUpdateDesiredImage,
		"agent-update-key-0001",
	)
	if err != nil {
		t.Fatalf("UpdateAgent() error = %v", err)
	}
	if response.Status != http.StatusAccepted || response.ContentKind != "application/json" {
		t.Fatalf("UpdateAgent() response = %#v", response)
	}
	request := replacement(tasks.capture)
	if request.AgentID != testAgentUpdateID || request.PreviousImage != testAgentUpdatePreviousImage ||
		request.DesiredImage != testAgentUpdateDesiredImage || request.StartingGeneration != 7 {
		t.Fatalf("captured update request = %#v", request)
	}
	if tasks.marker.Locator.Method != http.MethodPost ||
		tasks.marker.Locator.Route != agentUpdateRoute ||
		tasks.marker.Locator.Key != "agent-update-key-0001" {
		t.Fatalf("Agent update marker = %#v", tasks.marker)
	}
}

func TestAgentUpdateServiceReplaysBeforeTargetLookup(t *testing.T) {
	t.Parallel()

	want := testidempotency.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: []byte(`{"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`),
	}
	targets := &fakeAgentUpdateTargets{}
	service, err := NewUpdateService(
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
		t.Fatalf("NewUpdateService() error = %v", err)
	}
	got, err := service.UpdateAgent(
		context.Background(),
		testAgentUpdateID,
		testAgentUpdateDesiredImage,
		"agent-update-key-0002",
	)
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
	capture agentoperations.Capture
	marker  testidempotency.IdempotencyMarker
}

func (tasks *fakeAgentUpdateTasks) CreateAgentWork(
	_ context.Context,
	capture agentoperations.Capture,
	marker testidempotency.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	tasks.capture = capture
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
	string,
) (agentUpdateEvidence, error) {
	return agentUpdateEvidence{}, nil
}

func (idempotency *fakeAgentUpdateIdempotency) ResolveExisting(
	context.Context, testidempotency.IdempotencyLocator,

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
	context.Context, testidempotency.IdempotencyLocator,

	agentUpdateEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}
