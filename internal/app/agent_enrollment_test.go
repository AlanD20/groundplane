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

func TestAgentEnrollmentServiceCreatesControllerTaskFromBootstrapInput(t *testing.T) {
	t.Parallel()

	tasks := &fakeAgentEnrollmentTasks{}
	idempotency := &fakeAgentEnrollmentIdempotency{resolution: idempotentintent.Resolution{
		Kind: idempotentintent.ResolutionApplied,
	}}
	service, err := newAgentEnrollmentService(
		testAppAgentImage,
		localagent.Config{
			PullIntervalSeconds: 2, MaxConcurrentTasks: 3,
			Labels: map[string]string{"arch": "arm64"},
		},
		tasks,
		idempotency,
	)
	if err != nil {
		t.Fatalf("newAgentEnrollmentService() error = %v", err)
	}
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }
	response, err := service.EnrollAgent(context.Background(), "agent-enroll-key-0001")
	if err != nil {
		t.Fatalf("EnrollAgent() error = %v", err)
	}
	if response.Status != http.StatusAccepted || response.ContentKind != "application/json" {
		t.Fatalf("EnrollAgent() response = %#v", response)
	}
	var accepted apiTypes.TaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil || accepted.TaskID != tasks.task.ID {
		t.Fatalf("EnrollAgent() body = %s, %v", response.Body, err)
	}
	if tasks.task.Executor != etcd.TaskExecutorController || tasks.task.Type != etcd.TaskCreate ||
		tasks.task.TimeoutSeconds != 120 || tasks.task.Status != etcd.TaskStatusPending ||
		tasks.task.Target == "" || tasks.task.PlanHash == "" ||
		tasks.task.Params[agentTaskImageKey] != testAppAgentImage ||
		tasks.task.Params[agentTaskEnrollmentKey] != tasks.task.ID ||
		tasks.task.Params[agentTaskLabelPrefix+"arch"] != "arm64" {
		t.Fatalf("created Agent enrollment Task = %#v", tasks.task)
	}
	if tasks.marker.TaskID != tasks.task.ID || tasks.marker.Locator.ScopeKind != etcd.IdempotencyScopePlatform ||
		tasks.marker.Locator.ScopeID != "-" || tasks.marker.Locator.Route != agentEnrollmentRoute {
		t.Fatalf("created Agent enrollment marker = %#v", tasks.marker)
	}
	decoded, err := decodeAgentEnrollmentTask(tasks.task)
	if err != nil || decoded.EnrollmentTaskID != tasks.task.ID || decoded.AgentID != tasks.task.Target {
		t.Fatalf("decodeAgentEnrollmentTask() = %#v, %v", decoded, err)
	}
}

func TestAgentEnrollmentServiceReturnsExactReplay(t *testing.T) {
	t.Parallel()

	want := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: []byte(`{"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`),
	}
	service, err := newAgentEnrollmentService(
		testAppAgentImage,
		localagent.Config{PullIntervalSeconds: 2, MaxConcurrentTasks: 3, Labels: map[string]string{}},
		&fakeAgentEnrollmentTasks{},
		&fakeAgentEnrollmentIdempotency{resolution: idempotentintent.Resolution{
			Kind: idempotentintent.ResolutionReplay, Response: want,
		}},
	)
	if err != nil {
		t.Fatalf("newAgentEnrollmentService() error = %v", err)
	}
	got, err := service.EnrollAgent(context.Background(), "agent-enroll-key-0002")
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("EnrollAgent(replay) = %#v, %v", got, err)
	}
}

func TestAgentEnrollmentPlanHashIsStableForLabelOrder(t *testing.T) {
	t.Parallel()

	one, err := agentEnrollmentPlanHash("agt_01ARZ3NDEKTSV4RRFFQ69G5FAV", map[string]string{
		"label:b": "2", "label:a": "1",
	})
	if err != nil {
		t.Fatalf("agentEnrollmentPlanHash(one) error = %v", err)
	}
	two, err := agentEnrollmentPlanHash("agt_01ARZ3NDEKTSV4RRFFQ69G5FAV", map[string]string{
		"label:a": "1", "label:b": "2",
	})
	if err != nil || one != two {
		t.Fatalf("agentEnrollmentPlanHash order = %q/%q, %v", one, two, err)
	}
}

type fakeAgentEnrollmentTasks struct {
	task   etcd.TaskRecord
	marker etcd.IdempotencyMarker
}

func (tasks *fakeAgentEnrollmentTasks) CreateTask(
	_ context.Context,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	tasks.task = task
	tasks.marker = marker
	return etcd.IdempotencyTransactionResult{}, nil
}

type fakeAgentEnrollmentIdempotency struct {
	resolution idempotentintent.Resolution
}

func (idempotency *fakeAgentEnrollmentIdempotency) Prepare(context.Context) (agentEnrollmentEvidence, error) {
	return agentEnrollmentEvidence{}, nil
}

func (idempotency *fakeAgentEnrollmentIdempotency) ResolveKnown(
	context.Context,
	agentEnrollmentEvidence,
	etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}

func (idempotency *fakeAgentEnrollmentIdempotency) ResolveUnknown(
	context.Context,
	etcd.IdempotencyLocator,
	agentEnrollmentEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}
