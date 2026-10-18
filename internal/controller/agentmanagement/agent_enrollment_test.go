package agentmanagement

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	idempotentintent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func TestAgentEnrollmentServiceCreatesControllerTaskFromBootstrapInput(t *testing.T) {
	t.Parallel()

	tasks := &fakeAgentEnrollmentTasks{}
	idempotency := &fakeAgentEnrollmentIdempotency{resolution: idempotentintent.Resolution{
		Kind: idempotentintent.ResolutionApplied,
	}}
	service, err := NewEnrollmentService(
		fixedAgentImage(testAppAgentImage),
		localagent.Config{
			PullIntervalSeconds: 2, MaxConcurrentTasks: 3,
			Labels: map[string]string{"arch": "arm64"},
		},
		tasks,
		idempotency,
	)
	if err != nil {
		t.Fatalf("NewEnrollmentService() error = %v", err)
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
	if err := json.Unmarshal(response.Body, &accepted); err != nil || accepted.TaskID != tasks.capture.TaskID {
		t.Fatalf("EnrollAgent() body = %s, %v", response.Body, err)
	}
	if tasks.capture.Image != testAppAgentImage || tasks.capture.Enrollment == nil ||
		tasks.capture.Enrollment.Labels["arch"] != "arm64" {
		t.Fatalf("captured enrollment = %#v", tasks.capture)
	}
	if tasks.marker.TaskID != tasks.capture.TaskID ||
		tasks.marker.Locator.ScopeKind != testidempotency.IdempotencyScopePlatform ||
		tasks.marker.Locator.ScopeID != "-" ||
		tasks.marker.Locator.Route != agentEnrollmentRoute {
		t.Fatalf("created Agent enrollment marker = %#v", tasks.marker)
	}
	request := enrollment(tasks.capture)
	if request.EnrollmentTaskID != tasks.capture.TaskID || request.AgentID != tasks.capture.AgentID {
		t.Fatalf("captured enrollment request = %#v", request)
	}
}

func TestAgentEnrollmentServiceReturnsExactReplay(t *testing.T) {
	t.Parallel()

	want := testidempotency.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: []byte(`{"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`),
	}
	service, err := NewEnrollmentService(
		fixedAgentImage(testAppAgentImage),
		localagent.Config{PullIntervalSeconds: 2, MaxConcurrentTasks: 3, Labels: map[string]string{}},
		&fakeAgentEnrollmentTasks{},
		&fakeAgentEnrollmentIdempotency{resolution: idempotentintent.Resolution{
			Kind: idempotentintent.ResolutionReplay, Response: want,
		}},
	)
	if err != nil {
		t.Fatalf("NewEnrollmentService() error = %v", err)
	}
	got, err := service.EnrollAgent(context.Background(), "agent-enroll-key-0002")
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("EnrollAgent(replay) = %#v, %v", got, err)
	}
}

// Rationale: equivalent label order must not alter captured replay identity,
// while an actual changed label must invalidate that identity.
func TestAgentEnrollmentReferenceBindsLabelsNotMapOrder(t *testing.T) {
	at := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	capture := agentoperations.Capture{TaskID: ids.New(ids.KindTask), WorkItemID: ids.New(ids.KindWorkItem),
		PlanID: ids.New(ids.KindPlan), AgentID: ids.New(ids.KindAgent), Action: workexecution.ActionCreate,
		Image: testAppAgentImage, CreatedAt: at, Enrollment: &agentoperations.Enrollment{
			PullIntervalSeconds: 2, MaxConcurrentTasks: 3, Labels: map[string]string{"b": "2", "a": "1"},
		}}
	first, err := capture.Reference()
	if err != nil {
		t.Fatal(err)
	}
	capture.Enrollment.Labels = map[string]string{"a": "1", "b": "2"}
	reordered, err := capture.Reference()
	if err != nil || reordered != first {
		t.Fatalf("reordered labels changed reference: %v", err)
	}
	capture.Enrollment.Labels["a"] = "changed"
	changed, err := capture.Reference()
	if err != nil || changed == first {
		t.Fatalf("changed label did not alter reference: %v", err)
	}
}

type fakeAgentEnrollmentTasks struct {
	capture agentoperations.Capture
	marker  testidempotency.IdempotencyMarker
}

func (tasks *fakeAgentEnrollmentTasks) CreateAgentWork(
	_ context.Context,
	capture agentoperations.Capture,
	marker testidempotency.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	tasks.capture = capture
	tasks.marker = marker
	return etcd.IdempotencyTransactionResult{}, nil
}

type fakeAgentEnrollmentIdempotency struct {
	resolution idempotentintent.Resolution
}

func (idempotency *fakeAgentEnrollmentIdempotency) Prepare(context.Context) (agentEnrollmentEvidence, error) {
	return agentEnrollmentEvidence{}, nil
}

func (idempotency *fakeAgentEnrollmentIdempotency) ResolveExisting(
	context.Context, testidempotency.IdempotencyLocator, agentEnrollmentEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotency.resolution, idempotency.resolution.Kind == idempotentintent.ResolutionReplay, nil
}

type fixedAgentImage string

func (image fixedAgentImage) DesiredAgentImage(context.Context) (string, error) {
	return string(image), nil
}

func (idempotency *fakeAgentEnrollmentIdempotency) ResolveKnown(
	context.Context,
	agentEnrollmentEvidence,
	etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}

func (idempotency *fakeAgentEnrollmentIdempotency) ResolveUnknown(
	context.Context, testidempotency.IdempotencyLocator,

	agentEnrollmentEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}
