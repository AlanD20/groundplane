package etcd

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

// Rationale: a durable provider pin must retain the provider-neutral origin
// exactly and reject any origin that no longer names its managed Service.
func TestRouteProviderPinClonePreservesCanonicalOrigin(t *testing.T) {
	t.Parallel()
	componentID := ids.New(ids.KindComponent)
	serviceID := ids.New(ids.KindService)
	pinned := RouteProviderPin{
		ComponentID: componentID, DefinitionDigest: strings.Repeat("a", 64),
		CatalogDigest: strings.Repeat("b", 64), InputRevision: 7, InputGeneration: 9,
		Destination: "components/router/config", ActionID: "activate-config", ServiceID: serviceID,
		Input: componentsdk.HTTPRouterInput{
			ComponentID: componentID, Enabled: true, GeneratedServiceID: serviceID,
			ZoneID: ids.New(ids.KindNetwork), ZoneName: "frontend", PinnedIPv4: "10.40.0.2",
			Origin: componentsdk.HTTPRouterOrigin{ServiceName: "edge-router", URL: "http://edge-router:8080"},
			Routes: []componentsdk.HTTPRoute{{
				ID: "rte_one", Host: "app.example.com", Path: "/", BackendServiceID: "svc_backend",
				BackendServiceName: "backend", TargetPort: 8080,
				Exposure: componentsdk.HTTPRouteExposurePublic,
			}},
		},
	}
	if err := validateRouteProviderPin(&pinned); err != nil {
		t.Fatalf("validateRouteProviderPin() error = %v", err)
	}
	cloned := cloneRouteProviderPin(pinned)
	cloned.Input.Routes[0].Path = "/changed"
	if pinned.Input.Routes[0].Path != "/" || cloned.Input.Origin != pinned.Input.Origin {
		t.Fatalf("cloneRouteProviderPin() source/clone = %#v / %#v", pinned, cloned)
	}
	changed := pinned
	changed.Input.Origin.URL = "http://another-router:8080"
	if err := validateRouteProviderPin(&changed); err == nil {
		t.Fatal("validateRouteProviderPin() accepted a changed origin host")
	}
}

func TestRouteRepositoryPublishesDesiredMutationAndTaskAtomically(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, project, target := routeRepositoryTestHierarchy(t)
	record := routeRepositoryTestRecord(t, environment.Record.ID, target.Record.Desired.ID, 1200, "/api/*")
	createdAt := serviceRecordTestTime().Add(3 * time.Hour)
	task := validTaskRecord(createdAt)
	task.ID = ids.NewAt(ids.KindTask, createdAt, 1201)
	task.OperationID = ids.NewAt(ids.KindOperation, createdAt, 1202)
	task.PlanID = ids.NewAt(ids.KindPlan, createdAt, 1203)
	task.Owner = mustEnvironmentTaskOwner(t, project.Record, environment.Record)
	task.Executor = TaskExecutorController
	task.Type = TaskCreate
	task.Target = record.Desired.ID
	task.Status = TaskStatusPending
	task.Params = map[string]string{TaskResourceKindParam: TaskResourceRoute, TaskRouteEnvironmentParam: environment.Record.ID}
	task.Steps = []TaskStepRecord{{ID: ids.NewAt(ids.KindStep, createdAt, 1204)}}
	task.TimeoutSeconds = 30
	task.RenderGeneration = 1
	task.PlanHash = strings.Repeat("a", 64)
	task.IdempotencyKey = "route-create-task-key-0001"
	intent, err := NewRouteMutationIntent(task.ID, task.OperationID, environment.Record.ID, record, nil, nil, createdAt)
	if err != nil {
		t.Fatalf("NewRouteMutationIntent() error = %v", err)
	}
	body, err := json.Marshal(struct {
		Route struct {
			ID string `json:"id"`
		} `json:"route"`
		TaskID string `json:"task_id"`
	}{Route: struct {
		ID string `json:"id"`
	}{record.Desired.ID}, TaskID: task.ID})
	if err != nil {
		t.Fatalf("marshal acceptance: %v", err)
	}
	marker, err := NewCompletedDirectIdempotencyMarker(IdempotencyLocator{ScopeKind: IdempotencyScopeEnvironment, ScopeID: environment.Record.ID, Method: http.MethodPost, Route: "/routes", Key: task.IdempotencyKey}, testDirectMarker().Intent, IdempotencyResponse{Status: http.StatusAccepted, ContentKind: "application/json", Body: body}, createdAt)
	if err != nil {
		t.Fatalf("NewCompletedDirectIdempotencyMarker() error = %v", err)
	}
	result, err := repository.BeginRouteMutationWithTask(ctx, environment, project, target, nil, record, intent, task, marker)
	if err != nil {
		t.Fatalf("BeginRouteMutationWithTask() error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("mutation outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}
	stored, err := repository.GetRoute(ctx, record.Desired.ID)
	if err != nil || stored.Record != record {
		t.Fatalf("GetRoute() = %#v, %v", stored, err)
	}
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	storedIntent, found, err := hierarchy.GetRouteMutationIntent(ctx, task.ID)
	if err != nil || !found || storedIntent.Record.TaskID != task.ID {
		t.Fatalf("GetRouteMutationIntent() = %#v/%v/%v", storedIntent, found, err)
	}
	companions, err := store.GetMany(ctx, GetManyRequest{Keys: []string{taskKey(task.ID), taskQueueKey(task.Executor, task.ID), routeMutationIntentKey(task.ID), componentTaskActiveEnvironmentKey(environment.Record.ID)}})
	if err != nil || companions == nil || len(companions.Values) != 4 {
		t.Fatalf("GetMany(Task companions) = %#v, %v", companions, err)
	}
	for index, value := range companions.Values {
		if value == nil {
			t.Fatalf("Task companion %d is missing", index)
		}
	}
}
