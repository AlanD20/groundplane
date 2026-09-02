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
	"github.com/AlanD20/groundplane/internal/core"
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
	fenceKey := "/v1/test/route-service-fences/" + target.Record.Desired.ID
	fence, err := store.Transact(ctx, nil, []Mutation{{
		Type: MutationPut, Key: fenceKey, Value: []byte(target.Record.Desired.ID),
	}})
	if err != nil || !fence.Succeeded {
		t.Fatalf("seed Route service fence = %#v, %v", fence, err)
	}
	target.Record.desiredFenceKey = fenceKey
	target.Revision = fence.Revision
	target.ReadRevision = fence.Revision
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
	task.Steps = []TaskStepRecord{{Kind: TaskStepOperation, ID: ids.NewAt(ids.KindStep, createdAt, 1204)}}
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
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	if _, found, err := tasks.ClaimNextControllerTask(ctx, createdAt.Add(time.Second)); err != nil || !found {
		t.Fatalf("ClaimNextControllerTask() found/error = %v/%v", found, err)
	}
	terminalAt := createdAt.Add(2 * time.Second)
	if _, err := tasks.AcknowledgeControllerTask(ctx, task.ID, TaskStatusCompleted, terminalAt); err != nil {
		t.Fatalf("AcknowledgeControllerTask() error = %v", err)
	}
	observationRead, err := store.Get(ctx, routeObservationKey(record.Desired.ID))
	if err != nil || observationRead.Entry == nil {
		t.Fatalf("Get(desired-only observation) = %#v/%v", observationRead, err)
	}
	observation, err := decodeRouteObservation(observationRead.Entry.Value)
	if err != nil || observation.Observation.Status != RouteObservedUnserved {
		t.Fatalf("desired-only observation = %#v/%v", observation, err)
	}
	if _, err := tasks.AcknowledgeControllerTask(ctx, task.ID, TaskStatusCompleted, terminalAt); err != nil {
		t.Fatalf("AcknowledgeControllerTask(replay) error = %v", err)
	}

	current, err := repository.GetRoute(ctx, record.Desired.ID)
	if err != nil {
		t.Fatalf("GetRoute(edit predecessor) error = %v", err)
	}
	edited, err := ReplaceRouteDesired(current.Record, core.Route{
		ID: record.Desired.ID, Host: record.Desired.Host, Path: record.Desired.Path,
		Exposure: "internal", TargetServiceID: record.Desired.TargetServiceID,
		TargetPort: record.Desired.TargetPort,
	})
	if err != nil {
		t.Fatalf("ReplaceRouteDesired() error = %v", err)
	}
	editCreatedAt := terminalAt.Add(time.Second)
	editTask := validTaskRecord(editCreatedAt)
	editTask.ID = ids.NewAt(ids.KindTask, editCreatedAt, 1210)
	editTask.OperationID = ids.NewAt(ids.KindOperation, editCreatedAt, 1211)
	editTask.PlanID = ids.NewAt(ids.KindPlan, editCreatedAt, 1212)
	editTask.Owner = task.Owner
	editTask.Executor = TaskExecutorController
	editTask.Type = TaskUpdate
	editTask.Target = edited.Desired.ID
	editTask.Params = map[string]string{
		TaskResourceKindParam: TaskResourceRoute, TaskRouteEnvironmentParam: environment.Record.ID,
	}
	editTask.Steps = []TaskStepRecord{{Kind: TaskStepOperation, ID: ids.NewAt(ids.KindStep, editCreatedAt, 1213)}}
	editTask.TimeoutSeconds = 30
	editTask.RenderGeneration = int32(edited.DesiredGeneration)
	editTask.PlanHash = strings.Repeat("b", 64)
	editTask.IdempotencyKey = "route-edit-task-key-0001"
	editIntent, err := NewRouteMutationIntent(
		editTask.ID, editTask.OperationID, environment.Record.ID, edited, &current, nil, editCreatedAt,
	)
	if err != nil {
		t.Fatalf("NewRouteMutationIntent(edit) error = %v", err)
	}
	editBody, err := json.Marshal(struct {
		Route struct {
			ID string `json:"id"`
		} `json:"route"`
		TaskID string `json:"task_id"`
	}{Route: struct {
		ID string `json:"id"`
	}{edited.Desired.ID}, TaskID: editTask.ID})
	if err != nil {
		t.Fatalf("marshal edit acceptance: %v", err)
	}
	editMarker, err := NewCompletedDirectIdempotencyMarker(
		IdempotencyLocator{
			ScopeKind: IdempotencyScopeEnvironment, ScopeID: environment.Record.ID,
			Method: http.MethodPatch, Route: "/routes/{id}", Key: editTask.IdempotencyKey,
		},
		testDirectMarker().Intent,
		IdempotencyResponse{Status: http.StatusAccepted, ContentKind: "application/json", Body: editBody},
		editCreatedAt,
	)
	if err != nil {
		t.Fatalf("NewCompletedDirectIdempotencyMarker(edit) error = %v", err)
	}
	if _, err := repository.BeginRouteMutationWithTask(
		ctx, environment, project, target, &current, edited, editIntent, editTask, editMarker,
	); err != nil {
		t.Fatalf("BeginRouteMutationWithTask(edit) error = %v", err)
	}
	if _, found, err := tasks.ClaimNextControllerTask(ctx, editCreatedAt.Add(time.Second)); err != nil || !found {
		t.Fatalf("ClaimNextControllerTask(edit) found/error = %v/%v", found, err)
	}
	editTerminalAt := editCreatedAt.Add(2 * time.Second)
	failed, err := tasks.AcknowledgeControllerTask(ctx, editTask.ID, TaskStatusFailed, editTerminalAt)
	if err != nil || failed.Record.Status != TaskStatusFailed {
		t.Fatalf("AcknowledgeControllerTask(edit failed) = %#v/%v", failed, err)
	}
	if _, err := tasks.AcknowledgeControllerTask(ctx, editTask.ID, TaskStatusFailed, editTerminalAt); err != nil {
		t.Fatalf("AcknowledgeControllerTask(edit failed replay) error = %v", err)
	}
	retryID := ids.NewAt(ids.KindTask, editTerminalAt.Add(time.Second), 1214)
	retryMarker := pendingRetryMarker(
		failed.Record, retryID, editTerminalAt.Add(time.Second), "route-edit-retry-key-0001",
	)
	result, err = tasks.RetryTask(ctx, editTask.ID, retryID, TaskActorOperator, retryMarker)
	if err != nil {
		t.Fatalf("RetryTask(edit) error = %v", err)
	}
	if outcome, _, conflict, classifyErr := result.Classify(); classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("RetryTask(edit) outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}
	if _, found, err := tasks.ClaimNextControllerTask(ctx, editTerminalAt.Add(2*time.Second)); err != nil || !found {
		t.Fatalf("ClaimNextControllerTask(edit retry) found/error = %v/%v", found, err)
	}
	retryTerminalAt := editTerminalAt.Add(3 * time.Second)
	if _, err := tasks.AcknowledgeControllerTask(ctx, retryID, TaskStatusCompleted, retryTerminalAt); err != nil {
		t.Fatalf("AcknowledgeControllerTask(edit retry) error = %v", err)
	}
	if _, err := tasks.AcknowledgeControllerTask(ctx, retryID, TaskStatusCompleted, retryTerminalAt); err != nil {
		t.Fatalf("AcknowledgeControllerTask(edit retry replay) error = %v", err)
	}
}

func routeRepositoryTestProviderPin(serviceID string) *RouteProviderPin {
	componentID := ids.New(ids.KindComponent)
	return &RouteProviderPin{
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
}
