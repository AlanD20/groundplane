package etcd

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

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
