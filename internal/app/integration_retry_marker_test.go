package app

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
)

func createLifecycleTask(t *testing.T, repository *etcd.TaskRepository, task etcd.TaskRecord) {
	t.Helper()
	result, err := repository.CreateTask(context.Background(), task, pendingTaskMarker(task))
	if err != nil {
		t.Fatalf("CreateTask(%s) error = %v", task.ID, err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != etcd.IdempotencyKnownApplied {
		t.Fatalf(
			"CreateTask(%s) outcome/conflict/error = %v/%v/%v",
			task.ID,
			outcome,
			conflict,
			classifyErr,
		)
	}
}

func pendingRetryMarker(
	source etcd.TaskRecord,
	retryID string,
	createdAt time.Time,
	key string,
) testidempotency.IdempotencyMarker {
	marker := pendingTaskMarker(source)
	marker.Locator.Method = http.MethodPost
	marker.Locator.Route = "/tasks/{id}/retry"
	marker.Locator.Key = key
	marker.TaskID = retryID
	marker.CreatedAt = createdAt
	marker.UpdatedAt = createdAt
	marker.Response.Body, _ = json.Marshal(struct {
		TaskID string `json:"task_id"`
	}{TaskID: retryID})
	return marker
}
