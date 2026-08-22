package apiclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: the human CLI client must consume the generated task.show
// operation and preserve durable identifiers and fixed-revision step state.
func TestShowTaskUsesGeneratedOperation(t *testing.T) {
	t.Parallel()
	taskID := "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.RequestURI() != "/api/v1/tasks/"+taskID {
			t.Errorf("request = %s %s", request.Method, request.URL.RequestURI())
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write(
			[]byte(
				`{"id":"` + taskID + `","operation_id":"op_01ARZ3NDEKTSV4RRFFQ69G5FAV","type":"deploy","target":"svc_01ARZ3NDEKTSV4RRFFQ69G5FAV","status":"running","steps":[{"name":"step_01ARZ3NDEKTSV4RRFFQ69G5FAV","status":"completed"}]}`,
			),
		)
	}))
	defer server.Close()

	task, err := New(server.URL).ShowTask(context.Background(), taskID)
	if err != nil {
		t.Fatalf("ShowTask() error = %v", err)
	}
	if task.ID != taskID || task.Status != apiTypes.TaskRunning || len(task.Steps) != 1 ||
		task.Steps[0].Status != apiTypes.TaskCompleted {
		t.Fatalf("ShowTask() = %#v", task)
	}
}
