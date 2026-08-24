package apiclient

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func writeTaskClientTestResponse(t *testing.T, writer io.Writer, response []byte) {
	t.Helper()
	if _, err := writer.Write(response); err != nil {
		t.Errorf("write HTTP test response: %v", err)
	}
}

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
		writeTaskClientTestResponse(t, writer,
			[]byte(
				`{"id":"`+taskID+`","operation_id":"op_01ARZ3NDEKTSV4RRFFQ69G5FAV",`+
					`"type":"deploy","target":"svc_01ARZ3NDEKTSV4RRFFQ69G5FAV","status":"running",`+
					`"steps":[{"name":"step_01ARZ3NDEKTSV4RRFFQ69G5FAV","status":"completed"}]}`,
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

// Rationale: Task and Activity must carry the exact same generated scope,
// cursor, public ownership, actor, and timestamp projection.
func TestTaskJournalListsUseGeneratedScopedOperations(t *testing.T) {
	t.Parallel()
	const (
		taskID        = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		tenantID      = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		projectID     = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	)
	tests := []struct {
		name string
		path string
		list func(*Client) (apiTypes.Page[apiTypes.Task], error)
	}{
		{
			name: "tasks", path: "/api/v1/tasks",
			list: func(client *Client) (apiTypes.Page[apiTypes.Task], error) {
				return client.ListTasks(context.Background(), TaskListOptions{
					Limit: 25, Cursor: "shared-cursor", Environment: environmentID,
				})
			},
		},
		{
			name: "activity", path: "/api/v1/activity",
			list: func(client *Client) (apiTypes.Page[apiTypes.Task], error) {
				return client.ListActivity(context.Background(), TaskListOptions{
					Limit: 25, Cursor: "shared-cursor", Environment: environmentID,
				})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				query := request.URL.Query()
				if request.Method != http.MethodGet || request.URL.Path != test.path ||
					query.Get("environment") != environmentID || query.Get("workspace") != "" ||
					query.Get("limit") != "25" || query.Get("cursor") != "shared-cursor" {
					t.Errorf("request = %s %s", request.Method, request.URL.RequestURI())
				}
				writer.Header().Set("Content-Type", "application/json")
				writeTaskClientTestResponse(t, writer, []byte(`{"items":[{"id":"`+taskID+
					`","operation_id":"op_01ARZ3NDEKTSV4RRFFQ69G5FAV","type":"deploy",`+
					`"target":"svc_01ARZ3NDEKTSV4RRFFQ69G5FAV","status":"completed",`+
					`"workspace_type":"tenant","tenant_id":"`+tenantID+
					`","project_id":"`+projectID+`","environment_id":"`+environmentID+
					`","actor":"operator","created_at":"2026-08-24T10:00:00Z",`+
					`"updated_at":"2026-08-24T10:02:00Z","started_at":"2026-08-24T10:01:00Z",`+
					`"finished_at":"2026-08-24T10:02:00Z"}],"next_cursor":"next"}`))
			}))
			defer server.Close()

			page, err := test.list(New(server.URL))
			if err != nil {
				t.Fatalf("list journal: %v", err)
			}
			if len(page.Items) != 1 || page.NextCursor != "next" {
				t.Fatalf("journal page = %#v", page)
			}
			task := page.Items[0]
			if task.ID != taskID || task.WorkspaceType != apiTypes.TaskWorkspaceTenant ||
				task.TenantID != tenantID || task.ProjectID != projectID || task.EnvironmentID != environmentID ||
				task.Actor != apiTypes.TaskActorOperator || task.CreatedAt.IsZero() || task.UpdatedAt.IsZero() ||
				task.StartedAt == nil || task.FinishedAt == nil {
				t.Fatalf("public Task = %#v", task)
			}
		})
	}
}

// Rationale: an invalid dual-scope query must fail before any generated HTTP
// request can escape the CLI boundary.
func TestTaskJournalListRejectsDualScope(t *testing.T) {
	t.Parallel()
	_, err := taskListParams(TaskListOptions{Environment: "env_id", Workspace: "platform"})
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindValidationFailed {
		t.Fatalf("taskListParams() error = %v", err)
	}
}
