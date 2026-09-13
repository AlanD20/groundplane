package apiclient

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
// QA: TASK-01, UI-03/05; local journal requests/projections, not durable history.
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
					`"steps":[`+
					`{"name":"operation-step","status":"completed","kind":"operation"},`+
					`{"name":"script-step","status":"running","kind":"script",`+
					`"script_id":"scr_01ARZ3NDEKTSV4RRFFQ69G5FAV","script_slug":"deploy-script"}]}`,
			),
		)
	}))
	defer server.Close()

	task, err := New(server.URL).ShowTask(context.Background(), taskID)
	if err != nil {
		t.Fatalf("ShowTask() error = %v", err)
	}
	if task.ID != taskID || task.Status != apiTypes.TaskRunning || len(task.Steps) != 2 ||
		task.Steps[0].Kind != apiTypes.TaskStepOperation || task.Steps[0].Status != apiTypes.TaskCompleted ||
		task.Steps[0].ScriptID != "" || task.Steps[0].ScriptSlug != "" ||
		task.Steps[1].Kind != apiTypes.TaskStepScript || task.Steps[1].Status != apiTypes.TaskRunning ||
		task.Steps[1].ScriptID != "scr_01ARZ3NDEKTSV4RRFFQ69G5FAV" ||
		task.Steps[1].ScriptSlug != "deploy-script" {
		t.Fatalf("ShowTask() = %#v", task)
	}
}

// Rationale: Task and Activity must carry the exact same generated scope,
// cursor, public ownership, actor, and timestamp projection.
// QA: TASK-01, UI-03/05; local journal requests/projections, not durable history.
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
				task.Actor != apiTypes.TaskActorOperator ||
				!task.CreatedAt.Equal(time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)) ||
				!task.UpdatedAt.Equal(time.Date(2026, 8, 24, 10, 2, 0, 0, time.UTC)) ||
				task.StartedAt == nil || !task.StartedAt.Equal(time.Date(2026, 8, 24, 10, 1, 0, 0, time.UTC)) ||
				task.FinishedAt == nil || !task.FinishedAt.Equal(time.Date(2026, 8, 24, 10, 2, 0, 0, time.UTC)) {
				t.Fatalf("public Task = %#v", task)
			}
		})
	}
}

// Rationale: an invalid dual-scope query must fail before any generated HTTP
// request can escape the CLI boundary.
// QA: TASK-01, UI-03/05; local journal requests/projections, not durable history.
func TestTaskJournalListRejectsDualScope(t *testing.T) {
	t.Parallel()
	_, err := taskListParams(TaskListOptions{Environment: "env_id", Workspace: "platform"})
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindValidationFailed {
		t.Fatalf("taskListParams() error = %v", err)
	}
}

// QA: TASK-01, UI-03/05; local journal requests/projections, not durable history.
// Rationale: Reject unknown or contradictory step identities instead of displaying trusted progress.
func TestShowTaskRejectsInvalidStepProjection(t *testing.T) {
	t.Parallel()
	const taskID = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const taskPrefix = `{"id":"` + taskID + `","operation_id":"op_01ARZ3NDEKTSV4RRFFQ69G5FAV",` +
		`"type":"deploy","target":"svc_01ARZ3NDEKTSV4RRFFQ69G5FAV","status":"running","steps":[`
	tests := []struct {
		name string
		step string
	}{
		{name: "unknown kind", step: `{"name":"step","status":"running","kind":"future"}`},
		{name: "operation has Script identity", step: `{"name":"step","status":"running","kind":"operation",` +
			`"script_id":"scr_01ARZ3NDEKTSV4RRFFQ69G5FAV","script_slug":"deploy-script"}`},
		{name: "script has incomplete identity", step: `{"name":"step","status":"running","kind":"script",` +
			`"script_id":"scr_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`},
		{name: "script has illegal ID", step: `{"name":"step","status":"running","kind":"script",` +
			`"script_id":"script-invalid","script_slug":"deploy-script"}`},
		{name: "script has illegal slug", step: `{"name":"step","status":"running","kind":"script",` +
			`"script_id":"scr_01ARZ3NDEKTSV4RRFFQ69G5FAV","script_slug":"Deploy Script"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				writeTaskClientTestResponse(t, writer, []byte(taskPrefix+test.step+`]}`))
			}))
			defer server.Close()

			if _, err := New(server.URL).ShowTask(context.Background(), taskID); err == nil {
				t.Fatal("ShowTask() error = nil, want invalid step projection error")
			} else if kind, ok := errs.KindOf(err); !ok || kind != errs.KindInternal {
				t.Fatalf("ShowTask() error = %v, want internal error", err)
			}
		})
	}
}
