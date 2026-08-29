package cli

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

const taskJournalTestResponse = `{"items":[{"id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV",` +
	`"operation_id":"op_01ARZ3NDEKTSV4RRFFQ69G5FAV","type":"deploy",` +
	`"target":"svc_01ARZ3NDEKTSV4RRFFQ69G5FAV","status":"running",` +
	`"workspace_type":"tenant","tenant_id":"tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV",` +
	`"project_id":"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV",` +
	`"environment_id":"env_01ARZ3NDEKTSV4RRFFQ69G5FAV","actor":"operator",` +
	`"created_at":"2026-08-24T10:00:00Z","updated_at":"2026-08-24T10:01:00Z",` +
	`"started_at":"2026-08-24T10:00:30Z","finished_at":null}]}`

func writeTaskJournalTestResponse(t *testing.T, writer io.Writer, response string) {
	t.Helper()
	if _, err := io.WriteString(writer, response); err != nil {
		t.Errorf("write HTTP test response: %v", err)
	}
}

// Rationale: both CLI nouns must resolve an Environment label through its
// selected Tenant and Project before sending the identical stable-id filter.
func TestTaskAndActivityResolveEnvironmentJournalScope(t *testing.T) {
	t.Parallel()
	const (
		tenantID      = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		projectID     = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	)
	tests := []struct {
		name     string
		endpoint string
	}{
		{name: "tasks", endpoint: "/api/v1/tasks"},
		{name: "activity", endpoint: "/api/v1/activity"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				requests++
				writer.Header().Set("Content-Type", "application/json")
				switch request.URL.Path {
				case "/api/v1/tenants":
					writeTaskJournalTestResponse(t, writer, `{"items":[{"id":"`+tenantID+`","slug":"acme",`+
						`"name":"Acme","description":""}]}`)
				case "/api/v1/projects":
					writeTaskJournalTestResponse(t, writer, `{"items":[{"id":"`+projectID+`","tenant_id":"`+
						tenantID+`","slug":"console","name":"Console","description":"","kind":"tenant"}]}`)
				case "/api/v1/environments":
					writeTaskJournalTestResponse(t, writer, `{"items":[{"id":"`+environmentID+`","project_id":"`+
						projectID+`","name":"production","network_pool":"10.200.0.0/24",`+
						`"volume_dir":"/var/lib/groundplane/vol/env","provisioning_state":"ready"}]}`)
				case test.endpoint:
					query := request.URL.Query()
					if query.Get("environment") != environmentID || query.Get("project") != "" || query.Get("workspace") != "" ||
						query.Get("limit") != "50" || len(query) != 2 {
						t.Errorf("journal request = %s", request.URL.RequestURI())
					}
					writeTaskJournalTestResponse(t, writer, taskJournalTestResponse)
				default:
					t.Errorf("unexpected request: %s", request.URL.RequestURI())
				}
			}))
			defer server.Close()

			var output string
			if test.name == "tasks" {
				output = executeNoun(t, newTaskCmd(), server.URL, Scope{
					Tenant: "acme", Project: "console", Environment: "production",
				}, "list")
			} else {
				output = executeNoun(t, newActivityCmd(), server.URL, Scope{
					Tenant: "acme", Project: "console", Environment: "production",
				}, "list")
			}
			if requests != 4 || !strings.Contains(output, `"environment_id": "`+environmentID+`"`) ||
				!strings.Contains(output, `"actor": "operator"`) {
				t.Fatalf("requests/output = %d / %q", requests, output)
			}
		})
	}
}

// Rationale: a Tenant workspace is a renamable CLI label but the Controller
// must receive the stable Tenant id; the platform literal bypasses resolution.
func TestTaskJournalResolvesWorkspaceLabelAndPlatformLiteral(t *testing.T) {
	t.Parallel()
	const tenantID = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	tests := []struct {
		name      string
		workspace string
		want      string
		calls     int
	}{
		{name: "tenant label", workspace: "acme", want: tenantID, calls: 2},
		{name: "platform", workspace: "platform", want: "platform", calls: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				calls++
				writer.Header().Set("Content-Type", "application/json")
				if request.URL.Path == "/api/v1/tenants" {
					writeTaskJournalTestResponse(t, writer, `{"items":[{"id":"`+tenantID+`","slug":"acme",`+
						`"name":"Acme","description":""}]}`)
					return
				}
				if request.URL.Path != "/api/v1/tasks" || request.URL.Query().Get("workspace") != test.want ||
					request.URL.Query().Get("environment") != "" || request.URL.Query().Get("project") != "" {
					t.Errorf("journal request = %s", request.URL.RequestURI())
				}
				writeTaskJournalTestResponse(t, writer, taskJournalTestResponse)
			}))
			defer server.Close()

			executeNoun(t, newTaskCmd(), server.URL, Scope{}, "list", "--workspace", test.workspace)
			if calls != test.calls {
				t.Fatalf("request count = %d, want %d", calls, test.calls)
			}
		})
	}
}

// Rationale: Task and Activity are one pageable journal, so both CLI nouns
// must forward the same page size and interchangeable opaque cursor.
func TestTaskAndActivityForwardPagination(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		endpoint string
		command  func() *cobra.Command
	}{
		{name: "tasks", endpoint: "/api/v1/tasks", command: newTaskCmd},
		{name: "activity", endpoint: "/api/v1/activity", command: newActivityCmd},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				query := request.URL.Query()
				if request.URL.Path != test.endpoint || query.Get("limit") != "17" ||
					query.Get("cursor") != "shared-cursor" || len(query) != 2 {
					t.Errorf("journal request = %s", request.URL.RequestURI())
				}
				writer.Header().Set("Content-Type", "application/json")
				writeTaskJournalTestResponse(t, writer, taskJournalTestResponse)
			}))
			defer server.Close()

			executeNoun(t, test.command(), server.URL, Scope{},
				"list", "--limit", "17", "--cursor", "shared-cursor")
		})
	}
}

// Rationale: --id must bypass mutable label resolution, while selecting both
// accepted journal scopes must fail locally with the public validation kind.
func TestTaskJournalIDBypassAndDualScopeRejection(t *testing.T) {
	t.Parallel()
	const (
		tenantID      = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		projectID     = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	)
	command := newTaskCmd()
	command.SetContext(context.WithValue(context.Background(), appKey{}, &App{
		Scope: Scope{Environment: environmentID, AsID: true},
	}))
	options, err := resolveTaskJournalListOptions(command, "", 17, "shared-cursor")
	if err != nil || options.Environment != environmentID {
		t.Fatalf("id scope = %#v, %v", options, err)
	}
	if options.Limit != 17 || options.Cursor != "shared-cursor" {
		t.Fatalf("pagination = %#v", options)
	}
	projectCommand := newTaskCmd()
	projectCommand.SetContext(context.WithValue(context.Background(), appKey{}, &App{
		Scope: Scope{Project: projectID, AsID: true},
	}))
	projectOptions, err := resolveTaskJournalListOptions(projectCommand, "", 0, "")
	if err != nil || projectOptions.Project != projectID {
		t.Fatalf("Project id scope = %#v, %v", projectOptions, err)
	}
	tenantCommand := newTaskCmd()
	tenantCommand.SetContext(context.WithValue(context.Background(), appKey{}, &App{
		Scope: Scope{Tenant: tenantID, AsID: true},
	}))
	tenantOptions, err := resolveTaskJournalListOptions(tenantCommand, "", 0, "")
	if err != nil || tenantOptions.Workspace != tenantID {
		t.Fatalf("Tenant id scope = %#v, %v", tenantOptions, err)
	}

	_, err = resolveTaskJournalListOptions(command, "platform", 17, "shared-cursor")
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindValidationFailed {
		t.Fatalf("dual-scope error = %v", err)
	}
	_, err = resolveTaskJournalListOptions(projectCommand, "platform", 0, "")
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindValidationFailed {
		t.Fatalf("Project/workspace dual-scope error = %v", err)
	}
}
