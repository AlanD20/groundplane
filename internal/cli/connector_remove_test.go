package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: mutable Connector names are the default CLI operand while --id
// bypasses lookup; both modes must dispatch the same stable-id DELETE Task.
func TestConnectorRemoveResolvesNameUnlessIDModeIsSelected(t *testing.T) {
	t.Parallel()
	environmentID := "env_01K3D7R40G0000000000000000"
	connectorID := "con_01K3D7R40G0000000000000001"
	taskID := "task_01K3D7R40G0000000000000002"
	tenantID := "tnt_01K3D7R40G0000000000000003"
	projectID := "prj_01K3D7R40G0000000000000004"
	for _, test := range []struct {
		name       string
		verb       string
		argument   string
		scope      Scope
		asID       bool
		wantLookup bool
	}{
		{
			name: "name", verb: "remove", argument: "backups",
			scope:      Scope{Tenant: "acme", Project: "web", Environment: "production"},
			wantLookup: true,
		},
		{name: "id remove", verb: "remove", argument: connectorID, scope: Scope{Environment: environmentID}, asID: true},
		{name: "id rm", verb: "rm", argument: connectorID, scope: Scope{Environment: environmentID}, asID: true},
		{name: "id delete", verb: "delete", argument: connectorID, scope: Scope{Environment: environmentID}, asID: true},
		{name: "id del", verb: "del", argument: connectorID, scope: Scope{Environment: environmentID}, asID: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			lookupCalls := 0
			deleteCalls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case request.Method == http.MethodGet && request.URL.Path == "/api/v1/tenants":
					_ = json.NewEncoder(w).Encode(apiTypes.Page[apiTypes.Tenant]{Items: []apiTypes.Tenant{{
						ID: tenantID, Slug: "acme",
					}}})
				case request.Method == http.MethodGet && request.URL.Path == "/api/v1/projects":
					if request.URL.Query().Get("tenant") != tenantID {
						t.Errorf("project query = %q", request.URL.RawQuery)
					}
					_ = json.NewEncoder(w).Encode(apiTypes.Page[apiTypes.Project]{Items: []apiTypes.Project{{
						ID: projectID, TenantID: tenantID, Slug: "web", Kind: "tenant",
					}}})
				case request.Method == http.MethodGet && request.URL.Path == "/api/v1/environments":
					if request.URL.Query().Get("project") != projectID {
						t.Errorf("environment query = %q", request.URL.RawQuery)
					}
					_ = json.NewEncoder(w).Encode(apiTypes.Page[apiTypes.Environment]{Items: []apiTypes.Environment{{
						ID: environmentID, ProjectID: projectID, Name: "production",
					}}})
				case request.Method == http.MethodGet && request.URL.Path == "/api/v1/connectors":
					lookupCalls++
					if request.URL.Query().Get("environment") != environmentID {
						t.Errorf("lookup query = %q", request.URL.RawQuery)
					}
					_ = json.NewEncoder(w).Encode(apiTypes.Page[apiTypes.Connector]{Items: []apiTypes.Connector{{
						ID: connectorID, EnvironmentID: environmentID, Name: "backups",
					}}})
				case request.Method == http.MethodDelete && request.URL.Path == "/api/v1/connectors/"+connectorID:
					deleteCalls++
					if request.Header.Get("Idempotency-Key") == "" {
						t.Error("delete omitted Idempotency-Key")
					}
					w.WriteHeader(http.StatusAccepted)
					_ = json.NewEncoder(w).Encode(apiTypes.TaskAccepted{TaskID: taskID})
				default:
					http.NotFound(w, request)
				}
			}))
			defer server.Close()
			output := executeNoun(
				t,
				newConnectorCmd(),
				server.URL,
				Scope{
					Tenant: test.scope.Tenant, Project: test.scope.Project,
					Environment: test.scope.Environment, AsID: test.asID,
				},
				test.verb,
				test.argument,
			)
			if !strings.Contains(output, taskID) || deleteCalls != 1 || (lookupCalls == 1) != test.wantLookup {
				t.Fatalf("output/calls = %q/%d/%d", output, lookupCalls, deleteCalls)
			}
		})
	}
}
