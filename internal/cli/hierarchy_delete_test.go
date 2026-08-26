package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func TestTenantDeleteResolvesSlugAndDispatchesAuthoritativeTask(t *testing.T) {
	t.Parallel()
	const tenantID = "tnt_01K3D7R40G0000000000000000"
	const taskID = "task_01K3D7R40G0000000000000001"
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		writer.Header().Set("Content-Type", "application/json")
		switch requests {
		case 1:
			if request.Method != http.MethodGet || request.URL.Path != "/api/v1/tenants" {
				t.Fatalf("resolution request = %s %s", request.Method, request.URL.String())
			}
			_ = json.NewEncoder(writer).Encode(apiTypes.Page[apiTypes.Tenant]{Items: []apiTypes.Tenant{{ID: tenantID, Slug: "acme"}}})
		case 2:
			if request.Method != http.MethodDelete || request.URL.Path != "/api/v1/tenants/"+tenantID || len(request.Header.Values("Idempotency-Key")) != 1 {
				t.Fatalf("delete request = %s %s headers=%v", request.Method, request.URL.String(), request.Header.Values("Idempotency-Key"))
			}
			writer.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(writer).Encode(apiTypes.TaskAccepted{TaskID: taskID})
		default:
			t.Fatalf("unexpected request %d", requests)
		}
	}))
	defer server.Close()

	output := executeNoun(t, newTenantCmd(), server.URL, Scope{}, "delete", "acme")
	if !strings.Contains(output, taskID) || requests != 2 {
		t.Fatalf("output/requests = %q/%d", output, requests)
	}
}

func TestProjectDeleteResolvesScopedSlugAndDispatchesAuthoritativeTask(t *testing.T) {
	t.Parallel()
	const tenantID = "tnt_01K3D7R40G0000000000000000"
	const projectID = "prj_01K3D7R40G0000000000000001"
	const taskID = "task_01K3D7R40G0000000000000002"
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		writer.Header().Set("Content-Type", "application/json")
		switch requests {
		case 1:
			_ = json.NewEncoder(writer).Encode(apiTypes.Page[apiTypes.Tenant]{Items: []apiTypes.Tenant{{ID: tenantID, Slug: "acme"}}})
		case 2:
			if request.Method != http.MethodGet || request.URL.Path != "/api/v1/projects" || request.URL.Query().Get("tenant") != tenantID || request.URL.Query().Get("kind") != "tenant" {
				t.Fatalf("project resolution request = %s %s", request.Method, request.URL.String())
			}
			_ = json.NewEncoder(writer).Encode(apiTypes.Page[apiTypes.Project]{Items: []apiTypes.Project{{ID: projectID, TenantID: tenantID, Slug: "console", Kind: "tenant"}}})
		case 3:
			if request.Method != http.MethodDelete || request.URL.Path != "/api/v1/projects/"+projectID || len(request.Header.Values("Idempotency-Key")) != 1 {
				t.Fatalf("delete request = %s %s headers=%v", request.Method, request.URL.String(), request.Header.Values("Idempotency-Key"))
			}
			writer.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(writer).Encode(apiTypes.TaskAccepted{TaskID: taskID})
		default:
			t.Fatalf("unexpected request %d", requests)
		}
	}))
	defer server.Close()

	output := executeNoun(t, newProjectCmd(), server.URL, Scope{Tenant: "acme"}, "delete", "console")
	if !strings.Contains(output, taskID) || requests != 3 {
		t.Fatalf("output/requests = %q/%d", output, requests)
	}
}
