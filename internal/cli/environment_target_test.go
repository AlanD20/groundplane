package cli

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestEnvironmentNameTargetResolvesThroughFullScopeChain(t *testing.T) {
	t.Parallel()
	const tenantID = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const projectID = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		switch calls {
		case 1:
			if r.Method != http.MethodGet || r.URL.Path != "/api/v1/tenants" {
				t.Errorf("Tenant resolution request = %s %s", r.Method, r.URL.String())
			}
			_, _ = io.WriteString(w, `{"items":[{"id":"`+tenantID+`","slug":"acme","name":"Acme","description":""}]}`)
		case 2:
			query := r.URL.Query()
			if r.Method != http.MethodGet || r.URL.Path != "/api/v1/projects" ||
				query.Get("tenant") != tenantID || query.Get("kind") != "tenant" {
				t.Errorf("Project resolution request = %s %s", r.Method, r.URL.String())
			}
			_, _ = io.WriteString(
				w,
				`{"items":[{"id":"`+projectID+`","tenant_id":"`+tenantID+`","slug":"console","name":"Console","description":"","kind":"tenant"}]}`,
			)
		case 3:
			query := r.URL.Query()
			if r.Method != http.MethodGet || r.URL.Path != "/api/v1/environments" ||
				query.Get("project") != projectID || query.Get("limit") != "200" {
				t.Errorf("Environment resolution request = %s %s", r.Method, r.URL.String())
			}
			_, _ = io.WriteString(
				w,
				`{"items":[{"id":"`+environmentID+`","project_id":"`+projectID+`","name":"production","volume_dir":"/var/lib/groundplane/vol/x","provisioning_state":"ready","create_task_id":null}]}`,
			)
		case 4:
			body, _ := io.ReadAll(r.Body)
			if r.Method != http.MethodPost || r.URL.Path != "/api/v1/environments/"+environmentID+"/rename" ||
				string(body) != `{"name":"prod"}` {
				t.Errorf("Environment mutation request = %s %s %s", r.Method, r.URL.String(), body)
			}
			_, _ = io.WriteString(w, `{}`)
		default:
			t.Errorf("unexpected request %d: %s %s", calls, r.Method, r.URL.String())
		}
	}))
	defer server.Close()
	executeNoun(
		t,
		newEnvironmentCmd(),
		server.URL,
		Scope{Tenant: "acme", Project: "console"},
		"rename", "production", "--name", "prod",
	)
	if calls != 4 {
		t.Fatalf("request count = %d", calls)
	}
}

func TestEnvironmentEditResolvesTenantProjectAndEnvironmentLabels(t *testing.T) {
	t.Parallel()
	const tenantID = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const projectID = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		switch calls {
		case 1:
			if r.Method != http.MethodGet || r.URL.Path != "/api/v1/tenants" {
				t.Errorf("Tenant resolution request = %s %s", r.Method, r.URL.String())
			}
			_, _ = io.WriteString(w, `{"items":[{"id":"`+tenantID+`","slug":"acme","name":"Acme","description":""}]}`)
		case 2:
			query := r.URL.Query()
			if r.Method != http.MethodGet || r.URL.Path != "/api/v1/projects" ||
				query.Get("tenant") != tenantID || query.Get("kind") != "tenant" {
				t.Errorf("Project resolution request = %s %s", r.Method, r.URL.String())
			}
			_, _ = io.WriteString(
				w,
				`{"items":[{"id":"`+projectID+`","tenant_id":"`+tenantID+`","slug":"console","name":"Console","description":"","kind":"tenant"}]}`,
			)
		case 3:
			query := r.URL.Query()
			if r.Method != http.MethodGet || r.URL.Path != "/api/v1/environments" ||
				query.Get("project") != projectID || query.Get("limit") != "200" {
				t.Errorf("Environment resolution request = %s %s", r.Method, r.URL.String())
			}
			_, _ = io.WriteString(
				w,
				`{"items":[{"id":"`+environmentID+`","project_id":"`+projectID+`","name":"production","network_pool":"10.40.0.0/16","volume_dir":"/var/lib/groundplane/vol/x","provisioning_state":"ready","create_task_id":null}]}`,
			)
		case 4:
			body, _ := io.ReadAll(r.Body)
			if r.Method != http.MethodPatch || r.URL.Path != "/api/v1/environments/"+environmentID ||
				string(body) != `{"network_pool":"10.40.0.0/15"}` ||
				r.Header.Get("Idempotency-Key") == "" {
				t.Errorf("Environment edit request = %s %s %s", r.Method, r.URL.String(), body)
			}
			_, _ = io.WriteString(w, `{"id":"`+environmentID+`","project_id":"`+projectID+`","name":"production","network_pool":"10.40.0.0/15","volume_dir":"/var/lib/groundplane/vol/x","provisioning_state":"ready","create_task_id":null}`)
		default:
			t.Errorf("unexpected request %d: %s %s", calls, r.Method, r.URL.String())
		}
	}))
	defer server.Close()
	executeNoun(
		t,
		newEnvironmentCmd(),
		server.URL,
		Scope{Tenant: "acme", Project: "console"},
		"edit", "production", "--network-pool", "10.40.0.0/15",
	)
	if calls != 4 {
		t.Fatalf("request count = %d", calls)
	}
}
