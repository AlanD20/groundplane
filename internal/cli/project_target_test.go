package cli

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// QA: OWN-01, OWN-02, UI-02; local scoped lookup and PATCH routing only, not persisted rename safety.
// Rationale: a Project slug must resolve within the selected Tenant and the
// mutation must address the resulting stable Project ID.
func TestProjectSlugTargetResolvesThroughTenantAndProjectCollections(t *testing.T) {
	t.Parallel()
	const tenantID = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const projectID = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
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
				query.Get("tenant") != tenantID || query.Get("kind") != "tenant" ||
				query.Get("limit") != "200" || query.Get("cursor") != "" {
				t.Errorf("Project resolution request = %s %s", r.Method, r.URL.String())
			}
			_, _ = io.WriteString(
				w,
				`{"items":[{"id":"`+projectID+`","tenant_id":"`+tenantID+`","slug":"console","name":"Console","description":"","kind":"tenant"}]}`,
			)
		case 3:
			body, _ := io.ReadAll(r.Body)
			if r.Method != http.MethodPatch || r.URL.Path != "/api/v1/projects/"+projectID ||
				string(body) != `{"name":"Operator Console"}` {
				t.Errorf("Project mutation request = %s %s %s", r.Method, r.URL.String(), body)
			}
			_, _ = io.WriteString(w, `{}`)
		default:
			t.Errorf("unexpected request %d: %s %s", calls, r.Method, r.URL.String())
		}
	}))
	defer server.Close()
	executeNoun(t, newProjectCmd(), server.URL, Scope{Tenant: "acme"},
		"edit", "console", "--name", "Operator Console")
	if calls != 3 {
		t.Fatalf("request count = %d", calls)
	}
}

// QA: OWN-01, UI-01, UI-03; local create-body encoding only, not Project persistence.
// Rationale: ordinary Project creation must not send the response-only kind
// field and accidentally select backing-project semantics.
func TestProjectCreateOmitsResponseOnlyKind(t *testing.T) {
	t.Parallel()
	const tenantID = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		if calls == 1 {
			_, _ = io.WriteString(w, `{"items":[{"id":"`+tenantID+`","slug":"acme","name":"Acme","description":""}]}`)
			return
		}
		body, _ := io.ReadAll(r.Body)
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/projects" ||
			string(body) != `{"slug":"console","tenant_id":"`+tenantID+`"}` {
			t.Errorf("Project create request = %s %s %s", r.Method, r.URL.String(), body)
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{}`)
	}))
	defer server.Close()
	executeNoun(t, newProjectCmd(), server.URL, Scope{Tenant: "acme"}, "create", "console")
	if calls != 2 {
		t.Fatalf("request count = %d", calls)
	}
}
