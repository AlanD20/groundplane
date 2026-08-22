package cli

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Rationale: Secret keys are mutable scoped labels, so the default CLI show path must resolve the
// complete tenant/project scope and address the detail route by stable Secret id.
func TestSecretKeyTargetResolvesWithinProjectScope(t *testing.T) {
	t.Parallel()
	const tenantID = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const projectID = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const secretID = "sec_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls++
		writer.Header().Set("Content-Type", "application/json")
		switch calls {
		case 1:
			if request.Method != http.MethodGet || request.URL.Path != "/api/v1/tenants" {
				t.Errorf("Tenant resolution request = %s %s", request.Method, request.URL.String())
			}
			_, _ = io.WriteString(
				writer,
				`{"items":[{"id":"`+tenantID+`","slug":"acme","name":"Acme","description":""}]}`,
			)
		case 2:
			query := request.URL.Query()
			if request.Method != http.MethodGet || request.URL.Path != "/api/v1/projects" ||
				query.Get("tenant") != tenantID || query.Get("kind") != "tenant" {
				t.Errorf("Project resolution request = %s %s", request.Method, request.URL.String())
			}
			_, _ = io.WriteString(
				writer,
				`{"items":[{"id":"`+projectID+`","tenant_id":"`+tenantID+`","slug":"console","name":"Console","description":"","kind":"tenant"}]}`,
			)
		case 3:
			query := request.URL.Query()
			if request.Method != http.MethodGet || request.URL.Path != "/api/v1/secrets" ||
				query.Get("project") != projectID || query.Get("limit") != "200" {
				t.Errorf("Secret resolution request = %s %s", request.Method, request.URL.String())
			}
			_, _ = io.WriteString(
				writer,
				`{"items":[{"id":"`+secretID+`","scope":"project","project_id":"`+projectID+`","key":"DATABASE_PASSWORD","kind":"env_var","ref":"secrets/.env.`+projectID+`","updated_at":"2026-08-22T14:00:00Z"}]}`,
			)
		case 4:
			if request.Method != http.MethodGet || request.URL.Path != "/api/v1/secrets/"+secretID {
				t.Errorf("Secret detail request = %s %s", request.Method, request.URL.String())
			}
			_, _ = io.WriteString(
				writer,
				`{"id":"`+secretID+`","scope":"project","project_id":"`+projectID+`","key":"DATABASE_PASSWORD","kind":"env_var","ref":"secrets/.env.`+projectID+`","updated_at":"2026-08-22T14:00:00Z"}`,
			)
		default:
			t.Errorf("unexpected request %d: %s %s", calls, request.Method, request.URL.String())
		}
	}))
	defer server.Close()

	executeNoun(
		t,
		newSecretCmd(),
		server.URL,
		Scope{Tenant: "acme", Project: "console"},
		"show", "DATABASE_PASSWORD",
	)
	if calls != 4 {
		t.Fatalf("request count = %d", calls)
	}
}
