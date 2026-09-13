package cli

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// QA: OWN-01, OWN-02, UI-02; local slug lookup and PATCH routing only, not persisted hierarchy state.
// Rationale: the mutable Tenant slug operand must resolve to its stable ID
// before an edit so later label changes do not become wire identity.
func TestTenantSlugTargetResolvesToStableIDBeforeMutation(t *testing.T) {
	t.Parallel()

	const tenantID = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		switch calls {
		case 1:
			if r.Method != http.MethodGet || r.URL.Path != "/api/v1/tenants" ||
				r.URL.Query().Get("limit") != "200" || r.URL.Query().Get("cursor") != "" {
				t.Errorf("resolution request = %s %s", r.Method, r.URL.String())
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"items":[{"id":"`+tenantID+`","slug":"acme","name":"Acme","description":""}]}`)
		case 2:
			body, _ := io.ReadAll(r.Body)
			if r.Method != http.MethodPatch || r.URL.Path != "/api/v1/tenants/"+tenantID ||
				string(body) != `{"description":"Updated"}` {
				t.Errorf("mutation request = %s %s %s", r.Method, r.URL.String(), body)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{}`)
		default:
			t.Errorf("unexpected request %d: %s %s", calls, r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	executeNoun(
		t,
		newTenantCmd(),
		server.URL,
		Scope{},
		"edit",
		"acme",
		"--description",
		"Updated",
	)
	if calls != 2 {
		t.Fatalf("request count = %d", calls)
	}
}
