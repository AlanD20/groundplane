package cli

import (
	"net/http"
	"testing"
)

// QA: OWN-01, OWN-03, UI-01; local CLI request encoding only, not persisted hierarchy state.
// Rationale: Tenant create and partial edit must carry the operator-authored
// description instead of dropping it as presentation-only text.
func TestTenantCreateAndEditCarryDescription(t *testing.T) {
	t.Parallel()

	createServer := exactRequestServer(
		t,
		http.MethodPost,
		"/api/v1/tenants",
		`{"description":"Production workloads","name":"Acme","slug":"acme"}`,
		http.StatusCreated,
		`{}`,
	)
	defer createServer.Close()
	executeNoun(
		t, newTenantCmd(), createServer.URL, Scope{},
		"create", "acme", "--name", "Acme", "--description", "Production workloads",
	)

	editServer := exactRequestServer(
		t,
		http.MethodPatch,
		"/api/v1/tenants/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		`{"description":"Updated"}`,
		http.StatusOK,
		`{}`,
	)
	defer editServer.Close()
	executeNoun(
		t, newTenantCmd(), editServer.URL, Scope{AsID: true},
		"edit", "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV", "--description", "Updated",
	)
}
