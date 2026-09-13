package cli

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// QA: BAK-07, UI-02; local scope resolution and request query only, not concurrent point publication.
// Rationale: backup points accepts an Environment label but the generated
// operation must receive the stable id and only the documented cursor query.
func TestBackupPointsResolvesEnvironmentLabel(t *testing.T) {
	t.Parallel()
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/api/v1/tenants":
			_, _ = io.WriteString(writer, tenantPageJSON())
		case "/api/v1/projects":
			_, _ = io.WriteString(writer, projectPageJSON())
		case "/api/v1/environments":
			_, _ = io.WriteString(writer, environmentPageJSON())
		case "/api/v1/environments/" + backupPolicyTestEnvironmentID + "/recovery-points":
			if request.URL.Query().Get("cursor") != "opaque-current" || len(request.URL.Query()) != 1 {
				t.Errorf("Recovery Point query = %q", request.URL.RawQuery)
			}
			_, _ = io.WriteString(writer, `{"items":[]}`)
		default:
			t.Errorf("unexpected request: %s", request.URL.RequestURI())
		}
	}))
	defer server.Close()

	executeNoun(
		t,
		newBackupCmd(),
		server.URL,
		Scope{Tenant: "acme", Project: "storefront", Environment: "production"},
		"points",
		"--cursor", "opaque-current",
	)
	if requests != 4 {
		t.Fatalf("request count = %d, want 4", requests)
	}
}

// QA: BAK-07, UI-02; local ID routing only, not point ordering or fixed-revision pagination.
// Rationale: global --id is the explicit stable-id path and must bypass all
// mutable Environment label collection reads.
func TestBackupPointsHonorsGlobalIDMode(t *testing.T) {
	t.Parallel()
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		writer.Header().Set("Content-Type", "application/json")
		if request.URL.Path != "/api/v1/environments/"+backupPolicyTestEnvironmentID+"/recovery-points" ||
			len(request.URL.Query()) != 0 {
			t.Errorf("request = %s", request.URL.RequestURI())
		}
		_, _ = io.WriteString(writer, `{"items":[]}`)
	}))
	defer server.Close()

	executeNoun(
		t,
		newBackupCmd(),
		server.URL,
		Scope{Environment: backupPolicyTestEnvironmentID, AsID: true},
		"points",
	)
	if requests != 1 {
		t.Fatalf("request count = %d, want 1", requests)
	}
}
