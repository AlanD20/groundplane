package cli

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// QA: VOL-01, UI-01, UI-02; local scope lookup and list rendering only, not Volume persistence or data.
// Rationale: Volume is Environment-scoped, so direct list must resolve the
// normal Tenant/Project/Environment labels before sending the stable id.
func TestVolumeListResolvesEnvironmentLabel(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/api/v1/tenants":
			writeBackupPolicyTestResponse(t, writer, http.StatusOK, tenantPageJSON())
		case "/api/v1/projects":
			writeBackupPolicyTestResponse(t, writer, http.StatusOK, projectPageJSON())
		case "/api/v1/environments":
			writeBackupPolicyTestResponse(t, writer, http.StatusOK, environmentPageJSON())
		case "/api/v1/volumes":
			assertBackupPolicyEnvironmentQuery(t, request)
			writeBackupPolicyTestResponse(t, writer, http.StatusOK, volumePageJSON())
		default:
			t.Errorf("unexpected request %s %s", request.Method, request.URL.String())
			http.Error(writer, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()

	output := executeNoun(
		t,
		newVolumeCmd(),
		server.URL,
		Scope{Tenant: "acme", Project: "storefront", Environment: "production"},
		"list",
	)
	if !strings.Contains(output, "uploads") || !strings.Contains(output, "uploads-data") {
		t.Fatalf("list output = %q", output)
	}
}

// QA: VOL-01, UI-02; local ID routing only, not Volume ownership enforcement.
// Rationale: global --id makes the Environment scope stable already, so the
// typed Volume list must not perform any label collection reads.
func TestVolumeListHonorsGlobalIDMode(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if request.URL.Path != "/api/v1/volumes" {
			t.Errorf("unexpected label-resolution request %s %s", request.Method, request.URL.String())
			http.Error(writer, "unexpected request", http.StatusNotFound)
			return
		}
		assertBackupPolicyEnvironmentQuery(t, request)
		writeBackupPolicyTestResponse(t, writer, http.StatusOK, volumePageJSON())
	}))
	defer server.Close()

	_ = executeNoun(
		t,
		newVolumeCmd(),
		server.URL,
		Scope{Environment: backupPolicyTestEnvironmentID, AsID: true},
		"list",
	)
}

func volumePageJSON() string {
	return fmt.Sprintf(
		`{"items":[{"id":%q,"environment_id":%q,"slug":"uploads","key":"uploads-data","state":"active"}],"next_cursor":""}`,
		backupPolicyVolumeID,
		backupPolicyTestEnvironmentID,
	)
}
