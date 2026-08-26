package apiclient

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: the protected singleton replacement must use the generated JSON
// operation while preserving exact optional fields and ordered source ids.
func TestSetBackupPolicySendsExactProtectedJSON(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if request.Method != http.MethodPut {
			t.Errorf("method = %s, want PUT", request.Method)
		}
		if request.URL.Path != "/api/v1/environments/env_1/backup-policy" {
			t.Errorf("path = %q, want Backup Policy singleton", request.URL.Path)
		}
		if contentType := request.Header.Get("Content-Type"); contentType != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", contentType)
		}
		if request.Header.Get("Idempotency-Key") == "" {
			t.Error("Idempotency-Key is empty")
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(body, &fields); err != nil {
			t.Errorf("decode request fields: %v", err)
		}
		if len(fields) != 6 {
			t.Errorf("request field count = %d, want 6: %s", len(fields), body)
		}
		var input apiTypes.BackupPolicyReplacementRequest
		if err := json.Unmarshal(body, &input); err != nil {
			t.Errorf("decode request: %v", err)
		}
		assertBackupPolicyClientInput(t, input)
		writer.WriteHeader(http.StatusOK)
		if _, err := io.WriteString(
			writer,
			`{"enabled":true,"frequency":"Tue *-*-* 04:30:00","keep":7,`+
				`"encryption":"age","connector_id":"con_1","sources":`+
				`[{"id":"spt_1","kind":"attach","target_id":"att_1"}]}`,
		); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	defer server.Close()

	policy, err := New(server.URL).SetBackupPolicy(
		context.Background(),
		"env_1",
		apiTypes.BackupPolicyReplacementRequest{
			Enabled: true, Frequency: "Tue *-*-* 04:30:00", Keep: 7,
			Encryption: apiTypes.BackupEncryptionAge, ConnectorID: "con_1",
			Sources: []apiTypes.BackupSourceInput{
				{Kind: apiTypes.BackupSourceAttach, TargetID: "att_1"},
			},
		},
	)
	if err != nil {
		t.Fatalf("set Backup Policy: %v", err)
	}
	if !policy.Enabled || policy.ConnectorID != "con_1" || len(policy.Sources) != 1 {
		t.Fatalf("policy = %#v", policy)
	}
}

// Rationale: Volume label resolution depends on the generated collection
// query carrying the owning stable Environment id and pagination unchanged.
func TestListVolumesSendsEnvironmentQuery(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if request.Method != http.MethodGet || request.URL.Path != "/api/v1/volumes" {
			t.Errorf("request = %s %s, want GET /api/v1/volumes", request.Method, request.URL.Path)
		}
		query := request.URL.Query()
		if query.Get("environment") != "env_1" || query.Get("limit") != "25" || query.Get("cursor") != "next" {
			t.Errorf("query = %q", request.URL.RawQuery)
		}
		writer.WriteHeader(http.StatusOK)
		if _, err := io.WriteString(
			writer,
			`{"items":[{"id":"vol_1","environment_id":"env_1","slug":"uploads","key":"uploads-data","state":"active"}],"next_cursor":"after"}`,
		); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	defer server.Close()

	page, err := New(server.URL).ListVolumes(context.Background(), "env_1", 25, "next")
	if err != nil {
		t.Fatalf("list Volumes: %v", err)
	}
	if len(page.Items) != 1 || page.Items[0].ID != "vol_1" || page.NextCursor != "after" {
		t.Fatalf("page = %#v", page)
	}
}

func assertBackupPolicyClientInput(t *testing.T, input apiTypes.BackupPolicyReplacementRequest) {
	t.Helper()
	if !input.Enabled || input.Frequency != "Tue *-*-* 04:30:00" || input.Keep != 7 {
		t.Errorf("policy fields = %#v", input)
	}
	if input.Encryption != apiTypes.BackupEncryptionAge || input.ConnectorID != "con_1" {
		t.Errorf("policy destination = %#v", input)
	}
	if len(input.Sources) != 1 {
		t.Fatalf("source count = %d, want 1", len(input.Sources))
	}
	if input.Sources[0].Kind != apiTypes.BackupSourceAttach || input.Sources[0].TargetID != "att_1" {
		t.Errorf("source = %#v", input.Sources[0])
	}
}
