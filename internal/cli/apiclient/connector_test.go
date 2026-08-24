package apiclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: every Connector command must retain the generated transport's
// query, path, idempotency, redaction, and accepted-Task contract.
func TestConnectorClientUsesTypedGeneratedOperations(t *testing.T) {
	t.Parallel()
	environmentID := "env_01K3D7R40G0000000000000000"
	connectorID := "con_01K3D7R40G0000000000000001"
	pathStyle := false
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodPost && request.URL.Path == "/api/v1/connectors":
			if request.URL.Query().Get("environment") != environmentID ||
				request.Header.Get("Idempotency-Key") == "" {
				t.Errorf("create query/header = %q/%q", request.URL.RawQuery, request.Header)
			}
			var body apiTypes.ConnectorCreateRequest
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil || body.PathStyle == nil ||
				*body.PathStyle || body.Credentials["secret_key"].Value != "direct-secret" {
				t.Errorf("create body = %#v, %v", body, err)
			}
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(testConnectorClientResponse(connectorID, environmentID))
		case request.Method == http.MethodGet && request.URL.Path == "/api/v1/connectors":
			if request.URL.Query().Get("environment") != environmentID ||
				request.URL.Query().Get("limit") != "2" || request.URL.Query().Get("cursor") != "opaque" {
				t.Errorf("list query = %q", request.URL.RawQuery)
			}
			_ = json.NewEncoder(w).Encode(apiTypes.Page[apiTypes.Connector]{
				Items:      []apiTypes.Connector{testConnectorClientResponse(connectorID, environmentID)},
				NextCursor: "next",
			})
		case request.Method == http.MethodGet && request.URL.Path == "/api/v1/connectors/"+connectorID:
			_ = json.NewEncoder(w).Encode(testConnectorClientResponse(connectorID, environmentID))
		case request.Method == http.MethodDelete && request.URL.Path == "/api/v1/connectors/"+connectorID:
			if request.Header.Get("Idempotency-Key") == "" {
				t.Errorf("delete header = %q", request.Header)
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(apiTypes.TaskAccepted{TaskID: "task_01K3D7R40G0000000000000002"})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()
	client := New(server.URL)
	created, err := client.CreateConnector(context.Background(), environmentID, apiTypes.ConnectorCreateRequest{
		Name: "backups", Kind: "s3-compatible", Endpoint: "https://objects.example.test",
		Bucket: "groundplane-backups", Region: "auto", PathStyle: &pathStyle,
		Credentials: map[string]apiTypes.ConnectorCredentialInput{
			"access_key": {SecretRef: "S3_ACCESS_KEY"},
			"secret_key": {Value: "direct-secret"},
		},
	})
	if err != nil || created.ID != connectorID || created.PathStyle {
		t.Fatalf("CreateConnector() = %#v, %v", created, err)
	}
	page, err := client.ListConnectors(context.Background(), environmentID, 2, "opaque")
	if err != nil || len(page.Items) != 1 || page.NextCursor != "next" {
		t.Fatalf("ListConnectors() = %#v, %v", page, err)
	}
	shown, err := client.ShowConnector(context.Background(), connectorID)
	if err != nil || shown.ID != connectorID {
		t.Fatalf("ShowConnector() = %#v, %v", shown, err)
	}
	accepted, err := client.RemoveConnector(context.Background(), connectorID)
	if err != nil || accepted.TaskID != "task_01K3D7R40G0000000000000002" || requests != 4 {
		t.Fatalf("RemoveConnector() = %#v, %v; requests = %d", accepted, err, requests)
	}
}

func testConnectorClientResponse(id string, environmentID string) apiTypes.Connector {
	return apiTypes.Connector{
		ID: id, EnvironmentID: environmentID, Name: "backups", Kind: "s3-compatible",
		Endpoint: "https://objects.example.test", Bucket: "groundplane-backups",
		Region: "auto", PathStyle: false,
		Credentials: map[string]apiTypes.ConnectorCredential{
			"access_key": {Kind: apiTypes.ConnectorCredentialSecretRef, SecretRef: "S3_ACCESS_KEY"},
			"secret_key": {Kind: apiTypes.ConnectorCredentialDirect},
		},
	}
}
