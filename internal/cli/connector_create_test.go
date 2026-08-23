package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func TestConnectorAddRequiresExplicitAddressingAndFileBackedDirectValue(t *testing.T) {
	t.Parallel()
	environmentID := "env_01K3D7R40G0000000000000000"
	connectorID := "con_01K3D7R40G0000000000000001"
	var received apiTypes.ConnectorCreateRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/api/v1/connectors" ||
			request.URL.Query().Get("environment") != environmentID {
			t.Errorf("request = %s %s", request.Method, request.URL.String())
		}
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
			t.Errorf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(apiTypes.Connector{
			ID: connectorID, EnvironmentID: environmentID, Name: "backups", Kind: "s3-compatible",
			Endpoint: "https://objects.example.test", Bucket: "groundplane-backups",
			Region: "auto", PathStyle: false,
			Credentials: map[string]apiTypes.ConnectorCredential{
				"access_key": {Kind: apiTypes.ConnectorCredentialSecretRef, SecretRef: "S3_ACCESS_KEY"},
				"secret_key": {Kind: apiTypes.ConnectorCredentialDirect},
			},
		})
	}))
	defer server.Close()
	command := newConnectorCmd()
	command.SetIn(strings.NewReader("direct-secret"))
	output := executeNoun(
		t,
		command,
		server.URL,
		Scope{Environment: environmentID, AsID: true},
		"add", "backups",
		"--endpoint", "https://objects.example.test",
		"--bucket", "groundplane-backups",
		"--region", "auto",
		"--virtual-hosted-style",
		"--access-key-secret", "S3_ACCESS_KEY",
		"--secret-key-value-file", "-",
	)
	if !strings.Contains(output, connectorID) || received.PathStyle == nil || *received.PathStyle ||
		received.Credentials["access_key"].SecretRef != "S3_ACCESS_KEY" ||
		received.Credentials["secret_key"].Value != "direct-secret" {
		t.Fatalf("output/request = %q/%#v", output, received)
	}
}
