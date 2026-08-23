package api

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestServiceRuntimeIntentJSON(t *testing.T) {
	// Rationale: Service responses must expose runtime_intent with exact
	// snake_case JSON while keeping it out of Blueprint input.
	encoded, err := json.Marshal(Service{
		ID:            "svc_x",
		EnvironmentID: "env_x",
		Name:          "api",
		Image:         "app:latest",
		RuntimeIntent: ServiceRuntimeIntentStopped,
	})
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}

	want := `{"id":"svc_x","environment_id":"env_x","name":"api","image":"app:latest","runtime_intent":"stopped","resources":{},"logging":{}}`
	if string(encoded) != want {
		t.Fatalf("Marshal() = %s, want %s", encoded, want)
	}
}

func TestConnectorResponseOmitsDirectCredentialValue(t *testing.T) {
	// Rationale: connector direct credentials are write-only encrypted inputs;
	// a connector response may describe the source but must never round-trip
	// the plaintext value held by ConnectorCreateRequest.
	encoded, err := json.Marshal(Connector{
		ID:            "con_x",
		EnvironmentID: "env_x",
		Name:          "backups",
		Kind:          "s3-compatible",
		PathStyle:     true,
		Credentials: map[string]ConnectorCredential{
			"secret_key": {Kind: ConnectorCredentialDirect},
		},
	})
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}
	if strings.Contains(string(encoded), "plaintext") || strings.Contains(string(encoded), `"value"`) {
		t.Fatalf("Connector response leaked a write-only credential field: %s", encoded)
	}
	if !strings.Contains(string(encoded), `"path_style":true`) {
		t.Fatalf("Connector response omitted the addressing decision: %s", encoded)
	}
}

func TestConnectorCreateRequestIncludesPathStyleDecision(t *testing.T) {
	// Rationale: S3-compatible endpoints do not expose portable addressing
	// behavior, so desired state must carry the operator's explicit decision.
	pathStyle := true
	encoded, err := json.Marshal(ConnectorCreateRequest{
		Name:      "backups",
		Kind:      "s3-compatible",
		Endpoint:  "https://objects.example.test",
		Bucket:    "groundplane-backups",
		Prefix:    "production/",
		Region:    "auto",
		PathStyle: &pathStyle,
	})
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}
	if !strings.Contains(string(encoded), `"path_style":true`) {
		t.Fatalf("Connector create request omitted the addressing decision: %s", encoded)
	}
}

func TestConnectorCreateRequestDistinguishesMissingPathStyle(t *testing.T) {
	// Rationale: explicit false selects virtual-hosted addressing; omission is
	// not an alias for false and must remain detectable by request validation.
	var request ConnectorCreateRequest
	if err := json.Unmarshal([]byte(`{"path_style":false}`), &request); err != nil {
		t.Fatalf("Unmarshal(explicit false) error: %v", err)
	}
	if request.PathStyle == nil || *request.PathStyle {
		t.Fatalf("Unmarshal(explicit false) path_style = %#v", request.PathStyle)
	}
	request = ConnectorCreateRequest{}
	if err := json.Unmarshal([]byte(`{}`), &request); err != nil {
		t.Fatalf("Unmarshal(missing) error: %v", err)
	}
	if request.PathStyle != nil {
		t.Fatalf("Unmarshal(missing) path_style = %#v, want nil", request.PathStyle)
	}
}

func TestBackupSourceKindsUseCanonicalValues(t *testing.T) {
	// Rationale: Blueprint, API, and Console must use the same closed source
	// vocabulary; adapter keys belong to referenced attaches, not source_kind.
	want := []BackupSourceKind{"attach", "volume", "config"}
	got := []BackupSourceKind{BackupSourceAttach, BackupSourceVolume, BackupSourceConfig}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("backup source kind %d = %q, want %q", index, got[index], want[index])
		}
	}
}
