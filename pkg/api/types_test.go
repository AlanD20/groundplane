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
		Name:          "api",
		Image:         "app:latest",
		RuntimeIntent: ServiceRuntimeIntentStopped,
	})
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}

	want := `{"id":"svc_x","name":"api","image":"app:latest","runtime_intent":"stopped"}`
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
