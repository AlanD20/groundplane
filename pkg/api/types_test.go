package api

import (
	"encoding/json"
	"strings"
	"testing"
)

// QA: SVC-01, SVC-02, UI-01 - L0 response-model encoding only; no lifecycle
// action, persisted intent, Blueprint preservation, or runtime effect is exercised.
// Rationale: Service responses must expose runtime_intent with exact snake_case
// JSON so lifecycle state cannot disappear into a desired-service projection.
func TestServiceRuntimeIntentJSON(t *testing.T) {
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

// QA: CON-04, SEC-05, UI-01 - L0 response-model encoding only; no Controller
// redaction, encryption at rest, Secret resolution, or HTTP response is exercised.
// Rationale: a Connector response must preserve direct-source metadata and the
// addressing decision without acquiring the write-only plaintext input field.
func TestConnectorResponseOmitsDirectCredentialValue(t *testing.T) {
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
	const want = `{"id":"con_x","environment_id":"env_x","name":"backups","kind":"s3-compatible",` +
		`"endpoint":"","bucket":"","region":"","path_style":true,"credentials":{"secret_key":{"kind":"direct"}}}`
	if string(encoded) != want {
		t.Fatalf("Connector response = %s, want %s", encoded, want)
	}
}

// QA: CON-02, CON-03, UI-01 - L0 request-model encoding only; no endpoint
// validation, provider client, protected create, or durable Connector is exercised.
// Rationale: S3-compatible endpoints do not expose portable addressing behavior,
// so desired state must carry the operator's explicit decision.
func TestConnectorCreateRequestIncludesPathStyleDecision(t *testing.T) {
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

// QA: CON-03, UI-03 - L0 request-model presence proof only; no HTTP admission,
// default rejection, persistence, or object-store access is exercised.
// Rationale: explicit false selects virtual-hosted addressing; omission is not
// an alias for false and must remain detectable by request validation.
func TestConnectorCreateRequestDistinguishesMissingPathStyle(t *testing.T) {
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
