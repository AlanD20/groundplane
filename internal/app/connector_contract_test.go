package app

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestPrepareConnectorCreationSeparatesDirectValuesFromRedactedMetadata(t *testing.T) {
	// Rationale: plaintext belongs only to the transient encrypted bundle; the
	// durable primary and every read projection must retain source metadata only.
	pathStyle := false
	record, direct, err := prepareConnectorCreation(
		ids.NewAt(ids.KindConnector, time.Date(2026, 8, 23, 17, 0, 0, 0, time.UTC), 1),
		ids.NewAt(ids.KindEnvironment, time.Date(2026, 8, 23, 17, 0, 0, 0, time.UTC), 2),
		apiTypes.ConnectorCreateRequest{
			Name: "primary-backups", Kind: "s3-compatible",
			Endpoint: "https://objects.example.test/", Bucket: "groundplane-backups",
			Prefix: "production", Region: "auto", PathStyle: &pathStyle,
			Credentials: map[string]apiTypes.ConnectorCredentialInput{
				string(core.ConnectorCredentialAccessKey): {SecretRef: "S3_ACCESS_KEY"},
				string(core.ConnectorCredentialSecretKey): {Value: "direct-secret"},
			},
		},
	)
	if err != nil {
		t.Fatalf("prepareConnectorCreation() error = %v", err)
	}
	if record.Connector.Endpoint != "https://objects.example.test" ||
		record.Connector.Prefix != "production/" || record.Connector.PathStyle {
		t.Fatalf("prepareConnectorCreation() record = %#v", record)
	}
	if len(direct) != 1 || direct[string(core.ConnectorCredentialSecretKey)] != "direct-secret" {
		t.Fatalf("prepareConnectorCreation() direct = %#v", direct)
	}
	response := connectorResponse(record)
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if strings.Contains(string(encoded), "direct-secret") ||
		response.Credentials[string(core.ConnectorCredentialAccessKey)].SecretRef != "S3_ACCESS_KEY" ||
		response.Credentials[string(core.ConnectorCredentialSecretKey)].Kind != apiTypes.ConnectorCredentialDirect {
		t.Fatalf("connectorResponse() = %s", encoded)
	}
}

func TestPrepareConnectorCreationRejectsIncompleteDecisions(t *testing.T) {
	valid := apiTypes.ConnectorCreateRequest{
		Name: "primary-backups", Kind: "s3-compatible",
		Endpoint: "https://objects.example.test", Bucket: "groundplane-backups",
		Region: "auto", Credentials: map[string]apiTypes.ConnectorCredentialInput{
			string(core.ConnectorCredentialAccessKey): {SecretRef: "S3_ACCESS_KEY"},
			string(core.ConnectorCredentialSecretKey): {Value: "direct-secret"},
		},
	}
	environmentID := ids.NewAt(
		ids.KindEnvironment, time.Date(2026, 8, 23, 18, 0, 0, 0, time.UTC), 10,
	)
	connectorID := ids.NewAt(ids.KindConnector, time.Date(2026, 8, 23, 18, 0, 0, 0, time.UTC), 11)
	pathStyle := true
	tests := []struct {
		name   string
		mutate func(*apiTypes.ConnectorCreateRequest)
	}{
		{name: "missing path style", mutate: func(_ *apiTypes.ConnectorCreateRequest) {}},
		{name: "missing credential", mutate: func(input *apiTypes.ConnectorCreateRequest) {
			input.PathStyle = &pathStyle
			delete(input.Credentials, string(core.ConnectorCredentialSecretKey))
		}},
		{name: "extra credential", mutate: func(input *apiTypes.ConnectorCreateRequest) {
			input.PathStyle = &pathStyle
			input.Credentials["session_token"] = apiTypes.ConnectorCredentialInput{Value: "value"}
		}},
		{name: "ambiguous credential", mutate: func(input *apiTypes.ConnectorCreateRequest) {
			input.PathStyle = &pathStyle
			input.Credentials[string(core.ConnectorCredentialSecretKey)] = apiTypes.ConnectorCredentialInput{
				SecretRef: "S3_SECRET_KEY", Value: "value",
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := valid
			input.Credentials = make(map[string]apiTypes.ConnectorCredentialInput, len(valid.Credentials))
			for name, credential := range valid.Credentials {
				input.Credentials[name] = credential
			}
			test.mutate(&input)
			_, direct, err := prepareConnectorCreation(connectorID, environmentID, input)
			clearConnectorDirectValues(direct)
			if kind, ok := errs.KindOf(err); !ok || kind != errs.KindValidationFailed {
				t.Fatalf("prepareConnectorCreation() error = %v", err)
			}
		})
	}
}
