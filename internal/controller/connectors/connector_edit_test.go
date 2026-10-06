package connectors

import (
	"context"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"testing"
)

// CON-09: replacing one credential must preserve the other encrypted value,
// remove a switched direct value, and never mutate the caller's metadata map.
func TestConnectorEditPreservesOmittedCredentialAndRemovesReplacedDirectValue(t *testing.T) {
	protector, err := secretvalue.NewProtector(connectorCreationCipher{}, connectorCreationCipher{})
	if err != nil {
		t.Fatal(err)
	}
	service := connectorMutationService{connectorCreationService: &connectorCreationService{protector: protector}}
	_, environmentID := testConnectorCreationRepository(t, core.SecretKindEnvVar)
	pathStyle := true
	current, _, err := prepareConnectorCreation(
		"con_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		environmentID,
		apiTypes.ConnectorCreateRequest{
			Name: "backups", Kind: "s3-compatible", Endpoint: "https://storage.example.test", Bucket: "backups", Region: "auto", PathStyle: &pathStyle,
			Credentials: map[string]apiTypes.ConnectorCredentialInput{
				"access_key": {Value: "original-access"},
				"secret_key": {Value: "original-secret"},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	encrypted, err := connectorrecord.NewEncryptedCredentials(
		current.Connector.ID,
		[]byte(`sealed:{"access_key":"original-access","secret_key":"original-secret"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	changes := map[string]apiTypes.ConnectorCredentialInput{"secret_key": {Value: "replacement-secret"}}
	replacement, err := service.editCredentials(context.Background(), current, encrypted, changes)
	if err != nil {
		t.Fatal(err)
	}
	direct := map[string]string{}
	if err := json.Unmarshal(replacement.Ciphertext[len("sealed:"):], &direct); err != nil {
		t.Fatal(err)
	}
	if direct["access_key"] != "original-access" || direct["secret_key"] != "replacement-secret" {
		t.Fatal("rotation lost or reused a credential")
	}
	changes["secret_key"] = apiTypes.ConnectorCredentialInput{SecretRef: "BACKUP_SECRET"}
	record, err := applyConnectorEdit(current, apiTypes.ConnectorEditRequest{Credentials: changes})
	if err != nil {
		t.Fatal(err)
	}
	if current.Connector.Credentials[core.ConnectorCredentialSecretKey].Kind != core.ConnectorCredentialDirect ||
		record.Connector.Credentials[core.ConnectorCredentialSecretKey].Kind != core.ConnectorCredentialSecretRef {
		t.Fatal("edit mutated original credential metadata")
	}
	replacement, err = service.editCredentials(context.Background(), current, encrypted, changes)
	if err != nil {
		t.Fatal(err)
	}
	direct = map[string]string{}
	if err := json.Unmarshal(replacement.Ciphertext[len("sealed:"):], &direct); err != nil {
		t.Fatal(err)
	}
	if direct["access_key"] != "original-access" || len(direct) != 1 {
		t.Fatal("replaced direct credential remained in encrypted bundle")
	}
}
