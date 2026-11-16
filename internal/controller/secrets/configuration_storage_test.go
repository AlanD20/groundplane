package secrets

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// A secret's display classification must not override the explicitly selected
// storage mode, including empty plaintext values and future value replacements.
func TestConfigurationSensitivityAndEncryptionAreIndependent(t *testing.T) {
	protector, err := secretvalue.NewProtector(configurationStorageCipher{}, configurationStorageCipher{})
	if err != nil {
		t.Fatal(err)
	}
	for _, sensitive := range []bool{false, true} {
		for _, mode := range []string{"age", "none"} {
			for _, input := range []string{"", "configuration-value"} {
				record, err := prepareSecretCreation(apiTypes.SecretCreateRequest{
					Platform: true, Key: "TEST_VALUE", Kind: "env_var", Value: input,
					Sensitive: &sensitive, Encryption: mode,
				}, time.Now().UTC())
				if err != nil {
					t.Fatal(err)
				}
				value, err := storeConfigurationValue(
					context.Background(),
					protector,
					record.Secret.ID,
					record.Secret.Encryption,
					[]byte(input),
				)
				if err != nil {
					t.Fatal(err)
				}
				if err := secretrecord.ValidateSecretValueBinding(record, value); err != nil {
					t.Fatal(err)
				}
				public := secretCreationResponse(record)
				if public.Sensitive != sensitive || public.Encryption != mode {
					t.Fatal("classification changed the requested storage mode")
				}
				if mode == "none" && string(value.Ciphertext) != input {
					t.Fatal("explicit plaintext storage was encrypted")
				}
				wrong := record
				if mode == "none" {
					wrong.Secret.Encryption = "age"
				} else {
					wrong.Secret.Encryption = "none"
				}
				if secretrecord.ValidateSecretValueBinding(wrong, value) == nil {
					t.Fatal("metadata/payload encryption mismatch was accepted")
				}
				clear(value.Ciphertext)
			}
		}
	}
}

type configurationStorageCipher struct{}

func (configurationStorageCipher) Seal(_ context.Context, value []byte) ([]byte, error) {
	return append([]byte("sealed:"), value...), nil
}

func (configurationStorageCipher) Open(_ context.Context, value []byte) ([]byte, error) {
	return append([]byte(nil), value[len("sealed:"):]...), nil
}
