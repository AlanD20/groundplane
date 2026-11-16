package secrets

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func configurationEncryption(input apiTypes.SecretCreateRequest) string {
	if input.Encryption == "" {
		return "age"
	}
	return input.Encryption
}

func configurationSensitive(input apiTypes.SecretCreateRequest) bool {
	return input.Sensitive == nil || *input.Sensitive
}

func storeConfigurationValue(
	ctx context.Context,
	protector *secretvalue.Protector,
	id, encryption string,
	plaintext []byte,
) (secretrecord.StoredValue, error) {
	if ctx == nil {
		return secretrecord.StoredValue{}, errs.New(errs.KindInternal, "Configuration value context is required")
	}
	var envelope secretvalue.Envelope
	var err error
	switch encryption {
	case "age":
		envelope, err = protector.Seal(ctx, plaintext)
	case "none":
		if err = ctx.Err(); err == nil {
			envelope, err = secretvalue.Plain(plaintext)
		}
	default:
		return secretrecord.StoredValue{}, errs.New(
			errs.KindValidationFailed,
			"Configuration encryption must be age or none",
		)
	}
	if err != nil {
		return secretrecord.StoredValue{}, err
	}
	defer envelope.Clear()
	metadata := envelope.Metadata()
	return secretrecord.StoredValue{
		SecretID: id, EnvelopeVersion: uint8(metadata.Version),
		Cipher: string(metadata.Cipher), DigestAlgorithm: string(metadata.Digest.Algorithm),
		CiphertextSHA256: metadata.Digest.Value, Ciphertext: envelope.Ciphertext(),
	}, nil
}
