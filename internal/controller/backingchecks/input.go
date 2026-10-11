package backingchecks

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingaccess"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	checkrecord "github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SealInput is called inside the captured Attach fact callback. Only ciphertext
// crosses into storage; the safe Source must describe that exact private input.
func SealInput(ctx context.Context, protector *secretvalue.Protector, owner checkrecord.Owner,
	source checkrecord.Source, input backingaccess.Request, createdAt time.Time,
) (checkrecord.Record, error) {
	if protector == nil || !sameParameters(source.Parameters, input) || len(source.Parameters.Password) != 0 {
		return checkrecord.Record{}, errs.New(
			errs.KindValidationFailed,
			"Backing access input differs from its captured configuration",
		)
	}
	value, err := backingaccess.MarshalRequest(input)
	if err != nil {
		return checkrecord.Record{}, err
	}
	defer clear(value)
	envelope, err := protector.Seal(ctx, value)
	if err != nil {
		return checkrecord.Record{}, err
	}
	defer envelope.Clear()
	metadata := envelope.Metadata()
	if metadata.Version != secretvalue.EnvelopeVersion1 || metadata.Cipher != secretvalue.CipherSuiteAgeX25519 ||
		metadata.Digest.Algorithm != secretvalue.DigestAlgorithmSHA256 {
		return checkrecord.Record{}, errs.New(errs.KindInternal, "Backing access credential envelope is unsupported")
	}
	source.CredentialDigest = metadata.Digest.Value
	ciphertext := envelope.Ciphertext()
	defer clear(ciphertext)
	return checkrecord.NewRecord(owner, source, ciphertext, createdAt)
}

func (executor *Executor) withInput(
	ctx context.Context,
	record checkrecord.Record,
	consume func(backingaccess.Request) error,
) error {
	if record.Phase != checkrecord.Ready || len(record.EncryptedInput) == 0 {
		return errs.New(errs.KindStateConflict, "Backing access check has released its private input")
	}
	envelope, err := secretvalue.Restore(secretvalue.Metadata{
		Version: secretvalue.EnvelopeVersion1, Cipher: secretvalue.CipherSuiteAgeX25519,
		Digest: secretvalue.Digest{Algorithm: secretvalue.DigestAlgorithmSHA256, Value: record.Source.CredentialDigest},
	}, record.EncryptedInput)
	if err != nil {
		return err
	}
	defer envelope.Clear()
	return executor.protector.OpenOwned(ctx, &envelope, func(value []byte) error {
		input, err := backingaccess.ReadRequest(bytes.NewReader(value))
		if err != nil {
			return errs.New(errs.KindInternal, "captured Backing access request is invalid")
		}
		defer input.Clear()
		if !sameParameters(record.Source.Parameters, input) {
			return errs.New(errs.KindStateConflict, "captured Backing access credentials changed their configuration")
		}
		return consume(input)
	})
}

func sameParameters(expected, actual backingaccess.Request) bool {
	return expected.AttachID == actual.AttachID && expected.CredentialAttachID == actual.CredentialAttachID &&
		expected.EnvironmentID == actual.EnvironmentID && expected.ServiceID == actual.ServiceID &&
		expected.BackingServiceID == actual.BackingServiceID && expected.Family == actual.Family &&
		expected.Authentication == actual.Authentication && expected.Host == actual.Host && expected.Port == actual.Port &&
		expected.Database == actual.Database && expected.Username == actual.Username
}
