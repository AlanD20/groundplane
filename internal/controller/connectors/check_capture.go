package connectors

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorchecks"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *CheckService) captureCredentials(ctx context.Context, source etcd.ConnectorCheckSource,
	operationID string, replacements map[string]string,
) (connectorrecord.EncryptedCredentials, []connectorchecks.SecretSource, error) {
	pair := checkCredentialPair{}
	defer func() { pair.AccessKey, pair.SecretKey = "", "" }()
	if encrypted := source.DirectCredentials(); encrypted != nil {
		defer clear(encrypted.Ciphertext)
		err := service.openCapturedValue(ctx, encrypted.EnvelopeVersion, encrypted.Cipher, encrypted.DigestAlgorithm,
			encrypted.CiphertextSHA256, encrypted.Ciphertext, func(plaintext []byte) error {
				decoder := json.NewDecoder(bytes.NewReader(plaintext))
				decoder.DisallowUnknownFields()
				if err := decoder.Decode(&pair); err != nil {
					return errs.New(errs.KindInternal, "connector direct credentials are corrupt")
				}
				var trailing json.RawMessage
				if err := decoder.Decode(&trailing); err != io.EOF {
					clear(trailing)
					return errs.New(errs.KindInternal, "connector direct credentials contain trailing data")
				}
				return nil
			})
		if err != nil {
			return connectorrecord.EncryptedCredentials{}, nil, err
		}
	}
	connector := source.Connector().Connector
	for _, name := range []core.ConnectorCredentialName{core.ConnectorCredentialAccessKey, core.ConnectorCredentialSecretKey} {
		value := &pair.AccessKey
		if name == core.ConnectorCredentialSecretKey {
			value = &pair.SecretKey
		}
		if connector.Credentials[name].Kind == core.ConnectorCredentialSecretRef {
			*value = ""
			continue
		}
		if replacement, found := replacements[string(name)]; found {
			*value = replacement
		}
	}
	sources, values := source.SecretValues(operationID)
	defer func() {
		for index := range values {
			clear(values[index].Ciphertext)
		}
	}()
	for index, encrypted := range values {
		err := service.openCapturedValue(ctx, encrypted.EnvelopeVersion, encrypted.Cipher, encrypted.DigestAlgorithm,
			encrypted.CiphertextSHA256, encrypted.Ciphertext, func(plaintext []byte) error {
				if sources[index].Name == string(core.ConnectorCredentialAccessKey) {
					pair.AccessKey = string(plaintext)
				} else {
					pair.SecretKey = string(plaintext)
				}
				return nil
			})
		if err != nil {
			return connectorrecord.EncryptedCredentials{}, nil, err
		}
	}
	if pair.AccessKey == "" || pair.SecretKey == "" {
		return connectorrecord.EncryptedCredentials{}, nil, errs.New(
			errs.KindValidationFailed,
			"connector check requires non-empty access_key and secret_key values",
		)
	}
	plaintext, err := json.Marshal(pair)
	if err != nil {
		return connectorrecord.EncryptedCredentials{}, nil, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(plaintext)
	envelope, err := service.protector.Seal(ctx, plaintext)
	if err != nil {
		return connectorrecord.EncryptedCredentials{}, nil, err
	}
	defer envelope.Clear()
	ciphertext := envelope.Ciphertext()
	defer clear(ciphertext)
	encrypted, err := connectorrecord.NewEncryptedCredentials(connector.ID, ciphertext)
	return encrypted, sources, err
}

func (service *CheckService) openCapturedValue(ctx context.Context, version uint8,
	cipher, algorithm, digest string, ciphertext []byte, use func([]byte) error,
) error {
	envelope, err := secretvalue.Restore(secretvalue.Metadata{Version: secretvalue.EnvelopeVersion(version),
		Cipher: secretvalue.CipherSuite(
			cipher,
		), Digest: secretvalue.Digest{Algorithm: secretvalue.DigestAlgorithm(algorithm), Value: digest}}, ciphertext)
	if err != nil {
		return err
	}
	defer envelope.Clear()
	return service.protector.OpenOwned(ctx, &envelope, use)
}
