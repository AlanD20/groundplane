package connectors

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorchecks"
	"github.com/AlanD20/groundplane/internal/infra/s3compatible"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// The resolved pair is private encrypted Task input, not a new public credential
// format. It includes both direct and pinned Secret-backed values.
type checkCredentialPair struct {
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
}

func (executor *CheckExecutor) capturedCheckAdapter(
	ctx context.Context,
	record connectorchecks.Record,
) (*s3compatible.Adapter, error) {
	if record.Credentials == nil {
		return nil, errs.New(errs.KindStateConflict, "connector check has released its captured credentials")
	}
	encrypted := record.Credentials
	envelope, err := secretvalue.Restore(secretvalue.Metadata{
		Version: secretvalue.EnvelopeVersion(
			encrypted.EnvelopeVersion,
		), Cipher: secretvalue.CipherSuite(encrypted.Cipher),
		Digest: secretvalue.Digest{
			Algorithm: secretvalue.DigestAlgorithm(encrypted.DigestAlgorithm),
			Value:     encrypted.CiphertextSHA256,
		},
	}, encrypted.Ciphertext)
	if err != nil {
		return nil, err
	}
	defer envelope.Clear()
	connector := record.Connector.Connector
	var adapter *s3compatible.Adapter
	err = executor.protector.OpenOwned(ctx, &envelope, func(plaintext []byte) error {
		var pair checkCredentialPair
		defer func() { pair.AccessKey, pair.SecretKey = "", "" }()
		decoder := json.NewDecoder(bytes.NewReader(plaintext))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&pair); err != nil || pair.AccessKey == "" || pair.SecretKey == "" {
			return errs.New(errs.KindInternal, "connector check captured credential pair is invalid")
		}
		var trailing json.RawMessage
		if err := decoder.Decode(&trailing); err != io.EOF {
			clear(trailing)
			return errs.New(errs.KindInternal, "connector check captured credential pair has trailing data")
		}
		var err error
		adapter, err = s3compatible.New(s3compatible.Config{
			Endpoint: connector.Endpoint, Bucket: connector.Bucket, Prefix: connector.Prefix,
			Region: connector.Region, PathStyle: connector.PathStyle, AccessKey: pair.AccessKey, SecretKey: pair.SecretKey,
		})
		return err
	})
	return adapter, err
}
