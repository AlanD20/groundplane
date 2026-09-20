package connectors

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/s3connector"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	connectorRecordPrefix          = "/v1/records/connectors/"
	connectorCredentialValuePrefix = "/v1/secret-values/connectors/"
)

type Record struct {
	Connector core.Connector `json:"connector"`
}

// EncryptedCredentials is the Controller-key envelope for the JSON
// object containing only direct credential values. It is subordinate to the
// Connector and is committed at the same revision.
type EncryptedCredentials struct {
	ConnectorID      string `json:"connector_id"`
	EnvelopeVersion  uint8  `json:"envelope_version"`
	Cipher           string `json:"cipher"`
	DigestAlgorithm  string `json:"digest_algorithm"`
	CiphertextSHA256 string `json:"ciphertext_sha256"`
	Ciphertext       []byte `json:"ciphertext"`
}

func NewRecord(connector core.Connector) (Record, error) {
	connector.Endpoint = strings.TrimSuffix(connector.Endpoint, "/")
	if connector.Prefix != "" && !strings.HasSuffix(connector.Prefix, "/") {
		connector.Prefix += "/"
	}
	connector.Credentials = cloneConnectorCredentials(connector.Credentials)
	record := Record{Connector: connector}
	if err := ValidateRecord(record); err != nil {
		return Record{}, err
	}
	return record, nil
}

func NewEncryptedCredentials(
	connectorID string,
	ciphertext []byte,
) (EncryptedCredentials, error) {
	digest := sha256.Sum256(ciphertext)
	value := EncryptedCredentials{
		ConnectorID: connectorID, EnvelopeVersion: 1, Cipher: "age-x25519",
		DigestAlgorithm: "sha256", CiphertextSHA256: hex.EncodeToString(digest[:]),
		Ciphertext: append([]byte(nil), ciphertext...),
	}
	if err := ValidateEncryptedCredentials(value); err != nil {
		clear(value.Ciphertext)
		return EncryptedCredentials{}, err
	}
	return value, nil
}

func RecordKey(connectorID string) string { return connectorRecordPrefix + connectorID }

func CredentialValueKey(connectorID string) string {
	return connectorCredentialValuePrefix + connectorID
}

func EncodeRecord(record Record) ([]byte, error) {
	if err := ValidateRecord(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("connector", record)
}

func DecodeRecord(value []byte) (Record, error) {
	record, err := recordcodec.Decode[Record](value, "connector")
	if err != nil || ValidateRecord(record) != nil {
		return Record{}, CorruptRecord()
	}
	record.Connector.Credentials = cloneConnectorCredentials(record.Connector.Credentials)
	return record, nil
}

func HasDirectCredentials(record Record) bool {
	for _, credential := range record.Connector.Credentials {
		if credential.Kind == core.ConnectorCredentialDirect {
			return true
		}
	}
	return false
}

func EncodeEncryptedCredentials(value EncryptedCredentials) ([]byte, error) {
	if err := ValidateEncryptedCredentials(value); err != nil {
		return nil, err
	}
	copyOfValue := value
	copyOfValue.Ciphertext = append([]byte(nil), value.Ciphertext...)
	return recordcodec.Encode("connector_credentials", copyOfValue)
}

func DecodeEncryptedCredentials(value []byte) (EncryptedCredentials, error) {
	record, err := recordcodec.Decode[EncryptedCredentials](value, "connector_credentials")
	if err != nil || ValidateEncryptedCredentials(record) != nil {
		clear(record.Ciphertext)
		return EncryptedCredentials{}, CorruptRecord()
	}
	return record, nil
}

func ValidateRecord(record Record) error {
	connector := record.Connector
	if recordcodec.ValidateID(ids.KindConnector, connector.ID) != nil {
		return errs.New(errs.KindValidationFailed, "Connector stable identity is invalid")
	}
	if recordcodec.ValidateID(ids.KindEnvironment, connector.EnvironmentID) != nil {
		return errs.New(errs.KindConnectorScopeInvalid, "Connector Environment owner is invalid")
	}
	if err := recordcodec.ValidateLabel("Connector name", connector.Name); err != nil {
		return err
	}
	if connector.Kind != core.ConnectorKindS3Compatible {
		return errs.New(errs.KindValidationFailed, "Connector kind must be s3-compatible")
	}
	if err := validateConnectorEndpoint(connector.Endpoint); err != nil {
		return err
	}
	if !validConnectorBucket(connector.Bucket) {
		return errs.New(errs.KindValidationFailed, "Connector bucket is invalid")
	}
	if err := validateConnectorPrefix(connector.Prefix); err != nil {
		return err
	}
	if !validConnectorRegion(connector.Region) {
		return errs.New(errs.KindValidationFailed, "Connector region is invalid")
	}
	if len(connector.Credentials) != 2 {
		return errs.New(
			errs.KindValidationFailed,
			"Connector credentials must contain access_key and secret_key",
		)
	}
	for _, name := range []core.ConnectorCredentialName{
		core.ConnectorCredentialAccessKey, core.ConnectorCredentialSecretKey,
	} {
		credential, ok := connector.Credentials[name]
		if !ok {
			return errs.New(
				errs.KindValidationFailed,
				"Connector credentials must contain access_key and secret_key",
			)
		}
		switch credential.Kind {
		case core.ConnectorCredentialSecretRef:
			if !secretrecord.ValidEnvironmentKey(credential.SecretRef) {
				return errs.New(
					errs.KindValidationFailed,
					"Connector credential secret_ref is invalid",
				)
			}
		case core.ConnectorCredentialDirect:
			if credential.SecretRef != "" {
				return errs.New(
					errs.KindValidationFailed,
					"Direct Connector credential must not have a secret_ref",
				)
			}
		default:
			return errs.New(errs.KindValidationFailed, "Connector credential kind is invalid")
		}
	}
	return nil
}

func ValidateEncryptedCredentials(value EncryptedCredentials) error {
	if recordcodec.ValidateID(ids.KindConnector, value.ConnectorID) != nil || value.EnvelopeVersion != 1 ||
		value.Cipher != "age-x25519" || value.DigestAlgorithm != "sha256" ||
		len(value.Ciphertext) == 0 ||
		len(value.Ciphertext) > recordcodec.MaximumValueBytes ||
		!recordcodec.ValidSHA256(value.CiphertextSHA256) {
		return errs.New(
			errs.KindValidationFailed,
			"Connector encrypted credential envelope is invalid",
		)
	}
	digest := sha256.Sum256(value.Ciphertext)
	want, _ := hex.DecodeString(value.CiphertextSHA256)
	if subtle.ConstantTimeCompare(digest[:], want) != 1 {
		return errs.New(
			errs.KindValidationFailed,
			"Connector encrypted credential digest does not match",
		)
	}
	return nil
}

func validateConnectorEndpoint(value string) error {
	if !s3connector.ValidEndpoint(value) {
		return errs.New(errs.KindValidationFailed, "Connector endpoint is invalid")
	}
	return nil
}

func validConnectorBucket(value string) bool {
	return s3connector.ValidBucket(value)
}

func validateConnectorPrefix(value string) error {
	if !s3connector.ValidPrefix(value) {
		return errs.New(errs.KindValidationFailed, "Connector prefix is invalid")
	}
	return nil
}

func validConnectorRegion(value string) bool {
	return s3connector.ValidRegion(value)
}

func cloneConnectorCredentials(
	credentials map[core.ConnectorCredentialName]core.ConnectorCredential,
) map[core.ConnectorCredentialName]core.ConnectorCredential {
	if credentials == nil {
		return nil
	}
	copyOfCredentials := make(map[core.ConnectorCredentialName]core.ConnectorCredential, len(credentials))
	for name, credential := range credentials {
		copyOfCredentials[name] = credential
	}
	return copyOfCredentials
}

func CorruptRecord() error {
	return errs.New(errs.KindInternal, "Connector durable record is corrupt")
}
