// Package tasksecretpinrecord owns the Secret-only membership record that
// prevents deletion while a recoverable operation retains an exact value.
package tasksecretpinrecord

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const Prefix = "/v1/runtime/secret-task-pins/"

// Record identifies exact Secret metadata and ciphertext without retaining
// value bytes. OperationID is the recovery authority that owns the pin.
type Record struct {
	OperationID      string `json:"operation_id"`
	SecretID         string `json:"secret_id"`
	MetadataRevision int64  `json:"metadata_revision"`
	CiphertextSHA256 string `json:"ciphertext_sha256"`
}

func SecretPrefix(secretID string) string { return Prefix + secretID + "/" }

func Key(secretID, operationID string) string { return SecretPrefix(secretID) + operationID }

func Validate(record Record) error {
	if ids.Validate(ids.KindOperation, record.OperationID) != nil ||
		ids.Validate(ids.KindSecret, record.SecretID) != nil ||
		record.MetadataRevision <= 0 || !validSHA256(record.CiphertextSHA256) {
		return errs.New(errs.KindValidationFailed, "secret recovery pin is invalid")
	}
	return nil
}

func Encode(record Record) ([]byte, error) {
	if err := Validate(record); err != nil {
		return nil, err
	}
	value, err := json.Marshal(record)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return value, nil
}

func Decode(value []byte) (Record, error) {
	if len(value) == 0 || len(value) > 1024 {
		return Record{}, Corrupt()
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var record Record
	if err := decoder.Decode(&record); err != nil {
		return Record{}, Corrupt()
	}
	var trailing struct{}
	if err := decoder.Decode(&trailing); err != io.EOF || Validate(record) != nil {
		return Record{}, Corrupt()
	}
	canonical, err := Encode(record)
	if err != nil || !bytes.Equal(canonical, value) {
		return Record{}, Corrupt()
	}
	return record, nil
}

func Corrupt() error {
	return errs.New(errs.KindInternal, "secret recovery pin record is corrupt")
}

func validSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == value
}
