package api

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type backupPolicyReplacementWire struct {
	Enabled     *bool           `json:"enabled"`
	Frequency   json.RawMessage `json:"frequency"`
	Keep        json.RawMessage `json:"keep"`
	Encryption  json.RawMessage `json:"encryption"`
	ConnectorID json.RawMessage `json:"connector_id"`
	Sources     json.RawMessage `json:"sources"`
}

func (request *BackupPolicyReplacementRequest) UnmarshalJSON(value []byte) error {
	if request == nil {
		return errs.New(errs.KindMalformedRequest, "backup policy request is nil")
	}
	if err := rejectDuplicateJSONMembers(value); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var wire backupPolicyReplacementWire
	if err := decoder.Decode(&wire); err != nil {
		return errs.New(errs.KindMalformedRequest, "backup policy request is malformed")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errs.New(errs.KindMalformedRequest, "backup policy request must contain one JSON document")
	}
	if wire.Enabled == nil || len(wire.Sources) == 0 || bytes.Equal(bytes.TrimSpace(wire.Sources), []byte("null")) {
		return errs.New(errs.KindMalformedRequest, "backup policy enabled and non-null sources are required")
	}
	decoded := BackupPolicyReplacementRequest{Enabled: *wire.Enabled}
	if err := decodeOptionalBackupPolicyField(wire.Frequency, &decoded.Frequency, "frequency"); err != nil {
		return err
	}
	if err := decodeOptionalBackupPolicyField(wire.Keep, &decoded.Keep, "keep"); err != nil {
		return err
	}
	if err := decodeOptionalBackupPolicyField(wire.Encryption, &decoded.Encryption, "encryption"); err != nil {
		return err
	}
	if err := decodeOptionalBackupPolicyField(wire.ConnectorID, &decoded.ConnectorID, "connector_id"); err != nil {
		return err
	}
	sourceDecoder := json.NewDecoder(bytes.NewReader(wire.Sources))
	sourceDecoder.DisallowUnknownFields()
	if err := sourceDecoder.Decode(&decoded.Sources); err != nil || decoded.Sources == nil {
		return errs.New(errs.KindMalformedRequest, "backup policy sources are malformed")
	}
	if err := sourceDecoder.Decode(&struct{}{}); err != io.EOF {
		return errs.New(errs.KindMalformedRequest, "backup policy sources are malformed")
	}
	*request = decoded
	return nil
}

func decodeOptionalBackupPolicyField[T any](raw json.RawMessage, target *T, name string) error {
	if len(raw) == 0 {
		return nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, target) != nil {
		return errs.New(errs.KindMalformedRequest, "backup policy "+name+" is malformed")
	}
	return nil
}
