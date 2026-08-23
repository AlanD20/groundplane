package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const scriptPrefix = "/v1/records/scripts/"

// ScriptRecord is the durable boundary for one Environment-scoped Script.
// ServiceID is stable; Desired.ServiceName is the target's public label projection.
type ScriptRecord struct {
	EnvironmentID string      `json:"environment_id"`
	ServiceID     string      `json:"service_id"`
	Desired       core.Script `json:"desired"`
}

func NewScriptRecord(environmentID string, serviceID string, desired core.Script) (ScriptRecord, error) {
	record := ScriptRecord{EnvironmentID: environmentID, ServiceID: serviceID, Desired: desired}
	if err := validateScriptRecord(record); err != nil {
		return ScriptRecord{}, err
	}
	return record, nil
}

// ReplaceScriptDesired changes authored behavior while preserving identity, ownership, and target.
func ReplaceScriptDesired(record ScriptRecord, desired core.Script) (ScriptRecord, error) {
	if err := validateScriptRecord(record); err != nil {
		return ScriptRecord{}, err
	}
	if desired.ID != record.Desired.ID || desired.Name != record.Desired.Name {
		return ScriptRecord{}, errs.New(
			errs.KindValidationFailed,
			"Script replacement changed immutable identity or name",
		)
	}
	replacement := record
	replacement.Desired = desired
	if err := validateScriptRecord(replacement); err != nil {
		return ScriptRecord{}, err
	}
	return replacement, nil
}

func scriptKey(id string) string { return scriptPrefix + id }

func scriptOwnerPrefix(environmentID string) string {
	return "/v1/indexes/scripts/by-owner/environment/" + environmentID + "/"
}

func scriptOwnerKey(environmentID string, scriptID string) string {
	return scriptOwnerPrefix(environmentID) + scriptID
}

func scriptNameKey(environmentID string, name string) string {
	return "/v1/indexes/scripts/by-name/environment/" + environmentID + "/" + encodeDynamicSegment(name)
}

func validateScriptRecord(record ScriptRecord) error {
	if err := validateID(ids.KindEnvironment, record.EnvironmentID); err != nil {
		return err
	}
	if err := validateID(ids.KindService, record.ServiceID); err != nil {
		return err
	}
	if err := validateID(ids.KindScript, record.Desired.ID); err != nil {
		return err
	}
	if err := record.Desired.Validate(); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	return nil
}

func encodeScriptRecord(record ScriptRecord) ([]byte, error) {
	if err := validateScriptRecord(record); err != nil {
		return nil, err
	}
	return encodeEnvelope("script", record)
}

func decodeScriptRecord(value []byte) (ScriptRecord, error) {
	record, err := decodeEnvelope[ScriptRecord](value, "script")
	if err != nil {
		return ScriptRecord{}, err
	}
	if err := validateScriptRecord(record); err != nil {
		return ScriptRecord{}, corruptRecord()
	}
	return record, nil
}
