package scripts

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type SetGenerationRecord struct {
	EnvironmentID string `json:"environment_id"`
	GenerationID  string `json:"generation_id"`
}

type LocatorRecord struct {
	ScriptID      string `json:"script_id"`
	EnvironmentID string `json:"environment_id"`
}

type storedScriptDesired struct {
	ID          string                `json:"id"`
	Slug        string                `json:"slug"`
	ServiceName string                `json:"service"`
	When        core.ScriptHook       `json:"when"`
	Order       uint16                `json:"order,omitempty"`
	Execution   *core.ScriptExecution `json:"execution,omitempty"`
}

type StoredRecord struct {
	EnvironmentID       string              `json:"environment_id"`
	ServiceID           string              `json:"service_id"`
	Origin              string              `json:"origin"`
	ReconciliationKey   string              `json:"reconciliation_key,omitempty"`
	ActiveGeneration    uint64              `json:"active_generation"`
	ActiveReferences    uint64              `json:"active_references"`
	ScriptSetGeneration string              `json:"script_set_generation"`
	Desired             storedScriptDesired `json:"desired"`
}

// BodyGenerationRecord is one append-only Script body generation.
type BodyGenerationRecord struct {
	ScriptID   string `json:"script_id"`
	Generation uint64 `json:"generation"`
	BodySize   uint32 `json:"body_size"`
	Body       string `json:"script"`
	BodySHA256 string `json:"body_sha256"`
}

// Record is the durable boundary for one Environment-scoped Script.
// ServiceID is stable; Desired.ServiceName is the target's public label projection.
type Record struct {
	EnvironmentID       string      `json:"environment_id"`
	ServiceID           string      `json:"service_id"`
	Origin              string      `json:"origin"`
	ReconciliationKey   string      `json:"reconciliation_key,omitempty"`
	ActiveGeneration    uint64      `json:"active_generation"`
	ActiveReferences    uint64      `json:"active_references"`
	ScriptSetGeneration string      `json:"script_set_generation"`
	Desired             core.Script `json:"desired"`
}

func NewRecord(environmentID string, serviceID string, desired core.Script) (Record, error) {
	record := Record{
		EnvironmentID: environmentID, ServiceID: serviceID, Origin: "api",
		ActiveGeneration: 1, Desired: desired,
	}
	if err := ValidateRecord(record); err != nil {
		return Record{}, err
	}
	return record, nil
}

// ReplaceDesired changes authored behavior while preserving identity, ownership, and target.
func ReplaceDesired(record Record, desired core.Script) (Record, error) {
	if err := ValidateRecord(record); err != nil {
		return Record{}, err
	}
	if desired.ID != record.Desired.ID {
		return Record{}, errs.New(
			errs.KindValidationFailed,
			"Script replacement changed immutable identity",
		)
	}
	replacement := record
	replacement.Desired = desired
	if desired.Body != record.Desired.Body {
		if record.ActiveGeneration == ^uint64(0) {
			return Record{}, errs.New(errs.KindStateConflict, "Script generation is exhausted")
		}
		replacement.ActiveGeneration++
	}
	if err := ValidateRecord(replacement); err != nil {
		return Record{}, err
	}
	return replacement, nil
}

func ValidateRecord(record Record) error {
	if err := ValidateMetadata(record); err != nil {
		return err
	}
	if err := core.ValidateScriptBody(record.Desired.Body); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	return nil
}

func ValidateMetadata(record Record) error {
	if err := recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID); err != nil {
		return err
	}
	if err := recordcodec.ValidateID(ids.KindService, record.ServiceID); err != nil {
		return err
	}
	if err := recordcodec.ValidateID(ids.KindScript, record.Desired.ID); err != nil {
		return err
	}
	if err := record.Desired.ValidateMetadata(); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	if record.Origin != "api" && record.Origin != "blueprint" ||
		record.Origin == "api" && record.ReconciliationKey != "" ||
		record.Origin == "blueprint" && record.ReconciliationKey == "" || record.ActiveGeneration == 0 {
		return errs.New(errs.KindValidationFailed, "Script origin or generation is invalid")
	}
	return nil
}

func EncodeRecord(record Record) ([]byte, error) {
	if err := ValidateRecord(record); err != nil {
		return nil, err
	}
	stored := StoredRecord{
		EnvironmentID: record.EnvironmentID, ServiceID: record.ServiceID, Origin: record.Origin,
		ReconciliationKey: record.ReconciliationKey, ActiveGeneration: record.ActiveGeneration,
		ActiveReferences: record.ActiveReferences, ScriptSetGeneration: record.ScriptSetGeneration,
		Desired: storedScriptDesired{
			ID: record.Desired.ID, Slug: record.Desired.Slug,
			ServiceName: record.Desired.ServiceName, When: record.Desired.When, Order: record.Desired.Order,
			Execution: record.Desired.Execution,
		},
	}
	return recordcodec.Encode("script", stored)
}

func DecodeRecord(value []byte) (Record, error) {
	stored, err := recordcodec.Decode[StoredRecord](value, "script")
	if err != nil {
		return Record{}, err
	}
	record := Record{
		EnvironmentID: stored.EnvironmentID, ServiceID: stored.ServiceID, Origin: stored.Origin,
		ReconciliationKey: stored.ReconciliationKey, ActiveGeneration: stored.ActiveGeneration,
		ActiveReferences: stored.ActiveReferences, ScriptSetGeneration: stored.ScriptSetGeneration,
		Desired: core.Script{
			ID: stored.Desired.ID, Slug: stored.Desired.Slug,
			ServiceName: stored.Desired.ServiceName, When: stored.Desired.When, Order: stored.Desired.Order,
			Execution: stored.Desired.Execution,
		},
	}
	if err := ValidateMetadata(record); err != nil {
		return Record{}, recordcodec.CorruptRecord()
	}
	return record, nil
}

func ValidateScriptSetGeneration(record SetGenerationRecord) error {
	if recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID) != nil || record.GenerationID == "" ||
		len(record.GenerationID) > 128 || strings.Contains(record.GenerationID, "/") {
		return errs.New(errs.KindValidationFailed, "Script-set generation is invalid")
	}
	return nil
}

func EncodeScriptSetGeneration(record SetGenerationRecord) ([]byte, error) {
	if err := ValidateScriptSetGeneration(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("script_set_generation", record)
}

func DecodeScriptSetGeneration(value []byte) (SetGenerationRecord, error) {
	record, err := recordcodec.Decode[SetGenerationRecord](value, "script_set_generation")
	if err != nil || ValidateScriptSetGeneration(record) != nil {
		return SetGenerationRecord{}, recordcodec.CorruptRecord()
	}
	return record, nil
}

func EncodeScriptLocator(record LocatorRecord) ([]byte, error) {
	if recordcodec.ValidateID(ids.KindScript, record.ScriptID) != nil ||
		recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Script locator is invalid")
	}
	return recordcodec.Encode("script_locator", record)
}

func DecodeScriptLocator(value []byte) (LocatorRecord, error) {
	record, err := recordcodec.Decode[LocatorRecord](value, "script_locator")
	if err != nil || recordcodec.ValidateID(ids.KindScript, record.ScriptID) != nil ||
		recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID) != nil {
		return LocatorRecord{}, recordcodec.CorruptRecord()
	}
	return record, nil
}
