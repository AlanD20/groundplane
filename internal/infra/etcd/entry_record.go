package etcd

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const entryRecordPrefix = "/v1/records/entries/"

// EntryRecord is the durable metadata boundary for one environment Entry.
// CurrentValueGenerationID selects immutable bytes; secret bytes never enter
// this primary record.
type EntryRecord struct {
	EnvironmentID            string        `json:"environment_id"`
	Entry                    core.EnvEntry `json:"entry"`
	CurrentValueGenerationID string        `json:"current_value_generation_id"`
}

func NewEntryRecord(
	environmentID string,
	entry core.EnvEntry,
	valueGenerationID string,
) (EntryRecord, error) {
	record := EntryRecord{
		EnvironmentID: environmentID, Entry: entry, CurrentValueGenerationID: valueGenerationID,
	}
	if err := validateEntryRecord(record); err != nil {
		return EntryRecord{}, err
	}
	return record, nil
}

func ReplaceEntryDesired(
	current EntryRecord,
	desired core.EnvEntry,
	valueGenerationID string,
) (EntryRecord, error) {
	if err := validateEntryRecord(current); err != nil {
		return EntryRecord{}, err
	}
	if current.Entry.ID != desired.ID || current.Entry.Kind != desired.Kind ||
		current.Entry.Key != desired.Key || current.Entry.Path != desired.Path ||
		current.Entry.Secret != desired.Secret || !equalOptionalUint32(current.Entry.UID, desired.UID) ||
		!equalOptionalUint32(current.Entry.GID, desired.GID) {
		return EntryRecord{}, errs.New(
			errs.KindValidationFailed,
			"Entry edit changed immutable identity, destination, ownership, or storage class",
		)
	}
	return NewEntryRecord(current.EnvironmentID, desired, valueGenerationID)
}

func entryRecordKey(entryID string) string {
	return entryRecordPrefix + entryID
}

func encodeEntryRecord(record EntryRecord) ([]byte, error) {
	if err := validateEntryRecord(record); err != nil {
		return nil, err
	}
	return encodeEnvelope("entry", record)
}

func decodeEntryRecord(value []byte) (EntryRecord, error) {
	record, err := decodeEnvelope[EntryRecord](value, "entry")
	if err != nil || validateEntryRecord(record) != nil {
		return EntryRecord{}, corruptEntryRecord()
	}
	return record, nil
}

func validateEntryRecord(record EntryRecord) error {
	if validateStableID(ids.KindEnvironment, record.EnvironmentID) != nil ||
		validateStableID(ids.KindEnvEntry, record.Entry.ID) != nil ||
		validateStableID(ids.KindConfig, record.CurrentValueGenerationID) != nil {
		return errs.New(errs.KindValidationFailed, "Entry record identity is invalid")
	}
	if err := record.Entry.Validate(); err != nil {
		return errs.New(errs.KindValidationFailed, err.Error())
	}
	if record.Entry.Secret && record.Entry.Source.Kind == core.SourceLiteral &&
		record.Entry.Source.Literal != "" {
		return errs.New(errs.KindValidationFailed, "Entry secret literal leaked into durable metadata")
	}
	return nil
}

func equalEntryRecord(left EntryRecord, right EntryRecord) bool {
	return left.EnvironmentID == right.EnvironmentID &&
		left.CurrentValueGenerationID == right.CurrentValueGenerationID &&
		equalEnvEntry(left.Entry, right.Entry)
}

func equalEnvEntry(left core.EnvEntry, right core.EnvEntry) bool {
	return left.ID == right.ID && left.Kind == right.Kind && left.Key == right.Key &&
		left.Path == right.Path && equalOptionalUint32(left.UID, right.UID) &&
		equalOptionalUint32(left.GID, right.GID) && equalEntrySource(left.Source, right.Source) &&
		slices.Equal(left.Exposure, right.Exposure) && left.Secret == right.Secret
}

func equalEntrySource(left core.EntrySource, right core.EntrySource) bool {
	if left.Kind != right.Kind || left.Literal != right.Literal || left.SecretRef != right.SecretRef {
		return false
	}
	if left.Fact == nil || right.Fact == nil {
		return left.Fact == nil && right.Fact == nil
	}
	return *left.Fact == *right.Fact
}

func equalOptionalUint32(left *uint32, right *uint32) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func cloneEntryRecord(source EntryRecord) EntryRecord {
	clone := source
	clone.Entry.Exposure = append([]string(nil), source.Entry.Exposure...)
	if source.Entry.UID != nil {
		value := *source.Entry.UID
		clone.Entry.UID = &value
	}
	if source.Entry.GID != nil {
		value := *source.Entry.GID
		clone.Entry.GID = &value
	}
	if source.Entry.Source.Fact != nil {
		value := *source.Entry.Source.Fact
		clone.Entry.Source.Fact = &value
	}
	return clone
}

func corruptEntryRecord() error {
	return errs.New(errs.KindInternal, "Entry record is corrupt")
}
