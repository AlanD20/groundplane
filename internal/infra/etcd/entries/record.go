package entries

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const entryRecordPrefix = "/v1/records/entries/"

// Record is the durable metadata boundary for one environment Entry.
// CurrentValueGenerationID selects immutable bytes; secret bytes never enter
// this primary record.
type Record struct {
	EnvironmentID            string        `json:"environment_id"`
	BlueprintKey             string        `json:"blueprint_key,omitempty"`
	Entry                    core.EnvEntry `json:"entry"`
	CurrentValueGenerationID string        `json:"current_value_generation_id"`
}

func NewRecord(
	environmentID string,
	entry core.EnvEntry,
	valueGenerationID string,
) (Record, error) {
	record := Record{
		EnvironmentID: environmentID, Entry: entry, CurrentValueGenerationID: valueGenerationID,
	}
	if err := ValidateRecord(record); err != nil {
		return Record{}, err
	}
	return record, nil
}

func NewBlueprintRecord(
	environmentID string,
	blueprintKey string,
	entry core.EnvEntry,
	valueGenerationID string,
) (Record, error) {
	if blueprintKey == "" {
		return Record{}, errs.New(errs.KindValidationFailed, "Blueprint Entry key is required")
	}
	record, err := NewRecord(environmentID, entry, valueGenerationID)
	if err != nil {
		return Record{}, err
	}
	record.BlueprintKey = blueprintKey
	return record, nil
}

func ReplaceDesired(
	current Record,
	desired core.EnvEntry,
	valueGenerationID string,
) (Record, error) {
	if err := ValidateRecord(current); err != nil {
		return Record{}, err
	}
	if current.Entry.ID != desired.ID || current.Entry.Kind != desired.Kind ||
		current.Entry.Key != desired.Key || current.Entry.Path != desired.Path ||
		current.Entry.Secret != desired.Secret || !equalOptionalUint32(current.Entry.UID, desired.UID) ||
		!equalOptionalUint32(current.Entry.GID, desired.GID) {
		return Record{}, errs.New(
			errs.KindValidationFailed,
			"Entry edit changed immutable identity, destination, ownership, or storage class",
		)
	}
	replacement, err := NewRecord(current.EnvironmentID, desired, valueGenerationID)
	if err != nil {
		return Record{}, err
	}
	replacement.BlueprintKey = current.BlueprintKey
	return replacement, nil
}

func RecordKey(entryID string) string {
	return entryRecordPrefix + entryID
}

func EncodeRecord(record Record) ([]byte, error) {
	if err := ValidateRecord(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("entry", record)
}

func DecodeRecord(value []byte) (Record, error) {
	record, err := recordcodec.Decode[Record](value, "entry")
	if err != nil || ValidateRecord(record) != nil {
		return Record{}, CorruptRecord()
	}
	return record, nil
}

func ValidateRecord(record Record) error {
	if recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID) != nil ||
		recordcodec.ValidateID(ids.KindEnvEntry, record.Entry.ID) != nil ||
		recordcodec.ValidateID(ids.KindConfig, record.CurrentValueGenerationID) != nil {
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

func EqualRecord(left Record, right Record) bool {
	return left.EnvironmentID == right.EnvironmentID &&
		left.BlueprintKey == right.BlueprintKey &&
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

func CloneRecord(source Record) Record {
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

func CorruptRecord() error {
	return errs.New(errs.KindInternal, "Entry record is corrupt")
}
