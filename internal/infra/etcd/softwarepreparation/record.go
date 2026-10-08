package softwarepreparation

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	Prefix     = "/v1/software-preparations/"
	recordKind = "software_preparation"
)

// Record is the durable, API-independent preparation projection. TaskID is
// both its stable identity and the native Task that owns every side effect.
type Record struct {
	TaskID      string                     `json:"task_id"`
	OperationID string                     `json:"operation_id"`
	InputSHA256 string                     `json:"input_sha256"`
	Source      preparation.ResolvedSource `json:"source"`
	Progress    preparation.Progress       `json:"progress"`
	CreatedAt   time.Time                  `json:"created_at"`
	UpdatedAt   time.Time                  `json:"updated_at"`
}

func NewRecord(
	taskID string,
	operationID string,
	inputSHA256 string,
	source preparation.ResolvedSource,
	createdAt time.Time,
) (Record, error) {
	record := Record{
		TaskID: taskID, OperationID: operationID, InputSHA256: inputSHA256, Source: source,
		Progress:  preparation.Progress{Phase: preparation.PhaseAccepted, Result: preparation.Result{Source: source}},
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	return record, ValidateRecord(record)
}

func Key(taskID string) string { return Prefix + taskID }

func ValidateRecord(record Record) error {
	if ids.Validate(ids.KindTask, record.TaskID) != nil ||
		ids.Validate(ids.KindOperation, record.OperationID) != nil ||
		!recordcodec.ValidSHA256(record.InputSHA256) {
		return errs.New(errs.KindValidationFailed, "software preparation record identity is invalid")
	}
	if err := preparation.ValidateResolvedSource(record.Source); err != nil {
		return err
	}
	if err := preparation.ValidateProgress(record.Progress, record.Source); err != nil {
		return err
	}
	if !recordcodec.IsCanonicalUTC(record.CreatedAt) || !recordcodec.IsCanonicalUTC(record.UpdatedAt) ||
		record.UpdatedAt.Before(record.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "software preparation timestamps are invalid")
	}
	return nil
}

func encode(record Record) ([]byte, error) {
	if err := ValidateRecord(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode(recordKind, record)
}

func Encode(record Record) ([]byte, error) { return encode(record) }

func Decode(value []byte) (Record, error) {
	record, err := recordcodec.Decode[Record](value, recordKind)
	if err != nil || ValidateRecord(record) != nil {
		return Record{}, errs.New(errs.KindInternal, "durable software preparation record is corrupt")
	}
	return record, nil
}
