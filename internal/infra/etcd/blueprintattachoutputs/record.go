// Package blueprintattachoutputs owns immutable encrypted facts produced by
// one Blueprint Attach child execution.
package blueprintattachoutputs

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const recordPrefix = "/v1/private/blueprint-attach-output-generations/"

// Generation is the create-only result of one exact Agent assignment. The
// ciphertext is copied from its durable hook RESULT checkpoint.
type Generation struct {
	EnvironmentID  string                      `json:"environment_id"`
	ParentTaskID   string                      `json:"parent_task_id"`
	ChildTaskID    string                      `json:"child_task_id"`
	AttachID       string                      `json:"attach_id"`
	OperationID    string                      `json:"operation_id"`
	AssignmentID   string                      `json:"assignment_id"`
	ExecutionEpoch uint32                      `json:"execution_epoch"`
	StepID         string                      `json:"step_id"`
	PlanHash       string                      `json:"plan_hash"`
	ResultSHA256   string                      `json:"result_sha256"`
	Facts          attachrecord.EncryptedFacts `json:"facts"`
	CreatedAt      time.Time                   `json:"created_at"`
}

func Key(attachID, childTaskID string) string {
	return recordPrefix + attachID + "/" + childTaskID
}

func AttachPrefix(attachID string) string {
	return recordPrefix + attachID + "/"
}

func Encode(record Generation) ([]byte, error) {
	if err := Validate(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("blueprint_attach_output_generation", record)
}

func Decode(value []byte) (Generation, error) {
	record, err := recordcodec.Decode[Generation](value, "blueprint_attach_output_generation")
	if err != nil || Validate(record) != nil {
		Clear(&record)
		return Generation{}, recordcodec.CorruptRecord()
	}
	return record, nil
}

func Validate(record Generation) error {
	if ids.Validate(ids.KindEnvironment, record.EnvironmentID) != nil ||
		ids.Validate(ids.KindTask, record.ParentTaskID) != nil ||
		ids.Validate(ids.KindTask, record.ChildTaskID) != nil ||
		record.ParentTaskID == record.ChildTaskID ||
		ids.Validate(ids.KindAttach, record.AttachID) != nil ||
		ids.Validate(ids.KindOperation, record.OperationID) != nil ||
		ids.Validate(ids.KindAssignment, record.AssignmentID) != nil ||
		record.ExecutionEpoch == 0 || ids.Validate(ids.KindStep, record.StepID) != nil ||
		!recordcodec.ValidSHA256(record.PlanHash) ||
		!recordcodec.ValidSHA256(record.ResultSHA256) ||
		attachrecord.ValidateAttachEncryptedFacts(record.Facts) != nil ||
		record.Facts.AttachID != record.AttachID || record.CreatedAt.IsZero() {
		return errs.New(errs.KindValidationFailed, "Blueprint Attach output generation is invalid")
	}
	return nil
}

func Clear(record *Generation) {
	if record != nil {
		clear(record.Facts.Ciphertext)
	}
}
