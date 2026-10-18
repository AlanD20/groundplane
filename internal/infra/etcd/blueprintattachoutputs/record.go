// Package blueprintattachoutputs owns immutable encrypted facts produced by
// one Blueprint Attach work attempt.
package blueprintattachoutputs

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const recordPrefix = "/v1/private/blueprint-attach-output-generations/"

// Generation is the create-only result of one exact Agent attempt. The
// ciphertext is copied from its durable hook RESULT checkpoint.
type Generation struct {
	EnvironmentID string                      `json:"environment_id"`
	TaskID        string                      `json:"task_id"`
	WorkItemID    string                      `json:"work_item_id"`
	AttemptID     string                      `json:"attempt_id"`
	Executor      workexecution.Identity      `json:"executor"`
	Input         workexecution.Input         `json:"input"`
	AttachID      string                      `json:"attach_id"`
	OperationID   string                      `json:"operation_id"`
	ResultSHA256  string                      `json:"result_sha256"`
	Facts         attachrecord.EncryptedFacts `json:"facts"`
	CreatedAt     time.Time                   `json:"created_at"`
}

func Key(attachID, attemptID string) string {
	return recordPrefix + attachID + "/" + attemptID
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
		ids.Validate(ids.KindTask, record.TaskID) != nil ||
		ids.Validate(ids.KindWorkItem, record.WorkItemID) != nil ||
		ids.Validate(ids.KindAttempt, record.AttemptID) != nil ||
		record.Executor.Validate() != nil || record.Executor.Executor != workexecution.Agent ||
		record.Input.Validate() != nil ||
		ids.Validate(ids.KindAttach, record.AttachID) != nil ||
		ids.Validate(ids.KindOperation, record.OperationID) != nil ||
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
