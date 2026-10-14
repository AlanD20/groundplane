package taskjournal

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"time"
)

type DiagnosticExpiry struct {
	TaskID          string    `json:"task_id"`
	TaskRevision    int64     `json:"task_revision"`
	ThroughSequence uint64    `json:"through_sequence"`
	ExpiredAt       time.Time `json:"expired_at"`
}

func TaskDiagnosticExpiryKey(taskID string) string {
	return "/v1/runtime/task-diagnostic-expiry/" + taskID
}
func (expiry DiagnosticExpiry) Validate() error {
	if ids.Validate(ids.KindTask, expiry.TaskID) != nil || expiry.TaskRevision <= 0 ||
		recordcodec.ValidateTimestamp("diagnostic expired_at", expiry.ExpiredAt) != nil {
		return recordcodec.CorruptRecord()
	}
	return nil
}
func EncodeDiagnosticExpiry(expiry DiagnosticExpiry) ([]byte, error) {
	if err := expiry.Validate(); err != nil {
		return nil, err
	}
	return recordcodec.Encode("task-diagnostic-expiry", expiry)
}
func DecodeDiagnosticExpiry(value []byte) (DiagnosticExpiry, error) {
	if len(value) > 1024 {
		return DiagnosticExpiry{}, recordcodec.CorruptRecord()
	}
	valueRecord, err := recordcodec.Decode[DiagnosticExpiry](value, "task-diagnostic-expiry")
	if err != nil || valueRecord.Validate() != nil {
		return DiagnosticExpiry{}, recordcodec.CorruptRecord()
	}
	return valueRecord, nil
}
