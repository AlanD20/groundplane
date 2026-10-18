// Package executiondiagnostics stores bounded observations, never execution authority.
package executiondiagnostics

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maximumBytes = 64 << 10
const maximumStages = 64

type Record struct {
	TaskID     string                `json:"task_id"`
	WorkItemID string                `json:"work_item_id"`
	AttemptID  string                `json:"attempt_id"`
	Sequence   uint64                `json:"sequence"`
	UpdatedAt  time.Time             `json:"updated_at"`
	Stages     []workexecution.Stage `json:"stages"`
}

func Key(attemptID string) string { return "/v1/diagnostics/execution-stages/" + attemptID }

func (record Record) Validate() error {
	if ids.Validate(ids.KindTask, record.TaskID) != nil || ids.Validate(ids.KindWorkItem, record.WorkItemID) != nil ||
		ids.Validate(ids.KindAttempt, record.AttemptID) != nil || record.Sequence == 0 || !recordcodec.IsCanonicalUTC(record.UpdatedAt) ||
		len(record.Stages) == 0 || len(record.Stages) > maximumStages {
		return invalid()
	}
	seen := make(map[string]bool, len(record.Stages))
	for _, stage := range record.Stages {
		if stage.Validate() != nil || seen[stage.Key] || stage.StartedAt != nil && stage.StartedAt.After(record.UpdatedAt) || stage.FinishedAt != nil && stage.FinishedAt.After(record.UpdatedAt) {
			return invalid()
		}
		seen[stage.Key] = true
	}
	return nil
}

func encode(record Record) ([]byte, error) {
	if err := record.Validate(); err != nil {
		return nil, err
	}
	value, err := recordcodec.Encode("execution_diagnostics", record)
	if err != nil {
		return nil, err
	}
	if len(value) > maximumBytes {
		return nil, invalid()
	}
	return value, nil
}

func decode(value []byte) (Record, error) {
	if len(value) > maximumBytes {
		return Record{}, invalid()
	}
	record, err := recordcodec.Decode[Record](value, "execution_diagnostics")
	if err != nil {
		return Record{}, err
	}
	return record, record.Validate()
}

func invalid() error {
	return errs.New(errs.KindValidationFailed, "execution diagnostics exceed their bounds or contain invalid evidence")
}
