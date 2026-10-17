// Package workqueue owns atomic persistence of resource execution. Operation
// owners join its prepared changes to their own resource transactions.
package workqueue

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maximumRecordBytes = 64 * 1024
const maximumExecutorCapacity = 256

type ItemSnapshot struct {
	Item     workexecution.Item
	Revision int64
}

type AttemptSnapshot struct {
	Attempt  workexecution.Attempt
	Revision int64
}

// ExecutorRecord is dispatch eligibility, not a connection lease. Removing a
// connection does not free its reserved execution slots or resource claims.
type ExecutorRecord struct {
	Identity          workexecution.Identity    `json:"identity"`
	ProcessGeneration [16]byte                  `json:"process_generation"`
	Ready             bool                      `json:"ready"`
	Capacity          uint16                    `json:"capacity"`
	Operations        []workexecution.Operation `json:"operations"`
}

type ExecutorSnapshot struct {
	Record   ExecutorRecord
	Revision int64
}

type ClaimOwner struct {
	TaskID     string `json:"task_id"`
	WorkItemID string `json:"work_item_id"`
}

type ClaimSnapshot struct {
	Claim    workexecution.Claim
	Owner    *ClaimOwner
	Revision int64
}

type Slot struct {
	TaskID     string `json:"task_id"`
	WorkItemID string `json:"work_item_id"`
	AttemptID  string `json:"attempt_id"`
}

// Change is not a committed permission. The caller joins its conditions and
// writes with operation-owned conditions/writes in ONE measured transaction.
type Change struct {
	Conditions []keyvalue.Condition
	Mutations  []keyvalue.Mutation
}

func EncodeItem(item workexecution.Item) ([]byte, error) {
	if err := item.Validate(); err != nil {
		return nil, err
	}
	return encode("execution_work", item)
}

func DecodeItem(value []byte) (workexecution.Item, error) {
	if len(value) > maximumRecordBytes {
		return workexecution.Item{}, corrupt()
	}
	item, err := recordcodec.Decode[workexecution.Item](value, "execution_work")
	if err != nil {
		return workexecution.Item{}, err
	}
	if err := item.Validate(); err != nil {
		return workexecution.Item{}, corrupt()
	}
	return item, nil
}

func EncodeAttempt(attempt workexecution.Attempt) ([]byte, error) {
	if err := attempt.Validate(); err != nil {
		return nil, err
	}
	return encode("execution_attempt", attempt)
}

func DecodeAttempt(value []byte) (workexecution.Attempt, error) {
	if len(value) > maximumRecordBytes {
		return workexecution.Attempt{}, corrupt()
	}
	attempt, err := recordcodec.Decode[workexecution.Attempt](value, "execution_attempt")
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if err := attempt.Validate(); err != nil {
		return workexecution.Attempt{}, corrupt()
	}
	return attempt, nil
}

func EncodeExecutor(record ExecutorRecord) ([]byte, error) {
	if err := record.Validate(); err != nil {
		return nil, err
	}
	return encode("execution_executor", record)
}

func DecodeExecutor(value []byte) (ExecutorRecord, error) {
	if len(value) > maximumRecordBytes {
		return ExecutorRecord{}, corrupt()
	}
	record, err := recordcodec.Decode[ExecutorRecord](value, "execution_executor")
	if err != nil {
		return ExecutorRecord{}, err
	}
	if err := record.Validate(); err != nil {
		return ExecutorRecord{}, corrupt()
	}
	return record, nil
}

func (record ExecutorRecord) Validate() error {
	if err := record.Identity.Validate(); err != nil {
		return err
	}
	if (record.Identity.Executor == workexecution.Agent) != (record.ProcessGeneration != [16]byte{}) {
		return invalid("Agent executor requires a process identity; Controller executor must not carry one")
	}
	if record.Capacity == 0 || record.Capacity > maximumExecutorCapacity || len(record.Operations) == 0 || len(record.Operations) > 64 {
		return invalid("executor capacity or operation catalog is invalid")
	}
	seen := make(map[workexecution.Operation]struct{}, len(record.Operations))
	for _, operation := range record.Operations {
		if !workexecution.ValidOperation(operation) {
			return invalid("executor advertises an unknown operation")
		}
		if _, found := seen[operation]; found {
			return invalid("executor advertises a duplicate operation")
		}
		seen[operation] = struct{}{}
	}
	return nil
}

func validateOwner(owner ClaimOwner) error {
	if ids.Validate(ids.KindTask, owner.TaskID) != nil || ids.Validate(ids.KindWorkItem, owner.WorkItemID) != nil {
		return invalid("execution claim owner is invalid")
	}
	return nil
}

func encode[T any](kind string, value T) ([]byte, error) {
	encoded, err := recordcodec.Encode(kind, value)
	if err != nil {
		return nil, err
	}
	if len(encoded) > maximumRecordBytes {
		return nil, invalid("execution record exceeds its storage bound")
	}
	return encoded, nil
}

func invalid(message string) error { return errs.New(errs.KindValidationFailed, message) }
func corrupt() error               { return errs.New(errs.KindInternal, "execution storage record is invalid") }
