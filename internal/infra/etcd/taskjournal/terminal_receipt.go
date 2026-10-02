package taskjournal

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TaskTerminalWire(status TaskStatus) agentpb.TaskTerminal {
	switch status {
	case TaskStatusCompleted:
		return agentpb.TaskTerminal_TASK_TERMINAL_COMPLETED
	case TaskStatusFailed:
		return agentpb.TaskTerminal_TASK_TERMINAL_FAILED
	case TaskStatusTimedOut:
		return agentpb.TaskTerminal_TASK_TERMINAL_TIMED_OUT
	case TaskStatusAborted:
		return agentpb.TaskTerminal_TASK_TERMINAL_ABORTED
	default:
		return agentpb.TaskTerminal_TASK_TERMINAL_UNSPECIFIED
	}
}

// TaskTerminalReceiptRecord is immutable evidence committed with the terminal
// Task. Delivery progress belongs to a separate record: this authority contains
// no process generation, Ack digest, Task revision or self-digest.
type TaskTerminalReceiptRecord struct {
	Schema               uint32     `json:"schema"`
	TaskID               string     `json:"task_id"`
	AgentID              string     `json:"agent_id"`
	AgentGeneration      uint64     `json:"agent_generation"`
	AssignmentID         string     `json:"assignment_id"`
	AssignmentGeneration uint64     `json:"assignment_generation"`
	PlanHash             string     `json:"plan_hash"`
	Terminal             TaskStatus `json:"terminal"`
	TerminalTaskSHA256   string     `json:"terminal_task_sha256"`
}

func TaskTerminalReceiptKey(taskID string) string {
	return "/v1/runtime/task-terminal-receipts/" + taskID
}

func EncodeTaskTerminalReceipt(record TaskTerminalReceiptRecord) ([]byte, error) {
	if record.Schema != 1 || ids.Validate(ids.KindTask, record.TaskID) != nil ||
		ids.Validate(ids.KindAgent, record.AgentID) != nil || record.AgentGeneration == 0 ||
		ids.Validate(ids.KindAssignment, record.AssignmentID) != nil || record.AssignmentGeneration == 0 ||
		!recordcodec.ValidSHA256(record.PlanHash) || !recordcodec.ValidSHA256(record.TerminalTaskSHA256) ||
		!IsTerminalTaskStatus(record.Terminal) {
		return nil, errs.New(errs.KindValidationFailed, "Task terminal receipt identity is invalid")
	}
	return json.Marshal(record)
}

func DecodeTaskTerminalReceipt(value []byte) (TaskTerminalReceiptRecord, error) {
	if len(value) == 0 || len(value) > 4096 || recordcodec.RejectDuplicateFields(value) != nil {
		return TaskTerminalReceiptRecord{}, corruptTaskTerminalReceipt()
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var record TaskTerminalReceiptRecord
	if err := decoder.Decode(&record); err != nil || recordcodec.RequireEOF(decoder) != nil {
		return TaskTerminalReceiptRecord{}, corruptTaskTerminalReceipt()
	}
	if _, err := EncodeTaskTerminalReceipt(record); err != nil {
		return TaskTerminalReceiptRecord{}, corruptTaskTerminalReceipt()
	}
	return record, nil
}

func TaskTerminalReceiptSHA256(record TaskTerminalReceiptRecord) (string, error) {
	encoded, err := EncodeTaskTerminalReceipt(record)
	if err != nil {
		return "", err
	}
	defer clear(encoded)
	hash := sha256.New()
	_, _ = hash.Write([]byte("groundplane.task.terminal-receipt.schema-one.v1\x00"))
	_, _ = hash.Write(encoded)
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func corruptTaskTerminalReceipt() error {
	return errs.New(errs.KindInternal, "Task terminal receipt is corrupt")
}
