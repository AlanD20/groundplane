package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"

	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func taskTerminalDeliveryReceipt(task TaskRecord) (taskjournal.TaskTerminalReceiptRecord, error) {
	identity := task.TerminalAssignment
	if identity == nil || ValidateTaskRecord(task) != nil || task.Result == nil ||
		task.Result.Kind != taskjournal.TaskResultBackup || identity.AssignmentGeneration != task.Result.AssignmentGeneration {
		return taskjournal.TaskTerminalReceiptRecord{}, errs.New(
			errs.KindValidationFailed,
			"terminal Task lacks Backup assignment evidence",
		)
	}
	encoded, err := EncodeTaskRecord(task)
	if err != nil {
		return taskjournal.TaskTerminalReceiptRecord{}, err
	}
	defer clear(encoded)
	digest := sha256.Sum256(encoded)
	receipt := taskjournal.TaskTerminalReceiptRecord{
		Schema: 1, TaskID: task.ID, AgentID: identity.AgentID, AgentGeneration: identity.AgentGeneration,
		AssignmentID: identity.AssignmentID, AssignmentGeneration: identity.AssignmentGeneration,
		PlanHash: task.PlanHash, Terminal: task.Status, TerminalTaskSHA256: hex.EncodeToString(digest[:]),
	}
	if _, err := taskjournal.EncodeTaskTerminalReceipt(receipt); err != nil {
		return taskjournal.TaskTerminalReceiptRecord{}, err
	}
	return receipt, nil
}

// ResolveTaskTerminalReceipt requires exact, atomic terminal Task bytes. It is
// independent of MVCC history and does not recreate a missing receipt.
func (repository *TaskRepository) ResolveTaskTerminalReceipt(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	taskID, assignmentID string,
	assignmentGeneration uint64,
) (etcdstore.Versioned[taskjournal.TaskTerminalReceiptRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[taskjournal.TaskTerminalReceiptRecord]{}, err
	}
	keys := []string{taskjournal.TaskStorageKey(taskID), taskjournal.TaskTerminalReceiptKey(taskID)}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return etcdstore.Versioned[taskjournal.TaskTerminalReceiptRecord]{}, err
	}
	if read == nil || len(read.Values) != 2 || read.ReadRevision <= 0 || read.Values[0] == nil ||
		read.Values[1] == nil ||
		read.Values[0].ModRevision != read.Values[1].ModRevision ||
		read.Values[1].Version != 1 {
		return etcdstore.Versioned[taskjournal.TaskTerminalReceiptRecord]{}, errs.New(
			errs.KindStateConflict,
			"Task terminal delivery receipt is unavailable",
		)
	}
	defer etcdstore.ClearValues(read.Values)
	task, err := DecodeTaskRecord(read.Values[0].Value)
	if err != nil {
		return etcdstore.Versioned[taskjournal.TaskTerminalReceiptRecord]{}, err
	}
	receipt, err := taskjournal.DecodeTaskTerminalReceipt(read.Values[1].Value)
	if err != nil {
		return etcdstore.Versioned[taskjournal.TaskTerminalReceiptRecord]{}, err
	}
	expected, err := taskTerminalDeliveryReceipt(task)
	if err != nil || expected != receipt || receipt.TaskID != taskID || receipt.AgentID != agentID ||
		receipt.AgentGeneration != agentGeneration ||
		receipt.AssignmentID != assignmentID ||
		receipt.AssignmentGeneration != assignmentGeneration {
		return etcdstore.Versioned[taskjournal.TaskTerminalReceiptRecord]{}, errs.New(
			errs.KindStateConflict,
			"Task terminal delivery identity changed",
		)
	}
	canonical, err := EncodeTaskRecord(task)
	if err != nil {
		return etcdstore.Versioned[taskjournal.TaskTerminalReceiptRecord]{}, err
	}
	defer clear(canonical)
	if !bytes.Equal(canonical, read.Values[0].Value) {
		return etcdstore.Versioned[taskjournal.TaskTerminalReceiptRecord]{}, errs.New(
			errs.KindInternal,
			"terminal Task encoding is not canonical",
		)
	}
	return etcdstore.Versioned[taskjournal.TaskTerminalReceiptRecord]{
		Record:       receipt,
		Revision:     read.Values[1].ModRevision,
		ReadRevision: read.ReadRevision,
	}, nil
}
