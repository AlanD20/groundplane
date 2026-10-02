package etcd

import (
	"context"
	"encoding/hex"

	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func terminalDeliveryPruneAuthority(task TaskRecord, taskRevision int64, values []*etcdstore.KeyValue) (bool, error) {
	if len(values) != 2 {
		return false, taskjournal.CorruptPruneIntent()
	}
	assignedBackup := task.TerminalAssignment != nil && task.TerminalAssignment.AssignmentGeneration > 0
	if !assignedBackup {
		if values[0] != nil || values[1] != nil {
			return false, taskjournal.CorruptPruneIntent()
		}
		return false, nil
	}
	if values[0] == nil || values[0].Version != 1 || values[0].ModRevision != taskRevision {
		return false, taskjournal.CorruptPruneIntent()
	}
	stored, err := taskjournal.DecodeTaskTerminalReceipt(values[0].Value)
	if err != nil {
		return false, err
	}
	expected, err := taskTerminalDeliveryReceipt(task)
	if err != nil || stored != expected {
		return false, taskjournal.CorruptPruneIntent()
	}
	if values[1] == nil {
		return true, nil
	}
	delivery, err := taskjournal.DecodeTaskTerminalDelivery(values[1].Value)
	if err != nil {
		return false, err
	}
	digest, err := taskjournal.TaskTerminalReceiptSHA256(stored)
	if err != nil || delivery.Receipt.TaskId != task.ID || delivery.Receipt.AssignmentId != stored.AssignmentID ||
		delivery.Receipt.AssignmentGeneration != stored.AssignmentGeneration || delivery.Receipt.DurableTaskModRevision != taskRevision ||
		hex.EncodeToString(delivery.Receipt.TerminalReceiptSha256) != digest ||
		hex.EncodeToString(
			delivery.Receipt.PlanHash,
		) != task.PlanHash || delivery.Receipt.Terminal != taskjournal.TaskTerminalWire(task.Status) {
		return false, taskjournal.CorruptPruneIntent()
	}
	if validateTaskTerminalReport(task, delivery.Receipt, delivery.Report) != nil {
		return false, taskjournal.CorruptPruneIntent()
	}
	return delivery.Phase != taskjournal.TaskTerminalDeliveryRetired, nil
}

func (repository *TaskRepository) taskTerminalDeliveryPruneHeld(
	ctx context.Context,
	task TaskRecord,
	taskRevision, readRevision int64,
) (bool, error) {
	if task.TerminalAssignment == nil || task.TerminalAssignment.AssignmentGeneration == 0 {
		return false, nil
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			taskjournal.TaskTerminalReceiptKey(task.ID),
			taskjournal.TaskTerminalDeliveryKey(task.ID),
		}, Revision: readRevision,
	})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != readRevision {
		return false, errs.New(errs.KindInternal, "Task terminal delivery prune read is incomplete")
	}
	defer etcdstore.ClearValues(read.Values)
	return terminalDeliveryPruneAuthority(task, taskRevision, read.Values)
}
