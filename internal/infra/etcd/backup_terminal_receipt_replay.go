package etcd

import (
	"context"
	"encoding/json"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) validateBackupTerminalReceiptReplay(
	ctx context.Context,
	task etcdstore.Versioned[TaskRecord],
) error {
	if task.Revision <= 0 || (task.Record.Type != taskjournal.TaskBackup && task.Record.Type != taskjournal.TaskBackupPrune) ||
		!taskjournal.IsTerminalTaskStatus(task.Record.Status) {
		return errs.New(errs.KindInternal, "terminal backup Task is invalid")
	}
	keys := []string{taskjournal.TaskStorageKey(task.Record.ID), backupruntime.BackupTerminalReceiptKey(task.Record.ID)}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		return errs.New(errs.KindInternal, "backup terminal receipt read is incomplete")
	}
	if read.Values[0] == nil || read.Values[0].ModRevision != task.Revision {
		return errs.New(errs.KindStateConflict, "terminal backup Task changed")
	}
	if read.Values[1] == nil || read.Values[1].ModRevision != task.Revision {
		return errs.New(errs.KindInternal, "backup terminal receipt is not atomic with its Task")
	}
	storedTask, err := decodeTaskRecord(read.Values[0].Value)
	if err != nil {
		return err
	}
	storedDigest, err := backupTerminalTaskDigest(storedTask)
	if err != nil {
		return err
	}
	callerDigest, err := backupTerminalTaskDigest(task.Record)
	if err != nil || callerDigest != storedDigest {
		return errs.New(errs.KindStateConflict, "terminal backup Task changed")
	}
	receipt, err := backupruntime.DecodeBackupTerminalReceiptRecord(read.Values[1].Value)
	if err != nil {
		return err
	}
	if err := validateBackupTerminalReceiptTaskBinding(storedTask, receipt); err != nil ||
		receipt.PriorTaskRevision >= task.Revision ||
		receipt.PriorEnvironmentEpochRevision >= task.Revision {
		return errs.New(errs.KindInternal, "backup terminal receipt binding is invalid")
	}
	return repository.validateCurrentBackupTerminalAuthority(
		ctx,
		task.Record,
		task.Revision,
		receipt,
	)
}

func validateBackupTerminalReceiptTaskBinding(
	task TaskRecord,
	receipt backupruntime.BackupTerminalReceiptRecord,
) error {
	evidence, err := backupTerminalTaskEvidence(task)
	if err != nil {
		return err
	}
	left, err := json.Marshal(evidence)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	right, err := json.Marshal(receipt.Task)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if string(left) != string(right) {
		return errs.New(errs.KindStateConflict, "backup terminal receipt Task binding is invalid")
	}
	return nil
}
