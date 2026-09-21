package etcd

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type backupTerminalReceiptPruneCompanion struct {
	key      string
	revision int64
}

func prepareBackupTerminalReceiptPruneCompanion(
	task TaskRecord,
	taskRevision int64,
	value *etcdstore.KeyValue,
) (backupTerminalReceiptPruneCompanion, error) {
	if task.Type != TaskBackup && task.Type != TaskBackupPrune {
		return backupTerminalReceiptPruneCompanion{}, errs.New(
			errs.KindInternal,
			"ordinary Task requested a Backup terminal receipt",
		)
	}
	if value == nil || value.ModRevision != taskRevision || value.Key != backupTerminalReceiptKey(task.ID) {
		return backupTerminalReceiptPruneCompanion{}, corruptTaskPruneIntent()
	}
	receipt, err := decodeBackupTerminalReceiptRecord(value.Value)
	if err != nil || receipt.PriorTaskRevision >= taskRevision ||
		validateBackupTerminalReceiptTaskBinding(task, receipt) != nil {
		return backupTerminalReceiptPruneCompanion{}, corruptTaskPruneIntent()
	}
	return backupTerminalReceiptPruneCompanion{key: value.Key, revision: value.ModRevision}, nil
}

func (companion backupTerminalReceiptPruneCompanion) appendStartCondition(
	conditions []etcdstore.Condition,
) []etcdstore.Condition {
	if companion.revision <= 0 {
		return conditions
	}
	return append(conditions, etcdstore.Condition{Key: companion.key, ModRevision: companion.revision})
}

func appendBackupTerminalReceiptPruneFinalization(
	intent taskPruneIntent,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) ([]etcdstore.Condition, []etcdstore.Mutation) {
	if intent.BackupTerminalReceiptRevision <= 0 {
		return conditions, mutations
	}
	key := backupTerminalReceiptKey(intent.TaskID)
	conditions = append(conditions, etcdstore.Condition{Key: key, ModRevision: intent.BackupTerminalReceiptRevision})
	mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: key})
	return conditions, mutations
}
