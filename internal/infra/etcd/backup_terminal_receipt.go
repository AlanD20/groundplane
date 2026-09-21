package etcd

import (
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type backupTerminalReceiptPlan struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	record     backupruntime.BackupTerminalReceiptRecord
}

func (plan *backupTerminalReceiptPlan) clear() {
	etcdstore.ClearMutationValues(plan.mutations)
	plan.conditions = nil
	plan.mutations = nil
	plan.record = backupruntime.BackupTerminalReceiptRecord{}
}
