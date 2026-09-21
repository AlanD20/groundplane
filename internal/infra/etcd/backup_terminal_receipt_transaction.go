package etcd

import (
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func composeBackupRunTerminalTransaction(
	taskPlan backupTaskTerminalPlan,
	runPlan backupRunPublicationPlan,
	receiptPlan backupTerminalReceiptPlan,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	return composeBackupTerminalTransaction(
		taskPlan.conditions, taskPlan.mutations,
		runPlan.conditions, runPlan.mutations,
		receiptPlan,
	)
}

func composeBackupPruneTerminalTransaction(
	taskPlan backupTaskTerminalPlan,
	prunePlan backupPruneTransactionPlan,
	receiptPlan backupTerminalReceiptPlan,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	return composeBackupTerminalTransaction(
		taskPlan.conditions, taskPlan.mutations,
		prunePlan.conditions, prunePlan.mutations,
		receiptPlan,
	)
}

func composeBackupTerminalTransaction(
	taskConditions []etcdstore.Condition,
	taskMutations []etcdstore.Mutation,
	domainConditions []etcdstore.Condition,
	domainMutations []etcdstore.Mutation,
	receiptPlan backupTerminalReceiptPlan,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	boundReceipt, err := bindBackupTerminalReceiptEpoch(receiptPlan, domainConditions)
	if err != nil {
		return nil, nil, err
	}
	defer boundReceipt.clear()
	conditions := append(append([]etcdstore.Condition(nil), taskConditions...), domainConditions...)
	conditions = append(conditions, boundReceipt.conditions...)
	mutations := make([]etcdstore.Mutation, 0, len(taskMutations)+len(domainMutations)+len(boundReceipt.mutations))
	for _, plan := range [][]etcdstore.Mutation{taskMutations, domainMutations, boundReceipt.mutations} {
		for _, mutation := range plan {
			copyOfMutation := mutation
			copyOfMutation.Value = append([]byte(nil), mutation.Value...)
			mutations = append(mutations, copyOfMutation)
		}
	}
	if err := validateBackupRuntimeTransactionBounds(conditions, mutations); err != nil {
		clearBackupRuntimeMutations(mutations)
		return nil, nil, err
	}
	return conditions, mutations, nil
}

func bindBackupTerminalReceiptEpoch(
	plan backupTerminalReceiptPlan,
	domainConditions []etcdstore.Condition,
) (backupTerminalReceiptPlan, error) {
	wantKey := hierarchyrecord.EnvironmentMutationEpochKey(plan.record.Task.Owner.EnvironmentID)
	var revision int64
	for _, condition := range domainConditions {
		if condition.Key != wantKey {
			continue
		}
		if revision != 0 || condition.ModRevision <= 0 {
			return backupTerminalReceiptPlan{}, errs.New(
				errs.KindInternal,
				"backup terminal receipt epoch fence is ambiguous",
			)
		}
		revision = condition.ModRevision
	}
	if revision <= 0 {
		return backupTerminalReceiptPlan{}, errs.New(
			errs.KindInternal,
			"backup terminal receipt epoch fence is missing",
		)
	}
	record := plan.record
	record.PriorEnvironmentEpochRevision = revision
	receiptDigest, err := backupruntime.BackupTerminalReceiptDigest(record)
	if err != nil {
		return backupTerminalReceiptPlan{}, err
	}
	record.ReceiptDigest = receiptDigest
	value, err := backupruntime.EncodeBackupTerminalReceiptRecord(record)
	if err != nil {
		return backupTerminalReceiptPlan{}, err
	}
	return backupTerminalReceiptPlan{
		mutations: []etcdstore.Mutation{{
			Type: etcdstore.MutationPut, Key: backupruntime.BackupTerminalReceiptKey(record.Task.TaskID), Value: value,
		}},
		record: record,
	}, nil
}
