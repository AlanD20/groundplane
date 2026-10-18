package etcd

import (
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// backupRunPublicationPlan is the persistence-private admission for native work
// publication. Its lock, run, membership, exclusions, and epoch mutations are
// appended to the caller's Task mutations and committed once.
type backupRunPublicationPlan struct {
	hostID     string
	initiation TaskInitiation
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	record     backupruntime.BackupRunRecord
}

func (plan backupRunPublicationPlan) composeTransaction(
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	composedConditions := append(append([]etcdstore.Condition(nil), conditions...), plan.conditions...)
	composedMutations := make([]etcdstore.Mutation, 0, len(mutations)+len(plan.mutations))
	for _, mutation := range mutations {
		copyOfMutation := mutation
		copyOfMutation.Value = append([]byte(nil), mutation.Value...)
		composedMutations = append(composedMutations, copyOfMutation)
	}
	for _, mutation := range plan.mutations {
		copyOfMutation := mutation
		copyOfMutation.Value = append([]byte(nil), mutation.Value...)
		composedMutations = append(composedMutations, copyOfMutation)
	}
	if err := backupruntime.ValidateBackupRuntimeTransactionBounds(
		composedConditions,
		composedMutations,
	); err != nil {
		etcdstore.ClearMutationValues(composedMutations)
		return nil, nil, err
	}
	return composedConditions, composedMutations, nil
}

func (plan *backupRunPublicationPlan) clear() {
	for index := range plan.mutations {
		clear(plan.mutations[index].Value)
		plan.mutations[index].Value = nil
	}
}
