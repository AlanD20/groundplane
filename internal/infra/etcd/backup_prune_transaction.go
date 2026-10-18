package etcd

import (
	backupplanning "github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type backupPruneTransactionPlan struct {
	conditions   []etcdstore.Condition
	mutations    []etcdstore.Mutation
	evidence     []backupplanning.PruneExecutionEvidence
	readRevision int64
}

func (plan backupPruneTransactionPlan) composeTransaction(
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

func (plan *backupPruneTransactionPlan) clear() {
	etcdstore.ClearMutationValues(plan.mutations)
	plan.evidence = nil
	plan.readRevision = 0
}
