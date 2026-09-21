package etcd

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

type backupCheckpointPlan struct {
	conditions     []etcdstore.Condition
	mutations      []etcdstore.Mutation
	readRevision   int64
	commitRevision int64
	digest         string
	duplicate      bool
}

type backupCheckpointBinding struct {
	taskType taskjournal.TaskType
	ordinal  uint32
	pointID  string
}

func (plan backupCheckpointPlan) composeTransaction(
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	composedConditions := append(append([]etcdstore.Condition(nil), conditions...), plan.conditions...)
	composedMutations := make([]etcdstore.Mutation, 0, len(mutations)+len(plan.mutations))
	for _, mutation := range append(append([]etcdstore.Mutation(nil), mutations...), plan.mutations...) {
		copyOfMutation := mutation
		copyOfMutation.Value = append([]byte(nil), mutation.Value...)
		composedMutations = append(composedMutations, copyOfMutation)
	}
	if err := validateBackupRuntimeTransactionBounds(
		composedConditions,
		composedMutations,
	); err != nil {
		clearBackupRuntimeMutations(composedMutations)
		return nil, nil, err
	}
	return composedConditions, composedMutations, nil
}

func (plan *backupCheckpointPlan) clear() {
	clearBackupRuntimeMutations(plan.mutations)
}
