package etcd

import (
	"slices"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// releaseTaskPublicationFragment is the only authority allowed to contribute
// Task-owned keys to an atomic Release publication.
type releaseTaskPublicationFragment struct {
	condition Condition
	mutations []Mutation
}

func (repository *TaskRepository) prepareReleaseTaskPublicationFragment(
	task TaskRecord,
) (releaseTaskPublicationFragment, error) {
	if repository == nil || repository.store == nil {
		return releaseTaskPublicationFragment{}, errs.New(errs.KindInternal, "task repository is not configured")
	}
	if err := validateTaskRecord(task); err != nil {
		return releaseTaskPublicationFragment{}, err
	}
	if task.Executor != TaskExecutorAgent || task.Status != TaskStatusPending ||
		(task.Type != TaskDeploy && task.Type != TaskRollback) || task.Owner.EnvironmentID == "" {
		return releaseTaskPublicationFragment{}, errs.New(
			errs.KindValidationFailed,
			"release task publication shape is invalid",
		)
	}
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		return releaseTaskPublicationFragment{}, err
	}
	reference, err := encodeTaskReference(task.ID)
	if err != nil {
		clear(taskValue)
		return releaseTaskPublicationFragment{}, err
	}
	ownerKeys, err := taskOwnerIndexKeys(task.Owner, task.ID)
	if err != nil {
		clear(taskValue)
		clear(reference)
		return releaseTaskPublicationFragment{}, err
	}
	if len(ownerKeys) != 2 {
		clear(taskValue)
		clear(reference)
		return releaseTaskPublicationFragment{}, errs.New(
			errs.KindInternal,
			"release task owner indexes are incomplete",
		)
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: MutationPut, Key: ownerKeys[0], Value: []byte(task.ID)},
		{Type: MutationPut, Key: ownerKeys[1], Value: []byte(task.ID)},
		{Type: MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
	}
	return releaseTaskPublicationFragment{condition: Condition{Key: taskKey(task.ID)}, mutations: mutations}, nil
}

func clearReleaseTaskPublicationFragment(fragment releaseTaskPublicationFragment) {
	for index := range fragment.mutations {
		clear(fragment.mutations[index].Value)
	}
	fragment.mutations = nil
}

func cloneReleaseTaskMutations(fragment releaseTaskPublicationFragment) []Mutation {
	mutations := slices.Clone(fragment.mutations)
	for index := range mutations {
		mutations[index].Value = slices.Clone(mutations[index].Value)
	}
	return mutations
}
