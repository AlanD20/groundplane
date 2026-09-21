package etcd

import (
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"slices"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// releaseTaskPublicationFragment is the only authority allowed to contribute
// Task-owned keys to an atomic Release publication.
type releaseTaskPublicationFragment struct {
	condition etcdstore.Condition
	mutations []etcdstore.Mutation
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
	if task.Executor != taskjournal.TaskExecutorAgent || task.Status != taskjournal.TaskStatusPending ||
		(task.Type != taskjournal.TaskDeploy && task.Type != taskjournal.TaskRollback) || task.Owner.EnvironmentID == "" {
		return releaseTaskPublicationFragment{}, errs.New(
			errs.KindValidationFailed,
			"release task publication shape is invalid",
		)
	}
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		return releaseTaskPublicationFragment{}, err
	}
	reference, err := idempotencyrecord.EncodeTaskReference(task.ID)
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
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: ownerKeys[0], Value: []byte(task.ID)},
		{Type: etcdstore.MutationPut, Key: ownerKeys[1], Value: []byte(task.ID)},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskActiveOperationKey(task.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID), Value: reference},
	}
	return releaseTaskPublicationFragment{condition: etcdstore.Condition{Key: taskjournal.TaskStorageKey(task.ID)}, mutations: mutations}, nil
}

func clearReleaseTaskPublicationFragment(fragment releaseTaskPublicationFragment) {
	for index := range fragment.mutations {
		clear(fragment.mutations[index].Value)
	}
	fragment.mutations = nil
}

func cloneReleaseTaskMutations(fragment releaseTaskPublicationFragment) []etcdstore.Mutation {
	mutations := slices.Clone(fragment.mutations)
	for index := range mutations {
		mutations[index].Value = slices.Clone(mutations[index].Value)
	}
	return mutations
}
