package etcd

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const environmentBlueprintDeletionBatchSize int64 = 32

func (repository *TaskRepository) prepareEnvironmentCreationAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	readRevision int64,
) ([]etcdstore.Condition, []etcdstore.Mutation, []byte, error) {
	state, err := repository.readTaskEnvironmentMutationState(ctx, task.Target, readRevision, true)
	if err != nil {
		return nil, nil, nil, err
	}
	record := state.Environment.Record
	if record.ID != task.Target || record.CreateTaskID != task.ID {
		return nil, nil, nil, errs.New(
			errs.KindStateConflict,
			"environment provisioning belongs to another Task",
		)
	}
	replacement, err := hierarchyrecord.CompleteEnvironmentProvisioning(
		record,
		task.ID,
		terminalStatus == taskjournal.TaskStatusCompleted,
	)
	if err != nil {
		return nil, nil, nil, err
	}
	encoded, err := hierarchyrecord.EncodeEnvironment(replacement)
	if err != nil {
		return nil, nil, nil, err
	}
	conditions := []etcdstore.Condition{
		{Key: hierarchyrecord.EnvironmentKey(record.ID), ModRevision: state.Environment.Revision},
		{Key: hierarchyrecord.EnvironmentMutationEpochKey(record.ID), ModRevision: state.EpochRevision},
		{Key: hierarchyrecord.EnvironmentOperationLockKey(record.ID)},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: hierarchyrecord.EnvironmentKey(record.ID), Value: encoded},
		{
			Type:  etcdstore.MutationPut,
			Key:   hierarchyrecord.EnvironmentMutationEpochKey(record.ID),
			Value: state.EpochValue,
		},
	}
	return conditions, mutations, encoded, nil
}

func (repository *TaskRepository) validateEnvironmentCreationReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	readRevision int64,
) error {
	state, err := repository.readTaskEnvironmentMutationState(ctx, task.Target, readRevision, false)
	if err != nil {
		return err
	}
	record := state.Environment.Record
	want := hierarchyrecord.EnvironmentProvisioningFailed
	if terminalStatus == taskjournal.TaskStatusCompleted {
		want = hierarchyrecord.EnvironmentProvisioningReady
	}
	if record.ID != task.Target || record.CreateTaskID != task.ID ||
		record.ProvisioningState != want {
		return errs.New(
			errs.KindStateConflict,
			"environment provisioning terminal state does not match its Task",
		)
	}
	return nil
}
