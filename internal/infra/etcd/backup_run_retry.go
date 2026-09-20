package etcd

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type BackupRunRetryInput struct {
	SourceTaskID string
	TaskID       string
	CreatedAt    time.Time
}

type PreparedBackupRunRetry struct {
	SourceTask  TaskRecord
	Run         BackupRunRecord
	Owner       TaskOwner
	Publication *PreparedBackupRunPublication
}

type backupRunRetrySource struct {
	task            Versioned[TaskRecord]
	run             Versioned[BackupRunRecord]
	receiptRevision int64
}

func (repository *BackupRuntimeRepository) PrepareBackupRunRetry(
	ctx context.Context,
	input BackupRunRetryInput,
) (PreparedBackupRunRetry, error) {
	if repository == nil || repository.store == nil {
		return PreparedBackupRunRetry{}, errs.New(
			errs.KindInternal,
			"backup runtime repository is not configured",
		)
	}
	if err := validateContext(ctx); err != nil {
		return PreparedBackupRunRetry{}, err
	}
	if ids.Validate(ids.KindTask, input.SourceTaskID) != nil ||
		ids.Validate(ids.KindTask, input.TaskID) != nil ||
		input.SourceTaskID == input.TaskID || !validBackupRuntimeInstant(input.CreatedAt) {
		return PreparedBackupRunRetry{}, errs.New(
			errs.KindValidationFailed,
			"backup run retry input is invalid",
		)
	}
	source, err := repository.loadBackupRunRetrySource(ctx, input.SourceTaskID)
	if err != nil {
		return PreparedBackupRunRetry{}, err
	}
	if input.CreatedAt.Before(source.run.Record.UpdatedAt) {
		return PreparedBackupRunRetry{}, errs.New(
			errs.KindValidationFailed,
			"backup run retry predates its source",
		)
	}
	run, err := newBackupRunRetryRecord(source.run.Record, input.TaskID, input.CreatedAt)
	if err != nil {
		return PreparedBackupRunRetry{}, err
	}
	lock := BackupOperationLockRecord{
		EnvironmentID: run.EnvironmentID,
		OperationID:   run.OperationID,
		TaskID:        run.TaskID,
		Kind:          BackupOperationBackup,
		CreatedAt:     run.CreatedAt,
		UpdatedAt:     run.CreatedAt,
	}
	plan, err := repository.prepareBackupRunPublicationWithRetry(
		ctx,
		run,
		lock,
		&source,
		source.task.ReadRevision,
	)
	if err != nil {
		return PreparedBackupRunRetry{}, err
	}
	return PreparedBackupRunRetry{
		SourceTask: cloneTaskRecord(source.task.Record),
		Run:        cloneBackupRunPublicationRecord(run),
		Owner:      source.task.Record.Owner,
		Publication: &PreparedBackupRunPublication{state: &preparedBackupRunState{
			repository: repository,
			plan:       plan,
		}},
	}, nil
}

func (repository *BackupRuntimeRepository) loadBackupRunRetrySource(
	ctx context.Context,
	taskID string,
) (backupRunRetrySource, error) {
	keys := []string{taskKey(taskID), backupRunKey(taskID), backupTerminalReceiptKey(taskID)}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return backupRunRetrySource{}, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil {
		return backupRunRetrySource{}, errs.New(
			errs.KindTaskNotRetryable,
			"backup retry source is incomplete",
		)
	}
	defer clearKeyValues(read.Values)
	if read.Values[0].ModRevision != read.Values[1].ModRevision ||
		read.Values[0].ModRevision != read.Values[2].ModRevision {
		return backupRunRetrySource{}, errs.New(
			errs.KindStateConflict,
			"backup retry source is not atomically terminal",
		)
	}
	task, taskErr := decodeTaskRecord(read.Values[0].Value)
	run, runErr := decodeBackupRunRecord(read.Values[1].Value)
	receipt, receiptErr := decodeBackupTerminalReceiptRecord(read.Values[2].Value)
	if taskErr != nil || runErr != nil || receiptErr != nil || task.ID != taskID ||
		validateBackupRunTaskBinding(task, run) != nil ||
		validateBackupTerminalReceiptTaskBinding(task, receipt) != nil {
		return backupRunRetrySource{}, corruptBackupRuntimeRecord()
	}
	if task.Status != TaskStatusFailed && task.Status != TaskStatusTimedOut &&
		task.Status != TaskStatusAborted {
		return backupRunRetrySource{}, errs.Newf(
			errs.KindTaskNotRetryable,
			"task %s has status %s",
			task.ID,
			task.Status,
		)
	}
	wantRunState, err := backupRunStateForTaskStatus(task.Status)
	if err != nil || run.State != wantRunState {
		return backupRunRetrySource{}, corruptBackupRuntimeRecord()
	}
	versionedTask := Versioned[TaskRecord]{
		Record:       task,
		Revision:     read.Values[0].ModRevision,
		ReadRevision: read.ReadRevision,
	}
	tasks, err := newTaskRepository(repository.store)
	if err != nil {
		return backupRunRetrySource{}, err
	}
	if err := tasks.validateBackupTerminalReceiptReplay(ctx, versionedTask); err != nil {
		return backupRunRetrySource{}, err
	}
	return backupRunRetrySource{
		task: versionedTask,
		run: Versioned[BackupRunRecord]{
			Record:       run,
			Revision:     read.Values[1].ModRevision,
			ReadRevision: read.ReadRevision,
		},
		receiptRevision: read.Values[2].ModRevision,
	}, nil
}

func newBackupRunRetryRecord(
	source BackupRunRecord,
	taskID string,
	createdAt time.Time,
) (BackupRunRecord, error) {
	retry := cloneBackupRunPublicationRecord(source)
	retry.TaskID = taskID
	retry.RetryOfTaskID = source.TaskID
	retry.State = BackupRunQueued
	retry.CreatedAt = createdAt.UTC()
	retry.UpdatedAt = retry.CreatedAt
	retry.Sources = retry.Sources[:0]
	for _, sourceAttempt := range source.Sources {
		if sourceAttempt.State == BackupSourceAttemptSucceeded {
			continue
		}
		pointID := ids.New(ids.KindRecoveryPoint)
		pointCreatedAt, err := ids.Timestamp(ids.KindRecoveryPoint, pointID)
		if err != nil {
			return BackupRunRecord{}, errs.Wrap(errs.KindInternal, err)
		}
		attempt := sourceAttempt
		attempt.Ordinal = uint32(len(retry.Sources))
		attempt.Snapshot = cloneBackupRunSourceSnapshot(sourceAttempt.Snapshot)
		attempt.RecoveryPointID = pointID
		attempt.RecoveryPointCreatedAt = pointCreatedAt
		attempt.ObjectKey = retry.ConnectorPrefix + retry.EnvironmentID + "/" +
			attempt.SourceID + "/" + pointID + "/artifact.bin"
		attempt.State = BackupSourceAttemptPending
		attempt.Phase = BackupSourcePhaseCapture
		attempt.SizeBytes = 0
		attempt.SHA256 = ""
		attempt.FailureCode = ""
		retry.Sources = append(retry.Sources, attempt)
	}
	if len(retry.Sources) == 0 {
		return BackupRunRecord{}, errs.New(
			errs.KindTaskNotRetryable,
			"backup run has no incomplete source attempts",
		)
	}
	if err := validateBackupRunRecord(retry); err != nil {
		return BackupRunRecord{}, err
	}
	return retry, nil
}

func (repository *BackupRuntimeRepository) prepareBackupRetryConfigReferences(
	ctx context.Context,
	run BackupRunRecord,
	source BackupRunSourceAttemptRecord,
	retrySource BackupRunRecord,
	fixedRevision int64,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	var prior *BackupRunSourceAttemptRecord
	for index := range retrySource.Sources {
		if retrySource.Sources[index].SourceID == source.SourceID {
			prior = &retrySource.Sources[index]
			break
		}
	}
	if prior == nil || prior.Snapshot.Config == nil || source.Snapshot.Config == nil ||
		*prior.Snapshot.Config != *source.Snapshot.Config {
		return nil, nil, errs.New(
			errs.KindStateConflict,
			"backup retry config snapshot changed",
		)
	}
	snapshotID := source.Snapshot.Config.ConfigSnapshotID
	keys := []string{
		backupConfigSnapshotKey(snapshotID),
		backupConfigSnapshotTaskReferenceKey(retrySource.TaskID, snapshotID),
		backupConfigSnapshotReferenceTaskKey(snapshotID, retrySource.TaskID),
		backupConfigSnapshotTaskReferenceKey(run.TaskID, snapshotID),
		backupConfigSnapshotReferenceTaskKey(snapshotID, run.TaskID),
		hierarchyrecord.EnvironmentKey(run.EnvironmentID),
	}
	read, err := repository.readFixedKeys(ctx, keys, fixedRevision)
	if err != nil {
		return nil, nil, err
	}
	defer clearKeyValues(read.Values)
	if read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil ||
		read.Values[3] != nil || read.Values[4] != nil || read.Values[5] == nil ||
		string(read.Values[1].Value) != snapshotID ||
		string(read.Values[2].Value) != retrySource.TaskID ||
		read.Values[5].ModRevision != source.TargetRevision {
		return nil, nil, errs.New(
			errs.KindStateConflict,
			"backup retry config authority changed",
		)
	}
	stored, decodeErr := decodeBackupConfigSnapshotRecord(read.Values[0].Value)
	environment, environmentErr := hierarchyrecord.DecodeEnvironment(read.Values[5].Value)
	if decodeErr != nil || environmentErr != nil || environment.ID != run.EnvironmentID ||
		stored.SnapshotID != snapshotID || stored.EnvironmentID != run.EnvironmentID ||
		stored.SourceID != source.SourceID || stored.State == BackupConfigSnapshotUninitialized ||
		stored.ReadRevision != source.Snapshot.Config.ReadRevision ||
		stored.CreatedAt.After(retrySource.CreatedAt) {
		return nil, nil, errs.New(
			errs.KindStateConflict,
			"backup retry config snapshot changed",
		)
	}
	conditions := make([]etcdstore.Condition, len(keys))
	for index, key := range keys {
		conditions[index] = etcdstore.Condition{Key: key}
		if read.Values[index] != nil {
			conditions[index].ModRevision = read.Values[index].ModRevision
		}
	}
	return conditions, []etcdstore.Mutation{
		{
			Type:  etcdstore.MutationPut,
			Key:   keys[3],
			Value: []byte(snapshotID),
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   keys[4],
			Value: []byte(run.TaskID),
		},
	}, nil
}
