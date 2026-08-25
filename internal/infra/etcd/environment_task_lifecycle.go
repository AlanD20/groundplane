package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type environmentTaskChange struct {
	applies    bool
	conditions []Condition
	mutations  []Mutation
	values     [][]byte
}

type taskEnvironmentMutationState struct {
	Environment   Versioned[EnvironmentRecord]
	EpochRevision int64
	EpochValue    []byte
}

func (repository *TaskRepository) prepareEnvironmentTaskRetry(
	ctx context.Context,
	source TaskRecord,
	retry TaskRecord,
	revision int64,
) (environmentTaskChange, error) {
	if source.Executor == TaskExecutorAgent && source.Type == TaskRemove &&
		ids.Validate(ids.KindEnvironment, source.Target) == nil {
		return repository.prepareEnvironmentRemovalTaskRetry(ctx, source, retry, revision)
	}
	if source.Executor != TaskExecutorAgent || source.Type != TaskCreate ||
		ids.Validate(ids.KindEnvironment, source.Target) != nil {
		return environmentTaskChange{}, nil
	}
	if retry.Type != source.Type || retry.Target != source.Target {
		return environmentTaskChange{}, errs.New(
			errs.KindInternal,
			"environment retry changed its durable target",
		)
	}
	state, err := repository.readTaskEnvironmentMutationState(ctx, source.Target, revision, true)
	if err != nil {
		return environmentTaskChange{}, err
	}
	current := state.Environment
	if current.Record.CreateTaskID != source.ID {
		return environmentTaskChange{}, errs.New(
			errs.KindStateConflict,
			"source Task no longer owns Environment provisioning",
		)
	}
	retrying, err := RetryEnvironmentProvisioning(current.Record, retry.ID)
	if err != nil {
		return environmentTaskChange{}, err
	}
	value, err := encodeEnvironment(retrying)
	if err != nil {
		return environmentTaskChange{}, err
	}
	return environmentTaskChange{
		applies: true,
		conditions: []Condition{
			{Key: environmentKey(source.Target), ModRevision: current.Revision},
			{Key: environmentMutationEpochKey(source.Target), ModRevision: state.EpochRevision},
			{Key: environmentOperationLockKey(source.Target)},
		},
		mutations: []Mutation{
			{
				Type: MutationPut, Key: environmentKey(source.Target), Value: value,
			},
			{
				Type:  MutationPut,
				Key:   environmentMutationEpochKey(source.Target),
				Value: state.EpochValue,
			},
		},
		values: [][]byte{value, state.EpochValue},
	}, nil
}

func (repository *TaskRepository) readTaskEnvironmentMutationState(
	ctx context.Context,
	environmentID string,
	revision int64,
	requireUnlocked bool,
) (taskEnvironmentMutationState, error) {
	result, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			environmentKey(environmentID),
			environmentMutationEpochKey(environmentID),
			environmentOperationLockKey(environmentID),
		},
		Revision: revision,
	})
	if err != nil {
		return taskEnvironmentMutationState{}, err
	}
	if result == nil {
		return taskEnvironmentMutationState{}, errs.New(
			errs.KindInternal,
			"environment mutation fence read returned an empty result",
		)
	}
	if result.ReadRevision != revision || len(result.Values) != 3 {
		clearKeyValues(result.Values)
		return taskEnvironmentMutationState{}, errs.New(
			errs.KindInternal,
			"environment mutation fence read returned an invalid result",
		)
	}
	defer clearKeyValues(result.Values)
	environmentValue := result.Values[0]
	epochValue := result.Values[1]
	if environmentValue == nil {
		return taskEnvironmentMutationState{}, errs.New(
			errs.KindEnvironmentNotFound,
			"environment was not found",
		)
	}
	if epochValue == nil {
		return taskEnvironmentMutationState{}, errs.New(
			errs.KindInternal,
			"environment mutation epoch is missing",
		)
	}
	record, err := decodeEnvironment(environmentValue.Value)
	if err != nil {
		return taskEnvironmentMutationState{}, err
	}
	if record.ID != environmentID {
		return taskEnvironmentMutationState{}, errs.New(
			errs.KindInternal,
			"environment mutation fence returned the wrong Environment",
		)
	}
	epoch, err := decodeEnvironmentMutationEpochRecord(epochValue.Value)
	if err != nil || epoch.EnvironmentID != environmentID {
		return taskEnvironmentMutationState{}, errs.New(
			errs.KindInternal,
			"environment mutation epoch is corrupt",
		)
	}
	if result.Values[2] != nil {
		lock, lockErr := decodeBackupOperationLockRecord(result.Values[2].Value)
		if lockErr != nil || lock.EnvironmentID != environmentID {
			return taskEnvironmentMutationState{}, errs.New(
				errs.KindInternal,
				"environment operation lock is corrupt",
			)
		}
		if requireUnlocked {
			return taskEnvironmentMutationState{}, errs.New(
				errs.KindStateConflict,
				"environment has an active persistence operation",
			)
		}
	}
	canonicalEpoch, err := encodeEnvironmentMutationEpochRecord(epoch)
	if err != nil {
		return taskEnvironmentMutationState{}, err
	}
	return taskEnvironmentMutationState{
		Environment: Versioned[EnvironmentRecord]{
			Record:       record,
			Revision:     environmentValue.ModRevision,
			ReadRevision: result.ReadRevision,
		},
		EpochRevision: epochValue.ModRevision,
		EpochValue:    canonicalEpoch,
	}, nil
}

func clearEnvironmentTaskChange(change environmentTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}
