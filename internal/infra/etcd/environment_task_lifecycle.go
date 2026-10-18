package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type environmentTaskChange struct {
	applies    bool
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	values     [][]byte
}

type taskEnvironmentMutationState struct {
	Environment   etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	EpochRevision int64
	EpochValue    []byte
}

func (repository *TaskRepository) readTaskEnvironmentMutationState(
	ctx context.Context,
	environmentID string,
	revision int64,
	requireUnlocked bool,
) (taskEnvironmentMutationState, error) {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			hierarchyrecord.EnvironmentKey(environmentID),
			hierarchyrecord.EnvironmentMutationEpochKey(environmentID),
			hierarchyrecord.EnvironmentOperationLockKey(environmentID),
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
		etcdstore.ClearValues(result.Values)
		return taskEnvironmentMutationState{}, errs.New(
			errs.KindInternal,
			"environment mutation fence read returned an invalid result",
		)
	}
	defer etcdstore.ClearValues(result.Values)
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
	record, err := hierarchyrecord.DecodeEnvironment(environmentValue.Value)
	if err != nil {
		return taskEnvironmentMutationState{}, err
	}
	if record.ID != environmentID {
		return taskEnvironmentMutationState{}, errs.New(
			errs.KindInternal,
			"environment mutation fence returned the wrong Environment",
		)
	}
	epoch, err := backupruntime.DecodeEnvironmentMutationEpochRecord(epochValue.Value)
	if err != nil || epoch.EnvironmentID != environmentID {
		return taskEnvironmentMutationState{}, errs.New(
			errs.KindInternal,
			"environment mutation epoch is corrupt",
		)
	}
	if result.Values[2] != nil {
		lock, lockErr := backupruntime.DecodeBackupOperationLockRecord(result.Values[2].Value)
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
	canonicalEpoch, err := backupruntime.EncodeEnvironmentMutationEpochRecord(epoch)
	if err != nil {
		return taskEnvironmentMutationState{}, err
	}
	return taskEnvironmentMutationState{
		Environment: etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{
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
