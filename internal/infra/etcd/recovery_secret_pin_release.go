package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RecoverTaskSecretPinSources runs during construction, before requests or
// dispatch. It removes unpublished preparation and resumes authorized release.
func RecoverTaskSecretPinSources(ctx context.Context, store hierarchyStore) error {
	pins, err := recoverySecretPinRepository(store, "")
	if err != nil {
		return err
	}
	if err := pins.RecoverPreparations(ctx); err != nil {
		return err
	}
	for {
		progressed, err := pins.ResumeRelease(ctx)
		if err != nil || !progressed {
			return err
		}
	}
}

func (repository *TaskRepository) ResumeTaskSourceReleases(ctx context.Context) (bool, error) {
	if repository == nil || repository.store == nil {
		return false, errs.New(errs.KindInternal, "Task source release repository is missing")
	}
	pins, err := recoverySecretPinRepository(repository.store, "")
	if err != nil {
		return false, err
	}
	return pins.ResumeRelease(ctx)
}

func (repository *TaskRepository) prepareRecoverySecretPinTerminal(
	ctx context.Context, task TaskRecord,
) (taskMaterializationProjectionChange, error) {
	if task.Configuration == nil || task.Configuration.SecretPins == nil || task.Status != TaskStatusCompleted {
		return taskMaterializationProjectionChange{}, nil
	}
	return repository.beginRecoverySecretPinRelease(ctx, task)
}

func (repository *TaskRepository) beginRecoverySecretPinRelease(
	ctx context.Context, task TaskRecord,
) (taskMaterializationProjectionChange, error) {
	pins, err := recoverySecretPinRepository(repository.store, task.Owner.ProjectID)
	if err != nil {
		return taskMaterializationProjectionChange{}, err
	}
	root, found, err := pins.LoadActive(ctx, task.OperationID)
	if err != nil {
		return taskMaterializationProjectionChange{}, err
	}
	binding := task.Configuration.SecretPins
	if !found || root.AttemptID() != task.ID || root.TaskID() != binding.TaskID || root.MembershipCount() != binding.Count ||
		root.MembershipSHA256() != binding.SHA256 ||
		root.Phase() != tasksecretpins.RootPhaseActive {
		return taskMaterializationProjectionChange{}, errs.New(
			errs.KindStateConflict,
			"recovery Secret release lost ownership",
		)
	}
	fragment, err := tasksecretpins.BeginRelease(root)
	if err != nil {
		return taskMaterializationProjectionChange{}, err
	}
	conditions, mutations, err := recoverySecretPinFragment(fragment)
	if err != nil {
		fragment.Clear()
		return taskMaterializationProjectionChange{}, err
	}
	return taskMaterializationProjectionChange{applies: true, conditions: conditions, mutations: mutations}, nil
}

// Expiry releases only after Retry and recovery ownership are gone. A retry
// winning the active-operation compare preserves every membership.
func (repository *TaskRepository) prepareRecoverySecretPinExpiry(
	ctx context.Context, task TaskRecord, taskRevision int64, retention KeyValue, now time.Time,
) (bool, error) {
	if task.Configuration == nil || task.Configuration.SecretPins == nil {
		return false, nil
	}
	if !isTerminalTaskStatus(task.Status) || task.RetainUntil == nil || task.RetainUntil.After(now) {
		return true, errs.New(errs.KindStateConflict, "recovery Secret retry authority has not expired")
	}
	pins, err := recoverySecretPinRepository(repository.store, task.Owner.ProjectID)
	if err != nil {
		return true, err
	}
	root, found, err := pins.LoadActive(ctx, task.OperationID)
	if err != nil || !found {
		return false, err
	}
	if root.Phase() == tasksecretpins.RootPhaseReleasing {
		_, err := pins.ResumeRelease(ctx)
		return true, err
	}
	keys := []string{taskActiveOperationKey(task.OperationID), taskAssignmentIndexKey(root.AttemptID()),
		taskRecoveryProofRequiredKey(root.AttemptID()), releaseRecoveryKey(root.AttemptID()), taskKey(root.AttemptID())}
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys})
	if err != nil {
		return true, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		return true, errs.New(errs.KindInternal, "recovery Secret expiry evidence is incomplete")
	}
	defer clearKeyValues(read.Values)
	if read.Values[0] != nil || read.Values[1] != nil || read.Values[2] != nil {
		return true, nil
	}
	if read.Values[4] == nil {
		return true, corruptTaskPruneIntent()
	}
	latest, decodeErr := decodeTaskRecord(read.Values[4].Value)
	if decodeErr != nil || latest.ID != root.AttemptID() || latest.OperationID != task.OperationID ||
		latest.Configuration == nil || latest.Configuration.SecretPins == nil ||
		*latest.Configuration.SecretPins != *task.Configuration.SecretPins {
		return true, corruptTaskPruneIntent()
	}
	if !isTerminalTaskStatus(latest.Status) || latest.RetainUntil == nil || latest.RetainUntil.After(now) {
		return true, nil
	}
	if read.Values[3] != nil {
		recovery, decodeErr := decodeReleaseRecoveryRecord(read.Values[3].Value)
		if decodeErr != nil {
			return true, decodeErr
		}
		if recovery.Phase != ReleaseRecoveryPhaseProven {
			return true, nil
		}
	}
	fragment, err := tasksecretpins.BeginRelease(root)
	if err != nil {
		return true, err
	}
	defer fragment.Clear()
	conditions, mutations, err := recoverySecretPinFragment(fragment)
	if err != nil {
		return true, err
	}
	change := taskMaterializationProjectionChange{conditions: conditions, mutations: mutations}
	defer clearTaskMaterializationProjectionChange(change)
	change.conditions = append(change.conditions, Condition{Key: taskKey(task.ID), ModRevision: taskRevision},
		Condition{Key: retention.Key, ModRevision: retention.ModRevision})
	for index, key := range keys {
		change.conditions = append(
			change.conditions,
			Condition{Key: key, ModRevision: keyValueRevision(read.Values[index])},
		)
	}
	commit, err := repository.store.Transact(ctx, change.conditions, change.mutations)
	clearKeyValues(commit.FailureReads)
	if err != nil {
		return true, err
	}
	if !commit.Succeeded {
		return true, errs.New(errs.KindStateConflict, "recovery Secret expiry raced Task ownership")
	}
	_, err = pins.ResumeRelease(ctx)
	return true, err
}

func recoverySecretPinPruneConditions(task TaskRecord) []Condition {
	if task.Configuration == nil || task.Configuration.SecretPins == nil {
		return nil
	}
	return []Condition{{Key: tasksecretpins.RootKey(task.OperationID)},
		{Key: tasksecretpins.ReversePrefix(task.OperationID), Prefix: true}}
}
