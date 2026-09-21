package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RecoverTaskSecretPinSources runs during construction, before requests or
// dispatch. It removes unpublished preparation and resumes authorized release.
func RecoverTaskSecretPinSources(ctx context.Context, store hierarchyStore) error {
	pins, err := tasksecretpins.NewEtcdRepository(store, "")
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
	pins, err := tasksecretpins.NewEtcdRepository(repository.store, "")
	if err != nil {
		return false, err
	}
	return pins.ResumeRelease(ctx)
}

func (repository *TaskRepository) prepareRecoverySecretPinTerminal(
	ctx context.Context, task TaskRecord, revision int64,
) (taskMaterializationProjectionChange, error) {
	if task.Configuration == nil || task.Status != taskjournal.TaskStatusCompleted {
		return taskMaterializationProjectionChange{}, nil
	}
	change := taskMaterializationProjectionChange{}
	if task.Configuration.BackingHookInputs != nil {
		key := taskconfiguration.BackingHookTaskInputKey(task.OperationID)
		read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
		if err != nil {
			return taskMaterializationProjectionChange{}, err
		}
		if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
			if read != nil {
				etcdstore.ClearValues(read.Values)
			}
			return taskMaterializationProjectionChange{}, errs.New(
				errs.KindStateConflict,
				"Backing hook Task inputs are unavailable at completion",
			)
		}
		defer etcdstore.ClearValues(read.Values)
		stored, err := taskconfiguration.DecodeBackingHookEncryptedInputs(read.Values[0].Value)
		if err != nil {
			return taskMaterializationProjectionChange{}, err
		}
		defer clear(stored.Ciphertext)
		if stored.OperationID != task.OperationID ||
			stored.CiphertextSHA256 != task.Configuration.BackingHookInputs.CiphertextSHA256 {
			return taskMaterializationProjectionChange{}, errs.New(
				errs.KindStateConflict,
				"Backing hook Task input authority changed before completion",
			)
		}
		change.applies = true
		change.conditions = append(change.conditions, etcdstore.Condition{Key: key, ModRevision: read.Values[0].ModRevision})
		change.mutations = append(change.mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: key})
	}
	if task.Configuration.SecretPins == nil {
		return change, nil
	}
	pins, err := repository.beginRecoverySecretPinRelease(ctx, task)
	if err != nil {
		clearTaskMaterializationProjectionChange(change)
		return taskMaterializationProjectionChange{}, err
	}
	change.applies = change.applies || pins.applies
	change.conditions = append(change.conditions, pins.conditions...)
	change.mutations = append(change.mutations, pins.mutations...)
	return change, nil
}

func (repository *TaskRepository) beginRecoverySecretPinRelease(
	ctx context.Context, task TaskRecord,
) (taskMaterializationProjectionChange, error) {
	pins, err := tasksecretpins.NewEtcdRepository(repository.store, taskSecretPinProjectID(task))
	if err != nil {
		return taskMaterializationProjectionChange{}, err
	}
	root, found, err := pins.LoadActive(ctx, task.OperationID)
	if err != nil {
		return taskMaterializationProjectionChange{}, err
	}
	binding := task.Configuration.SecretPins
	if !found || root.AttemptID() != task.ID || root.TaskID() != binding.TaskID ||
		root.MembershipCount() != binding.Count ||
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
	conditions, mutations, err := tasksecretpins.EtcdFragment(fragment)
	if err != nil {
		fragment.Clear()
		return taskMaterializationProjectionChange{}, err
	}
	return taskMaterializationProjectionChange{applies: true, conditions: conditions, mutations: mutations}, nil
}

// Expiry releases only after Retry and recovery ownership are gone. A retry
// winning the active-operation compare preserves every membership.
func (repository *TaskRepository) prepareRecoverySecretPinExpiry(
	ctx context.Context, task TaskRecord, taskRevision int64, retention etcdstore.KeyValue, now time.Time,
) (bool, error) {
	if task.Configuration == nil {
		return false, nil
	}
	if task.Configuration.SecretPins == nil {
		return repository.prepareBackingHookInputExpiry(ctx, task, taskRevision, retention, now)
	}
	if !taskjournal.IsTerminalTaskStatus(task.Status) || task.RetainUntil == nil || task.RetainUntil.After(now) {
		return true, errs.New(errs.KindStateConflict, "recovery Secret retry authority has not expired")
	}
	pins, err := tasksecretpins.NewEtcdRepository(repository.store, taskSecretPinProjectID(task))
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
	keys := []string{taskjournal.TaskActiveOperationKey(task.OperationID), taskjournal.TaskAssignmentIndexKey(root.AttemptID()),
		taskjournal.TaskRecoveryProofRequiredKey(root.AttemptID()), taskassignments.ReleaseRecoveryKey(root.AttemptID()), taskjournal.TaskStorageKey(root.AttemptID())}
	hookInputIndex := -1
	if task.Configuration.BackingHookInputs != nil {
		hookInputIndex = len(keys)
		keys = append(keys, taskconfiguration.BackingHookTaskInputKey(task.OperationID))
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return true, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		return true, errs.New(errs.KindInternal, "recovery Secret expiry evidence is incomplete")
	}
	defer etcdstore.ClearValues(read.Values)
	if read.Values[0] != nil || read.Values[1] != nil || read.Values[2] != nil {
		return true, nil
	}
	if read.Values[4] == nil {
		return true, taskjournal.CorruptPruneIntent()
	}
	latest, decodeErr := decodeTaskRecord(read.Values[4].Value)
	if decodeErr != nil || latest.ID != root.AttemptID() || latest.OperationID != task.OperationID ||
		latest.Configuration == nil || latest.Configuration.SecretPins == nil ||
		*latest.Configuration.SecretPins != *task.Configuration.SecretPins {
		return true, taskjournal.CorruptPruneIntent()
	}
	if hookInputIndex >= 0 {
		if read.Values[hookInputIndex] == nil || latest.Configuration.BackingHookInputs == nil ||
			!taskconfiguration.SameTaskBackingHookInputSet(
				latest.Configuration.BackingHookInputs,
				task.Configuration.BackingHookInputs,
			) {
			return true, taskjournal.CorruptPruneIntent()
		}
		stored, decodeErr := taskconfiguration.DecodeBackingHookEncryptedInputs(read.Values[hookInputIndex].Value)
		if decodeErr != nil {
			return true, decodeErr
		}
		clear(stored.Ciphertext)
		if stored.OperationID != task.OperationID ||
			stored.CiphertextSHA256 != task.Configuration.BackingHookInputs.CiphertextSHA256 {
			return true, taskjournal.CorruptPruneIntent()
		}
	}
	if !taskjournal.IsTerminalTaskStatus(latest.Status) || latest.RetainUntil == nil || latest.RetainUntil.After(now) {
		return true, nil
	}
	if read.Values[3] != nil {
		recovery, decodeErr := taskassignments.DecodeReleaseRecoveryRecord(read.Values[3].Value)
		if decodeErr != nil {
			return true, decodeErr
		}
		if recovery.Phase != taskassignments.ReleaseRecoveryPhaseProven {
			return true, nil
		}
	}
	fragment, err := tasksecretpins.BeginRelease(root)
	if err != nil {
		return true, err
	}
	defer fragment.Clear()
	conditions, mutations, err := tasksecretpins.EtcdFragment(fragment)
	if err != nil {
		return true, err
	}
	change := taskMaterializationProjectionChange{conditions: conditions, mutations: mutations}
	defer clearTaskMaterializationProjectionChange(change)
	change.conditions = append(change.conditions, etcdstore.Condition{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskRevision},
		etcdstore.Condition{Key: retention.Key, ModRevision: retention.ModRevision})
	for index, key := range keys {
		change.conditions = append(
			change.conditions,
			etcdstore.Condition{Key: key, ModRevision: keyValueRevision(read.Values[index])},
		)
	}
	if hookInputIndex >= 0 {
		change.mutations = append(change.mutations, etcdstore.Mutation{
			Type: etcdstore.MutationDelete, Key: taskconfiguration.BackingHookTaskInputKey(task.OperationID),
		})
	}
	commit, err := repository.store.Transact(ctx, change.conditions, change.mutations)
	etcdstore.ClearValues(commit.FailureReads)
	if err != nil {
		return true, err
	}
	if !commit.Succeeded {
		return true, errs.New(errs.KindStateConflict, "recovery Secret expiry raced Task ownership")
	}
	_, err = pins.ResumeRelease(ctx)
	return true, err
}

func (repository *TaskRepository) prepareBackingHookInputExpiry(
	ctx context.Context,
	task TaskRecord,
	taskRevision int64,
	retention etcdstore.KeyValue,
	now time.Time,
) (bool, error) {
	if task.Configuration.BackingHookInputs == nil {
		return false, nil
	}
	if !taskjournal.IsTerminalTaskStatus(task.Status) || task.RetainUntil == nil || task.RetainUntil.After(now) {
		return true, errs.New(errs.KindStateConflict, "Backing hook retry authority has not expired")
	}
	keys := []string{
		taskjournal.TaskActiveOperationKey(task.OperationID), taskjournal.TaskAssignmentIndexKey(task.ID),
		taskconfiguration.BackingHookTaskInputKey(task.OperationID),
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return true, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		return true, errs.New(errs.KindInternal, "Backing hook expiry evidence is incomplete")
	}
	defer etcdstore.ClearValues(read.Values)
	if read.Values[0] != nil || read.Values[1] != nil {
		return true, nil
	}
	if read.Values[2] == nil {
		return false, nil
	}
	stored, err := taskconfiguration.DecodeBackingHookEncryptedInputs(read.Values[2].Value)
	if err != nil {
		return true, err
	}
	defer clear(stored.Ciphertext)
	if stored.OperationID != task.OperationID ||
		stored.CiphertextSHA256 != task.Configuration.BackingHookInputs.CiphertextSHA256 {
		return true, taskjournal.CorruptPruneIntent()
	}
	conditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskRevision},
		{Key: retention.Key, ModRevision: retention.ModRevision},
		{Key: keys[0]}, {Key: keys[1]},
		{Key: keys[2], ModRevision: read.Values[2].ModRevision},
	}
	commit, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{{Type: etcdstore.MutationDelete, Key: keys[2]}})
	etcdstore.ClearValues(commit.FailureReads)
	if err != nil {
		return true, err
	}
	if !commit.Succeeded {
		return true, errs.New(errs.KindStateConflict, "Backing hook expiry raced Task ownership")
	}
	return true, nil
}

func recoverySecretPinPruneConditions(task TaskRecord) []etcdstore.Condition {
	if task.Configuration == nil || task.Configuration.SecretPins == nil {
		return nil
	}
	return []etcdstore.Condition{{Key: tasksecretpins.RootKey(task.OperationID)},
		{Key: tasksecretpins.ReversePrefix(task.OperationID), Prefix: true}}
}
