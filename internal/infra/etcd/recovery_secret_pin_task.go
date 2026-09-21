package etcd

import (
	"cmp"
	"context"
	"encoding/hex"
	"errors"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TaskSecretPinSet binds the operation's original owning Task and exact member
// identity through restart and Retry. It never contains Secret value bytes.
type TaskSecretPinSet struct {
	TaskID string `json:"task_id"`
	Count  uint64 `json:"count"`
	SHA256 string `json:"sha256"`
}

func validateTaskSecretPinSet(binding *TaskSecretPinSet) error {
	if binding == nil {
		return nil
	}
	digest, err := hex.DecodeString(binding.SHA256)
	if ids.Validate(ids.KindTask, binding.TaskID) != nil || binding.Count == 0 ||
		binding.Count > tasksecretpins.MaximumPins || err != nil || len(digest) != 32 ||
		hex.EncodeToString(digest) != binding.SHA256 {
		return errs.New(errs.KindValidationFailed, "Task Secret pin set identity is invalid")
	}
	return nil
}

func prepareRecoverySecretPins(
	ctx context.Context, store hierarchyStore, task TaskRecord,
) (TaskRecord, tasksecretpins.Prepared, error) {
	if task.Configuration == nil || len(task.Materializations) == 0 &&
		(task.Configuration.BackingHookInputs == nil || len(task.Configuration.BackingHookInputs.SecretSources) == 0) {
		return task, tasksecretpins.Prepared{}, nil
	}
	if task.Configuration.SecretPins != nil {
		return TaskRecord{}, tasksecretpins.Prepared{}, errs.New(
			errs.KindStateConflict,
			"Task Secret pins are already prepared",
		)
	}
	bySecret := make(map[string]tasksecretpinrecord.Record)
	if task.Configuration.BackingHookInputs != nil {
		for _, pin := range task.Configuration.BackingHookInputs.SecretSources {
			if err := addRecoverySecretPin(task.OperationID, pin, bySecret); err != nil {
				return TaskRecord{}, tasksecretpins.Prepared{}, err
			}
		}
	}
	if len(task.Materializations) == 0 {
		return prepareRecoverySecretPinSet(ctx, store, task, bySecret)
	}
	read, err := store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{runtimeConfigurationHeadKey(task.Owner.EnvironmentID)}},
	)
	if err != nil {
		return TaskRecord{}, tasksecretpins.Prepared{}, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 1 {
		return TaskRecord{}, tasksecretpins.Prepared{}, errs.New(
			errs.KindInternal,
			"Secret pin source read is incomplete",
		)
	}
	defer clearKeyValues(read.Values)
	configuration, err := runtimeconfiguration.NewRepository(runtimeConfigurationStore{store: store})
	if err != nil {
		return TaskRecord{}, tasksecretpins.Prepared{}, err
	}
	if err := collectRecoverySecretPins(task.OperationID, task.Materializations, bySecret); err != nil {
		return TaskRecord{}, tasksecretpins.Prepared{}, err
	}
	if task.Configuration.Prior != nil {
		snapshot, loadErr := configuration.Load(ctx, *task.Configuration.Prior, read.ReadRevision)
		if loadErr != nil {
			return TaskRecord{}, tasksecretpins.Prepared{}, loadErr
		}
		affected := make(map[string]struct{}, len(task.Materializations))
		for _, file := range task.Materializations {
			affected[file.Destination] = struct{}{}
		}
		priorFiles := make([]taskmaterialization.Record, 0, len(affected))
		for _, file := range snapshot.Files {
			if _, changed := affected[file.Destination]; changed {
				priorFiles = append(priorFiles, file)
			}
		}
		if err := collectRecoverySecretPins(task.OperationID, priorFiles, bySecret); err != nil {
			return TaskRecord{}, tasksecretpins.Prepared{}, err
		}
	}
	return prepareRecoverySecretPinSet(ctx, store, task, bySecret)
}

func prepareRecoverySecretPinSet(
	ctx context.Context,
	store hierarchyStore,
	task TaskRecord,
	bySecret map[string]tasksecretpinrecord.Record,
) (TaskRecord, tasksecretpins.Prepared, error) {
	if len(bySecret) == 0 {
		return task, tasksecretpins.Prepared{}, nil
	}
	pins := make([]tasksecretpinrecord.Record, 0, len(bySecret))
	for _, pin := range bySecret {
		pins = append(pins, pin)
	}
	slices.SortFunc(
		pins,
		func(left, right tasksecretpinrecord.Record) int { return cmp.Compare(left.SecretID, right.SecretID) },
	)
	repository, err := tasksecretpins.NewEtcdRepository(store, taskSecretPinProjectID(task))
	if err != nil {
		return TaskRecord{}, tasksecretpins.Prepared{}, err
	}
	prepared, err := repository.Prepare(ctx, task.OperationID, task.ID, pins)
	if err != nil {
		return TaskRecord{}, tasksecretpins.Prepared{}, finishRecoverySecretPreparation(ctx, store, task, err)
	}
	task = cloneTaskRecord(task)
	task.Configuration.SecretPins = &TaskSecretPinSet{
		TaskID: prepared.TaskID(), Count: prepared.MembershipCount(), SHA256: prepared.MembershipSHA256(),
	}
	return task, prepared, nil
}

func addRecoverySecretPin(
	operationID string,
	pin tasksecretpinrecord.Record,
	selected map[string]tasksecretpinrecord.Record,
) error {
	if tasksecretpinrecord.Validate(pin) != nil || pin.OperationID != operationID {
		return errs.New(errs.KindValidationFailed, "recovery Secret pin is invalid")
	}
	if previous, found := selected[pin.SecretID]; found && previous != pin {
		return errs.New(errs.KindStateConflict, "recovery requires conflicting versions of one Secret")
	}
	selected[pin.SecretID] = pin
	if len(selected) > tasksecretpins.MaximumPins {
		return errs.New(errs.KindValidationFailed, "recovery Secret pin set exceeds its bound")
	}
	return nil
}

func collectRecoverySecretPins(
	operationID string, files []taskmaterialization.Record, selected map[string]tasksecretpinrecord.Record,
) error {
	for _, file := range files {
		if file.Source.GeneratedEnvironment == nil {
			continue
		}
		for _, value := range file.Source.GeneratedEnvironment.Values {
			if value.Secret == nil {
				continue
			}
			source := value.Secret
			pin := tasksecretpinrecord.Record{OperationID: operationID, SecretID: source.SecretID,
				MetadataRevision: source.Revision, CiphertextSHA256: source.CiphertextSHA256}
			if err := addRecoverySecretPin(operationID, pin, selected); err != nil {
				return err
			}
		}
	}
	return nil
}

// A rejected or uncertain publication may discard preparation only after
// proving its Task did not publish. Cleanup uses a separate bounded context.
func finishRecoverySecretPreparation(ctx context.Context, store hierarchyStore, task TaskRecord, cause error) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	read, err := store.GetMany(cleanup, etcdstore.GetManyRequest{Keys: []string{taskKey(task.ID)}})
	if err == nil && (read == nil || len(read.Values) != 1) {
		err = errs.New(errs.KindInternal, "Secret pin cleanup Task read is incomplete")
	}
	if err == nil {
		defer clearKeyValues(read.Values)
		if read.Values[0] != nil {
			return cause
		}
		repository, createErr := tasksecretpins.NewEtcdRepository(store, taskSecretPinProjectID(task))
		if createErr != nil {
			err = createErr
		} else {
			err = repository.Abandon(cleanup, task.OperationID)
		}
	}
	if err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, errors.Join(cause, err))
	}
	return cause
}

func recoverySecretPinActivation(prepared tasksecretpins.Prepared) (taskMaterializationProjectionChange, error) {
	if prepared.IsZero() {
		return taskMaterializationProjectionChange{}, nil
	}
	fragment, err := tasksecretpins.Activation(prepared)
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

func bindRecoverySecretPinPublication(
	change taskMaterializationProjectionChange, conditions []etcdstore.Condition, mutations []etcdstore.Mutation,
	classify func(int64, []*etcdstore.KeyValue) error,
) ([]etcdstore.Condition, []etcdstore.Mutation, func(int64, []*etcdstore.KeyValue) error) {
	base := len(conditions)
	conditions = append(conditions, change.conditions...)
	mutations = append(mutations, change.mutations...)
	return conditions, mutations, func(revision int64, values []*etcdstore.KeyValue) error {
		if len(values) != base+len(change.conditions) {
			return errs.New(errs.KindInternal, "recovery Secret publication compare evidence is incomplete")
		}
		if err := classify(revision, values[:base]); err != nil {
			return err
		}
		for index, condition := range change.conditions {
			if keyValueRevision(values[base+index]) != condition.ModRevision {
				return errs.New(errs.KindStateConflict, "recovery Secret preparation changed before publication")
			}
		}
		return nil
	}
}

func (repository *TaskRepository) recoverySecretPinClaimConditions(
	ctx context.Context,
	task TaskRecord,
) ([]etcdstore.Condition, error) {
	if task.Configuration == nil || task.Configuration.SecretPins == nil {
		return nil, nil
	}
	root, err := repository.loadTaskSecretPinRoot(ctx, task)
	if err != nil {
		return nil, err
	}
	if root.AttemptID() != task.ID {
		return nil, errs.New(errs.KindStateConflict, "Task recovery Secret authority belongs to another attempt")
	}
	return []etcdstore.Condition{{Key: tasksecretpins.RootKey(task.OperationID), ModRevision: root.Revision()}}, nil
}

func (repository *TaskRepository) loadTaskSecretPinRoot(
	ctx context.Context, task TaskRecord,
) (tasksecretpins.ActiveRoot, error) {
	pins, err := tasksecretpins.NewEtcdRepository(repository.store, taskSecretPinProjectID(task))
	if err != nil {
		return tasksecretpins.ActiveRoot{}, err
	}
	root, found, err := pins.LoadActive(ctx, task.OperationID)
	if err != nil {
		return tasksecretpins.ActiveRoot{}, err
	}
	binding := task.Configuration.SecretPins
	if !found || root.Phase() != tasksecretpins.RootPhaseActive || root.TaskID() != binding.TaskID ||
		root.MembershipCount() != binding.Count || root.MembershipSHA256() != binding.SHA256 {
		return tasksecretpins.ActiveRoot{}, errs.New(
			errs.KindStateConflict,
			"Task no longer owns its recovery Secret values",
		)
	}
	return root, nil
}

func taskSecretPinProjectID(task TaskRecord) string {
	if task.Configuration != nil && task.Configuration.BackingHookInputs != nil {
		return task.Configuration.BackingHookInputs.ProjectID
	}
	return task.Owner.ProjectID
}
