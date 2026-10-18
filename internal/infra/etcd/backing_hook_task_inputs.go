package etcd

import (
	"context"

	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func BindBackingHookTaskInputs(
	task TaskRecord,
	projectID string,
	input taskconfiguration.BackingHookEncryptedInputs,
) (TaskRecord, error) {
	if ids.Validate(ids.KindProject, projectID) != nil || input.OperationID != task.OperationID ||
		taskconfiguration.ValidateBackingHookEncryptedInputs(input) != nil {
		return TaskRecord{}, errs.New(errs.KindValidationFailed, "Backing hook Task input binding is invalid")
	}
	bound := cloneTaskRecord(task)
	if bound.Configuration == nil {
		bound.Configuration = &taskconfiguration.TaskConfiguration{}
	}
	if bound.Configuration.BackingHookInputs != nil || bound.Configuration.SecretPins != nil {
		return TaskRecord{}, errs.New(errs.KindStateConflict, "Backing hook Task inputs are already bound")
	}
	bound.Configuration.BackingHookInputs = &taskconfiguration.TaskBackingHookInputSet{
		ProjectID: projectID, CiphertextSHA256: input.CiphertextSHA256,
		SecretSources: append([]tasksecretpinrecord.Record(nil), input.SecretSources...),
	}
	if err := validateTaskBackingHookInputSet(bound); err != nil {
		return TaskRecord{}, err
	}
	return bound, nil
}

func validateTaskBackingHookInputSet(task TaskRecord) error {
	if task.Configuration == nil || task.Configuration.BackingHookInputs == nil {
		return nil
	}
	binding := task.Configuration.BackingHookInputs
	if ids.Validate(ids.KindProject, binding.ProjectID) != nil || !recordcodec.ValidSHA256(binding.CiphertextSHA256) {
		return errs.New(errs.KindValidationFailed, "Task backing hook input authority is invalid")
	}
	previous := ""
	for _, source := range binding.SecretSources {
		if tasksecretpinrecord.Validate(source) != nil || source.OperationID != task.OperationID ||
			source.SecretID <= previous {
			return errs.New(errs.KindValidationFailed, "Task backing hook Secret sources are invalid")
		}
		previous = source.SecretID
	}
	return nil
}

func (repository *AttachRepository) GetBackingHookTaskInputs(
	ctx context.Context,
	task TaskRecord,
) (taskconfiguration.BackingHookEncryptedInputs, error) {
	if err := ValidateTaskRecord(task); err != nil || task.Configuration == nil ||
		task.Configuration.BackingHookInputs == nil {
		return taskconfiguration.BackingHookEncryptedInputs{}, errs.New(
			errs.KindValidationFailed,
			"Task backing hook input authority is missing",
		)
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{taskconfiguration.BackingHookTaskInputKey(task.OperationID)},
	})
	if err != nil {
		return taskconfiguration.BackingHookEncryptedInputs{}, err
	}
	if result == nil || len(result.Values) != 1 || result.Values[0] == nil {
		return taskconfiguration.BackingHookEncryptedInputs{}, errs.New(
			errs.KindStateConflict,
			"Backing hook Task inputs are unavailable",
		)
	}
	defer etcdstore.ClearValues(result.Values)
	record, err := taskconfiguration.DecodeBackingHookEncryptedInputs(result.Values[0].Value)
	if err != nil {
		return taskconfiguration.BackingHookEncryptedInputs{}, err
	}
	if record.OperationID != task.OperationID ||
		record.CiphertextSHA256 != task.Configuration.BackingHookInputs.CiphertextSHA256 {
		clear(record.Ciphertext)
		return taskconfiguration.BackingHookEncryptedInputs{}, errs.New(
			errs.KindStateConflict,
			"Backing hook Task input authority changed",
		)
	}
	return record, nil
}
