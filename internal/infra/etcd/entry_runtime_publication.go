package etcd

import (
	"slices"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const TaskEntryRuntimeEpochParam = "entry_runtime_epoch_revision"

// EntryTaskRuntime pins operational intent separately from desired input. An
// explicit empty set means materialization only; absent capture is not authority
// to reconstruct or publish an Entry update.
type EntryTaskRuntime struct {
	RunningServiceIDs []string `json:"running_service_ids"`
}

func validateTaskEntryRuntime(task TaskRecord) error {
	if task.EntryRuntime == nil {
		// Historical Tasks remain readable without inventing capture authority.
		return nil
	}
	if task.Type != TaskUpdate || task.Executor != TaskExecutorAgent ||
		task.Params[TaskResourceKindParam] != TaskResourceEntry || task.EntryRuntime.RunningServiceIDs == nil {
		return errs.New(errs.KindValidationFailed, "Entry runtime capture shape is invalid")
	}
	for index, id := range task.EntryRuntime.RunningServiceIDs {
		if ids.Validate(ids.KindService, id) != nil ||
			index > 0 && task.EntryRuntime.RunningServiceIDs[index-1] >= id {
			return errs.New(errs.KindValidationFailed, "Entry running Service ids must be valid, sorted and unique")
		}
	}
	return nil
}

func cloneEntryTaskRuntime(runtime *EntryTaskRuntime) *EntryTaskRuntime {
	if runtime == nil {
		return nil
	}
	return &EntryTaskRuntime{RunningServiceIDs: slices.Clone(runtime.RunningServiceIDs)}
}

func EntryRuntimeEpochRevision(task TaskRecord) (int64, error) {
	if task.EntryRuntime == nil {
		return 0, errs.New(errs.KindValidationFailed, "Entry runtime capture is absent")
	}
	if err := validateTaskEntryRuntime(task); err != nil {
		return 0, err
	}
	value := task.Params[TaskEntryRuntimeEpochParam]
	epoch, err := strconv.ParseInt(value, 10, 64)
	if err != nil || epoch <= 0 || strconv.FormatInt(epoch, 10) != value ||
		task.Type != TaskUpdate || task.Params[TaskResourceKindParam] != TaskResourceEntry {
		return 0, errs.New(errs.KindValidationFailed, "Entry runtime capture epoch is invalid")
	}
	return epoch, nil
}

func validateEntryRuntimePublication(task TaskRecord, fence environmentMutationFenceEvidence) error {
	entryMutation := task.Type == TaskUpdate && task.Params[TaskResourceKindParam] == TaskResourceEntry
	if !entryMutation && task.Params[TaskEntryRuntimeEpochParam] == "" && task.EntryRuntime == nil {
		return nil
	}
	epoch, err := EntryRuntimeEpochRevision(task)
	if err != nil {
		return err
	}
	for _, condition := range fence.conditions {
		if condition.kind != environmentMutationFenceEpoch {
			continue
		}
		if condition.modRevision != epoch {
			return errs.New(errs.KindStateConflict, "Entry serving runtime changed before publication")
		}
		return nil
	}
	return errs.New(errs.KindInternal, "Entry runtime publication has no Environment epoch fence")
}
