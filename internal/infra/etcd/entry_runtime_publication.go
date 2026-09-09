package etcd

import (
	"strconv"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const TaskEntryRuntimeEpochParam = "entry_runtime_epoch_revision"

func EntryRuntimeEpochRevision(task TaskRecord) (int64, error) {
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
	if !entryMutation && task.Params[TaskEntryRuntimeEpochParam] == "" {
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
