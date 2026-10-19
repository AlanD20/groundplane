package etcd

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

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
