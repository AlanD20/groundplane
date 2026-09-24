package taskjournal

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// IsBlueprintChild distinguishes private execution from an operator action.
// The caller must validate the complete Task before using this role.
func IsBlueprintChild(params map[string]string) bool {
	_, child := params[TaskBlueprintParentParam]
	return child
}

func ValidateBlueprintChild(taskID string, actor TaskActor, executor TaskExecutor, params map[string]string) error {
	parent, child := params[TaskBlueprintParentParam]
	if child && (ids.Validate(ids.KindTask, parent) != nil || parent == taskID ||
		actor != TaskActorSystem || executor != TaskExecutorAgent) {
		return errs.New(errs.KindValidationFailed, "Blueprint child Task identity is invalid")
	}
	return nil
}
