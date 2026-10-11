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

func IsReleaseNetworkChild(params map[string]string) bool {
	_, child := params[TaskReleaseNetworkParentParam]
	return child
}

func IsInternalChild(params map[string]string) bool {
	return IsBlueprintChild(params) || IsReleaseNetworkChild(params)
}

func ValidateReleaseNetworkChild(
	taskID string,
	actor TaskActor,
	executor TaskExecutor,
	taskType TaskType,
	params map[string]string,
) error {
	parent, child := params[TaskReleaseNetworkParentParam]
	if child && (ids.Validate(ids.KindTask, parent) != nil || parent == taskID || len(params) != 1 ||
		actor != TaskActorSystem || executor != TaskExecutorAgent || taskType != TaskUpdate) {
		return errs.New(errs.KindValidationFailed, "release network child Task identity is invalid")
	}
	return nil
}

func ValidateReleaseNetworkParent(
	taskID string,
	executor TaskExecutor,
	taskType TaskType,
	params map[string]string,
) error {
	child, declared := params[TaskReleaseNetworkChildParam]
	if declared && (ids.Validate(ids.KindTask, child) != nil || child == taskID || executor != TaskExecutorAgent ||
		(taskType != TaskDeploy && taskType != TaskRollback)) {
		return errs.New(errs.KindValidationFailed, "release network parent Task identity is invalid")
	}
	return nil
}

func ValidateBlueprintChild(taskID string, actor TaskActor, executor TaskExecutor, params map[string]string) error {
	parent, child := params[TaskBlueprintParentParam]
	if child && (ids.Validate(ids.KindTask, parent) != nil || parent == taskID ||
		actor != TaskActorSystem || executor != TaskExecutorAgent) {
		return errs.New(errs.KindValidationFailed, "Blueprint child Task identity is invalid")
	}
	return nil
}
