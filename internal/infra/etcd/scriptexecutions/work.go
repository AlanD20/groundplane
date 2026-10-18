package scriptexecutions

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// WorkBinding is the immutable lookup for a standalone Script work item.
// Release hooks are owned by the captured Release input, not this binding.
type WorkBinding struct {
	TaskID      string              `json:"task_id"`
	WorkItemID  string              `json:"work_item_id"`
	ExecutionID string              `json:"execution_id"`
	Input       workexecution.Input `json:"input"`
}

func WorkKey(workID string) string { return "/v1/script-work/" + workID }

func (binding WorkBinding) Validate() error {
	if ids.Validate(ids.KindTask, binding.TaskID) != nil || ids.Validate(ids.KindWorkItem, binding.WorkItemID) != nil ||
		!ValidRawScriptExecutionID(binding.ExecutionID) || binding.Input.Validate() != nil {
		return errs.New(errs.KindValidationFailed, "Script work binding is invalid")
	}
	return nil
}
