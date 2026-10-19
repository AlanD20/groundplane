package releaserender

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// GroupMemberBinding describes a forward Service member of one accepted Group
// action. Restoration is a distinct typed operation, not another deployment
// member or a changed outcome of this source work.
type GroupMemberBinding struct {
	GroupID     string `json:"group_id"`
	OperationID string `json:"operation_id"`
	Ordinal     uint32 `json:"ordinal"`
}

func (binding GroupMemberBinding) Validate() error {
	if ids.Validate(ids.KindReleaseGroup, binding.GroupID) != nil ||
		ids.Validate(ids.KindOperation, binding.OperationID) != nil ||
		binding.Ordinal == 0 ||
		binding.Ordinal > 32 {
		return errs.New(errs.KindValidationFailed, "Release Group member binding is invalid")
	}
	return nil
}

func (capture ServiceRolloutCapture) matchesActionTarget(task workexecution.Task, item workexecution.Item) bool {
	if capture.Action != task.Action {
		return false
	}
	if group := capture.Group; group != nil {
		return group.Validate() == nil && task.Action != workexecution.ActionApply && task.TargetID == group.GroupID
	}
	return task.Action == workexecution.ActionApply || task.TargetID == item.TargetID
}
