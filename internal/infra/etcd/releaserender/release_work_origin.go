package releaserender

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReleaseOperationKind is the closed mapping between an accepted operator
// action and its retained Release intent. A Rollback cannot be published or
// replayed as Deploy merely because both run the same typed executor.
func ReleaseOperationKind(action workexecution.Action) (domain.OperationKind, error) {
	switch action {
	case workexecution.ActionApply:
		return domain.OperationBlueprintApply, nil
	case workexecution.ActionDeploy:
		return domain.OperationDeploy, nil
	case workexecution.ActionRollback:
		return domain.OperationRollback, nil
	default:
		return "", errs.New(errs.KindValidationFailed, "Task action does not authorize a Service release")
	}
}

func IsReleaseAction(action workexecution.Action) bool {
	_, err := ReleaseOperationKind(action)
	return err == nil
}

// ServiceRolloutKey belongs to execution, not the authored Blueprint revision.
// Resource identity and applied runtime survive collection of this capture.
func ServiceRolloutKey(environmentID, taskID, workItemID string) string {
	return "/v1/execution/service-rollouts/" + environmentID + "/" + taskID + "/" + workItemID
}

// MatchesWork validates provenance as well as ordinary input ownership. Direct
// Deploy/Rollback retain their authored projection identity; Apply must own the
// projection it accepted. None reads a desired head to fill missing input.
func (capture ServiceRolloutCapture) MatchesWork(task workexecution.Task, item workexecution.Item) error {
	render, err := OpenServiceRolloutCapture(capture)
	if err != nil {
		return err
	}
	if task.Validate() != nil || item.Validate() != nil || capture.Action != task.Action ||
		item.TaskID != task.ID || item.Operation != workexecution.Release ||
		capture.TaskID != task.ID || capture.WorkItemID != item.ID || capture.Scope != task.Scope ||
		capture.Placement != item.Placement || render.ServiceID != item.TargetID ||
		!capture.CreatedAt.Equal(item.CreatedAt) || !capture.Deadline.Equal(item.Deadline) ||
		capture.Deadline.After(task.Deadline) {
		return errs.New(errs.KindStateConflict, "Service rollout belongs to another Task action, work item or host")
	}
	return nil
}
