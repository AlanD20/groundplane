package blueprintattachinputs

import "github.com/AlanD20/groundplane/internal/common/workexecution"

// MatchesWork binds captured provisioning to its actual operator action. A
// direct Attach is not represented as a one-resource Blueprint Apply.
func (generation Generation) MatchesWork(task workexecution.Task, item workexecution.Item) bool {
	if task.Validate() != nil || item.Validate() != nil || item.TaskID != task.ID ||
		item.Operation != workexecution.Attach || generation.ParentTaskID != task.ID ||
		generation.RevisionID != task.ID || generation.WorkItemID != item.ID ||
		generation.AttachID != item.TargetID || generation.EnvironmentID != task.Scope.EnvironmentID {
		return false
	}
	switch task.Action {
	case workexecution.ActionApply:
		return task.TargetID == generation.EnvironmentID
	case workexecution.ActionAttach:
		return task.TargetID == generation.AttachID && task.ExpectedItems == 1 &&
			len(item.Dependencies) == 0 && generation.Binding == nil
	default:
		return false
	}
}
