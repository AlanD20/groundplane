package executionplan

import "github.com/AlanD20/groundplane/proto/agentpb"

// UsesEnvironmentDirectoryResult identifies the only plan shapes whose Task
// acknowledgement uses EnvironmentDirectoryTaskResult instead of ComposeTaskResult.
func UsesEnvironmentDirectoryResult(plan *agentpb.ExecutionPlan) bool {
	if plan == nil {
		return false
	}
	steps := plan.GetSteps()
	if len(steps) == 1 {
		return steps[0].GetEnvironmentDirectoryCreate() != nil || steps[0].GetEnvironmentDirectoryRemove() != nil
	}
	if len(steps) != 2 && len(steps) != 3 {
		return false
	}
	path := steps[len(steps)-1].GetManagedVolumeDirectoryRemove()
	docker := steps[len(steps)-2].GetManagedVolumeRemove()
	return path != nil && docker != nil && path.VolumeId == docker.VolumeId &&
		(len(steps) == 2 || steps[0].GetComposeApply() != nil)
}
