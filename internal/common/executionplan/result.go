package executionplan

import "github.com/AlanD20/groundplane/proto/agentpb"

// UsesEnvironmentDirectoryResult identifies the only plan shapes whose Task
// acknowledgement uses EnvironmentDirectoryTaskResult instead of ComposeTaskResult.
func UsesEnvironmentDirectoryResult(plan *agentpb.ExecutionPlan) bool {
	if plan == nil || len(plan.GetSteps()) != 1 {
		return false
	}
	step := plan.GetSteps()[0]
	return step.GetEnvironmentDirectoryCreate() != nil || step.GetEnvironmentDirectoryRemove() != nil
}
