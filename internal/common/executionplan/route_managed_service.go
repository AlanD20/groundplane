package executionplan

import (
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Route file reconciliation may start a missing router but must not force a
// replacement. Its closed forward and recovery procedure grants no native up.
func routeManagedServiceSelection(
	plan *agentpb.ExecutionPlan,
	step *agentpb.ExecutionStep,
	artifacts map[string]*agentpb.ComposeArtifact,
) bool {
	if len(plan.GetSteps()) < 3 ||
		plan.Steps[1] != step {
		return false
	}
	_, err := routeComponentRecoveryEnvironment(plan, artifacts)
	return err == nil
}
