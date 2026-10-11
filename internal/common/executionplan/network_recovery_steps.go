package executionplan

import "github.com/AlanD20/groundplane/proto/agentpb"

// These selectors operate only on an already validated closed procedure.
func NetworkRecoveryForwardSteps(procedure *agentpb.ManagedNetworkRecoveryProcedure) []string {
	if removal := procedure.GetRemoval(); removal != nil {
		return []string{removal.GetStepId()}
	}
	var steps []string
	for _, member := range procedure.GetPreparation().GetMembers() {
		steps = append(steps, member.GetEnsureStepId())
	}
	return steps
}

func NetworkRecoverySettlementSteps(procedure *agentpb.ManagedNetworkRecoveryProcedure) []string {
	if removal := procedure.GetRemoval(); removal != nil {
		return []string{removal.GetStepId()}
	}
	var steps []string
	for _, member := range procedure.GetPreparation().GetMembers() {
		steps = append(steps, member.GetProbeStepId())
	}
	return steps
}
