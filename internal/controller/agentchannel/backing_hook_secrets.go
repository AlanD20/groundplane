package agentchannel

import "github.com/AlanD20/groundplane/proto/agentpb"

func clearBackingHookPlanSecrets(plan *agentpb.ExecutionPlan) {
	if plan == nil {
		return
	}
	for _, step := range plan.GetSteps() {
		procedure := step.GetBackingHookProcedure()
		if procedure == nil {
			continue
		}
		for _, value := range append(procedure.GetInputs(), procedure.GetFacts()...) {
			if value != nil {
				clear(value.Value)
				value.Value = nil
			}
		}
	}
}
