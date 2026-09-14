package agent

import (
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// The executor tests use deliberately small proxy fixtures, not assignment-
// admission fixtures. Supply their selected pairs explicitly so the executor
// consumes the sealed procedure rather than inferring one from host state.
func bindProxyRecoveryFixture(t *testing.T, plan *agentpb.ExecutionPlan) {
	t.Helper()
	if plan.CandidateReleaseProcedure != nil {
		return
	}
	procedure := &agentpb.CandidateReleaseProcedure{}
	for _, step := range plan.Steps {
		probe := step.GetServiceProxyProbe()
		if probe == nil {
			continue
		}
		compensateID := ""
		for _, selected := range plan.Steps {
			compensate := selected.GetServiceProxyCompensate()
			if compensate != nil && compensate.ServiceId == probe.ServiceId {
				if compensateID != "" {
					t.Fatal("ambiguous proxy recovery fixture")
				}
				compensateID = selected.StepId
			}
		}
		if compensateID == "" {
			t.Fatal("proxy recovery fixture has no compensation pair")
		}
		procedure.Members = append(procedure.Members, &agentpb.CandidateReleaseMember{ServiceId: probe.ServiceId,
			ServingPredecessor: &agentpb.ServingPredecessorRestoration{
				ProbeStepId:      step.StepId,
				CompensateStepId: compensateID,
			}})
	}
	plan.CandidateReleaseProcedure = procedure
}
