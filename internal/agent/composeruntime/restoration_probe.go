package composeruntime

import (
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validUnrestoredProbeResponse(
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
	response *agentpb.ComposeHelperResponse,
) bool {
	return response.GetSchema() == composeHelperSchema && executionplan.RejectUnknown(response) == nil &&
		response.GetExitCode() == 0 && response.GetDiagnostic() == agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE &&
		response.GetProxyEvidence() == nil && response.GetRecreateEvidence() == nil && response.GetCandidateAbsenceEvidence() == nil &&
		selectedServingProbe(assignment, step)
}

// A negative observation can select only the compensation already sealed for
// this exact serving member. It is not a new target or restoration evidence.
func selectedServingProbe(assignment taskassignment.Assignment, step *agentpb.ExecutionStep) bool {
	probe := step.GetCandidateRestorationProbe()
	authority := assignment.RestorationAuthority
	if probe == nil || step.GetStepId() == "" ||
		step.GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE ||
		!taskassignment.HasServingPredecessorAuthority(authority, probe.GetServiceId()) ||
		probe.GetCandidateArtifactId() != authority.GetCandidateArtifactId() ||
		executionplan.RestorationTargetForService(
			authority,
			probe.GetServiceId(),
		) != agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR {
		return false
	}
	matched := false
	for _, member := range assignment.Plan.GetCandidateReleaseProcedure().GetMembers() {
		if member.GetServiceId() != probe.GetServiceId() {
			continue
		}
		if matched || member.GetServingPredecessor() == nil ||
			member.GetCandidateReleaseId() != probe.GetCandidateReleaseId() ||
			member.GetCandidateArtifactId() != probe.GetCandidateArtifactId() ||
			member.GetServingPredecessor().GetProbeStepId() != step.GetStepId() {
			return false
		}
		matched = true
	}
	if !matched {
		return false
	}
	for _, member := range authority.GetCandidates() {
		if member.GetServiceId() == probe.GetServiceId() {
			return member.GetReleaseId() == probe.GetCandidateReleaseId()
		}
	}
	return false
}
