package taskplanning

import (
	domain "github.com/AlanD20/groundplane/internal/core/release"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// A retained slot or rolling predecessor requires the shared restoration
// executor. Single-target compensation cannot prove restoration of the complete
// captured set, including when a new recreate Release replaced rolling replicas.
func bindRetainedOrdinaryRecovery(member releaserender.ReleaseTaskRenderMember, steps []*agentpb.ExecutionStep) {
	if member.Render.PriorRuntime == nil ||
		len(member.Render.PriorRuntime.RetainedPriorArtifact) == 0 &&
			member.Render.PriorTarget != domain.WorkloadRolling {
		return
	}
	for _, step := range steps {
		switch step.GetPolicy() {
		case agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE:
			step.Payload = &agentpb.ExecutionStep_CandidateRestorationProbe{
				CandidateRestorationProbe: &agentpb.CandidateRestorationProbe{
					CandidateArtifactId: member.Render.ArtifactID, ServiceId: member.Render.ServiceID,
					CandidateReleaseId: member.Intent.ID,
				},
			}
		case agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE:
			step.Payload = &agentpb.ExecutionStep_CandidateRestorationCompensate{
				CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{
					CandidateArtifactId: member.Render.ArtifactID, ServiceId: member.Render.ServiceID,
					CandidateReleaseId: member.Intent.ID,
				},
			}
		}
	}
}
