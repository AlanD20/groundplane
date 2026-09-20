package taskplanning

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// A retained inactive slot requires the shared restoration executor, which
// restores and observes both captured native artifacts. A traffic-only proxy
// compensation cannot prove the inactive workload's pre-attempt configuration.
func bindRetainedOrdinaryRecovery(members []etcd.ReleaseTaskRenderMember, steps []*agentpb.ExecutionStep) {
	for index, member := range members {
		if member.Render.PriorRuntime == nil || len(member.Render.PriorRuntime.RetainedPriorArtifact) == 0 {
			continue
		}
		probe, compensate := steps[index*5+3], steps[index*5+4]
		probe.Payload = &agentpb.ExecutionStep_CandidateRestorationProbe{
			CandidateRestorationProbe: &agentpb.CandidateRestorationProbe{
				CandidateArtifactId: member.Render.ArtifactID, ServiceId: member.Render.ServiceID,
				CandidateReleaseId: member.Intent.ID,
			},
		}
		compensate.Payload = &agentpb.ExecutionStep_CandidateRestorationCompensate{
			CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{
				CandidateArtifactId: member.Render.ArtifactID, ServiceId: member.Render.ServiceID,
				CandidateReleaseId: member.Intent.ID,
			},
		}
	}
}
