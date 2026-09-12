package executionplan

import (
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type CandidateRestorationSelection struct {
	ServiceID string
	ReleaseID string
	Target    agentpb.ReleaseRestorationTarget
}

// RestorationTargetForService reads an already-selected authority. Missing or
// duplicate members return UNSPECIFIED, which executors must reject.
func RestorationTargetForService(
	authority *agentpb.ReleaseRestorationAuthority,
	serviceID string,
) agentpb.ReleaseRestorationTarget {
	selected := agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_UNSPECIFIED
	found := false
	for _, member := range authority.GetCandidates() {
		if serviceID == "" || member.GetServiceId() != serviceID {
			continue
		}
		if found {
			return agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_UNSPECIFIED
		}
		found, selected = true, member.GetTarget()
	}
	return selected
}
