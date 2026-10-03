package composeruntime

import (
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// The observer has authenticated every declared member. A Service proxy shares
// its Service ID with the workload but has its own labels and image authority.
// Consumer evidence selects the workload; an unrecognized member remains in
// the result so the caller rejects its labels or image.
func backupConsumerObservation(observed *agentpb.ObservedProject,
	artifact *agentpb.ComposeArtifact, serviceID string,
) *agentpb.ObservedProject {
	selected := proto.CloneOf(observed)
	selected.Containers = nil
	for _, container := range observed.Containers {
		if container.ServiceId != serviceID {
			continue
		}
		proxy := false
		for _, member := range artifact.Services {
			if member.ServiceId == serviceID &&
				member.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY &&
				labelPairsEqual(container.Labels, member.ExpectedLabels) {
				proxy = true
				break
			}
		}
		if !proxy {
			selected.Containers = append(selected.Containers, proto.CloneOf(container))
		}
	}
	return selected
}
