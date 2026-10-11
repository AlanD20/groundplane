package executionplan

import (
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Attach and Entry projections may change configuration, never the serving
// Release, image, replica count or physical identity. Every current replica must
// be captured before replacing a runtime; a partial set cannot drop its peers.
func validateRuntimeWorkloadReplacement(previous *agentpb.ComposeArtifact,
	selected []*agentpb.ComposeService, releaseID, target string,
) error {
	if len(selected) == 0 {
		return invalidAttachRuntime("selected workload set is empty")
	}
	slices.SortFunc(selected, func(left, right *agentpb.ComposeService) int {
		return strings.Compare(left.GetComposeName(), right.GetComposeName())
	})
	prior, _, err := nativePredecessorServices(previous, selected[0].GetServiceId())
	if err != nil || len(prior) != len(selected) || validateNativeWorkloadSet(selected) != nil {
		return invalidAttachRuntime("selected workloads do not cover the captured replica set")
	}
	for index, workload := range selected {
		before := prior[index]
		if workload.GetComposeName() != before.GetComposeName() ||
			nativePredecessorReleaseID(workload) != releaseID || nativePredecessorReleaseID(before) != releaseID ||
			attachRuntimeTarget(workload) != target || attachRuntimeTarget(before) != target ||
			workload.GetExpectedReplicas() != before.GetExpectedReplicas() ||
			workload.GetImageReference() != before.GetImageReference() ||
			workload.GetRollingReplicaOrdinal() != before.GetRollingReplicaOrdinal() {
			return invalidAttachRuntime("selected workload changed its captured Release, image or replica identity")
		}
	}
	return nil
}
