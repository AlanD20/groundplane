package releaseoperation

import (
	"strings"
	"testing"

	domain "github.com/AlanD20/groundplane/internal/core/release"
)

// Rationale: even a traffic-only blue-green switch must retain its exact prior
// workload authority for recovery; lack of a topology conversion is not absence.
func TestReleaseRetainsEveryServingPredecessorArtifact(t *testing.T) {
	for _, strategy := range []domain.Strategy{domain.StrategyRecreate, domain.StrategyBlueGreen} {
		t.Run(string(strategy), func(t *testing.T) {
			candidate := releaseCandidateInput{strategy: strategy, priorStrategy: strategy}
			if strategy == domain.StrategyBlueGreen {
				candidate.slot = domain.SlotGreen
				candidate.planning.Projection.ServingSlot = domain.SlotBlue
			}
			if releaseNeedsPriorArtifact(candidate) {
				t.Fatal("first Release invented a predecessor artifact")
			}
			candidate.priorWorkload = &domain.WorkloadSeal{
				RequestedReference: "api:old", LocalImageID: "sha256:" + strings.Repeat("a", 64), ReplicaCount: 1,
			}
			if !releaseNeedsPriorArtifact(candidate) {
				t.Fatal("serving predecessor workload authority would be discarded")
			}
		})
	}
}
