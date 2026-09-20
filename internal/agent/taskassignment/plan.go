package taskassignment

import (
	"crypto/sha256"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

func PlanDigest(plan *agentpb.ExecutionPlan) [sha256.Size]byte {
	var hash [sha256.Size]byte
	if plan != nil {
		copy(hash[:], plan.PlanHash)
	}
	return hash
}

func ComposeArtifact(plan *agentpb.ExecutionPlan, artifactID string) *agentpb.ComposeArtifact {
	if plan == nil {
		return nil
	}
	for _, artifact := range plan.GetArtifacts() {
		if artifact != nil && artifact.GetArtifactId() == artifactID {
			return artifact
		}
	}
	return nil
}
