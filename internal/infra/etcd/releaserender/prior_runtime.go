package releaserender

import (
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
)

func validateOrdinaryPriorRuntime(render ReleaseRenderInput) error {
	witness := render.PriorRuntime
	if witness == nil {
		return nil
	}
	if witness.ServiceID != render.ServiceID || render.PriorWorkload == nil ||
		executionplan.ValidateNativePredecessorWitness(render.EnvironmentID, render.ServiceID,
			witness.CurrentArtifact, witness.RetainedPriorArtifact) != nil {
		return releases.CorruptReleaseRecord()
	}
	artifact, err := taskassignments.OpenRestorationWitness(render.EnvironmentID, witness.CurrentArtifact)
	if err != nil || artifact.ArtifactId != render.PriorArtifactID || artifact.ArtifactId == render.ArtifactID {
		return releases.CorruptReleaseRecord()
	}
	return nil
}
