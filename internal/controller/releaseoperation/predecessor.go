package releaseoperation

import (
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"

	"github.com/AlanD20/groundplane/internal/controller/servicelifecycle"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func bindServingRuntime(
	render *releaserender.ReleaseRenderInput,
	priorReleaseID string,
	captured servicelifecycle.AcknowledgedRuntimeCapture,
) error {
	if captured.Release.ServingReleaseID != priorReleaseID {
		captured.Clear()
		return errs.New(errs.KindStateConflict, "ordinary Release serving predecessor changed")
	}
	if render.PriorTarget != captured.Release.Current.CandidateTarget {
		captured.Clear()
		return errs.New(errs.KindStateConflict, "ordinary Release predecessor target changed")
	}
	render.PriorRuntime = &releaserender.ServingRuntimeCapture{
		ServiceID: render.ServiceID, CurrentArtifact: captured.CurrentArtifact,
		RetainedPriorArtifact: captured.RetainedPriorArtifact,
	}
	render.PriorArtifactID = captured.CurrentArtifactID
	return nil
}
