package releaseoperation

import (
	"context"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releasequeries "github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"

	"github.com/AlanD20/groundplane/internal/controller/servicelifecycle"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// captureServingRuntime seals acknowledged per-Service bytes before publication.
// The Environment epoch guards the fixed read; its latest Compose artifact is
// deliberately not used as evidence that this Service is (or is not) serving.
func (service *Service) captureServingRuntime(
	ctx context.Context,
	scope releasequeries.ReleasePlanningScope,
	render *releaserender.ReleaseRenderInput,
	priorReleaseID string,
) error {
	return captureServingRuntime(ctx, service.ledger, scope, render, priorReleaseID)
}

func captureServingRuntime(
	ctx context.Context,
	reader servicelifecycle.AcknowledgedRuntimeReader,
	scope releasequeries.ReleasePlanningScope,
	render *releaserender.ReleaseRenderInput,
	priorReleaseID string,
) error {
	if priorReleaseID == "" {
		return nil
	}
	captured, err := servicelifecycle.CaptureAcknowledgedRuntime(ctx, reader,
		etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{ReadRevision: scope.ReadRevision},
		render.EnvironmentID, render.ServiceID, render.PriorArtifactID)
	if err != nil {
		return err
	}
	return bindServingRuntime(render, priorReleaseID, captured)
}

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
	render.PriorRuntime = &taskassignments.ReleaseNativePredecessorAuthority{
		ServiceID: render.ServiceID, CurrentArtifact: captured.CurrentArtifact,
		RetainedPriorArtifact: captured.RetainedPriorArtifact,
	}
	return nil
}
