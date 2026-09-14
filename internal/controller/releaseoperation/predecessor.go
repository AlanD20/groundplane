package releaseoperation

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller/servicelifecycle"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// captureServingRuntime seals acknowledged per-Service bytes before publication.
// The Environment epoch guards the fixed read; its latest Compose artifact is
// deliberately not used as evidence that this Service is (or is not) serving.
func (service *Service) captureServingRuntime(
	ctx context.Context,
	scope etcd.ReleasePlanningScope,
	render *etcd.ReleaseRenderInput,
	priorReleaseID string,
) error {
	return captureServingRuntime(ctx, service.ledger, scope, render, priorReleaseID)
}

func captureServingRuntime(
	ctx context.Context,
	reader servicelifecycle.AcknowledgedRuntimeReader,
	scope etcd.ReleasePlanningScope,
	render *etcd.ReleaseRenderInput,
	priorReleaseID string,
) error {
	if priorReleaseID == "" {
		return nil
	}
	captured, err := servicelifecycle.CaptureAcknowledgedRuntime(ctx, reader,
		etcd.Versioned[etcd.EnvironmentComposeProjection]{ReadRevision: scope.ReadRevision},
		render.EnvironmentID, render.ServiceID, render.PriorArtifactID)
	if err != nil {
		return err
	}
	return bindServingRuntime(render, priorReleaseID, captured)
}

func bindServingRuntime(
	render *etcd.ReleaseRenderInput,
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
	render.PriorRuntime = &etcd.ReleaseNativePredecessorAuthority{
		ServiceID: render.ServiceID, CurrentArtifact: captured.CurrentArtifact,
		RetainedPriorArtifact: captured.RetainedPriorArtifact,
	}
	return nil
}
