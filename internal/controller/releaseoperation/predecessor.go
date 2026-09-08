package releaseoperation

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/servicelifecycle"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"google.golang.org/protobuf/proto"
)

// captureServingRuntime seals per-Service historical bytes before publication.
// The Environment epoch guards the fixed read; its latest Compose artifact is
// deliberately not used as evidence that this Service is (or is not) serving.
func (service *Service) captureServingRuntime(
	ctx context.Context,
	scope etcd.ReleasePlanningScope,
	render *etcd.ReleaseRenderInput,
	priorReleaseID string,
) error {
	if priorReleaseID == "" {
		return nil
	}
	captured, err := servicelifecycle.CaptureRelease(ctx, service.ledger,
		etcd.Versioned[etcd.EnvironmentComposeProjection]{ReadRevision: scope.ReadRevision},
		render.EnvironmentID, render.ServiceID)
	if err != nil {
		return err
	}
	if captured.ServingReleaseID != priorReleaseID {
		return errs.New(errs.KindStateConflict, "ordinary Release serving predecessor changed")
	}
	artifacts, err := service.plans.RenderRetainedServiceRuntime(ctx, captured)
	if err != nil {
		return err
	}
	if len(artifacts) < 1 || len(artifacts) > 2 || (len(artifacts) == 2) != (captured.RetainedPrior != nil) {
		return errs.New(errs.KindStateConflict, "ordinary Release predecessor rendering is incomplete")
	}
	witness := &etcd.ReleaseNativePredecessorAuthority{ServiceID: render.ServiceID}
	for index, artifact := range artifacts {
		if index == 0 {
			artifact.ArtifactId = render.PriorArtifactID
		} else {
			artifact.ArtifactId = ids.New(ids.KindConfig)
		}
		encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
		if err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
		if index == 0 {
			witness.CurrentArtifact = encoded
		} else {
			witness.RetainedPriorArtifact = encoded
		}
	}
	render.PriorRuntime = witness
	return nil
}
