package releaseoperation

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// captureDesiredProjection freezes current Attach decisions for new candidates.
// Standalone Attach/Detach does not advance the desired Compose revision, so its
// older artifact cannot select the next Release's network memberships. Historical
// recovery remains separately captured; this does not rewrite either source.
func (service *Service) captureDesiredProjection(
	ctx context.Context, scope etcd.ReleasePlanningScope,
) (etcd.EnvironmentComposeProjection, error) {
	attaches, err := service.ledger.LoadPlanningAttaches(ctx, scope)
	if err != nil {
		return etcd.EnvironmentComposeProjection{}, err
	}
	projection := scope.Compose.Record
	joins, err := controller.ResolveAttachNetworkJoins(projection.EnvironmentID, projection, attaches, "")
	if err != nil {
		return etcd.EnvironmentComposeProjection{}, err
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(projection.ComposeArtifact, artifact); err != nil {
		return etcd.EnvironmentComposeProjection{}, errs.New(errs.KindInternal, "release desired artifact is corrupt")
	}
	bound, err := controller.MutateAttachNetworkArtifact(ctx, artifact, projection, joins, artifact.ArtifactId)
	if err != nil {
		return etcd.EnvironmentComposeProjection{}, err
	}
	projection.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(bound)
	if err != nil {
		return etcd.EnvironmentComposeProjection{}, errs.Wrap(errs.KindInternal, err)
	}
	return projection, nil
}
