package releaseoperation

import (
	"context"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	releasequeries "github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"

	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// captureDesiredProjection freezes current Attach decisions for new candidates.
// Standalone Attach/Detach does not advance the desired Compose revision, so its
// older artifact cannot select the next Release's network memberships. Historical
// recovery remains separately captured; this does not rewrite either source.
func (service *Service) captureDesiredProjection(
	ctx context.Context, scope releasequeries.ReleasePlanningScope,
) (projectionrecord.EnvironmentComposeProjection, error) {
	attaches, err := service.ledger.LoadPlanningAttaches(ctx, scope)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	projection := scope.Compose.Record
	joins, err := taskplanning.ResolveAttachNetworkJoins(projection.EnvironmentID, projection, attaches, "")
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(projection.ComposeArtifact, artifact); err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, errs.New(errs.KindInternal, "release desired artifact is corrupt")
	}
	bound, err := taskplanning.MutateAttachNetworkArtifact(ctx, artifact, projection, joins, artifact.ArtifactId)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	projection.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(bound)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, errs.Wrap(errs.KindInternal, err)
	}
	return projection, nil
}
