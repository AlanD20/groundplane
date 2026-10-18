package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
)

func (service *Service) compileAuthoredProxyCandidateInput(ctx context.Context,
	parent workexecution.Task, source authoredRuntimeProjectionSource, snapshot blueprintunits.Snapshot,
	pending authoredPendingProxy,
) (projectionrecord.EnvironmentComposeProjection, error) {
	if err := requireAuthoredProjectionInputReceipts(source.desired.Record, source.identities.Record,
		snapshot, pending.record.Desired.ID); err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	components, err := service.loadAuthoredComponentSelections(ctx, parent, source, snapshot, &pending)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	attaches, err := service.loadAuthoredRuntimeAttaches(
		ctx,
		parent.Scope.EnvironmentID,
		source.identities.Record,
		snapshot,
	)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	requirements, err := service.loadAuthoredRuntimeRequirements(ctx, source.desired, snapshot.HeadRevision)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	projection, err := service.compileAuthoredEnvironmentInput(
		ctx,
		parent,
		source,
		attaches,
		authoredComponentCompileInput{
			records:   components.records,
			projected: components.projected,
			sources:   components.sources,
		},
		requirements,
	)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	backup, err := service.prepareAuthoredRuntimeBackup(ctx, parent, source, snapshot, projection, attaches)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	defer backup.Clear()
	projection.Backup = backup.Projection()
	return projection, nil
}
