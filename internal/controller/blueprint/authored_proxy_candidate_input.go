package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// prepareAuthoredProxyCandidateInput renders the complete future Environment
// from one prepared router recipe and the other units' real receipts. It does
// not seal runtime, publish a Task, or acknowledge the router's effects.
func (service *Service) prepareAuthoredProxyCandidateInput(ctx context.Context,
	parent etcd.TaskRecord, source authoredRuntimeProjectionSource,
	snapshot blueprintunits.Snapshot, execution componentplanning.ChildExecution,
) (projectionrecord.EnvironmentComposeProjection, error) {
	if validateAuthoredRuntimeProjectionParent(parent, snapshot) != nil ||
		execution.ParentTaskID != parent.ID || execution.EnvironmentID != parent.Owner.EnvironmentID ||
		!execution.Component.Desired.Kind.IsEnvironmentProxy() {
		return projectionrecord.EnvironmentComposeProjection{}, errs.New(errs.KindValidationFailed,
			"prepared router input belongs to another Blueprint Apply")
	}
	plan, err := execution.ExecutionPlan()
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	if plan.GetRenderGeneration() != uint64(parent.RenderGeneration) {
		return projectionrecord.EnvironmentComposeProjection{}, errs.New(errs.KindStateConflict,
			"prepared router input generation changed")
	}
	pending := authoredPendingProxy{record: execution.Component}
	if execution.Component.Desired.Enabled {
		var candidate *agentpb.ComposeArtifact
		for _, artifact := range plan.GetArtifacts() {
			if artifact.GetArtifactId() == plan.GetComponentRecoveryProcedure().GetCandidateArtifactId() {
				candidate = artifact
			}
		}
		if candidate == nil {
			return projectionrecord.EnvironmentComposeProjection{}, errs.New(errs.KindStateConflict,
				"prepared router recipe has no candidate artifact")
		}
		value, err := (proto.MarshalOptions{Deterministic: true}).Marshal(candidate)
		if err != nil {
			return projectionrecord.EnvironmentComposeProjection{}, errs.Wrap(errs.KindInternal, err)
		}
		defer clear(value)
		pending.source, err = projectionrecord.PrepareManagedComponentRuntimeSource(
			projectionrecord.EnvironmentComposeProjection{
				EnvironmentID: execution.EnvironmentID, RevisionID: parent.ID,
				RenderGeneration: uint64(parent.RenderGeneration), ComposeArtifact: value,
			}, execution.Component, candidate)
		if err != nil {
			return projectionrecord.EnvironmentComposeProjection{}, err
		}
	}
	return service.compileAuthoredProxyCandidateInput(ctx, parent, source, snapshot, pending)
}

func (service *Service) compileAuthoredProxyCandidateInput(ctx context.Context,
	parent etcd.TaskRecord, source authoredRuntimeProjectionSource, snapshot blueprintunits.Snapshot,
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
		parent.Owner.EnvironmentID,
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
