package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/blueprintcoordinator"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (service *Service) prepareAuthoredServiceRemovalChild(
	ctx context.Context, parent etcd.TaskRecord, unit blueprintunits.Unit, timeoutSeconds int64,
) (blueprintcoordinator.PreparedChild, error) {
	if !unit.Removal || unit.Target.Kind != ids.KindService || service.plans == nil {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindValidationFailed, "Blueprint Service removal child identity is invalid",
		)
	}
	input, err := service.loadAuthoredParentInput(ctx, parent)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	declaredRevisionID := ""
	for _, omission := range input.identities.PendingOmissions {
		if omission.Kind == ids.KindService && omission.ID == unit.Target.ID {
			declaredRevisionID = omission.SourceRevisionID
			break
		}
	}
	if declaredRevisionID == "" {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindStateConflict, "Blueprint Service omission has no retained declaration",
		)
	}
	source, err := service.repository.GetAuthoredServiceRemovalSource(
		ctx, parent.Owner.EnvironmentID, declaredRevisionID, unit.Target.ID,
	)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	applied, current := source.Projection, source.Service
	if err := service.repository.ValidateBlueprintProxyServiceSource(ctx, parent.Owner.EnvironmentID,
		parent.ID, unit.Target.ID, applied.ReadRevision); err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	if current.Record.EnvironmentID != parent.Owner.EnvironmentID {
		return blueprintcoordinator.PreparedChild{}, errs.New(errs.KindStateConflict, "Blueprint Service owner changed")
	}
	acknowledged, err := service.repository.GetServiceRemovalRuntime(
		ctx, parent.Owner.EnvironmentID, unit.Target.ID, applied.ReadRevision,
	)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	head, found, err := service.repository.GetEnvironmentBlueprintHead(ctx, parent.Owner.EnvironmentID)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	if !found || head.Record.RevisionID != parent.ID {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindStateConflict, "Blueprint Service removal parent is no longer current",
		)
	}
	createdAt := service.now().UTC()
	child := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation), PlanID: ids.New(ids.KindPlan),
		Owner: parent.Owner, Actor: taskjournal.TaskActorSystem, Executor: taskjournal.TaskExecutorAgent,
		Type: taskjournal.TaskRemove, Target: unit.Target.ID,
		TargetName:     "Service " + current.Record.Desired.Name,
		Params:         map[string]string{taskjournal.TaskBlueprintParentParam: parent.ID},
		TimeoutSeconds: timeoutSeconds, Status: taskjournal.TaskStatusPending, NextEventSequence: 1,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	projectionArtifactID := ids.New(ids.KindConfig)
	candidate, err := buildAuthoredServiceRemovalProjection(
		applied.Record, current.Record.Desired.ID, current.Record.Desired.Name,
		parent.ID, uint64(parent.RenderGeneration), projectionArtifactID,
	)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	intent, err := environmentchanges.NewAuthoredServiceRemovalIntent(
		child.ID, current, applied, acknowledged, head.Revision, parent.ID,
		uint64(parent.RenderGeneration), candidate, createdAt,
		source.Kind, source.AppliedProjectionRevision,
	)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	child, err = service.plans.PrepareServiceRemovalTask(
		ctx, child, intent, projectionArtifactID, ids.New(ids.KindStep),
	)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	publication, err := etcd.NewBlueprintServiceRemovalPublication(child, intent)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	return blueprintcoordinator.PreparedChild{Task: child, ServiceRemovalPublication: publication}, nil
}

func buildAuthoredServiceRemovalProjection(
	current projectionrecord.EnvironmentComposeProjection,
	serviceID, serviceName, revisionID string,
	renderGeneration uint64,
	artifactID string,
) (projectionrecord.EnvironmentComposeProjection, error) {
	candidate := projectionrecord.CloneEnvironmentComposeProjection(current)
	candidate.RevisionID, candidate.RenderGeneration = revisionID, renderGeneration
	candidate.DesiredServices = nil
	removed := false
	for _, desired := range current.DesiredServices {
		if desired.Desired.ID == serviceID && desired.Desired.Name == serviceName {
			removed = true
			continue
		}
		candidate.DesiredServices = append(candidate.DesiredServices, desired)
	}
	if !removed {
		return projectionrecord.EnvironmentComposeProjection{}, errs.New(
			errs.KindStateConflict, "Blueprint Service is absent from the applied projection",
		)
	}
	candidate.VolumeMounts = nil
	for _, mount := range current.VolumeMounts {
		if mount.ServiceID != serviceID {
			candidate.VolumeMounts = append(candidate.VolumeMounts, mount)
		}
	}
	candidate.ServiceDependencyPlans = current.ServiceDependencyPlans.Clone().WithoutService(serviceName)
	delete(candidate.ServiceExtensions, serviceName)
	if len(candidate.ServiceExtensions) == 0 {
		candidate.ServiceExtensions = nil
	}
	artifact := new(agentpb.ComposeArtifact)
	if (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(current.ComposeArtifact, artifact) != nil {
		return projectionrecord.EnvironmentComposeProjection{}, errs.New(
			errs.KindInternal, "Blueprint Service applied artifact is corrupt",
		)
	}
	artifact, err := composerender.RemoveEnvironmentServiceArtifact(artifact, serviceID, artifactID)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	candidate.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, errs.Wrap(errs.KindInternal, err)
	}
	normalized, err := composerender.NormalizedEnvironmentArtifact(current)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	normalized, err = composerender.RemoveEnvironmentServiceArtifact(normalized, serviceID, artifactID)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, err
	}
	candidate.NormalizedCompose = append([]byte(nil), normalized.GetCanonicalYaml()...)
	return candidate, nil
}
