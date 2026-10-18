package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/common/zonedns"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// This is captured input, not publication permission. The native coordinator
// must join its graph/epoch and DNS comparisons with the Release owner's prepared
// change, which owns projection and prerequisite checks. No hidden Task is created.
type authoredServiceWorkInput struct {
	Release blueprintrelease.PrepareServiceUnitInput
	DNS     etcd.ZoneDNSActivation
}

func (service *Service) prepareAuthoredServiceWorkInput(ctx context.Context,
	work workqueue.View, unit blueprintunits.Unit,
) (authoredServiceWorkInput, error) {
	var empty authoredServiceWorkInput
	task, item := work.Task.Task, work.Item.Item
	if ctx == nil || service == nil || task.Validate() != nil || item.Validate() != nil ||
		work.Task.Revision <= 0 || work.Item.Revision <= 0 ||
		work.ReadRevision < work.Task.Revision || work.ReadRevision < work.Item.Revision ||
		task.Action != workexecution.ActionApply || item.TaskID != task.ID ||
		item.Operation != workexecution.Release || item.Placement.Executor != workexecution.Agent ||
		item.Phase != workexecution.Waiting || item.Wait == nil || item.Wait.Reason != workexecution.WaitInput ||
		item.Input != nil || item.AttemptNumber != 0 || item.ForwardRevoked ||
		task.Stop != nil || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		blueprintunits.ValidateUnit(unit) != nil || unit.Removal ||
		unit.Target.Kind != ids.KindService || unit.Target.ID != item.TargetID {
		return empty, errs.New(errs.KindValidationFailed, "Blueprint Service input requires its waiting native work")
	}
	input, err := service.loadAuthoredParentInput(ctx, task)
	if err != nil {
		return empty, err
	}
	project, err := composerender.LoadNormalizedEnvironmentDesiredProject(ctx, input.desired, input.identities)
	if err != nil {
		return empty, err
	}
	name := ""
	for _, identity := range input.identities.Services {
		if identity.ID == item.TargetID {
			name = identity.Name
			break
		}
	}
	selected, found := project.Services[name]
	if !found {
		return empty, errs.New(errs.KindStateConflict, "Blueprint Service is absent from its captured authored input")
	}
	upstreams, err := zonedns.Upstreams(selected, project.Networks)
	if err != nil {
		return empty, err
	}
	dns, err := service.repository.ReadZoneDNSActivation(ctx, upstreams)
	if err != nil {
		return empty, err
	}
	projection, found, err := service.repository.GetEnvironmentComposeProjectionRevision(ctx, task.Scope.EnvironmentID, task.ID)
	if err != nil {
		return empty, err
	}
	if !found || projection.Revision <= 0 || projection.Revision > work.ReadRevision ||
		projection.Record.EnvironmentID != task.Scope.EnvironmentID || projection.Record.RevisionID != task.ID ||
		projection.Record.RenderGeneration != input.desired.RenderGeneration {
		return empty, errs.New(errs.KindStateConflict, "Blueprint Service runtime projection is not available at its work snapshot")
	}
	artifact := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(projection.Record.ComposeArtifact, artifact) != nil ||
		ids.Validate(ids.KindConfig, artifact.GetArtifactId()) != nil || artifact.GetOwnerId() != task.Scope.EnvironmentID {
		return empty, errs.New(errs.KindStateConflict, "Blueprint Service runtime artifact is invalid")
	}
	allScripts, _, err := service.listBlueprintScripts(ctx, task.Scope.EnvironmentID, service.repository)
	if err != nil {
		return empty, err
	}
	var scripts []scriptrecord.Record
	for _, versioned := range allScripts {
		if versioned.Record.ScriptSetGeneration != task.ID || versioned.Revision > work.ReadRevision {
			return empty, errs.New(errs.KindStateConflict, "Blueprint Script set differs from the work snapshot")
		}
		if versioned.Record.ServiceID == item.TargetID {
			scripts = append(scripts, versioned.Record)
		}
	}
	planID := ids.DeriveAt(ids.KindPlan, item.CreatedAt, item.ID, "release-input")
	allocate := func(kind ids.Kind, name string) string { return ids.DeriveAt(kind, item.CreatedAt, planID, name) }
	files, err := service.prepareAuthoredServiceFiles(ctx, work, projection.Record, input, artifact, allocate)
	if err != nil {
		return empty, err
	}
	return authoredServiceWorkInput{
		Release: blueprintrelease.PrepareServiceUnitInput{
			ServiceID: item.TargetID, Projection: projection.Record, Scripts: scripts, Files: files, Work: work,
			OperationID: ids.DeriveAt(ids.KindOperation, item.CreatedAt, item.ID, "release-operation"),
			PlanID:      planID, AllocateNamed: allocate, CreatedAt: item.CreatedAt,
		},
		DNS: dns,
	}, nil
}
