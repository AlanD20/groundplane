package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (service *Service) prepareAuthoredProxyServiceRollouts(ctx context.Context,
	owner workqueue.View, services []workqueue.View, projection projectionrecord.EnvironmentComposeProjection,
	reference componentplanning.ProxyCandidateInputReference, preflight componentplanning.ProxyServicePreflight,
) ([]componentplanning.ProxyServiceRolloutReference, error) {
	parent := owner.Task.Task
	if parent.Validate() != nil || parent.Action != workexecution.ActionApply || parent.Stop != nil ||
		parent.FinishedAt != nil || parent.CollectionStartedAt != nil || owner.ReadRevision <= 0 ||
		owner.Task.Revision <= 0 || owner.Task.Revision > owner.ReadRevision ||
		reference.Validate() != nil || reference.ParentTaskID != parent.ID ||
		reference.EnvironmentID != parent.Scope.EnvironmentID || projection.RevisionID != parent.ID ||
		projection.EnvironmentID != parent.Scope.EnvironmentID || preflight.ParentTaskID != parent.ID ||
		preflight.EnvironmentID != parent.Scope.EnvironmentID {
		return nil, errs.New(errs.KindStateConflict, "router rollout capture requires one active Apply snapshot")
	}
	byService := make(map[string]workqueue.View, len(services))
	for _, work := range services {
		item := work.Item.Item
		if work.Task.Task.ID != parent.ID || work.Task.Revision != owner.Task.Revision ||
			work.ReadRevision != owner.ReadRevision || work.Item.Revision <= 0 || work.Item.Revision > owner.ReadRevision ||
			item.Validate() != nil || item.TaskID != parent.ID || item.Operation != workexecution.Release ||
			item.Placement.Executor != workexecution.Agent || item.AttemptNumber != 0 || item.ForwardRevoked ||
			item.Phase != workexecution.Waiting || item.Input != nil || item.Wait == nil || item.Wait.Reason != workexecution.WaitInput {
			return nil, errs.New(errs.KindStateConflict, "router rollout Service is not waiting in the captured Apply")
		}
		if _, duplicate := byService[item.TargetID]; duplicate {
			return nil, errs.New(errs.KindStateConflict, "router rollout Service membership is duplicated")
		}
		byService[item.TargetID] = work
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(projection.ComposeArtifact, artifact); err != nil {
		return nil, errs.New(errs.KindStateConflict, "prepared router Environment artifact is unreadable")
	}
	allScripts, _, err := service.listBlueprintScripts(ctx, parent.Scope.EnvironmentID, service.repository)
	if err != nil {
		return nil, err
	}
	for _, script := range allScripts {
		if script.Record.ScriptSetGeneration != parent.ID || script.Revision > owner.ReadRevision {
			return nil, errs.New(errs.KindStateConflict, "Blueprint Script set changed before router preparation")
		}
	}
	input, err := service.loadAuthoredParentInput(ctx, parent)
	if err != nil {
		return nil, err
	}
	var result []componentplanning.ProxyServiceRolloutReference
	for _, source := range preflight.Services {
		if !source.ReplacementRequired {
			continue
		}
		work, found := byService[source.ServiceID]
		if !found {
			return nil, errs.New(errs.KindStateConflict, "router replacement has no captured Service work")
		}
		delete(byService, source.ServiceID)
		item := work.Item.Item
		planID := ids.DeriveAt(ids.KindPlan, item.CreatedAt, item.ID, "release-input")
		allocate := func(kind ids.Kind, name string) string {
			return ids.DeriveAt(kind, item.CreatedAt, planID, name)
		}
		scripts := make([]scriptrecord.Record, 0)
		for _, script := range allScripts {
			if script.Record.ServiceID == source.ServiceID {
				scripts = append(scripts, script.Record)
			}
		}
		files, err := service.prepareAuthoredServiceFiles(
			ctx,
			work,
			projection,
			input,
			artifact,
			allocate,
		)
		if err != nil {
			return nil, err
		}
		captured, err := service.blueprintReleases.CaptureProxyServiceRollout(ctx,
			blueprintrelease.PrepareServiceUnitInput{
				ServiceID: source.ServiceID, Projection: projection, Scripts: scripts,
				Work: work, Files: files, CreatedAt: item.CreatedAt, AllocateNamed: allocate,
				OperationID: ids.DeriveAt(ids.KindOperation, item.CreatedAt, item.ID, "release-operation"), PlanID: planID,
			}, reference, preflight)
		if err != nil {
			return nil, err
		}
		result = append(result, captured)
	}
	if len(byService) != 0 {
		return nil, errs.New(errs.KindStateConflict, "router rollout includes Services outside its captured replacement membership")
	}
	return result, nil
}
