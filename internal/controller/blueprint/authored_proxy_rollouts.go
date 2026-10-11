package blueprint

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type authoredProxyRolloutOwner struct {
	ID             string
	CreatedAt      time.Time
	TimeoutSeconds int64
}

func (service *Service) prepareAuthoredProxyServiceRollouts(ctx context.Context,
	parent etcd.TaskRecord, owner authoredProxyRolloutOwner, projection projectionrecord.EnvironmentComposeProjection,
	reference componentplanning.ProxyCandidateInputReference, preflight componentplanning.ProxyServicePreflight,
) ([]componentplanning.ProxyServiceRolloutReference, error) {
	artifact := &agentpb.ComposeArtifact{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(projection.ComposeArtifact, artifact); err != nil {
		return nil, errs.New(errs.KindStateConflict, "prepared router Environment artifact is unreadable")
	}
	allScripts, _, err := service.listBlueprintScripts(ctx, parent.Owner.EnvironmentID, service.repository)
	if err != nil {
		return nil, err
	}
	for _, script := range allScripts {
		if script.Record.ScriptSetGeneration != parent.ID {
			return nil, errs.New(errs.KindStateConflict, "Blueprint Script set changed before router preparation")
		}
	}
	var result []componentplanning.ProxyServiceRolloutReference
	for _, source := range preflight.Services {
		if !source.ReplacementRequired {
			continue
		}
		allocate := func(kind ids.Kind, name string) string {
			return ids.DeriveAt(kind, owner.CreatedAt, owner.ID, "service-rollout/"+source.ServiceID+"/"+name)
		}
		child := etcd.TaskRecord{
			ID: allocate(ids.KindTask, "task"), OperationID: allocate(ids.KindOperation, "operation"),
			PlanID: allocate(ids.KindPlan, "plan"), Owner: parent.Owner,
			Actor: taskjournal.TaskActorSystem, Executor: taskjournal.TaskExecutorAgent,
			Type: taskjournal.TaskUpdate, Target: parent.Owner.EnvironmentID, RenderGeneration: parent.RenderGeneration,
			Status: taskjournal.TaskStatusPending, NextEventSequence: 1, TimeoutSeconds: owner.TimeoutSeconds,
			CreatedAt: owner.CreatedAt, UpdatedAt: owner.CreatedAt,
			Params: map[string]string{
				blueprints.EnvironmentDesiredRevisionParam:      parent.ID,
				taskjournal.TaskBlueprintParentParam:            parent.ID,
				taskjournal.TaskMaterializationEnvironmentParam: parent.Owner.EnvironmentID,
				taskcontract.EnvironmentBlueprintArtifactParam:  artifact.GetArtifactId(),
				taskcontract.EnvironmentBlueprintProcedureParam: string(taskcontract.BlueprintComposeProcedureNone),
			},
		}
		scripts := make([]scriptrecord.Record, 0)
		for _, script := range allScripts {
			if script.Record.ServiceID == source.ServiceID {
				scripts = append(scripts, script.Record)
			}
		}
		captured, err := service.blueprintReleases.CaptureProxyServiceRollout(ctx,
			blueprintrelease.PrepareServiceUnitInput{
				ServiceID: source.ServiceID, Projection: projection, Scripts: scripts,
				Task: child, CreatedAt: child.CreatedAt, AllocateNamed: allocate,
			}, reference, preflight)
		if err != nil {
			return nil, err
		}
		result = append(result, captured)
	}
	return result, nil
}
