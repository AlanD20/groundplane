package blueprintrelease

import (
	"context"
	"errors"

	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (service *Service) prepareCapturedProxyServiceUnit(ctx context.Context,
	input PrepareServiceUnitInput, scope releasequeries.ReleasePlanningScope,
	reference componentplanning.ProxyServiceRolloutReference,
) (Prepared, error) {
	stored, capture, err := service.ledger.ReadBlueprintProxyServiceRollout(ctx,
		input.Projection.EnvironmentID, input.Projection.RevisionID, reference)
	if err != nil {
		return Prepared{}, err
	}
	if err := exactServiceUnitProjection(stored.Members[0].Render.Projection, input.Projection); err != nil {
		return Prepared{}, err
	}
	expectedScope := workexecution.Scope{TenantID: scope.Project.Record.TenantID,
		ProjectID: scope.Project.Record.ID, EnvironmentID: scope.Environment.Record.ID}
	task, item := input.Work.Task.Task, input.Work.Item.Item
	if capture.Record.Scope != expectedScope || capture.Record.Scope != task.Scope ||
		capture.Record.TaskID != task.ID || capture.Record.WorkItemID != item.ID ||
		capture.Record.OperationID != input.OperationID || capture.Record.Render.PlanID != input.PlanID ||
		capture.Record.Placement != item.Placement || !capture.Record.CreatedAt.Equal(item.CreatedAt) ||
		!capture.Record.Deadline.Equal(item.Deadline) {
		return Prepared{}, errs.New(errs.KindStateConflict, "pre-switch Service rollout hierarchy changed")
	}
	planning, err := service.ledger.LoadPlanningServices(ctx, scope, []string{input.ServiceID})
	if err != nil {
		return Prepared{}, err
	}
	if len(planning) != 1 || planning[0].Service.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning {
		return Prepared{}, errs.New(errs.KindStateConflict, "pre-switch Service is no longer runnable")
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(input.Projection.ComposeArtifact, artifact); err != nil {
		return Prepared{}, errs.New(errs.KindStateConflict, "pre-switch Service Environment artifact changed")
	}
	current := planning[0].Service
	change, err := serviceUnitDesiredChange(input.Projection, input.ServiceID, current)
	if err != nil {
		return Prepared{}, err
	}
	preparedInput := prepareInput{
		DesiredRevisionID: input.Projection.RevisionID, Tenant: scope.Tenant, Project: scope.Project,
		Environment: scope.Environment, Projection: input.Projection, Work: input.Work, Artifact: artifact,
		OperationID: input.OperationID, PlanID: input.PlanID,
		CreatedAt: input.CreatedAt, AllocateNamed: input.AllocateNamed,
		Files:          taskmaterialization.Clone(input.Files),
		ServiceChanges: []blueprints.EnvironmentBlueprintServiceChange{change},
	}
	proxies := []core.Service{change.Record.Desired}
	proxies[0].Expose = proxies[0].ProxyExposures()
	addresses, err := taskplanning.PrepareServiceProxyAddresses(ctx, service.ledger.Planner, input.Projection, proxies)
	if err != nil {
		return Prepared{}, err
	}
	defer addresses.Clear()
	native := preparedSelectedRuntime{
		capture: capture.Record, members: stored.Members, manifest: stored.Manifest,
		predecessors: stored.NativePredecessors,
		hooks: preparedHooks{
			members:    stored.Members,
			sources:    stored.Hooks,
			executions: len(stored.Hooks),
		},
	}
	prepared, err := service.finishSelectedRuntime(ctx, preparedInput, native, addresses)
	if err != nil {
		return Prepared{}, err
	}
	conditions, err := etcd.ProxyServiceRolloutConditions(stored.Input, reference, task, item)
	if err != nil {
		return Prepared{}, errors.Join(err, prepared.Abandon(ctx))
	}
	prepared.sourceConditions = append(prepared.sourceConditions, conditions...)
	return prepared, nil
}
