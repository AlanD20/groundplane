package blueprintrelease

import (
	"context"
	"errors"

	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
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
	defer clear(capture.Record.NativePlan)
	if err := exactServiceUnitProjection(stored.Members[0].Render.Projection, input.Projection); err != nil {
		return Prepared{}, err
	}
	expectedOwner, err := taskjournal.EnvironmentTaskOwner(scope.Project.Record, scope.Environment.Record)
	if err != nil {
		return Prepared{}, err
	}
	if stored.Task.Owner != expectedOwner || stored.Task.Owner != input.Task.Owner {
		return Prepared{}, errs.New(errs.KindStateConflict, "pre-switch Service rollout hierarchy changed")
	}
	planning, err := service.ledger.LoadPlanningServices(ctx, scope, []string{input.ServiceID})
	if err != nil {
		return Prepared{}, err
	}
	if len(planning) != 1 || planning[0].Service.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning {
		return Prepared{}, errs.New(errs.KindStateConflict, "pre-switch Service is no longer runnable")
	}
	runtime, err := taskplanning.RestoreBlueprintReleaseRuntime(capture.Record)
	if err != nil {
		return Prepared{}, err
	}
	task := stored.Task
	task.TimeoutSeconds = min(task.TimeoutSeconds, input.Task.TimeoutSeconds)
	task.CreatedAt, task.UpdatedAt = input.CreatedAt, input.CreatedAt
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
		Environment: scope.Environment, Projection: input.Projection, Task: task, Artifact: artifact,
		CreatedAt: input.CreatedAt, AllocateNamed: input.AllocateNamed,
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
		task: task, runtime: runtime, members: stored.Members, manifest: stored.Manifest,
		predecessors: stored.NativePredecessors,
		hooks: preparedHooks{
			task:       task,
			members:    stored.Members,
			sources:    stored.Hooks,
			executions: len(stored.Hooks),
		},
	}
	prepared, err := service.finishSelectedRuntime(ctx, preparedInput, native, addresses)
	if err != nil {
		return Prepared{}, err
	}
	publication, err := prepared.Publication.WithProxyServiceRolloutCapture(stored.Input, reference, prepared.Task)
	if err != nil {
		return Prepared{}, errors.Join(err, prepared.Abandon(ctx))
	}
	prepared.Publication = publication
	return prepared, nil
}
