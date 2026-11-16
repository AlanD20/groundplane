package edgechecks

import (
	"context"
	contract "github.com/AlanD20/groundplane/internal/common/edgeconnectioncheck"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	checkrecord "github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"time"
)

func (service *Service) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{
		acceptedaction.Registration(
			service.submitter,
			"route-check",
			workexecution.ActionCheck,
			ids.KindRoute,
			service.prepareRouteCheck,
		),
		acceptedaction.Registration(
			service.submitter,
			"tunnel-check",
			workexecution.ActionCheck,
			ids.KindComponent,
			service.prepareTunnelCheck,
		),
	}
}

func (service *Service) prepareRouteCheck(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	_ int64,
	at time.Time,
	input apiTypes.EdgeCheckRequest,
) (taskdependencies.Prepared, error) {
	ctx = imagefence.WithScope(ctx)
	source, err := service.routes.LoadEdgeCheckSource(
		ctx,
		task.Task.Scope.EnvironmentID,
		input.RouteID,
		input.Revision,
		input.ConnectorID,
		input.Path,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	return service.captureCheck(ctx, task, source, at)
}

func (service *Service) prepareTunnelCheck(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	_ int64,
	at time.Time,
	input apiTypes.TunnelCheckRequest,
) (taskdependencies.Prepared, error) {
	ctx = imagefence.WithScope(ctx)
	source, err := service.routes.LoadTunnelCheckSource(
		ctx,
		task.Task.Scope.EnvironmentID,
		task.Task.TargetID,
		input.Revision,
		input.OriginServiceID,
		input.OriginPort,
		contract.PublicTarget{Hostname: input.Hostname, Path: input.Path},
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	return service.captureCheck(ctx, task, source, at)
}

func (service *Service) captureCheck(
	ctx context.Context,
	accepted workqueue.TaskSnapshot,
	source etcd.EdgeCheckSource,
	at time.Time,
) (taskdependencies.Prepared, error) {
	checkOwner := checkrecord.Owner{
		TaskID:        accepted.Task.ID,
		WorkItemID:    ids.DeriveAt(ids.KindWorkItem, accepted.Task.CreatedAt, accepted.Task.ID, "edge-check"),
		PlanID:        ids.DeriveAt(ids.KindPlan, accepted.Task.CreatedAt, accepted.Task.ID, "edge-check"),
		OperationID:   ids.DeriveAt(ids.KindOperation, accepted.Task.CreatedAt, accepted.Task.ID, "edge-check"),
		EnvironmentID: accepted.Task.Scope.EnvironmentID,
	}
	if err := source.BindObservationAction(service.action, service.readyURL, checkOwner.OperationID); err != nil {
		return taskdependencies.Prepared{}, err
	}
	images, err := service.images.ResolveWorkloadImages(
		ctx,
		source.AgentID(),
		[]*agentpb.WorkloadImageSelector{source.ImageSelector()},
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	observed, err := service.observations.ObserveServices(ctx, source.AgentID(), source.ObservationTargets())
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if err := source.BindHostSelection(images, observed); err != nil {
		return taskdependencies.Prepared{}, err
	}
	configuration, err := source.Configuration()
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	now := at
	check, err := checkrecord.NewRecord(checkOwner, configuration, now)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	hash, err := checkrecord.PlanHash(check)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	targetName := configuration.Request.Target.Hostname
	if len(targetName) > 160 {
		targetName = targetName[:157] + "..."
	}
	task := workexecution.CloneTask(accepted.Task)
	task.Intent, task.ExpectedItems, task.UpdatedAt = workexecution.Input{ID: checkOwner.PlanID, SHA256: hash}, 1, at
	item, authority, err := service.routes.PrepareEdgeCheck(ctx, source, check, task)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	item.TargetName = targetName
	item.Deadline = acceptedaction.ExecutionDeadline(task, at, time.Duration(contract.TaskTimeoutSeconds)*time.Second)
	return taskdependencies.Prepared{Task: task, Graph: []workexecution.Item{item}, Authority: authority}, nil
}
