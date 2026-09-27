package network

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *routeMutationService) prepareRouteMutationTask(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	record routerecord.Record,
	previous *etcdstore.Versioned[routerecord.Record],
	idempotencyKey string,
) (etcd.RouteMutationTaskPreparation, error) {
	projection, found, err := service.repository.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil {
		return etcd.RouteMutationTaskPreparation{}, err
	}
	var desired *etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]
	if found {
		desired = &projection
	}
	taskID := ids.New(ids.KindTask)
	operationID := ids.New(ids.KindOperation)
	owner, err := taskjournal.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return etcd.RouteMutationTaskPreparation{}, err
	}
	now := service.now().UTC()
	task := etcd.TaskRecord{
		ID: taskID, OperationID: operationID, IdempotencyKey: idempotencyKey,
		Owner: owner, Actor: taskjournal.TaskActorOperator, PlanID: ids.New(ids.KindPlan),
		Type: taskjournal.TaskCreate, Target: record.Desired.ID,
		Status: taskjournal.TaskStatusPending, NextEventSequence: 1,
		CreatedAt: now, UpdatedAt: now,
	}
	if previous != nil {
		task.Type = taskjournal.TaskUpdate
	}
	intent, err := environmentchanges.NewRouteMutationIntent(
		taskID, operationID, environment.Record.ID, record, previous, desired, now,
	)
	if err != nil {
		return etcd.RouteMutationTaskPreparation{}, err
	}
	procedures := environmentchanges.RouteMutationProcedureIDs{
		ArtifactID: ids.New(ids.KindConfig), MaterializationID: ids.New(ids.KindConfig),
		MaterializeStepID: ids.New(ids.KindStep), ApplyStepID: ids.New(ids.KindStep),
		ActivateStepID: ids.New(ids.KindStep),
	}
	preparation, err := service.planner.PrepareRouteMutationTask(ctx, task, intent, procedures)
	if err != nil {
		return etcd.RouteMutationTaskPreparation{}, err
	}
	if preparation.Intent.TaskID == "" {
		preparation.Intent = intent
	}
	if preparation.Task.ID == "" {
		preparation.Task = task
	}
	if preparation.Intent.TaskID != taskID || preparation.Task.ID != taskID {
		return etcd.RouteMutationTaskPreparation{}, errs.New(
			errs.KindInternal, "Route mutation planner returned mismatched durable identities",
		)
	}
	return preparation, nil
}

func (service *routeMutationService) routeHierarchy(
	ctx context.Context,
	environmentID string,
	targetServiceID string,
) (
	etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	etcdstore.Versioned[servicerecord.ServiceRecord],
	error,
) {
	environment, err := service.repository.GetEnvironment(ctx, environmentID)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, etcdstore.Versioned[hierarchyrecord.ProjectRecord]{},
			etcdstore.Versioned[servicerecord.ServiceRecord]{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, etcdstore.Versioned[hierarchyrecord.ProjectRecord]{},
			etcdstore.Versioned[servicerecord.ServiceRecord]{}, err
	}
	target, err := service.repository.GetService(ctx, targetServiceID)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, etcdstore.Versioned[hierarchyrecord.ProjectRecord]{},
			etcdstore.Versioned[servicerecord.ServiceRecord]{}, err
	}
	return environment, project, target, nil
}
