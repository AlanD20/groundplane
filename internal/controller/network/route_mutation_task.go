package network

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
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
	var applied *etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]
	{
		var projection etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]
		var found bool
		var err error
		if projectionRepository, ok := service.repository.(routeMutationDesiredProjectionRepository); ok {
			projection, found, err = projectionRepository.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
		} else if service.planner != nil {
			return etcd.RouteMutationTaskPreparation{}, errs.New(
				errs.KindInternal, "Route desired projection repository is not configured",
			)
		} else if projectionRepository, ok := service.repository.(routeMutationProjectionRepository); ok {
			projection, found, err = projectionRepository.GetEnvironmentAppliedComposeProjection(ctx, environment.Record.ID)
		} else {
			found = false
		}
		if err != nil {
			return etcd.RouteMutationTaskPreparation{}, err
		}
		if found {
			applied = &projection
		}
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
	intent, err := etcd.NewRouteMutationIntent(
		taskID, operationID, environment.Record.ID, record, previous, applied, now,
	)
	if err != nil {
		return etcd.RouteMutationTaskPreparation{}, err
	}
	if service.planner == nil && applied != nil {
		candidate, applyErr := projectionrecord.ApplyEnvironmentRoute(applied.Record, intent.Route)
		if applyErr != nil {
			return etcd.RouteMutationTaskPreparation{}, applyErr
		}
		candidate.RevisionID = task.ID
		intent.CandidateProjection = &candidate
	}
	if service.planner == nil && applied == nil {
		intent.CurrentProjection = nil
		intent.CurrentProjectionRevision = 0
		task, err = prepareControllerRouteMutationTask(task, intent)
		if err != nil {
			return etcd.RouteMutationTaskPreparation{}, err
		}
		return etcd.RouteMutationTaskPreparation{Intent: intent, Task: task}, nil
	}
	procedures := etcd.RouteMutationProcedureIDs{
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

func prepareControllerRouteMutationTask(
	task etcd.TaskRecord,
	intent etcd.RouteMutationIntent,
) (etcd.TaskRecord, error) {
	if intent.Provider != nil || intent.CurrentProjection != nil || intent.CandidateProjection != nil {
		return etcd.TaskRecord{}, errs.New(errs.KindInternal, "desired-only Route mutation has provider state")
	}
	task.Executor = taskjournal.TaskExecutorController
	task.TimeoutSeconds = 30
	task.RenderGeneration = int32(intent.Route.DesiredGeneration)
	task.Params = map[string]string{
		taskjournal.TaskResourceKindParam:     taskjournal.TaskResourceRoute,
		taskjournal.TaskRouteEnvironmentParam: intent.EnvironmentID,
	}
	task.Steps = []taskjournal.TaskStepRecord{{Kind: taskjournal.TaskStepOperation, ID: ids.New(ids.KindStep)}}
	value, err := json.Marshal(struct {
		Version    int    `json:"version"`
		TaskID     string `json:"task_id"`
		RouteID    string `json:"route_id"`
		Generation uint64 `json:"generation"`
	}{1, task.ID, intent.RouteID, intent.Route.DesiredGeneration})
	if err != nil {
		return etcd.TaskRecord{}, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(value)
	clear(value)
	task.PlanHash = hex.EncodeToString(digest[:])
	return task, nil
}
