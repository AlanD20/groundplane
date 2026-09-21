package etcd

import (
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	componentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type routeTaskChange struct {
	applies    bool
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	values     [][]byte
}

func routeMutationSelectedProjection(
	projection projectionrecord.EnvironmentComposeProjection,
	task TaskRecord,
	intent environmentchanges.RouteMutationIntent,
) bool {
	if intent.CandidateProjection != nil {
		return environmentchanges.SameRouteRemovalProjection(projection, *intent.CandidateProjection)
	}
	selectedRevisionID := task.ID
	if task.RetryOf != "" {
		selectedRevisionID = task.RetryOf
	}
	if intent.Provider != nil || projection.RevisionID != selectedRevisionID {
		return false
	}
	for _, route := range projection.DesiredRoutes {
		if route.Desired.ID == intent.RouteID {
			return route.DesiredGeneration == intent.Route.DesiredGeneration &&
				componentplanning.RouteDesiredEqual(route.Desired, intent.Route.Desired)
		}
	}
	return false
}

func validateRouteMutationTaskOwner(task TaskRecord, intent environmentchanges.RouteMutationIntent) error {
	executor := taskjournal.TaskExecutorController
	if intent.Provider != nil {
		executor = taskjournal.TaskExecutorAgent
	}
	if task.ID != intent.TaskID || task.OperationID != intent.OperationID || task.Executor != executor ||
		(task.Type != taskjournal.TaskCreate && task.Type != taskjournal.TaskUpdate) || task.Target != intent.RouteID ||
		!task.CreatedAt.Equal(intent.CreatedAt) || len(task.Params) < 2 ||
		task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceRoute ||
		task.Params[taskjournal.TaskRouteEnvironmentParam] != intent.EnvironmentID {
		return errs.New(errs.KindStateConflict, "Route mutation intent does not belong to its Task")
	}
	return nil
}

func sameRouteDesiredVersion(left routerecord.Record, right routerecord.Record) bool {
	return left.EnvironmentID == right.EnvironmentID && left.Desired == right.Desired &&
		left.DesiredGeneration == right.DesiredGeneration
}

func validateRouteRemovalTaskOwner(task TaskRecord, intent environmentchanges.RouteRemovalIntent) error {
	expectedExecutor := taskjournal.TaskExecutorController
	validParams := len(task.Params) == 2 && task.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceRoute &&
		task.Params[taskjournal.TaskRouteEnvironmentParam] == intent.EnvironmentID
	if intent.Provider != nil {
		expectedExecutor = taskjournal.TaskExecutorAgent
		validParams = intent.CandidateProjection != nil && len(task.Params) == 4 &&
			task.Params[taskjournal.TaskRouteEnvironmentParam] == intent.EnvironmentID &&
			task.Params[taskjournal.TaskMaterializationEnvironmentParam] == intent.EnvironmentID &&
			task.Params[blueprints.EnvironmentDesiredRevisionParam] == intent.CandidateProjection.RevisionID
	}
	if task.ID != intent.TaskID || task.Executor != expectedExecutor || task.Type != taskjournal.TaskRemove ||
		task.Target != intent.RouteID || !task.CreatedAt.Equal(intent.CreatedAt) || !validParams {
		return errs.New(errs.KindStateConflict, "Route removal intent does not belong to its Task")
	}
	return nil
}

func routeRemovalTombstonePhase(intent environmentchanges.RouteRemovalIntent) deletionrecord.DeletionPhase {
	if intent.Provider != nil {
		return deletionrecord.DeletionPhaseHostEffects
	}
	return deletionrecord.DeletionPhaseFinalizing
}

func clearRouteTaskChange(change routeTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}
