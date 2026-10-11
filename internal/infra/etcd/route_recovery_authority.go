package etcd

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func componentRecoveryTask(task TaskRecord) bool {
	return task.Params[taskjournal.TaskBlueprintComponentUnitParam] != "" ||
		task.Executor == taskjournal.TaskExecutorAgent &&
			ids.Validate(ids.KindRoute, task.Target) == nil &&
			task.Params[taskjournal.TaskRouteEnvironmentParam] != ""
}

func (repository *TaskRepository) routeRecoveryPlanAtRevision(
	ctx context.Context, task TaskRecord, revision int64,
) (*agentpb.ExecutionPlan, *agentpb.ComponentRecoveryProcedure, keyvalue.Condition, error) {
	removal := task.Type == taskjournal.TaskRemove
	key := environmentchanges.RouteMutationIntentKey(task.ID)
	if removal {
		key = environmentchanges.RouteRemovalIntentKey(task.ID)
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, nil, keyvalue.Condition{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, nil, keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Route recovery intent is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	var plan *agentpb.ExecutionPlan
	if removal {
		intent, decodeErr := environmentchanges.DecodeRouteRemovalIntent(read.Values[0].Value)
		if decodeErr != nil {
			return nil, nil, keyvalue.Condition{}, decodeErr
		}
		if validateRouteRemovalTaskOwner(task, intent) != nil || intent.Status != taskjournal.TaskStatusPending ||
			intent.CandidateProjection == nil {
			return nil, nil, keyvalue.Condition{}, errs.New(
				errs.KindStateConflict,
				"Route removal recovery owner changed",
			)
		}
		plan, err = environmentchanges.OpenRouteExecutionPlan(intent.Plan, intent.Provider, intent.EnvironmentID,
			intent.RouteID, intent.CandidateProjection.RenderGeneration, true)
	} else {
		intent, decodeErr := environmentchanges.DecodeRouteMutationIntent(read.Values[0].Value)
		if decodeErr != nil {
			return nil, nil, keyvalue.Condition{}, decodeErr
		}
		if validateRouteMutationTaskOwner(task, intent) != nil || intent.Status != taskjournal.TaskStatusPending ||
			intent.CandidateProjection == nil {
			return nil, nil, keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Route mutation recovery owner changed")
		}
		plan, err = environmentchanges.OpenRouteExecutionPlan(intent.Plan, intent.Provider, intent.EnvironmentID,
			intent.RouteID, intent.CandidateProjection.RenderGeneration, false)
	}
	if err != nil {
		return nil, nil, keyvalue.Condition{}, err
	}
	if task.PlanID != plan.GetPlanId() || task.PlanHash != hex.EncodeToString(plan.GetPlanHash()) ||
		uint64(task.RenderGeneration) != plan.GetRenderGeneration() || task.Owner.EnvironmentID !=
		task.Params[taskjournal.TaskRouteEnvironmentParam] {
		return nil, nil, keyvalue.Condition{}, errs.New(
			errs.KindStateConflict,
			"Route recovery plan differs from its Task",
		)
	}
	return plan, plan.GetComponentRecoveryProcedure(), keyvalue.Condition{
		Key: key, ModRevision: read.Values[0].ModRevision,
	}, nil
}
