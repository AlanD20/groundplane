package componentwork

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type RouteDefinitionResult struct {
	TaskID        string    `json:"task_id"`
	WorkItemID    string    `json:"work_item_id"`
	EnvironmentID string    `json:"environment_id"`
	RouteID       string    `json:"route_id"`
	Removed       bool      `json:"removed"`
	At            time.Time `json:"at"`
}

func routeDefinitionKey(itemID string) string { return "/v1/route-definition-result/" + itemID }

// PrepareRouteDefinition records an atomic metadata decision. The publishing
// transaction must also prove no router runtime needs changing; this function
// does not substitute success for a missing host execution.
func PrepareRouteDefinition(task workexecution.Task, item workexecution.Item, marker idempotency.IdempotencyMarker, at time.Time) (workqueue.Change, error) {
	if item.Operation != workexecution.Component || item.Placement.Executor != workexecution.Controller ||
		item.TargetID != task.TargetID || ids.Validate(ids.KindRoute, task.TargetID) != nil ||
		(task.Action != workexecution.ActionCreate && task.Action != workexecution.ActionUpdate && task.Action != workexecution.ActionRemove) {
		return workqueue.Change{}, conflict("Route definition requires its Controller-only action")
	}
	value, err := recordcodec.Encode("route-definition-result", RouteDefinitionResult{
		TaskID: task.ID, WorkItemID: item.ID, EnvironmentID: task.Scope.EnvironmentID,
		RouteID: task.TargetID, Removed: task.Action == workexecution.ActionRemove, At: at,
	})
	if err != nil {
		return workqueue.Change{}, err
	}
	digest := sha256.Sum256(value)
	change, err := workqueue.PrepareSettledDecision(task, item, workexecution.Input{
		ID: ids.DeriveAt(ids.KindPlan, item.CreatedAt, item.ID, "route-definition-result"), SHA256: hex.EncodeToString(digest[:]),
	}, marker, at)
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: routeDefinitionKey(item.ID)})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: routeDefinitionKey(item.ID), Value: value})
	return change, nil
}

func (repository *InputRepository) RetireRouteDefinition(ctx context.Context, view workqueue.View, at time.Time, final bool) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Validate() != nil || item.Validate() != nil || task.CollectionStartedAt == nil || task.RetainUntil == nil ||
		task.FinishedAt == nil || at.Before(*task.CollectionStartedAt) || at.Before(*task.RetainUntil) ||
		view.Task.Revision <= 0 || view.Item.Revision <= 0 || view.ReadRevision <= 0 ||
		item.Operation != workexecution.Component || item.Placement.Executor != workexecution.Controller ||
		item.TaskID != task.ID || item.TargetID != task.TargetID || ids.Validate(ids.KindRoute, task.TargetID) != nil ||
		(task.Action != workexecution.ActionCreate && task.Action != workexecution.ActionUpdate && task.Action != workexecution.ActionRemove) ||
		item.Phase != workexecution.Settled || item.Outcome != workexecution.Succeeded || item.Cleanup != workexecution.CleanupNone ||
		item.AttemptNumber != 0 || item.Input != nil || item.Result == nil || !final {
		return false, workqueue.Change{}, conflict("Route definition retention requires an expired completed decision")
	}
	key := routeDefinitionKey(item.ID)
	read, err := repository.read(ctx, []string{key}, view.ReadRevision)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil {
		return false, workqueue.Change{}, conflict("Route definition result is missing")
	}
	result, err := recordcodec.Decode[RouteDefinitionResult](value.Value, "route-definition-result")
	digest := sha256.Sum256(value.Value)
	if err != nil || result.TaskID != task.ID || result.WorkItemID != item.ID || result.EnvironmentID != task.Scope.EnvironmentID ||
		result.RouteID != task.TargetID || result.Removed != (task.Action == workexecution.ActionRemove) || !result.At.Equal(item.UpdatedAt) ||
		item.Result.ID != ids.DeriveAt(ids.KindPlan, item.CreatedAt, item.ID, "route-definition-result") || item.Result.SHA256 != hex.EncodeToString(digest[:]) {
		return false, workqueue.Change{}, conflict("Route definition result differs from its settled action")
	}
	return true, workqueue.Change{
		Conditions: []keyvalue.Condition{
			{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
			{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision},
			{Key: key, ModRevision: value.ModRevision},
			{Key: workqueue.AttemptIndexPrefix(item.ID), Prefix: true},
		},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: key}},
	}, nil
}
