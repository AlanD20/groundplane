package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A Blueprint parent is the replay-visible desired head. The effect Task is a
// private Agent child, so its own marker and queue never replace parent replay.
type blueprintTaskPair struct {
	parent      TaskRecord
	childMarker idempotency.IdempotencyMarker
}

func validateBlueprintTaskPair(
	claim blueprints.EnvironmentBlueprintStageClaim, environmentID string,
	child TaskRecord, parentMarker idempotency.IdempotencyMarker, pair blueprintTaskPair,
) error {
	if claim.SourceKind != blueprints.EnvironmentBlueprintSourceApply ||
		pair.parent.ID != claim.TaskID || pair.parent.Executor != taskjournal.TaskExecutorBlueprint ||
		child.Actor != taskjournal.TaskActorSystem || child.Executor != taskjournal.TaskExecutorAgent ||
		child.Params[taskjournal.TaskBlueprintParentParam] != pair.parent.ID ||
		child.Owner != pair.parent.Owner || child.ID == pair.parent.ID ||
		child.OperationID == pair.parent.OperationID ||
		pair.childMarker.Kind != idempotency.IdempotencyMarkerTask ||
		pair.childMarker.State != idempotency.IdempotencyMarkerPending ||
		pair.childMarker.TaskID != child.ID ||
		pair.childMarker.Locator.ScopeKind != idempotency.IdempotencyScopeEnvironment ||
		pair.childMarker.Locator.ScopeID != environmentID ||
		!pair.childMarker.CreatedAt.Equal(child.CreatedAt) ||
		!pair.childMarker.UpdatedAt.Equal(child.CreatedAt) ||
		idempotency.ValidateIdempotencyMarker(pair.childMarker) != nil ||
		(parentMarker.Locator.Key != "" && pair.parent.IdempotencyKey != "" && pair.parent.IdempotencyKey != parentMarker.Locator.Key) ||
		(pair.childMarker.Locator.Key != "" && child.IdempotencyKey != "" && child.IdempotencyKey != pair.childMarker.Locator.Key) {
		return errs.New(errs.KindValidationFailed, "Blueprint parent and child publication authority is invalid")
	}
	return nil
}

type blueprintChildPublication struct {
	markerKey  string
	conditions []keyvalue.Condition
	mutations  []keyvalue.Mutation
}

func (publication *blueprintChildPublication) clear() {
	keyvalue.ClearMutationValues(publication.mutations)
}

func prepareBlueprintChildPublication(
	ctx context.Context, child TaskRecord, marker idempotency.IdempotencyMarker,
	parentMarker idempotency.IdempotencyMarker,
) (blueprintChildPublication, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return blueprintChildPublication{}, err
	}
	if child.Params[taskjournal.TaskBlueprintParentParam] != parentMarker.TaskID ||
		child.OperationID == "" ||
		child.Status != taskjournal.TaskStatusPending || child.IdempotencyKey != marker.Locator.Key ||
		marker.TaskID != child.ID || marker.Kind != idempotency.IdempotencyMarkerTask ||
		marker.State != idempotency.IdempotencyMarkerPending ||
		idempotency.ValidateIdempotencyMarker(marker) != nil {
		return blueprintChildPublication{}, errs.New(errs.KindValidationFailed, "Blueprint child publication is invalid")
	}
	if err := ValidateTaskRecord(child); err != nil {
		return blueprintChildPublication{}, err
	}
	markerKey, err := idempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		return blueprintChildPublication{}, err
	}
	parentMarkerKey, err := idempotency.IdempotencyMarkerKey(parentMarker.Locator)
	if err != nil || markerKey == parentMarkerKey {
		return blueprintChildPublication{}, errs.New(errs.KindValidationFailed, "Blueprint child marker conflicts with parent")
	}
	taskValue, err := EncodeTaskRecord(child)
	if err != nil {
		return blueprintChildPublication{}, err
	}
	reference, err := idempotency.EncodeTaskReference(child.ID)
	if err != nil {
		clear(taskValue)
		return blueprintChildPublication{}, err
	}
	markerValue, err := idempotency.EncodeIdempotencyMarker(marker)
	if err != nil {
		clear(taskValue)
		clear(reference)
		return blueprintChildPublication{}, err
	}
	publication := blueprintChildPublication{
		markerKey: markerKey,
		conditions: []keyvalue.Condition{
			{Key: taskjournal.TaskStorageKey(child.ID)},
			{Key: taskjournal.TaskOperationIndexKey(child.OperationID, child.ID)},
			{Key: taskjournal.TaskActiveOperationKey(child.OperationID)},
			{Key: taskjournal.TaskQueueKey(child.Executor, child.ID)},
			{Key: markerKey},
		},
		mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationPut, Key: taskjournal.TaskStorageKey(child.ID), Value: taskValue},
			{Type: keyvalue.MutationPut, Key: taskjournal.TaskOperationIndexKey(child.OperationID, child.ID), Value: reference},
			{Type: keyvalue.MutationPut, Key: taskjournal.TaskActiveOperationKey(child.OperationID), Value: reference},
			{Type: keyvalue.MutationPut, Key: taskjournal.TaskQueueKey(child.Executor, child.ID), Value: reference},
			{Type: keyvalue.MutationPut, Key: markerKey, Value: markerValue},
		},
	}
	indexKeys, err := taskJournalIndexKeys(child)
	if err != nil {
		publication.clear()
		return blueprintChildPublication{}, err
	}
	for _, key := range indexKeys {
		publication.conditions = append(publication.conditions, keyvalue.Condition{Key: key})
		publication.mutations = append(publication.mutations, keyvalue.Mutation{
			Type: keyvalue.MutationPut, Key: key, Value: []byte(child.ID),
		})
	}
	return publication, nil
}
