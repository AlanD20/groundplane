package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core/blueprintreconcile"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PublishBlueprintChild atomically creates one private Agent Task and claims
// its ready unit. The parent, desired head, plan and ledger epoch are all
// compared in the same transaction; a superseded or duplicate unit cannot
// enter the Agent queue.
func (repository *TaskRepository) PublishBlueprintChild(
	ctx context.Context, parentID string, child TaskRecord, unit blueprintunits.Unit,
	marker idempotency.IdempotencyMarker,
) (keyvalue.Versioned[TaskRecord], error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	if ids.Validate(ids.KindTask, parentID) != nil ||
		child.Params[taskjournal.TaskBlueprintParentParam] != parentID ||
		child.Actor != taskjournal.TaskActorSystem || child.Executor != taskjournal.TaskExecutorAgent ||
		child.Status != taskjournal.TaskStatusPending || child.idempotencyMarker != nil ||
		child.Owner.EnvironmentID == "" || child.PlanID == "" {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint child identity is invalid",
		)
	}
	if marker.Kind != idempotency.IdempotencyMarkerTask ||
		marker.State != idempotency.IdempotencyMarkerPending || marker.TaskID != child.ID ||
		marker.Locator.ScopeKind != idempotency.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != child.Owner.EnvironmentID ||
		(child.IdempotencyKey != "" && child.IdempotencyKey != marker.Locator.Key) ||
		!marker.CreatedAt.Equal(child.CreatedAt) || !marker.UpdatedAt.Equal(marker.CreatedAt) ||
		idempotency.ValidateIdempotencyMarker(marker) != nil {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint child marker is invalid",
		)
	}
	child = cloneTaskRecord(child)
	if child.IdempotencyKey == "" {
		child.IdempotencyKey = marker.Locator.Key
	}
	child.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := ValidateTaskRecord(child); err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	snapshot, err := ledger.Load(ctx, child.Owner.EnvironmentID)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	if snapshot.HeadTaskID != parentID || snapshot.Desired == nil ||
		snapshot.Desired.Record.ParentTaskID != parentID {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(
			errs.KindStateConflict,
			"Blueprint child parent is no longer current",
		)
	}
	selected, err := blueprintunits.Select(snapshot)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	if !slices.Contains(selected.Ready, blueprintreconcile.ResourceKey{Kind: unit.Target.Kind, ID: unit.Target.ID}) ||
		!desiredBlueprintUnitMatches(snapshot.Desired.Record.Units, unit) {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(
			errs.KindStateConflict,
			"Blueprint unit is not ready for execution",
		)
	}
	parentKey := taskjournal.TaskStorageKey(parentID)
	claimKey := taskjournal.BlueprintParentClaimKey(parentID)
	parentRead, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{parentKey, claimKey}, Revision: snapshot.ReadRevision,
	})
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	if parentRead == nil || len(parentRead.Values) != 2 || parentRead.Values[0] == nil || parentRead.Values[1] == nil {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(
			errs.KindStateConflict,
			"Blueprint parent claim is unavailable",
		)
	}
	defer keyvalue.ClearValues(parentRead.Values)
	parent, err := DecodeTaskRecord(parentRead.Values[0].Value)
	claimID, claimErr := idempotency.DecodeTaskReference(parentRead.Values[1].Value)
	if err != nil || claimErr != nil || claimID != parentID ||
		validateBlueprintParentClaimTask(parent) != nil || parent.Status != taskjournal.TaskStatusRunning ||
		parent.Owner != child.Owner ||
		parent.Params[blueprints.EnvironmentDesiredRevisionParam] != child.Params[blueprints.EnvironmentDesiredRevisionParam] {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(errs.KindStateConflict, "Blueprint parent claim changed")
	}
	execution := blueprintunits.ExecutionRecord{
		EnvironmentID: child.Owner.EnvironmentID, ParentTaskID: parentID,
		TaskID: child.ID, PlanID: child.PlanID, Unit: unit, State: blueprintunits.Pending,
	}
	claim, err := blueprintunits.PrepareMutation(snapshot, nil, []blueprintunits.ExecutionChange{{
		PlanID: child.PlanID, Next: &execution,
	}})
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	defer claim.Clear()
	childValue, err := EncodeTaskRecord(child)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	defer clear(childValue)
	reference, err := idempotency.EncodeTaskReference(child.ID)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	defer clear(reference)
	markerKey, err := idempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	markerValue, err := idempotency.EncodeIdempotencyMarker(marker)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	defer clear(markerValue)
	conditions := append(claim.Conditions(),
		keyvalue.Condition{Key: parentKey, ModRevision: parentRead.Values[0].ModRevision},
		keyvalue.Condition{Key: claimKey, ModRevision: parentRead.Values[1].ModRevision},
		keyvalue.Condition{Key: taskjournal.BlueprintParentAbortKey(parentID)},
		keyvalue.Condition{Key: taskjournal.TaskStorageKey(child.ID)},
		keyvalue.Condition{Key: taskjournal.TaskOperationIndexKey(child.OperationID, child.ID)},
		keyvalue.Condition{Key: taskjournal.TaskActiveOperationKey(child.OperationID)},
		keyvalue.Condition{Key: taskjournal.TaskQueueKey(child.Executor, child.ID)},
		keyvalue.Condition{Key: markerKey},
	)
	mutations := append(
		claim.Mutations(),
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: taskjournal.TaskStorageKey(child.ID), Value: childValue},
		keyvalue.Mutation{
			Type:  keyvalue.MutationPut,
			Key:   taskjournal.TaskOperationIndexKey(child.OperationID, child.ID),
			Value: reference,
		},
		keyvalue.Mutation{
			Type:  keyvalue.MutationPut,
			Key:   taskjournal.TaskActiveOperationKey(child.OperationID),
			Value: reference,
		},
		keyvalue.Mutation{
			Type:  keyvalue.MutationPut,
			Key:   taskjournal.TaskQueueKey(child.Executor, child.ID),
			Value: reference,
		},
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: markerKey, Value: markerValue},
	)
	indexKeys, err := taskJournalIndexKeys(child)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	for _, key := range indexKeys {
		conditions = append(conditions, keyvalue.Condition{Key: key})
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: []byte(child.ID)})
	}
	defer keyvalue.ClearMutationValues(mutations)
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	keyvalue.ClearValues(transaction.FailureReads)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	if !transaction.Succeeded {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(errs.KindStateConflict, "Blueprint child publication raced")
	}
	return keyvalue.Versioned[TaskRecord]{
		Record: cloneTaskRecord(child), Revision: transaction.Revision, ReadRevision: transaction.Revision,
	}, nil
}

func desiredBlueprintUnitMatches(units []blueprintunits.Unit, candidate blueprintunits.Unit) bool {
	for _, unit := range units {
		if unit.Target == candidate.Target {
			return unit.Removal == candidate.Removal && unit.Fingerprint == candidate.Fingerprint &&
				slices.Equal(unit.Reads, candidate.Reads) && slices.Equal(unit.Writes, candidate.Writes) &&
				slices.Equal(unit.After, candidate.After)
		}
	}
	return false
}
