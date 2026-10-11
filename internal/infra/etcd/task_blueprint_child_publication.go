package etcd

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core/blueprintreconcile"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	componentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PublishBlueprintChild atomically creates one private Agent Task and claims
// its ready unit. The parent, desired head, plan and ledger epoch are all
// compared in the same transaction; a superseded or duplicate unit cannot
// enter the Agent queue.
func (repository *TaskRepository) PublishBlueprintChild(
	ctx context.Context, parentID string, child TaskRecord, unit blueprintunits.Unit,
	releasePublication BlueprintReleasePublication,
	componentPublication componentplanning.Publication,
	entryRemovalPublication BlueprintEntryRemovalPublication,
	serviceRemovalPublication BlueprintServiceRemovalPublication,
	zoneRemovalPublication BlueprintZoneRemovalPublication,
	attachRemovalPublication *BlueprintAttachRemovalPublication,
) (published keyvalue.Versioned[TaskRecord], resultErr error) {
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
	releaseChild := unit.Target.Kind == ids.KindService && !unit.Removal && !releasePublication.IsZero() &&
		componentPublication.IsZero() && entryRemovalPublication.IsZero() && serviceRemovalPublication.IsZero() &&
		child.Params[releaserender.TaskReleasePublicationParam] != ""
	networkChild := blueprintNetworkChildTaskMatches(child, unit) && releasePublication.IsZero() &&
		componentPublication.IsZero() && entryRemovalPublication.IsZero() && serviceRemovalPublication.IsZero() &&
		child.Params[releaserender.TaskReleasePublicationParam] == ""
	volumeChild := blueprintVolumeChildTaskMatches(child, unit) && releasePublication.IsZero() &&
		componentPublication.IsZero() && entryRemovalPublication.IsZero() && serviceRemovalPublication.IsZero() &&
		child.Params[releaserender.TaskReleasePublicationParam] == ""
	attachChild := unit.Target.Kind == ids.KindAttach && !unit.Removal &&
		child.Params[attachinputs.TaskAttachIDParam] == unit.Target.ID && releasePublication.IsZero() &&
		componentPublication.IsZero() && entryRemovalPublication.IsZero() && serviceRemovalPublication.IsZero() &&
		child.Params[releaserender.TaskReleasePublicationParam] == ""
	componentChild := unit.Target.Kind == ids.KindComponent &&
		child.Params[taskjournal.TaskBlueprintComponentUnitParam] == unit.Target.ID &&
		releasePublication.IsZero() && !componentPublication.IsZero() && entryRemovalPublication.IsZero() && serviceRemovalPublication.IsZero() &&
		child.Params[releaserender.TaskReleasePublicationParam] == ""
	entryChild := unit.Target.Kind == ids.KindEnvEntry && unit.Removal &&
		authoredBlueprintEntryRemovalTask(child) && releasePublication.IsZero() &&
		componentPublication.IsZero() && !entryRemovalPublication.IsZero() && serviceRemovalPublication.IsZero() &&
		child.Params[releaserender.TaskReleasePublicationParam] == ""
	serviceRemovalChild := unit.Target.Kind == ids.KindService && unit.Removal &&
		authoredBlueprintServiceRemovalTask(child) && releasePublication.IsZero() &&
		componentPublication.IsZero() && entryRemovalPublication.IsZero() && !serviceRemovalPublication.IsZero() &&
		child.Params[releaserender.TaskReleasePublicationParam] == ""
	zoneRemovalChild := unit.Target.Kind == ids.KindNetwork && unit.Removal &&
		authoredBlueprintZoneRemovalTask(child) && child.Target == unit.Target.ID &&
		releasePublication.IsZero() && componentPublication.IsZero() && entryRemovalPublication.IsZero() &&
		serviceRemovalPublication.IsZero() && !zoneRemovalPublication.IsZero() &&
		child.Params[releaserender.TaskReleasePublicationParam] == ""
	attachRemovalChild := unit.Target.Kind == ids.KindAttach && unit.Removal &&
		blueprintAttachChildTaskMatches(child, unit) && attachRemovalPublication != nil &&
		!attachRemovalPublication.IsZero() && releasePublication.IsZero() && componentPublication.IsZero() &&
		entryRemovalPublication.IsZero() && serviceRemovalPublication.IsZero() && zoneRemovalPublication.IsZero() &&
		child.Params[releaserender.TaskReleasePublicationParam] == ""
	if attachRemovalPublication != nil && !attachRemovalChild ||
		!zoneRemovalPublication.IsZero() && !zoneRemovalChild ||
		!releaseChild && !networkChild && !volumeChild && !attachChild && !componentChild && !entryChild &&
			!serviceRemovalChild && !zoneRemovalChild && !attachRemovalChild {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(
			errs.KindStateConflict, "Blueprint child has no supported effect publication",
		)
	}
	if releaseChild {
		if err := releasePublication.validate(child.Owner.EnvironmentID, child); err != nil {
			return keyvalue.Versioned[TaskRecord]{}, err
		}
	}
	if componentChild {
		if err := componentPublication.ValidateChildExecution(child.ID, parentID, child.PlanID, child.PlanHash, uint64(child.RenderGeneration), child.Configuration, child.Materializations); err != nil {
			return keyvalue.Versioned[TaskRecord]{}, err
		}
		if err := componentPublication.ValidateBlueprintChild(
			child.Owner.EnvironmentID,
			unit.Target.ID,
			unit.Removal,
			componentplanning.TaskIdentity{
				ID: child.ID, Target: child.Target, Executor: child.Executor,
				Type: child.Type, CreatedAt: child.CreatedAt,
			},
		); err != nil {
			return keyvalue.Versioned[TaskRecord]{}, err
		}
	}
	child = cloneTaskRecord(child)
	if child.IdempotencyKey != "" && child.IdempotencyKey != child.ID {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(
			errs.KindValidationFailed, "Blueprint child idempotency key is invalid",
		)
	}
	child.IdempotencyKey = child.ID
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
	configuration := configurationTaskPublication{task: child}
	if componentChild || entryChild || releaseChild {
		configuration, err = prepareConfigurationTaskPublication(ctx, repository.store, child, snapshot.ReadRevision)
		if err != nil {
			return keyvalue.Versioned[TaskRecord]{}, err
		}
		defer configuration.clear()
		defer func() {
			if resultErr != nil {
				resultErr = configuration.finish(ctx, repository.store, resultErr)
			}
		}()
		child = configuration.task
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
	if volumeChild {
		create, err := blueprintVolumeNeedsCreate(snapshot, unit)
		if err != nil {
			return keyvalue.Versioned[TaskRecord]{}, err
		}
		expectedMode := taskjournal.TaskBlueprintVolumeModeVerify
		if create {
			expectedMode = taskjournal.TaskBlueprintVolumeModeCreate
		}
		if child.Params[taskjournal.TaskBlueprintVolumeModeParam] != expectedMode {
			return keyvalue.Versioned[TaskRecord]{}, errs.New(
				errs.KindStateConflict, "Blueprint Volume child execution mode changed",
			)
		}
	}
	if releaseChild {
		if err := repository.validateBlueprintReleaseChildPublication(
			ctx, child, unit, releasePublication, snapshot.ReadRevision,
		); err != nil {
			return keyvalue.Versioned[TaskRecord]{}, err
		}
	}
	attachPublication := blueprintAttachChildPublication{}
	if attachChild {
		attachPublication, err = repository.prepareBlueprintAttachChildPublication(
			ctx, child, unit, snapshot.ReadRevision,
		)
		if err != nil {
			return keyvalue.Versioned[TaskRecord]{}, err
		}
		defer attachPublication.clear()
	}
	entryConditions, entryMutations := []keyvalue.Condition(nil), []keyvalue.Mutation(nil)
	if entryChild {
		entryConditions, entryMutations, err = entryRemovalPublication.prepare(
			ctx, repository, child, unit, snapshot.ReadRevision,
		)
		if err != nil {
			return keyvalue.Versioned[TaskRecord]{}, err
		}
	}
	serviceRemovalConditions, serviceRemovalMutations := []keyvalue.Condition(nil), []keyvalue.Mutation(nil)
	if serviceRemovalChild {
		serviceRemovalConditions, serviceRemovalMutations, err = serviceRemovalPublication.prepare(
			ctx, repository, child, unit, snapshot.ReadRevision,
		)
		if err != nil {
			return keyvalue.Versioned[TaskRecord]{}, err
		}
	}
	zoneRemovalConditions, zoneRemovalMutations := []keyvalue.Condition(nil), []keyvalue.Mutation(nil)
	if zoneRemovalChild {
		zoneRemovalConditions, zoneRemovalMutations, err = zoneRemovalPublication.prepare(
			ctx, repository, child, unit, snapshot.ReadRevision,
		)
		if err != nil {
			return keyvalue.Versioned[TaskRecord]{}, err
		}
	}
	attachRemovalConditions, attachRemovalMutations := []keyvalue.Condition(nil), []keyvalue.Mutation(nil)
	if attachRemovalChild {
		defer func() {
			if resultErr != nil {
				resultErr = attachRemovalPublication.finish(ctx, repository, resultErr)
			}
		}()
		child, attachRemovalConditions, attachRemovalMutations, err = attachRemovalPublication.prepare(
			ctx, repository, child, unit, snapshot.ReadRevision,
		)
		if err != nil {
			return keyvalue.Versioned[TaskRecord]{}, err
		}
		defer keyvalue.ClearMutationValues(attachRemovalMutations)
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
		parent.idempotencyMarker == nil ||
		parent.Owner != child.Owner ||
		parent.Params[blueprints.EnvironmentDesiredRevisionParam] != child.Params[blueprints.EnvironmentDesiredRevisionParam] {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(errs.KindStateConflict, "Blueprint parent claim changed")
	}
	if componentChild {
		if err := componentPublication.ValidateProxySourceHoldParent(parent.ID, parent.OperationID); err != nil {
			return keyvalue.Versioned[TaskRecord]{}, err
		}
	}
	parentMarkerKey, err := idempotency.IdempotencyMarkerKey(*parent.idempotencyMarker)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	parentMarkerRead, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{parentMarkerKey}, Revision: snapshot.ReadRevision,
	})
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	if parentMarkerRead == nil || len(parentMarkerRead.Values) != 1 || parentMarkerRead.Values[0] == nil {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(
			errs.KindStateConflict, "Blueprint parent marker is missing",
		)
	}
	defer keyvalue.ClearValues(parentMarkerRead.Values)
	parentMarker, err := idempotency.DecodeIdempotencyMarker(
		parentMarkerRead.Values[0].Value, *parent.idempotencyMarker,
	)
	if err != nil || parentMarker.Kind != idempotency.IdempotencyMarkerTask ||
		parentMarker.State != idempotency.IdempotencyMarkerPending || parentMarker.TaskID != parentID {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(
			errs.KindStateConflict, "Blueprint parent marker changed",
		)
	}
	defer clear(parentMarker.Intent.Ciphertext)
	defer clear(parentMarker.Response.Body)
	if volumeChild {
		intentDigest, err := blueprints.ProtectedBlueprintIntentDigest(parentMarker.Intent)
		if err != nil {
			return keyvalue.Versioned[TaskRecord]{}, err
		}
		if hex.EncodeToString(intentDigest[:]) != child.Params[taskjournal.TaskBlueprintVolumeIntentParam] {
			return keyvalue.Versioned[TaskRecord]{}, errs.New(
				errs.KindStateConflict, "Blueprint Volume child intent authority changed",
			)
		}
	}
	// The child has no operator replay surface. Its private marker reuses the
	// parent's protected intent so terminal lifecycle can retain exact authority
	// without inventing a second user request or exposing the hidden Task.
	responseBody, err := json.Marshal(struct {
		TaskID string `json:"task_id"`
	}{TaskID: child.ID})
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	marker := idempotency.IdempotencyMarker{
		Kind: idempotency.IdempotencyMarkerTask, State: idempotency.IdempotencyMarkerPending,
		Locator: idempotency.IdempotencyLocator{
			ScopeKind: idempotency.IdempotencyScopeEnvironment, ScopeID: child.Owner.EnvironmentID,
			Method: http.MethodPost, Route: "/internal/blueprint-child", Key: child.ID,
		},
		Intent: parentMarker.Intent,
		Response: idempotency.IdempotencyResponse{
			Status: http.StatusAccepted, ContentKind: "application/json", Body: responseBody,
		},
		TaskID: child.ID, CreatedAt: child.CreatedAt, UpdatedAt: child.CreatedAt,
	}
	if err := idempotency.ValidateIdempotencyMarker(marker); err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	child.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := ValidateTaskRecord(child); err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
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
	conditions := append(
		claim.Conditions(),
		keyvalue.Condition{
			Key:         blueprintunits.DesiredPlanKey(child.Owner.EnvironmentID),
			ModRevision: snapshot.Desired.Revision,
		},
		keyvalue.Condition{Key: parentKey, ModRevision: parentRead.Values[0].ModRevision},
		keyvalue.Condition{Key: claimKey, ModRevision: parentRead.Values[1].ModRevision},
		keyvalue.Condition{Key: parentMarkerKey, ModRevision: parentMarkerRead.Values[0].ModRevision},
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
	if releaseChild {
		releasePublication, err = releasePublication.withExistingComparisons(conditions)
		if err != nil {
			return keyvalue.Versioned[TaskRecord]{}, err
		}
		conditions = append(conditions, releasePublication.conditions...)
		mutations = append(mutations, releasePublication.mutations...)
	}
	if attachChild {
		conditions = append(conditions, attachPublication.conditions...)
		mutations = append(mutations, attachPublication.mutations...)
	}
	if componentChild || entryChild || releaseChild {
		if componentChild {
			conditions = append(conditions, componentPublication.Conditions()...)
			mutations = append(mutations, componentPublication.Mutations()...)
		}
		if entryChild {
			conditions = append(conditions, entryConditions...)
			mutations = append(mutations, entryMutations...)
		}
		condition, present, err := taskConfigurationCondition(child)
		if err != nil {
			return keyvalue.Versioned[TaskRecord]{}, err
		}
		if present {
			conditions = append(conditions, condition)
		}
		conditions = append(conditions, configuration.pins.conditions...)
		mutations = append(mutations, configuration.pins.mutations...)
	}
	if serviceRemovalChild {
		conditions = append(conditions, serviceRemovalConditions...)
		mutations = append(mutations, serviceRemovalMutations...)
	}
	if zoneRemovalChild {
		conditions = append(conditions, zoneRemovalConditions...)
		mutations = append(mutations, zoneRemovalMutations...)
	}
	if attachRemovalChild {
		conditions = append(conditions, attachRemovalConditions...)
		mutations = append(mutations, attachRemovalMutations...)
	}
	defer keyvalue.ClearMutationValues(mutations)
	if repository.blueprintChildStore == nil {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(
			errs.KindInternal,
			"Blueprint child transaction store is not configured",
		)
	}
	envelope := BlueprintChildTransaction{parentID: parentID, childID: child.ID, planID: child.PlanID,
		environmentID: child.Owner.EnvironmentID, conditions: conditions, mutations: mutations}
	transaction, err := repository.blueprintChildStore.TransactBlueprintChild(ctx, envelope)
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
