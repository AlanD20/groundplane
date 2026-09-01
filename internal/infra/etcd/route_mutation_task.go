package etcd

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// BeginRouteMutationWithTask publishes a desired Route and its immutable
// reconciliation intent in the same transaction as the Task journal entry.
// The route write is deliberately independent of the candidate projection:
// a failed or disabled reconciler must never discard desired state.
func (repository *RouteRepository) BeginRouteMutationWithTask(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	target Versioned[ServiceRecord],
	current *Versioned[RouteRecord],
	record RouteRecord,
	intent RouteMutationIntent,
	task TaskRecord,
	directMarker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateRouteHierarchy(ctx, environment, project, target, record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if current != nil {
		if err := validateRouteVersion(*current); err != nil {
			return IdempotencyTransactionResult{}, err
		}
		if intent.Kind != RouteMutationEdit || intent.RouteRevision != current.Revision {
			return IdempotencyTransactionResult{}, errs.New(
				errs.KindStateConflict, "Route mutation current revision changed",
			)
		}
	} else if intent.Kind != RouteMutationCreate {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "Route mutation create intent is required",
		)
	}
	if intent.RouteID != record.Desired.ID || intent.EnvironmentID != record.EnvironmentID || intent.Route != record {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "Route mutation intent does not match desired Route",
		)
	}
	if err := validateRouteMutationIntent(intent); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateRouteTaskAcceptanceMarker(directMarker, record.EnvironmentID); err != nil {
		return IdempotencyTransactionResult{}, err
	}

	publicationCurrent := intent.CurrentProjection
	publicationCandidate := intent.CandidateProjection
	publicationRevision := intent.CurrentProjectionRevision
	if publicationCurrent == nil && publicationCandidate == nil {
		selected, found, readErr := currentEnvironmentProjectionAtRevision(
			ctx, repository.store, intent.EnvironmentID, 0,
		)
		if readErr != nil || !found {
			return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Route desired head is unavailable")
		}
		candidate, applyErr := ApplyEnvironmentRoute(selected.Record, record)
		if applyErr != nil {
			return IdempotencyTransactionResult{}, applyErr
		}
		candidate.RevisionID = task.ID
		publicationCurrent = &selected.Record
		publicationCandidate = &candidate
		publicationRevision = selected.Revision
	} else if publicationCurrent == nil || publicationCandidate == nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Route desired head is unavailable")
	}
	action := EnvironmentRouteMutationEdit
	request := &EnvironmentRouteMutationRequest{Exposure: record.Desired.Exposure}
	if intent.Kind == RouteMutationCreate {
		action = EnvironmentRouteMutationCreate
		request = &EnvironmentRouteMutationRequest{
			EnvironmentID: record.EnvironmentID, Host: record.Desired.Host, Path: record.Desired.Path,
			Exposure: record.Desired.Exposure, TargetServiceID: record.Desired.TargetServiceID,
			TargetPort: record.Desired.TargetPort,
		}
	}
	audit := EnvironmentDesiredMutationAudit{Route: &EnvironmentRouteMutationAudit{
		Action: action, BaseRevisionID: publicationCurrent.RevisionID,
		RouteID: record.Desired.ID, Request: request,
	}}
	publication, err := prepareRouteHeadPublication(
		ctx, repository.store, task, directMarker, publicationCurrent,
		*publicationCandidate, audit, publicationRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearRouteHeadPublication(publication)
	conditions := []Condition{
		{Key: environmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: projectKey(project.Record.ID), ModRevision: project.Revision},
		serviceDesiredCondition(target),
		{Key: deletionTombstoneKey("route", record.Desired.ID)},
		{Key: deletionTombstoneKey("environment", environment.Record.ID)},
		{Key: deletionTombstoneKey("project", project.Record.ID)},
		{Key: deletionTombstoneKey("service", target.Record.Desired.ID)},
		{Key: componentTaskActiveEnvironmentKey(environment.Record.ID)},
	}
	var mutations []Mutation

	task = cloneTaskRecord(task)
	if task.ID != intent.TaskID || task.OperationID != intent.OperationID ||
		task.Target != record.Desired.ID || task.Status != TaskStatusPending {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "Route mutation Task does not match its intent",
		)
	}
	taskMarker := routeMutationTaskMarker(directMarker, task)
	task.idempotencyMarker = cloneIdempotencyLocator(&taskMarker.Locator)
	if task.IdempotencyKey == "" {
		task.IdempotencyKey = directMarker.Locator.Key
	}

	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := encodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)
	intentValue, err := encodeRouteMutationIntent(intent)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(intentValue)

	conditions = append(conditions,
		Condition{Key: taskKey(task.ID)},
		Condition{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		Condition{Key: taskActiveOperationKey(task.OperationID)},
		Condition{Key: taskQueueKey(task.Executor, task.ID)},
		Condition{Key: routeMutationIntentKey(task.ID)},
	)
	mutations = append(mutations,
		Mutation{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue},
		Mutation{Type: MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		Mutation{Type: MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		Mutation{Type: MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		Mutation{Type: MutationPut, Key: routeMutationIntentKey(task.ID), Value: intentValue},
	)
	conditions = append(conditions, publication.conditions...)
	mutations = append(mutations, publication.mutations...)
	mutations = append(mutations, Mutation{
		Type: MutationPut, Key: componentTaskActiveEnvironmentKey(environment.Record.ID), Value: []byte(task.ID),
	})

	taskTenant, err := loadTaskInitiationTenant(ctx, repository.store, project)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(taskTenant, project, environment, TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	classify := func(_ int64, values []*KeyValue) error {
		if len(values) != len(conditions) {
			return errs.New(errs.KindInternal, "Route mutation compare evidence is incomplete")
		}
		for index, condition := range conditions {
			if !conditionMatchesRead(condition, values[index]) {
				return errs.New(errs.KindStateConflict, "Route mutation desired head changed")
			}
		}
		return errs.New(errs.KindStateConflict, "Route mutation state changed")
	}
	plan, err := newTaskIdempotencyMutationPlan(task, initiation, conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return applyRouteMutationTaskMarkers(ctx, repository.store, plan, taskMarker, directMarker)
}

func validateRouteTaskAcceptanceMarker(marker IdempotencyMarker, environmentID string) error {
	if marker.Kind != IdempotencyMarkerDirect || marker.State != IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != IdempotencyScopeEnvironment || marker.Locator.ScopeID != environmentID {
		return errs.New(errs.KindValidationFailed, "Route Task acceptance marker is invalid")
	}
	return validateIdempotencyMarker(marker)
}

func routeMutationTaskMarker(directMarker IdempotencyMarker, task TaskRecord) IdempotencyMarker {
	body, _ := json.Marshal(struct {
		TaskID string `json:"task_id"`
	}{TaskID: task.ID})
	return IdempotencyMarker{
		Kind: IdempotencyMarkerTask, State: IdempotencyMarkerPending,
		Locator: IdempotencyLocator{
			ScopeKind: directMarker.Locator.ScopeKind, ScopeID: directMarker.Locator.ScopeID,
			Method: http.MethodPost, Route: "/routes/{id}/reconcile", Key: task.ID,
		},
		Intent: directMarker.Intent,
		Response: IdempotencyResponse{
			Status: http.StatusAccepted, ContentKind: "application/json", Body: body,
		},
		TaskID: task.ID, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
	}
}

func applyRouteMutationTaskMarkers(
	ctx context.Context,
	store idempotencyRepositoryStore,
	plan *idempotencyMutationPlan,
	taskMarker IdempotencyMarker,
	directMarker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if plan == nil || plan.markerKind != IdempotencyMarkerTask {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "Route mutation Task plan is invalid")
	}
	if err := validateIdempotencyMarker(taskMarker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateIdempotencyMarker(directMarker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	taskKeyValue, err := idempotencyMarkerKey(taskMarker.Locator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	directKeyValue, err := idempotencyMarkerKey(directMarker.Locator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	taskValue, err := encodeIdempotencyMarker(taskMarker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	directValue, err := encodeIdempotencyMarker(directMarker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(directValue)
	conditions, mutations, classify, err := plan.consume()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearMutationValues(mutations)
	baseConditionCount := len(conditions)
	conditions = append(conditions,
		Condition{Key: taskKeyValue},
		Condition{Key: directKeyValue},
	)
	mutations = append(mutations,
		Mutation{Type: MutationPut, Key: taskKeyValue, Value: taskValue},
		Mutation{Type: MutationPut, Key: directKeyValue, Value: directValue},
	)
	if !directMarker.RetainUntil.IsZero() {
		retentionKey, err := idempotencyRetentionKey(directKeyValue, directMarker.RetainUntil)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		retentionValue, err := json.Marshal(retentionReferenceJSON{Schema: 1, MarkerKey: directKeyValue})
		if err != nil {
			return IdempotencyTransactionResult{}, errs.Wrap(errs.KindInternal, err)
		}
		mutations = append(mutations, Mutation{Type: MutationPut, Key: retentionKey, Value: retentionValue})
	}
	if validate := plan.transactionValidator(); validate != nil {
		if err := validate(conditions, mutations); err != nil {
			return IdempotencyTransactionResult{}, err
		}
	}
	result, err := store.Transact(ctx, conditions, mutations)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearKeyValues(result.FailureReads)
	if result.Succeeded {
		return IdempotencyTransactionResult{kind: idempotencyTransactionApplied, revision: result.Revision}, nil
	}
	if len(result.FailureReads) != len(conditions) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "Route mutation compare evidence is incomplete")
	}
	if result.FailureReads[baseConditionCount] != nil || result.FailureReads[baseConditionCount+1] != nil {
		return IdempotencyTransactionResult{
			kind: idempotencyTransactionConflict, revision: result.Revision,
			conflict: errs.New(errs.KindStateConflict, "Route mutation idempotency marker already exists"),
		}, nil
	}
	conflict := classify(result.Revision, result.FailureReads[:baseConditionCount])
	if conflict == nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "Route mutation conflict was not classified")
	}
	return IdempotencyTransactionResult{
		kind: idempotencyTransactionConflict, revision: result.Revision, conflict: conflict,
	}, nil
}
