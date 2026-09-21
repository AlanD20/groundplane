package etcd

import (
	"context"
	"encoding/json"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"net/http"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// BeginRouteMutationWithTask publishes a desired Route and its immutable
// reconciliation intent in the same transaction as the Task journal entry.
// The route write is deliberately independent of the candidate projection:
// a failed or disabled reconciler must never discard desired state.
func (repository *RouteRepository) BeginRouteMutationWithTask(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[servicerecord.ServiceRecord],
	current *etcdstore.Versioned[routerecord.Record],
	record routerecord.Record,
	intent RouteMutationIntent,
	task TaskRecord,
	directMarker idempotencyrecord.IdempotencyMarker,
) (_ IdempotencyTransactionResult, publicationErr error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
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
		candidate, applyErr := projectionrecord.ApplyEnvironmentRoute(selected.Record, record)
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
	action := blueprints.EnvironmentRouteMutationEdit
	request := &blueprints.EnvironmentRouteMutationRequest{Exposure: record.Desired.Exposure}
	if intent.Kind == RouteMutationCreate {
		action = blueprints.EnvironmentRouteMutationCreate
		request = &blueprints.EnvironmentRouteMutationRequest{
			EnvironmentID: record.EnvironmentID, Host: record.Desired.Host, Path: record.Desired.Path,
			Exposure: record.Desired.Exposure, TargetServiceID: record.Desired.TargetServiceID,
			TargetPort: record.Desired.TargetPort,
		}
	}
	audit := blueprints.EnvironmentDesiredMutationAudit{Route: &blueprints.EnvironmentRouteMutationAudit{
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
	conditions := []etcdstore.Condition{
		{Key: hierarchyrecord.EnvironmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: hierarchyrecord.ProjectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletionTombstoneKey("route", record.Desired.ID)},
		{Key: deletionTombstoneKey("environment", environment.Record.ID)},
		{Key: deletionTombstoneKey("project", project.Record.ID)},
		{Key: deletionTombstoneKey("service", target.Record.Desired.ID)},
		{Key: componentTaskActiveEnvironmentKey(environment.Record.ID)},
	}
	var mutations []etcdstore.Mutation

	task = cloneTaskRecord(task)
	if task.ID != intent.TaskID || task.OperationID != intent.OperationID ||
		task.Target != record.Desired.ID || task.Status != taskjournal.TaskStatusPending {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "Route mutation Task does not match its intent",
		)
	}
	taskMarker := routeMutationTaskMarker(directMarker, task)
	task.idempotencyMarker = cloneIdempotencyLocator(&taskMarker.Locator)
	if task.IdempotencyKey == "" {
		task.IdempotencyKey = directMarker.Locator.Key
	}
	configuration, err := prepareConfigurationTaskPublication(
		ctx, repository.store, task, publicationRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	task = configuration.task
	defer configuration.clear()
	defer func() { publicationErr = configuration.finish(ctx, repository.store, publicationErr) }()

	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := idempotencyrecord.EncodeTaskReference(task.ID)
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
		etcdstore.Condition{Key: taskjournal.TaskStorageKey(task.ID)},
		etcdstore.Condition{Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID)},
		etcdstore.Condition{Key: taskjournal.TaskActiveOperationKey(task.OperationID)},
		etcdstore.Condition{Key: taskjournal.TaskQueueKey(task.Executor, task.ID)},
		etcdstore.Condition{Key: routeMutationIntentKey(task.ID)},
	)
	mutations = append(mutations,
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: taskValue},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: taskjournal.TaskActiveOperationKey(task.OperationID), Value: reference},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: taskjournal.TaskQueueKey(task.Executor, task.ID), Value: reference},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: routeMutationIntentKey(task.ID), Value: intentValue},
	)
	conditions = append(conditions, publication.conditions...)
	conditions, err = routeHeadTargetConditions(conditions, servicerecord.ServiceDesiredCondition(target))
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	mutations = append(mutations, publication.mutations...)
	mutations = append(mutations, etcdstore.Mutation{
		Type: etcdstore.MutationPut, Key: componentTaskActiveEnvironmentKey(environment.Record.ID), Value: []byte(task.ID),
	})

	taskTenant, err := loadTaskInitiationTenant(ctx, repository.store, project)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(taskTenant, project, environment, taskjournal.TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	baseConditions := append([]etcdstore.Condition(nil), conditions...)
	classify := func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != len(baseConditions) {
			return errs.New(errs.KindInternal, "Route mutation compare evidence is incomplete")
		}
		for index, condition := range baseConditions {
			if !conditionMatchesRead(condition, values[index]) {
				return errs.New(errs.KindStateConflict, "Route mutation desired head changed")
			}
		}
		return errs.New(errs.KindStateConflict, "Route mutation state changed")
	}
	conditions, mutations, classify, err = configuration.bind(conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(task, initiation, conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return applyRouteMutationTaskMarkers(ctx, repository.store, plan, taskMarker, directMarker)
}

// A Blueprint-owned target Service and its Route may share the same desired
// head. One exact comparison fences both; different revisions are a conflict.
func routeHeadTargetConditions(conditions []etcdstore.Condition, target etcdstore.Condition) ([]etcdstore.Condition, error) {
	for _, condition := range conditions {
		if condition.Key != target.Key {
			continue
		}
		if condition != target {
			return nil, errs.New(errs.KindStateConflict, "Route target desired head changed")
		}
		return conditions, nil
	}
	return append(conditions, target), nil
}

func validateRouteTaskAcceptanceMarker(marker idempotencyrecord.IdempotencyMarker, environmentID string) error {
	if marker.Kind != idempotencyrecord.IdempotencyMarkerDirect || marker.State != idempotencyrecord.IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment || marker.Locator.ScopeID != environmentID {
		return errs.New(errs.KindValidationFailed, "Route Task acceptance marker is invalid")
	}
	return idempotencyrecord.ValidateIdempotencyMarker(marker)
}

func routeMutationTaskMarker(directMarker idempotencyrecord.IdempotencyMarker, task TaskRecord) idempotencyrecord.IdempotencyMarker {
	body, _ := json.Marshal(struct {
		TaskID string `json:"task_id"`
	}{TaskID: task.ID})
	return idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: idempotencyrecord.IdempotencyLocator{
			ScopeKind: directMarker.Locator.ScopeKind, ScopeID: directMarker.Locator.ScopeID,
			Method: http.MethodPost, Route: "/routes/{id}/reconcile", Key: task.ID,
		},
		Intent: directMarker.Intent,
		Response: idempotencyrecord.IdempotencyResponse{
			Status: http.StatusAccepted, ContentKind: "application/json", Body: body,
		},
		TaskID: task.ID, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
	}
}

func applyRouteMutationTaskMarkers(
	ctx context.Context,
	store idempotencyRepositoryStore,
	plan *idempotencyMutationPlan,
	taskMarker idempotencyrecord.IdempotencyMarker,
	directMarker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if plan == nil || plan.markerKind != idempotencyrecord.IdempotencyMarkerTask {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "Route mutation Task plan is invalid")
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(taskMarker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(directMarker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	taskKeyValue, err := idempotencyrecord.IdempotencyMarkerKey(taskMarker.Locator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	directKeyValue, err := idempotencyrecord.IdempotencyMarkerKey(directMarker.Locator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	taskValue, err := idempotencyrecord.EncodeIdempotencyMarker(taskMarker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	directValue, err := idempotencyrecord.EncodeIdempotencyMarker(directMarker)
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
		etcdstore.Condition{Key: taskKeyValue},
		etcdstore.Condition{Key: directKeyValue},
	)
	mutations = append(mutations,
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: taskKeyValue, Value: taskValue},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: directKeyValue, Value: directValue},
	)
	if !directMarker.RetainUntil.IsZero() {
		retentionKey, err := idempotencyrecord.IdempotencyRetentionKey(directKeyValue, directMarker.RetainUntil)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		retentionValue, err := json.Marshal(idempotencyrecord.RetentionReferenceJSON{Schema: 1, MarkerKey: directKeyValue})
		if err != nil {
			return IdempotencyTransactionResult{}, errs.Wrap(errs.KindInternal, err)
		}
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: retentionKey, Value: retentionValue})
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
	defer etcdstore.ClearValues(result.FailureReads)
	if result.Succeeded {
		return IdempotencyTransactionResult{kind: idempotencyTransactionApplied, revision: result.Revision}, nil
	}
	if len(result.FailureReads) != len(conditions) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"Route mutation compare evidence is incomplete",
		)
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
