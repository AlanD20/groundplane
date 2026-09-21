package etcd

import (
	"context"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PublishPlatformDNSResolverTask publishes the first Platform-owned resolver
// Agent Task together with its pinned render input and projection. It is kept
// repository-private in spirit: callers must supply the already-rendered,
// typed input and cannot create a public DNS operation.
func (repository *TaskRepository) PublishPlatformDNSResolverTask(
	ctx context.Context,
	current etcdstore.Versioned[componentrecord.Record],
	projection HostResolutionProjectionRecord,
	task TaskRecord,
	renderInput PlatformComponentTaskRenderInput,
	marker idempotencyrecord.IdempotencyMarker,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if err := validatePlatformComponentRecord(current.Record); err != nil {
		return err
	}
	if task.Owner != PlatformTaskOwner() || task.Actor != TaskActorSystem || task.Executor != TaskExecutorAgent ||
		task.Type != TaskUpdate || task.Status != TaskStatusPending || task.Target != current.Record.Desired.ID ||
		task.Params[TaskResourceKindParam] != TaskResourceComponent ||
		task.Params[TaskAutomaticReconcileParam] != "true" || len(task.Params) != 2 {
		return errs.New(errs.KindValidationFailed, "platform DNS resolver Task shape is invalid")
	}
	if task.PlanID != renderInput.PlanID || task.ID != renderInput.TaskID || task.Target != renderInput.ComponentID ||
		task.PlanHash != renderInput.ExecutionPlanSHA256 ||
		renderInput.HostResolutionInputRevision != projection.InputRevision {
		return errs.New(errs.KindValidationFailed, "platform DNS resolver render input is not pinned")
	}
	desiredDigest, err := PlatformComponentDesiredDigest(current.Record)
	if err != nil {
		return err
	}
	if renderInput.DesiredSHA256 != desiredDigest {
		return errs.New(errs.KindValidationFailed, "platform DNS resolver desired state is not pinned")
	}
	task, err = bindPlatformComponentTaskMarker(task, marker)
	if err != nil {
		return err
	}
	task.Params[TaskPlatformComponentDesiredSHA256Param] = desiredDigest
	if err := validateTaskRecord(task); err != nil {
		return err
	}
	if err := validatePlatformComponentTaskRenderInput(renderInput); err != nil {
		return err
	}
	projectionValue, err := encodeHostResolutionProjectionRecord(projection)
	if err != nil {
		return err
	}
	defer clear(projectionValue)
	renderValue, err := encodePlatformComponentTaskRenderInput(renderInput)
	if err != nil {
		return err
	}
	defer clear(renderValue)
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		return err
	}
	defer clear(taskValue)
	reference, err := idempotencyrecord.EncodeTaskReference(task.ID)
	if err != nil {
		return err
	}
	defer clear(reference)
	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			platformComponentOwnerKey(task.Target),
			platformComponentKindKey(current.Record.Desired.Kind),
			platformComponentBootstrapKey(task.Target),
		},
		Revision: current.ReadRevision,
	})
	if err != nil {
		return err
	}
	if indexes == nil || len(indexes.Values) != 3 || indexes.Values[0] == nil || indexes.Values[1] == nil {
		return errs.New(errs.KindInternal, "platform Component indexes are missing")
	}
	if indexes.Values[2] == nil || indexes.Values[2].ModRevision != current.Revision ||
		string(indexes.Values[2].Value) != task.Target {
		return errs.New(errs.KindStateConflict, "platform Component bootstrap provenance is unavailable")
	}
	conditions := []etcdstore.Condition{
		{Key: componentrecord.RecordKey(task.Target), ModRevision: current.Revision},
		{Key: platformComponentOwnerKey(task.Target), ModRevision: indexes.Values[0].ModRevision},
		{Key: platformComponentKindKey(current.Record.Desired.Kind), ModRevision: indexes.Values[1].ModRevision},
		{Key: platformComponentBootstrapKey(task.Target), ModRevision: indexes.Values[2].ModRevision},
		{Key: hostResolutionProjectionKey},
		{Key: platformComponentTaskActiveKey(task.Target)},
		{Key: platformComponentTaskRenderInputKey(task.PlanID)},
		{Key: taskKey(task.ID)}, {Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)}, {Key: taskQueueKey(task.Executor, task.ID)},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: hostResolutionProjectionKey, Value: projectionValue},
		{Type: etcdstore.MutationPut, Key: platformComponentTaskRenderInputKey(task.PlanID), Value: renderValue},
		{Type: etcdstore.MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: platformComponentTaskActiveKey(task.Target), Value: []byte(task.ID)},
		{Type: etcdstore.MutationDelete, Key: platformComponentBootstrapKey(task.Target)},
	}
	initiation, err := newPlatformTaskInitiation(TaskActorSystem)
	if err != nil {
		return err
	}
	plan, err := newTaskIdempotencyMutationPlan(task, initiation, conditions, mutations,
		func(_ int64, _ []*etcdstore.KeyValue) error {
			return errs.New(errs.KindStateConflict, "platform DNS resolver Task publication conflicted")
		})
	if err != nil {
		return err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return err
	}
	result, err := idempotency.Apply(ctx, marker, plan)
	if err != nil {
		return err
	}
	return requireAppliedPlatformComponentTask(result)
}

// preparePlatformDNSResolverTaskContribution seals one automatic Task into
// the caller's transaction. A nil active value creates a new fence; a
// non-nil active value replaces the terminal Task's fence with its successor.
func (repository *TaskRepository) preparePlatformDNSResolverTaskContribution(
	ctx context.Context,
	current etcdstore.Versioned[componentrecord.Record],
	projection HostResolutionProjectionRecord,
	task TaskRecord,
	active *etcdstore.KeyValue,
	sealedPredecessor *TaskRecord,
	priorObservation *ComponentObservationRecord,
	baseConditions []etcdstore.Condition,
) (hostResolutionReconciliationChange, error) {
	if repository.platformResolverTaskPreparer == nil {
		return hostResolutionReconciliationChange{}, errs.New(
			errs.KindInternal, "platform resolver Task preparer is not configured",
		)
	}
	if active != nil && (active.Key != platformComponentTaskActiveKey(current.Record.Desired.ID) ||
		active.ModRevision <= 0 || string(active.Value) == "") {
		return hostResolutionReconciliationChange{}, errs.New(
			errs.KindInternal,
			"platform resolver active fence is corrupt",
		)
	}
	desiredSHA256, err := PlatformComponentDesiredDigest(current.Record)
	if err != nil {
		return hostResolutionReconciliationChange{}, err
	}
	task = cloneTaskRecord(task)
	if task.RetryOf != "" {
		return hostResolutionReconciliationChange{}, errs.New(
			errs.KindValidationFailed,
			"automatic platform resolver successor cannot be a retry",
		)
	}
	if task.Params == nil {
		task.Params = make(map[string]string, 2)
	}
	task.Params[TaskPlatformComponentDesiredSHA256Param] = desiredSHA256
	ensureService := len(current.Record.Runtime.GeneratedServices) == 0 || !current.Record.Runtime.Healthy
	task.Steps = platformResolverTaskSteps(PlatformComponentTaskRenderInput{EnsureService: ensureService})
	var finalLineage *platformResolverLiveLineage
	if sealedPredecessor != nil {
		if active == nil || string(active.Value) != sealedPredecessor.ID {
			return hostResolutionReconciliationChange{}, errs.New(
				errs.KindStateConflict,
				"platform resolver predecessor does not own the active fence",
			)
		}
		predecessorRead, readErr := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{taskKey(sealedPredecessor.ID)}, Revision: active.ModRevision,
		})
		if readErr != nil {
			return hostResolutionReconciliationChange{}, readErr
		}
		if predecessorRead == nil || len(predecessorRead.Values) != 1 || predecessorRead.Values[0] == nil {
			return hostResolutionReconciliationChange{}, errs.New(
				errs.KindStateConflict,
				"platform resolver predecessor Task is missing",
			)
		}
		predecessor, decodeErr := decodeTaskRecord(predecessorRead.Values[0].Value)
		if decodeErr != nil {
			return hostResolutionReconciliationChange{}, decodeErr
		}
		predecessorInputRead, readErr := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{platformComponentTaskRenderInputKey(predecessor.PlanID)}, Revision: active.ModRevision,
		})
		if readErr != nil {
			return hostResolutionReconciliationChange{}, readErr
		}
		if predecessorInputRead == nil || len(predecessorInputRead.Values) != 1 ||
			predecessorInputRead.Values[0] == nil {
			return hostResolutionReconciliationChange{}, errs.New(
				errs.KindStateConflict,
				"platform resolver predecessor render input is missing",
			)
		}
		predecessorInput, decodeErr := decodePlatformComponentTaskRenderInput(predecessorInputRead.Values[0].Value)
		if decodeErr != nil {
			return hostResolutionReconciliationChange{}, decodeErr
		}
		if sealedPredecessor == nil || predecessor.ID != sealedPredecessor.ID ||
			!platformResolverTaskInputBelongsToTask(predecessor, predecessorInput) ||
			predecessorInput.PlanID != predecessor.PlanID || predecessorInput.ComponentID != task.Target {
			return hostResolutionReconciliationChange{}, errs.New(
				errs.KindStateConflict,
				"platform resolver predecessor lineage is corrupt",
			)
		}
		if sealedPredecessor.ID != predecessor.ID ||
			sealedPredecessor.PlanID != predecessor.PlanID || sealedPredecessor.Target != predecessor.Target {
			return hostResolutionReconciliationChange{}, errs.New(
				errs.KindStateConflict,
				"platform resolver sealed predecessor is missing",
			)
		}
		lineage, proven := platformResolverFinalLiveLineage(*sealedPredecessor, predecessorInput)
		if !proven {
			return hostResolutionReconciliationChange{}, nil
		}
		finalLineage = &lineage
		priorObservation = platformResolverObservationForLineage(predecessorInput, lineage, priorObservation)
	}
	renderInput, err := repository.platformResolverTaskPreparer(ctx, current, projection, task, priorObservation)
	if err != nil {
		return hostResolutionReconciliationChange{}, err
	}
	if finalLineage != nil {
		renderInput.PriorObservationModRevision = finalLineage.priorObservationModRevision
		if renderInput.PriorObservationRevision != finalLineage.priorObservationRevision ||
			renderInput.PredecessorTaskID != finalLineage.predecessorTaskID ||
			renderInput.ExpectedPreviousArtifactSHA256 != finalLineage.expectedPreviousArtifactSHA256 ||
			renderInput.ExpectedPreviousArtifactID != finalLineage.expectedPreviousArtifactID ||
			renderInput.ExpectedPreviousGeneration != finalLineage.expectedPreviousGeneration {
			return hostResolutionReconciliationChange{}, errs.New(
				errs.KindStateConflict,
				"platform resolver successor predecessor authority changed during planning",
			)
		}
	}
	task.PlanHash = renderInput.ExecutionPlanSHA256
	if err := validateTaskRecord(task); err != nil {
		return hostResolutionReconciliationChange{}, err
	}
	if task.Params[TaskPlatformComponentDesiredSHA256Param] != renderInput.DesiredSHA256 ||
		renderInput.HostResolutionInputRevision != projection.InputRevision ||
		renderInput.HostResolutionSHA256 != projection.InputSHA256 {
		return hostResolutionReconciliationChange{}, errs.New(
			errs.KindStateConflict,
			"platform resolver render input is not pinned",
		)
	}
	if err := validatePlatformComponentTaskRenderInput(renderInput); err != nil {
		return hostResolutionReconciliationChange{}, err
	}
	if renderInput.TaskID != task.ID || renderInput.PlanID != task.PlanID || renderInput.ComponentID != task.Target {
		return hostResolutionReconciliationChange{}, errs.New(
			errs.KindStateConflict,
			"platform resolver render input identity changed",
		)
	}
	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			platformComponentOwnerKey(current.Record.Desired.ID),
			platformComponentKindKey(current.Record.Desired.Kind),
		},
		Revision: current.ReadRevision,
	})
	if err != nil {
		return hostResolutionReconciliationChange{}, err
	}
	if indexes == nil || indexes.ReadRevision != current.ReadRevision || len(indexes.Values) != 2 ||
		indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(
			indexes.Values[0].Value,
		) != current.Record.Desired.ID || string(indexes.Values[1].Value) != current.Record.Desired.ID {
		return hostResolutionReconciliationChange{}, errs.New(
			errs.KindInternal,
			"platform resolver Component indexes are missing",
		)
	}
	renderValue, err := encodePlatformComponentTaskRenderInput(renderInput)
	if err != nil {
		return hostResolutionReconciliationChange{}, err
	}
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		clear(renderValue)
		return hostResolutionReconciliationChange{}, err
	}
	reference, err := idempotencyrecord.EncodeTaskReference(task.ID)
	if err != nil {
		clear(renderValue)
		clear(taskValue)
		return hostResolutionReconciliationChange{}, err
	}
	conditions := append([]etcdstore.Condition(nil), baseConditions...)
	for _, condition := range []etcdstore.Condition{
		{Key: componentrecord.RecordKey(current.Record.Desired.ID), ModRevision: current.Revision},
		{Key: platformComponentOwnerKey(current.Record.Desired.ID), ModRevision: indexes.Values[0].ModRevision},
		{Key: platformComponentKindKey(current.Record.Desired.Kind), ModRevision: indexes.Values[1].ModRevision},
		{Key: platformComponentTaskRenderInputKey(task.PlanID)},
		{Key: taskKey(task.ID)}, {Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)}, {Key: taskQueueKey(task.Executor, task.ID)},
		{Key: taskWorkspacePlatformIndexKey(task.ID)},
	} {
		conditions = appendHostResolutionCondition(conditions, condition)
	}
	activeCondition := etcdstore.Condition{Key: platformComponentTaskActiveKey(current.Record.Desired.ID)}
	if active != nil {
		activeCondition.ModRevision = active.ModRevision
	}
	conditions = appendHostResolutionCondition(conditions, activeCondition)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: platformComponentTaskRenderInputKey(task.PlanID), Value: renderValue},
		{Type: etcdstore.MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskWorkspacePlatformIndexKey(task.ID), Value: []byte(task.ID)},
		{Type: etcdstore.MutationPut, Key: platformComponentTaskActiveKey(current.Record.Desired.ID), Value: []byte(task.ID)},
	}
	return hostResolutionReconciliationChange{
		applies: true, conditions: conditions, mutations: mutations,
		values: [][]byte{renderValue, taskValue, reference, []byte(task.ID)},
	}, nil
}
