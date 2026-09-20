package etcd

import (
	"bytes"
	"context"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// PublishExecutionWithTask reserves the complete immutable source set before
// atomically exposing the manual Task and its source root. The source module
// alone changes membership counts, including the Script-wide body aggregate.
func (repository *ScriptRepository) PublishExecutionWithTask(
	ctx context.Context,
	sources ScriptExecutionSources,
	execution ScriptExecutionRecord,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (result IdempotencyTransactionResult, err error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return result, err
	}
	if repository == nil || repository.store == nil || task.Type != TaskScript {
		return result, errs.New(errs.KindValidationFailed, "manual Script publication is invalid")
	}
	execution.ScriptSetGeneration = sources.Script.Record.ScriptSetGeneration
	if err := validateScriptExecutionSources(sources, execution); err != nil {
		return result, err
	}
	if validateScriptExecutionRecord(execution) != nil || execution.State != ScriptExecutionNotStarted ||
		!execution.ActiveReference || execution.SourceMembershipCount != 0 {
		return result, errs.New(errs.KindValidationFailed, "new Script execution record is invalid")
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask || marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != task.ID || !marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) || task.ID != execution.CurrentTaskID ||
		task.OperationID != execution.OperationID || len(task.Steps) != 1 || task.Steps[0].ID != execution.StepID {
		return result, errs.New(errs.KindValidationFailed, "Script execution marker does not match its Task")
	}
	initiation, err := newEnvironmentTaskInitiation(
		&sources.Tenant,
		sources.Project,
		sources.Environment,
		TaskActorOperator,
	)
	if err != nil {
		return result, err
	}
	task = cloneTaskRecord(task)
	if task.IdempotencyKey == "" {
		task.IdempotencyKey = marker.Locator.Key
	}
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := validateTaskInitiation(task, initiation, true); err != nil {
		return result, err
	}
	if err := validateTaskRecord(task); err != nil {
		return result, err
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return result, err
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}
	snapshotRevision, err := repository.prepareManualScriptSnapshot(ctx, execution)
	if err != nil {
		return result, err
	}
	members, err := repository.manualScriptSourceMembers(ctx, sources, execution, snapshotRevision)
	if err != nil {
		return result, err
	}
	defer clearScriptSourcePreparationMembers(members)
	authority, err := newScriptSourceReferenceAuthority(repository.store)
	if err != nil {
		return result, err
	}
	defer func() {
		if result.kind == idempotencyTransactionApplied {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		// Abandon fences the descriptor and refuses an active root. This also
		// protects a publication whose commit succeeded but response was lost.
		if cleanupErr := authority.Abandon(cleanupCtx, task.OperationID, members); cleanupErr != nil && err == nil {
			err = cleanupErr
		}
	}()
	prepared, err := authority.Prepare(ctx, task.OperationID, members)
	if err != nil {
		return result, err
	}
	fragment, err := authority.FinalPublicationFragment(ctx, prepared)
	if err != nil {
		return result, err
	}
	defer fragment.Clear()
	execution.SourceMembershipCount, execution.SourceMembershipSHA256 = prepared.membershipCount, prepared.membershipSHA256
	primary, err := preparedScriptPrimary(ctx, repository.store, sources)
	if err != nil {
		return result, err
	}
	conditions := []etcdstore.Condition{
		{Key: scriptExecutionKey(execution.ID)},
		{Key: taskKey(task.ID)},
		{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)},
		{Key: taskQueueKey(task.Executor, task.ID)},
		primary,
		{Key: scriptrecord.ScriptSetBodyGenerationKey(execution.EnvironmentID, execution.ScriptSetGeneration,
			execution.ScriptID, execution.ScriptGeneration), ModRevision: sources.BodyGeneration.Revision},
		{Key: scriptrecord.ScriptSetActiveKey(execution.EnvironmentID), ModRevision: sources.ScriptSet.Revision},
		{Key: releaseProjectionKey(execution.ServiceID), ModRevision: sources.Release.ProjectionRevision},
		{Key: releaseIntentStagingKey("", execution.ReleaseID), ModRevision: sources.Release.IntentRevision},
		{Key: releaseRenderInputStagingKey("", execution.ReleaseID), ModRevision: sources.RenderInput.Revision},
		{Key: scriptRunnerSnapshotKey(execution.SnapshotID), ModRevision: snapshotRevision},
	}
	sourceConditions, err := manualScriptSourceConditions(sources)
	if err != nil {
		return result, err
	}
	conditions = append(conditions, sourceConditions...)
	conditions = append(conditions, fragment.conditions...)
	executionValue, err := recordcodec.Encode("script-execution", execution)
	if err != nil {
		return result, err
	}
	defer clear(executionValue)
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		return result, err
	}
	defer clear(taskValue)
	taskReference, err := idempotencyrecord.EncodeTaskReference(task.ID)
	if err != nil {
		return result, err
	}
	defer clear(taskReference)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: scriptExecutionKey(execution.ID), Value: executionValue},
		{Type: etcdstore.MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: taskReference},
		{Type: etcdstore.MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: taskReference},
		{Type: etcdstore.MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: taskReference},
	}
	mutations = append(mutations, fragment.mutations...)
	plan, err := newTaskIdempotencyMutationPlan(task, initiation, conditions, mutations,
		classifyScriptExecutionPublication(len(conditions)))
	if err != nil {
		return result, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return result, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func (repository *ScriptRepository) prepareManualScriptSnapshot(
	ctx context.Context,
	execution ScriptExecutionRecord,
) (int64, error) {
	value, err := recordcodec.Encode("script-runner-snapshot", storedScriptRunnerSnapshot{
		ExecutionID: execution.ID, SnapshotID: execution.SnapshotID, SHA256: execution.SnapshotSHA256, Payload: execution.Snapshot,
	})
	if err != nil {
		return 0, err
	}
	defer clear(value)
	key := scriptRunnerSnapshotKey(execution.SnapshotID)
	result, err := repository.store.Transact(
		ctx,
		[]etcdstore.Condition{{Key: key}},
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: key, Value: value}},
	)
	if err != nil {
		return 0, err
	}
	defer clearKeyValues(result.FailureReads)
	if result.Succeeded {
		return result.Revision, nil
	}
	if len(result.FailureReads) != 1 || result.FailureReads[0] == nil ||
		!bytes.Equal(result.FailureReads[0].Value, value) {
		return 0, errs.New(errs.KindStateConflict, "manual Script snapshot identity is occupied")
	}
	return result.FailureReads[0].ModRevision, nil
}

// manualScriptSourceConditions fences the desired sources captured for a manual
// execution. Release image and render-input fences are owned by its publication.
func manualScriptSourceConditions(sources ScriptExecutionSources) ([]etcdstore.Condition, error) {
	conditions := []etcdstore.Condition{serviceDesiredCondition(sources.Service)}
	byKey := map[string]etcdstore.Condition{conditions[0].Key: conditions[0]}
	for _, condition := range scriptExecutionProjectionConditions(sources) {
		if existing, found := byKey[condition.Key]; found {
			if existing != condition {
				return nil, errs.New(errs.KindStateConflict, "Script execution source revisions disagree")
			}
			continue
		}
		byKey[condition.Key] = condition
		conditions = append(conditions, condition)
	}
	return conditions, nil
}

func classifyScriptExecutionPublication(expected int) idempotencyPlanClassifier {
	return func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != expected {
			return errs.New(errs.KindInternal, "Script execution publication compare evidence is incomplete")
		}
		if values[3] != nil {
			return errs.New(errs.KindStateConflict, "Script operation already has an active Task")
		}
		for index, value := range values {
			if index < 5 && value != nil {
				return errs.New(errs.KindInternal, "Script execution identity collided with durable state")
			}
		}
		return errs.New(errs.KindStateConflict, "Script execution source changed before publication")
	}
}
