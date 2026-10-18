package etcd

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	scriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	scriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	scriptsourcepublication "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	scriptsourcequeries "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// PublishScriptWork reserves the complete immutable source set before
// atomically exposing the manual Task and its source root. The source module
// alone changes membership counts, including the Script-wide body aggregate.
func (repository *ScriptRepository) PublishScriptWork(
	ctx context.Context,
	sources scriptsourcequeries.ScriptExecutionSources,
	execution scriptexecutions.ScriptExecutionRecord,
	task workexecution.Task,
	item workexecution.Item,
	marker idempotencyrecord.IdempotencyMarker,
) (result IdempotencyTransactionResult, err error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return result, err
	}
	if repository == nil || repository.store == nil || task.Validate() != nil || item.Validate() != nil ||
		task.Action != workexecution.ActionScript || task.ExpectedItems != 1 ||
		task.Actor != workexecution.OperatorActor || item.TaskID != task.ID ||
		item.Operation != workexecution.Script || item.Placement.Executor != workexecution.Agent ||
		item.TargetID != execution.ScriptID || task.TargetID != execution.ScriptID ||
		task.Scope.EnvironmentID != sources.Environment.Record.ID ||
		task.Scope.ProjectID != sources.Project.Record.ID || task.Scope.TenantID != sources.Project.Record.TenantID ||
		item.Input == nil || *item.Input != execution.Input || *item.Input != task.Intent {
		return result, errs.New(errs.KindValidationFailed, "manual Script publication is invalid")
	}
	if execution.ScriptSetGeneration != sources.Script.Record.ScriptSetGeneration {
		return result, errs.New(errs.KindStateConflict, "Script source generation differs from captured input")
	}
	if err := validateScriptExecutionSources(sources, execution); err != nil {
		return result, err
	}
	if scriptexecutions.ValidateScriptExecutionRecord(execution) != nil ||
		execution.State != scriptexecutions.ScriptExecutionNotStarted ||
		!execution.ActiveReference ||
		execution.SourceMembershipCount != 0 {
		return result, errs.New(errs.KindValidationFailed, "new Script execution record is invalid")
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask ||
		marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != task.ID ||
		!marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) ||
		task.ID != execution.CurrentTaskID ||
		item.ID != execution.WorkItemID {
		return result, errs.New(errs.KindValidationFailed, "Script execution marker does not match its Task")
	}
	initiation, err := newEnvironmentTaskInitiation(
		&sources.Tenant,
		sources.Project,
		sources.Environment,
		taskjournal.TaskActorOperator,
	)
	if err != nil {
		return result, err
	}
	work, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		return result, err
	}
	defer etcdstore.ClearMutationValues(work.Mutations)
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
	authority, err := scriptsourcepublication.NewAuthority(repository.store)
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
		if cleanupErr := authority.Abandon(cleanupCtx, execution.OperationID, members); cleanupErr != nil && err == nil {
			err = cleanupErr
		}
	}()
	prepared, err := authority.Prepare(ctx, execution.OperationID, members)
	if err != nil {
		return result, err
	}
	fragment, err := authority.FinalPublicationFragment(ctx, prepared)
	if err != nil {
		return result, err
	}
	defer fragment.Clear()
	workInput := *item.Input
	execution.WorkInput = &workInput
	execution.SourceMembershipCount, execution.SourceMembershipSHA256 = prepared.MembershipCount(), prepared.MembershipSHA256()
	primary, err := preparedScriptPrimary(ctx, repository.store, sources)
	if err != nil {
		return result, err
	}
	conditions := []etcdstore.Condition{
		{Key: scriptexecutions.ScriptExecutionKey(execution.ID)},
		{Key: scriptexecutions.WorkKey(item.ID)},
		primary,
		{Key: scriptrecord.ScriptSetBodyGenerationKey(execution.EnvironmentID, execution.ScriptSetGeneration,
			execution.ScriptID, execution.ScriptGeneration), ModRevision: sources.BodyGeneration.Revision},
		{Key: scriptrecord.ScriptSetActiveKey(execution.EnvironmentID), ModRevision: sources.ScriptSet.Revision},
		{Key: releases.ReleaseProjectionKey(execution.ServiceID), ModRevision: sources.Release.ProjectionRevision},
		{Key: releases.ReleaseIntentStagingKey("", execution.ReleaseID), ModRevision: sources.Release.IntentRevision},
		{
			Key:         releases.ReleaseRenderInputStagingKey("", execution.ReleaseID),
			ModRevision: sources.RenderInput.Revision,
		},
		{Key: scriptexecutions.ScriptRunnerSnapshotKey(execution.SnapshotID), ModRevision: snapshotRevision},
	}
	sourceConditions, err := manualScriptSourceConditions(sources)
	if err != nil {
		return result, err
	}
	conditions = append(conditions, sourceConditions...)
	conditions = append(conditions, fragment.Conditions()...)
	executionValue, err := recordcodec.Encode("script-execution", execution)
	if err != nil {
		return result, err
	}
	defer clear(executionValue)
	bindingValue, err := recordcodec.Encode("script-work", scriptexecutions.WorkBinding{
		TaskID: task.ID, WorkItemID: item.ID, ExecutionID: execution.ID, Input: execution.Input,
	})
	if err != nil {
		return result, err
	}
	defer clear(bindingValue)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: scriptexecutions.ScriptExecutionKey(execution.ID), Value: executionValue},
		{Type: etcdstore.MutationPut, Key: scriptexecutions.WorkKey(item.ID), Value: bindingValue},
	}
	mutations = append(mutations, fragment.Mutations()...)
	conditions = append(conditions, work.Conditions...)
	mutations = append(mutations, work.Mutations...)
	conditions, classify, err := prepareTaskInitiationFences(initiation, conditions,
		classifyScriptExecutionPublication(conditions))
	if err != nil {
		return result, err
	}
	plan, err := newIdempotencyMutationPlanForMarker(idempotencyrecord.IdempotencyMarkerTask,
		conditions, mutations, classify)
	if err != nil {
		return result, err
	}
	idempotency, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return result, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func (repository *ScriptRepository) prepareManualScriptSnapshot(
	ctx context.Context,
	execution scriptexecutions.ScriptExecutionRecord,
) (int64, error) {
	value, err := recordcodec.Encode("script-runner-snapshot", scriptsourceevidence.StoredScriptRunnerSnapshot{
		ExecutionID: execution.ID, SnapshotID: execution.SnapshotID, SHA256: execution.SnapshotSHA256, Payload: execution.Snapshot,
	})
	if err != nil {
		return 0, err
	}
	defer clear(value)
	key := scriptexecutions.ScriptRunnerSnapshotKey(execution.SnapshotID)
	result, err := repository.store.Transact(
		ctx,
		[]etcdstore.Condition{{Key: key}},
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: key, Value: value}},
	)
	if err != nil {
		return 0, err
	}
	defer etcdstore.ClearValues(result.FailureReads)
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
func manualScriptSourceConditions(sources scriptsourcequeries.ScriptExecutionSources) ([]etcdstore.Condition, error) {
	conditions := []etcdstore.Condition{servicerecord.ServiceDesiredCondition(sources.Service)}
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

func classifyScriptExecutionPublication(conditions []etcdstore.Condition) idempotencyPlanClassifier {
	return func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != len(conditions) {
			return errs.New(errs.KindInternal, "Script execution publication compare evidence is incomplete")
		}
		for index, condition := range conditions {
			if etcdstore.RevisionOf(values[index]) != condition.ModRevision {
				return errs.New(errs.KindStateConflict, "Script input or work identity changed before publication")
			}
		}
		return errs.New(errs.KindStateConflict, "Script execution source changed before publication")
	}
}
