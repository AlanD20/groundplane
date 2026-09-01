package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/proto"
)

const (
	ScriptExecutionIDParam = "script_execution_id"
	ScriptGenerationParam  = "script_generation"

	scriptExecutionPrefix           = "/v1/script-executions/"
	scriptRunnerSnapshotPrefix      = "/v1/script-runner-snapshots/"
	scriptBodyForwardRefSegment     = "/references/"
	scriptBodyReverseRefSegment     = "/body-reference"
	maximumReleaseHookTerminalBatch = 8
)

type ScriptExecutionState string

const (
	ScriptExecutionNotStarted       ScriptExecutionState = "not_started"
	ScriptExecutionStartAuthorized  ScriptExecutionState = "start_authorized"
	ScriptExecutionBodyPrepared     ScriptExecutionState = "body_prepared"
	ScriptExecutionContainerCreated ScriptExecutionState = "container_created"
	ScriptExecutionOutcomeRecorded  ScriptExecutionState = "outcome_recorded"
	ScriptExecutionCleanupProven    ScriptExecutionState = "cleanup_proven"
)

// ScriptExecutionRecord is the durable recovery authority for one one-off
// Script container. Plan and Snapshot contain no Script body or secret bytes.
type ScriptExecutionRecord struct {
	ID                     string                          `json:"id"`
	SnapshotID             string                          `json:"snapshot_id"`
	OperationID            string                          `json:"operation_id"`
	CurrentTaskID          string                          `json:"current_task_id"`
	AssignmentID           string                          `json:"assignment_id,omitempty"`
	StepID                 string                          `json:"step_id"`
	ScriptID               string                          `json:"script_id"`
	ScriptGeneration       uint64                          `json:"script_generation"`
	ScriptSetGeneration    string                          `json:"script_set_generation"`
	EnvironmentID          string                          `json:"environment_id"`
	ServiceID              string                          `json:"service_id"`
	ReleaseID              string                          `json:"release_id"`
	RenderGeneration       uint64                          `json:"render_generation"`
	PlanHash               string                          `json:"plan_hash"`
	SnapshotSHA256         string                          `json:"snapshot_sha256"`
	BodySHA256             string                          `json:"body_sha256"`
	RunnerProjectionSHA256 string                          `json:"runner_projection_sha256"`
	Plan                   []byte                          `json:"plan"`
	Snapshot               []byte                          `json:"snapshot"`
	State                  ScriptExecutionState            `json:"state"`
	StartAuthorized        bool                            `json:"start_authorized"`
	BodyPrepared           *ScriptBodyPreparedEvidence     `json:"body_prepared,omitempty"`
	ContainerCreated       *ScriptContainerCreatedEvidence `json:"container_created,omitempty"`
	Outcome                *ScriptOutcomeEvidence          `json:"outcome,omitempty"`
	Cleanup                *ScriptCleanupEvidence          `json:"cleanup,omitempty"`
	LastCheckpointSHA256   string                          `json:"last_checkpoint_sha256,omitempty"`
	ReconciliationRequired bool                            `json:"reconciliation_required"`
	ActiveReference        bool                            `json:"active_reference"`
	CreatedAt              time.Time                       `json:"created_at"`
	UpdatedAt              time.Time                       `json:"updated_at"`
}

type releaseHookExecutionStep struct {
	stepID      string
	executionID string
}

type releaseHookExecutionRetryTransfer struct {
	conditions []Condition
	mutations  []Mutation
}

func (transfer *releaseHookExecutionRetryTransfer) clear() {
	if transfer == nil {
		return
	}
	clearMutations(transfer.mutations)
	transfer.conditions = nil
	transfer.mutations = nil
}

// finalizeReleaseHookExecutionBatch releases up to eight hook execution
// references after a non-recoverable parent release terminal result. A skipped
// hook retains its not-started checkpoint as audit evidence but no longer pins
// its Script generation. A run hook is released only after cleanup is proven.
func (repository *TaskRepository) finalizeReleaseHookExecutionBatch(
	ctx context.Context,
	task TaskRecord,
	terminalAt time.Time,
	revision int64,
) (bool, error) {
	steps, err := releaseHookExecutionSteps(task)
	if err != nil || len(steps) == 0 {
		return false, err
	}
	keys := make([]string, len(steps))
	for index, step := range steps {
		keys[index] = scriptExecutionKey(step.executionID)
	}
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(steps) {
		return false, corruptReleaseRecord()
	}
	type activeHook struct {
		step   releaseHookExecutionStep
		record ScriptExecutionRecord
		value  *KeyValue
	}
	active := make([]activeHook, 0, maximumReleaseHookTerminalBatch)
	seenScripts := make(map[string]struct{}, len(steps))
	for index, step := range steps {
		value := read.Values[index]
		if value == nil {
			return false, corruptReleaseRecord()
		}
		record, decodeErr := decodeEnvelope[ScriptExecutionRecord](value.Value, "script-execution")
		if decodeErr != nil || validateScriptExecutionRecord(record) != nil || record.ID != step.executionID ||
			record.CurrentTaskID != task.ID || record.OperationID != task.OperationID ||
			record.StepID != step.stepID || record.PlanHash != task.PlanHash {
			return false, corruptReleaseRecord()
		}
		if _, duplicate := seenScripts[record.ScriptID]; duplicate {
			return false, corruptReleaseRecord()
		}
		seenScripts[record.ScriptID] = struct{}{}
		if !record.ActiveReference {
			continue
		}
		if record.State != ScriptExecutionNotStarted && record.State != ScriptExecutionCleanupProven {
			return false, errs.New(errs.KindStateConflict, "release hook execution has not reached a releasable checkpoint")
		}
		if !terminalAt.After(record.UpdatedAt) {
			return false, errs.New(errs.KindStateConflict, "release hook terminal timestamp is not monotonic")
		}
		if len(active) < maximumReleaseHookTerminalBatch {
			active = append(active, activeHook{step: step, record: record, value: value})
		}
	}
	if len(active) == 0 {
		return false, nil
	}
	detailKeys := make([]string, 0, len(active)*3)
	for _, hook := range active {
		detailKeys = append(detailKeys,
			scriptSetScriptKey(hook.record.EnvironmentID, hook.record.ScriptSetGeneration, hook.record.ScriptID),
			scriptSetBodyForwardReferenceKey(
				hook.record.EnvironmentID,
				hook.record.ScriptSetGeneration,
				hook.record.ScriptID,
				hook.record.ScriptGeneration,
				hook.record.ID,
			),
			scriptBodyReverseReferenceKey(hook.record.ID),
		)
	}
	details, err := repository.store.GetMany(ctx, GetManyRequest{Keys: detailKeys, Revision: revision})
	if err != nil {
		return false, err
	}
	if details == nil || details.ReadRevision != revision || len(details.Values) != len(detailKeys) {
		return false, corruptReleaseRecord()
	}
	conditions := make([]Condition, 0, len(active)*4)
	mutations := make([]Mutation, 0, len(active)*4)
	defer clearMutations(mutations)
	for index, hook := range active {
		values := details.Values[index*3 : index*3+3]
		if values[0] == nil || values[1] == nil || values[2] == nil {
			return false, corruptReleaseRecord()
		}
		script, decodeErr := decodeScriptRecord(values[0].Value)
		if decodeErr != nil || script.Desired.ID != hook.record.ScriptID ||
			script.EnvironmentID != hook.record.EnvironmentID ||
			script.ScriptSetGeneration != hook.record.ScriptSetGeneration || script.ActiveReferences == 0 {
			return false, corruptReleaseRecord()
		}
		if err := validateScriptBodyReference(values[1].Value, hook.record); err != nil ||
			validateScriptBodyReference(values[2].Value, hook.record) != nil {
			return false, corruptReleaseRecord()
		}
		next := hook.record
		next.ActiveReference = false
		next.UpdatedAt = terminalAt.UTC()
		if validateScriptExecutionRecord(next) != nil {
			return false, corruptReleaseRecord()
		}
		executionValue, encodeErr := encodeEnvelope("script-execution", next)
		if encodeErr != nil {
			return false, encodeErr
		}
		scriptValue, encodeErr := decrementStoredScriptActiveReferences(values[0].Value, hook.record.ScriptID)
		if encodeErr != nil {
			clear(executionValue)
			return false, encodeErr
		}
		conditions = append(conditions,
			Condition{Key: hook.value.Key, ModRevision: hook.value.ModRevision},
			Condition{Key: values[0].Key, ModRevision: values[0].ModRevision},
			Condition{Key: values[1].Key, ModRevision: values[1].ModRevision},
			Condition{Key: values[2].Key, ModRevision: values[2].ModRevision},
		)
		mutations = append(mutations,
			Mutation{Type: MutationPut, Key: hook.value.Key, Value: executionValue},
			Mutation{Type: MutationPut, Key: values[0].Key, Value: scriptValue},
			Mutation{Type: MutationDelete, Key: values[1].Key},
			Mutation{Type: MutationDelete, Key: values[2].Key},
		)
	}
	if len(conditions)+len(mutations) > maximumTransactionOperations {
		return false, errs.New(errs.KindInternal, "release hook terminal batch exceeds the transaction ceiling")
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return false, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return false, errs.New(errs.KindStateConflict, "release hook terminal evidence changed")
	}
	return true, nil
}

func (repository *TaskRepository) prepareReleaseHookExecutionRetryTransfer(
	ctx context.Context,
	source TaskRecord,
	retry TaskRecord,
	revision int64,
) (releaseHookExecutionRetryTransfer, error) {
	steps, err := releaseHookExecutionSteps(source)
	if err != nil || len(steps) == 0 {
		return releaseHookExecutionRetryTransfer{}, err
	}
	if retry.PlanID != source.PlanID || retry.PlanHash != source.PlanHash || len(retry.Steps) != len(source.Steps) {
		return releaseHookExecutionRetryTransfer{}, corruptReleaseRecord()
	}
	for index, step := range source.Steps {
		if retry.Steps[index].ID != step.ID ||
			retry.Params[ReleaseHookStepExecutionParam(step.ID)] != source.Params[ReleaseHookStepExecutionParam(step.ID)] {
			return releaseHookExecutionRetryTransfer{}, corruptReleaseRecord()
		}
	}
	keys := make([]string, len(steps))
	for index, step := range steps {
		keys[index] = scriptExecutionKey(step.executionID)
	}
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return releaseHookExecutionRetryTransfer{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(steps) {
		return releaseHookExecutionRetryTransfer{}, corruptReleaseRecord()
	}
	transfer := releaseHookExecutionRetryTransfer{
		conditions: make([]Condition, 0, len(steps)), mutations: make([]Mutation, 0, len(steps)),
	}
	seenExecutions := make(map[string]struct{}, len(steps))
	for index, step := range steps {
		value := read.Values[index]
		if value == nil {
			transfer.clear()
			return releaseHookExecutionRetryTransfer{}, corruptReleaseRecord()
		}
		record, decodeErr := decodeEnvelope[ScriptExecutionRecord](value.Value, "script-execution")
		if decodeErr != nil || validateScriptExecutionRecord(record) != nil || record.ID != step.executionID ||
			record.CurrentTaskID != source.ID || record.OperationID != source.OperationID ||
			record.StepID != step.stepID || record.PlanHash != source.PlanHash || !record.ActiveReference ||
			!retry.CreatedAt.After(record.UpdatedAt) {
			transfer.clear()
			return releaseHookExecutionRetryTransfer{}, corruptReleaseRecord()
		}
		if _, duplicate := seenExecutions[record.ID]; duplicate {
			transfer.clear()
			return releaseHookExecutionRetryTransfer{}, corruptReleaseRecord()
		}
		seenExecutions[record.ID] = struct{}{}
		next := record
		next.CurrentTaskID = retry.ID
		next.AssignmentID = ""
		next.UpdatedAt = retry.CreatedAt.UTC()
		encoded, encodeErr := encodeEnvelope("script-execution", next)
		if encodeErr != nil {
			transfer.clear()
			return releaseHookExecutionRetryTransfer{}, encodeErr
		}
		transfer.conditions = append(transfer.conditions, Condition{Key: value.Key, ModRevision: value.ModRevision})
		transfer.mutations = append(transfer.mutations, Mutation{Type: MutationPut, Key: value.Key, Value: encoded})
	}
	return transfer, nil
}

func releaseHookExecutionSteps(task TaskRecord) ([]releaseHookExecutionStep, error) {
	if task.Type != TaskDeploy && task.Type != TaskRollback {
		return nil, errs.New(errs.KindValidationFailed, "release hook execution Task is invalid")
	}
	steps := make([]releaseHookExecutionStep, 0)
	seen := make(map[string]struct{})
	for _, step := range task.Steps {
		executionID := task.Params[ReleaseHookStepExecutionParam(step.ID)]
		if executionID == "" {
			continue
		}
		if !validRawScriptExecutionID(executionID) {
			return nil, corruptReleaseRecord()
		}
		if _, duplicate := seen[executionID]; duplicate {
			return nil, corruptReleaseRecord()
		}
		seen[executionID] = struct{}{}
		steps = append(steps, releaseHookExecutionStep{stepID: step.ID, executionID: executionID})
	}
	return steps, nil
}

func validateScriptBodyReference(value []byte, execution ScriptExecutionRecord) error {
	reference, err := decodeEnvelope[struct {
		ExecutionID         string `json:"script_execution_id"`
		ScriptID            string `json:"script_id"`
		Generation          uint64 `json:"generation"`
		ScriptSetGeneration string `json:"script_set_generation"`
	}](value, "script-body-reference")
	if err != nil || reference.ExecutionID != execution.ID || reference.ScriptID != execution.ScriptID ||
		reference.Generation != execution.ScriptGeneration ||
		reference.ScriptSetGeneration != execution.ScriptSetGeneration {
		return corruptReleaseRecord()
	}
	return nil
}

func decrementStoredScriptActiveReferences(value []byte, scriptID string) ([]byte, error) {
	stored, err := decodeEnvelope[storedScriptRecord](value, "script")
	if err != nil || stored.Desired.ID != scriptID || stored.ActiveReferences == 0 {
		return nil, corruptReleaseRecord()
	}
	stored.ActiveReferences--
	encoded, err := encodeEnvelope("script", stored)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

func NewScriptExecutionRecord(
	task TaskRecord,
	plan *agentpb.ExecutionPlan,
	at time.Time,
) (ScriptExecutionRecord, error) {
	records, err := NewScriptExecutionRecords(task, plan, at)
	if err != nil {
		return ScriptExecutionRecord{}, err
	}
	if task.Type != TaskScript || len(records) != 1 {
		for index := range records {
			clear(records[index].Plan)
			clear(records[index].Snapshot)
		}
		return ScriptExecutionRecord{}, errs.New(errs.KindValidationFailed, "manual Script Task must own one execution")
	}
	return records[0], nil
}

// NewScriptExecutionRecords binds every RunScript step to one durable
// Controller-owned checkpoint record. Release hooks share their parent Task
// and sealed plan; they never create secondary Tasks.
func NewScriptExecutionRecords(
	task TaskRecord,
	plan *agentpb.ExecutionPlan,
	at time.Time,
) ([]ScriptExecutionRecord, error) {
	validated, err := executionplan.Validate(plan)
	if err != nil {
		return nil, err
	}
	releaseTask := task.Type == TaskDeploy || task.Type == TaskRollback
	if task.Executor != TaskExecutorAgent || task.Status != TaskStatusPending ||
		(!releaseTask && task.Type != TaskScript) || len(validated.ScriptRunnerSnapshots) == 0 ||
		len(validated.ScriptRunnerSnapshots) != len(validated.ScriptBodyArtifacts) ||
		validated.RenderGeneration > math.MaxInt32 || task.PlanID != validated.PlanId ||
		task.RenderGeneration != int32(validated.RenderGeneration) ||
		task.PlanHash != hex.EncodeToString(validated.PlanHash) || len(task.Steps) != len(validated.Steps) {
		return nil, errs.New(errs.KindValidationFailed, "Script Task and execution plan shape do not match")
	}
	for index := range task.Steps {
		if task.Steps[index].ID != validated.Steps[index].StepId {
			return nil, errs.New(errs.KindValidationFailed, "Script Task steps do not bind their sealed plan")
		}
	}
	planBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(validated)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(planBytes)
	snapshots := make(map[string]*agentpb.ResolvedRunnerSnapshot, len(validated.ScriptRunnerSnapshots))
	for _, snapshot := range validated.ScriptRunnerSnapshots {
		snapshots[snapshot.ScriptExecutionId] = snapshot
	}
	records := make([]ScriptExecutionRecord, 0, len(snapshots))
	for _, step := range validated.Steps {
		run := step.GetRunScript()
		if run == nil {
			continue
		}
		snapshot := snapshots[run.ScriptExecutionId]
		if snapshot == nil || snapshot.SnapshotId != run.RunnerSnapshotId {
			return nil, errs.New(errs.KindValidationFailed, "RunScript snapshot is missing")
		}
		if task.Type == TaskScript && (task.Target != run.ScriptId || len(validated.Steps) != 1 ||
			task.TimeoutSeconds != executionplan.ScriptExecutionTimeoutSeconds ||
			task.Params[ScriptExecutionIDParam] != run.ScriptExecutionId ||
			task.Params[ScriptGenerationParam] != strconv.FormatUint(run.ScriptGeneration, 10)) {
			return nil, errs.New(errs.KindValidationFailed, "Script Task does not bind its sealed execution plan")
		}
		snapshotBytes, marshalErr := (proto.MarshalOptions{Deterministic: true}).Marshal(snapshot)
		if marshalErr != nil {
			return nil, errs.Wrap(errs.KindInternal, marshalErr)
		}
		record := ScriptExecutionRecord{
			ID: run.ScriptExecutionId, SnapshotID: run.RunnerSnapshotId,
			OperationID: task.OperationID, CurrentTaskID: task.ID, StepID: step.StepId,
			ScriptID: run.ScriptId, ScriptGeneration: run.ScriptGeneration,
			EnvironmentID: run.EnvironmentId, ServiceID: run.ServiceId, ReleaseID: run.ReleaseId,
			RenderGeneration: run.RenderGeneration, PlanHash: hex.EncodeToString(validated.PlanHash),
			SnapshotSHA256: hex.EncodeToString(run.RunnerSnapshotSha256), BodySHA256: hex.EncodeToString(run.BodySha256),
			RunnerProjectionSHA256: hex.EncodeToString(snapshot.RunnerProjectionSha256),
			Plan:                   append([]byte(nil), planBytes...), Snapshot: snapshotBytes,
			State: ScriptExecutionNotStarted, ActiveReference: true, CreatedAt: at.UTC(), UpdatedAt: at.UTC(),
		}
		if err := validateScriptExecutionRecord(record); err != nil {
			clear(record.Plan)
			clear(record.Snapshot)
			return nil, err
		}
		records = append(records, record)
	}
	if len(records) != len(snapshots) {
		return nil, errs.New(errs.KindValidationFailed, "Script execution set does not match its plan")
	}
	return records, nil
}

// PublishExecutionWithTask atomically claims the body generation, source
// snapshot, Environment-owned Task, queue membership, and idempotency marker.
func (repository *ScriptRepository) PublishExecutionWithTask(
	ctx context.Context,
	sources ScriptExecutionSources,
	execution ScriptExecutionRecord,
	task TaskRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	execution.ScriptSetGeneration = sources.Script.Record.ScriptSetGeneration
	if err := validateScriptExecutionSources(sources, execution); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateScriptExecutionRecord(execution); err != nil ||
		execution.State != ScriptExecutionNotStarted || !execution.ActiveReference {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "new Script execution record is invalid")
	}
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != task.ID || !marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) || task.ID != execution.CurrentTaskID ||
		task.OperationID != execution.OperationID || len(task.Steps) != 1 || task.Steps[0].ID != execution.StepID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "Script execution marker does not match its Task")
	}
	initiation, err := newEnvironmentTaskInitiation(&sources.Tenant, sources.Project, sources.Environment, TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	task = cloneTaskRecord(task)
	if task.IdempotencyKey == "" {
		task.IdempotencyKey = marker.Locator.Key
	}
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := validateTaskInitiation(task, initiation, true); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if sources.Script.Record.ActiveReferences == math.MaxUint64 {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Script active reference count is exhausted")
	}
	updatedScript := sources.Script.Record
	updatedScript.ActiveReferences++

	scriptValue, err := encodeScriptRecord(updatedScript)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(scriptValue)
	executionValue, err := encodeEnvelope("script-execution", execution)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(executionValue)
	snapshotValue, err := encodeEnvelope("script-runner-snapshot", struct {
		ExecutionID string `json:"script_execution_id"`
		SnapshotID  string `json:"snapshot_id"`
		SHA256      string `json:"sha256"`
		Payload     []byte `json:"payload"`
	}{
		ExecutionID: execution.ID, SnapshotID: execution.SnapshotID,
		SHA256: execution.SnapshotSHA256, Payload: execution.Snapshot,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(snapshotValue)
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	taskReference, err := encodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskReference)
	bodyReference, err := encodeEnvelope("script-body-reference", struct {
		ExecutionID         string `json:"script_execution_id"`
		ScriptID            string `json:"script_id"`
		Generation          uint64 `json:"generation"`
		ScriptSetGeneration string `json:"script_set_generation"`
	}{ExecutionID: execution.ID, ScriptID: execution.ScriptID, Generation: execution.ScriptGeneration,
		ScriptSetGeneration: execution.ScriptSetGeneration})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(bodyReference)

	conditions := []Condition{
		{Key: scriptExecutionKey(execution.ID)},
		{Key: scriptRunnerSnapshotKey(execution.SnapshotID)},
		{Key: scriptSetBodyForwardReferenceKey(execution.EnvironmentID, execution.ScriptSetGeneration, execution.ScriptID, execution.ScriptGeneration, execution.ID)},
		{Key: scriptBodyReverseReferenceKey(execution.ID)},
		{Key: taskKey(task.ID)},
		{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)},
		{Key: taskQueueKey(task.Executor, task.ID)},
		{Key: scriptSetScriptKey(execution.EnvironmentID, execution.ScriptSetGeneration, execution.ScriptID), ModRevision: sources.Script.Revision},
		{Key: scriptSetBodyGenerationKey(execution.EnvironmentID, execution.ScriptSetGeneration, execution.ScriptID, execution.ScriptGeneration), ModRevision: sources.BodyGeneration.Revision},
		{Key: scriptSetActiveKey(execution.EnvironmentID)},
		serviceDesiredCondition(sources.Service),
		{Key: releaseProjectionKey(execution.ServiceID), ModRevision: sources.Release.ProjectionRevision},
		{Key: releaseIntentStagingKey("", execution.ReleaseID), ModRevision: sources.Release.IntentRevision},
		{Key: releaseRenderInputStagingKey("", execution.ReleaseID), ModRevision: sources.RenderInput.Revision},
	}
	conditions = append(conditions, scriptExecutionProjectionConditions(sources)...)
	conditions[10].ModRevision = sources.Environment.ReadRevision
	active, err := readActiveScriptSet(ctx, repository.store, execution.EnvironmentID, sources.Revision)
	if err != nil || active.Record.GenerationID != execution.ScriptSetGeneration {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Script-set generation changed")
	}
	conditions[10].ModRevision = active.Revision
	activeValue, err := encodeScriptSetGeneration(active.Record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(activeValue)
	mutations := []Mutation{
		{Type: MutationPut, Key: scriptSetScriptKey(execution.EnvironmentID, execution.ScriptSetGeneration, execution.ScriptID), Value: scriptValue},
		{Type: MutationPut, Key: scriptExecutionKey(execution.ID), Value: executionValue},
		{Type: MutationPut, Key: scriptRunnerSnapshotKey(execution.SnapshotID), Value: snapshotValue},
		{Type: MutationPut, Key: scriptSetBodyForwardReferenceKey(execution.EnvironmentID, execution.ScriptSetGeneration, execution.ScriptID, execution.ScriptGeneration, execution.ID), Value: bodyReference},
		{Type: MutationPut, Key: scriptBodyReverseReferenceKey(execution.ID), Value: bodyReference},
		{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: taskReference},
		{Type: MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: taskReference},
		{Type: MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: taskReference},
		{Type: MutationPut, Key: scriptSetActiveKey(execution.EnvironmentID), Value: activeValue},
	}
	plan, err := newTaskIdempotencyMutationPlan(
		task, initiation, conditions, mutations, classifyScriptExecutionPublication(len(conditions)),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func (repository *ScriptRepository) GetScriptExecutionPlan(
	ctx context.Context,
	task TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	executionID := task.Params[ScriptExecutionIDParam]
	if ctx == nil || repository == nil || repository.store == nil || task.Type != TaskScript ||
		!validRawScriptExecutionID(executionID) {
		return nil, errs.New(errs.KindValidationFailed, "Script execution plan request is invalid")
	}
	read, err := repository.store.Get(ctx, scriptExecutionKey(executionID))
	if err != nil {
		return nil, err
	}
	if read == nil || read.Entry == nil {
		return nil, errs.New(errs.KindStateConflict, "Script execution record is missing")
	}
	record, err := decodeEnvelope[ScriptExecutionRecord](read.Entry.Value, "script-execution")
	if err != nil || validateScriptExecutionRecord(record) != nil || record.CurrentTaskID != task.ID ||
		record.OperationID != task.OperationID || record.StepID != task.Steps[0].ID || record.PlanHash != task.PlanHash {
		return nil, errs.New(errs.KindInternal, "Script execution record is corrupt")
	}
	plan := &agentpb.ExecutionPlan{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(record.Plan, plan); err != nil {
		return nil, errs.New(errs.KindInternal, "Script execution plan is corrupt")
	}
	validated, err := executionplan.Validate(plan)
	if err != nil || hex.EncodeToString(validated.PlanHash) != task.PlanHash {
		return nil, errs.New(errs.KindInternal, "Script execution plan is corrupt")
	}
	return validated, nil
}

func (repository *ScriptRepository) GetReleaseScriptExecutionPlan(
	ctx context.Context,
	task TaskRecord,
) (*agentpb.ExecutionPlan, bool, error) {
	if ctx == nil || repository == nil || repository.store == nil ||
		(task.Type != TaskDeploy && task.Type != TaskRollback) {
		return nil, false, errs.New(errs.KindValidationFailed, "release Script execution plan request is invalid")
	}
	executionIDs := make(map[string]string)
	for _, step := range task.Steps {
		if executionID := task.Params[ReleaseHookStepExecutionParam(step.ID)]; executionID != "" {
			if !validRawScriptExecutionID(executionID) {
				return nil, false, errs.New(errs.KindInternal, "release Script execution identity is corrupt")
			}
			executionIDs[step.ID] = executionID
		}
	}
	if len(executionIDs) == 0 {
		return nil, false, nil
	}
	var sealed *agentpb.ExecutionPlan
	var sealedBytes []byte
	for stepID, executionID := range executionIDs {
		read, err := repository.store.Get(ctx, scriptExecutionKey(executionID))
		if err != nil {
			return nil, false, err
		}
		if read == nil || read.Entry == nil {
			return nil, false, errs.New(errs.KindStateConflict, "release Script execution record is missing")
		}
		record, err := decodeEnvelope[ScriptExecutionRecord](read.Entry.Value, "script-execution")
		if err != nil || validateScriptExecutionRecord(record) != nil || record.ID != executionID ||
			record.CurrentTaskID != task.ID || record.OperationID != task.OperationID || record.StepID != stepID ||
			record.PlanHash != task.PlanHash {
			return nil, false, errs.New(errs.KindInternal, "release Script execution record is corrupt")
		}
		if sealed == nil {
			sealed = &agentpb.ExecutionPlan{}
			if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(record.Plan, sealed); err != nil {
				return nil, false, errs.New(errs.KindInternal, "release Script execution plan is corrupt")
			}
			validated, err := executionplan.Validate(sealed)
			if err != nil || hex.EncodeToString(validated.PlanHash) != task.PlanHash {
				return nil, false, errs.New(errs.KindInternal, "release Script execution plan is corrupt")
			}
			sealed = validated
			sealedBytes = append([]byte(nil), record.Plan...)
		} else if !bytes.Equal(record.Plan, sealedBytes) {
			return nil, false, errs.New(errs.KindInternal, "release Script execution plans disagree")
		}
	}
	for _, step := range sealed.Steps {
		if step.GetRunScript() != nil && executionIDs[step.StepId] != step.GetRunScript().ScriptExecutionId {
			return nil, false, errs.New(errs.KindInternal, "release Script execution plan authority is incomplete")
		}
	}
	return sealed, true, nil
}

// ResolveScriptAssignmentArtifacts returns the private body bytes only after
// the durable execution, sealed plan, and immutable generation agree.
func (repository *ScriptRepository) ResolveScriptAssignmentArtifacts(
	ctx context.Context,
	task TaskRecord,
	plan *agentpb.ExecutionPlan,
) (*agentpb.ScriptAssignmentArtifacts, error) {
	validated, err := executionplan.Validate(plan)
	if err != nil {
		return nil, err
	}
	if repository == nil || repository.store == nil || len(validated.ScriptBodyArtifacts) == 0 ||
		(task.Type != TaskScript && task.Type != TaskDeploy && task.Type != TaskRollback) ||
		hex.EncodeToString(validated.PlanHash) != task.PlanHash {
		return nil, errs.New(errs.KindValidationFailed, "Script assignment artifact request is invalid")
	}
	steps := make(map[string]string, len(validated.ScriptBodyArtifacts))
	for _, step := range validated.Steps {
		if run := step.GetRunScript(); run != nil {
			steps[run.ScriptExecutionId] = step.StepId
		}
	}
	artifacts := &agentpb.ScriptAssignmentArtifacts{}
	for _, metadata := range validated.ScriptBodyArtifacts {
		executionRead, readErr := repository.store.Get(ctx, scriptExecutionKey(metadata.ScriptExecutionId))
		if readErr != nil {
			return nil, readErr
		}
		if executionRead == nil || executionRead.Entry == nil {
			return nil, errs.New(errs.KindStateConflict, "Script execution record is missing")
		}
		execution, decodeErr := decodeEnvelope[ScriptExecutionRecord](executionRead.Entry.Value, "script-execution")
		if decodeErr != nil || validateScriptExecutionRecord(execution) != nil || execution.CurrentTaskID != task.ID ||
			execution.OperationID != task.OperationID || execution.StepID != steps[metadata.ScriptExecutionId] ||
			execution.PlanHash != task.PlanHash || !execution.ActiveReference {
			return nil, errs.New(errs.KindInternal, "Script execution record is corrupt")
		}
		bodyRead, readErr := repository.store.Get(ctx, scriptSetBodyGenerationKey(
			execution.EnvironmentID, execution.ScriptSetGeneration, metadata.ScriptId, metadata.Generation,
		))
		if readErr != nil {
			return nil, readErr
		}
		if bodyRead == nil || bodyRead.Entry == nil {
			return nil, errs.New(errs.KindInternal, "Script body generation is missing")
		}
		body, decodeErr := decodeScriptBodyGeneration(bodyRead.Entry.Value)
		if decodeErr != nil || body.ScriptID != metadata.ScriptId || body.Generation != metadata.Generation ||
			body.BodySize != metadata.Size || body.BodySHA256 != hex.EncodeToString(metadata.Sha256) ||
			execution.BodySHA256 != body.BodySHA256 {
			return nil, errs.New(errs.KindInternal, "Script body generation does not match its execution")
		}
		ownedBody := []byte(body.Body)
		digest := sha256.Sum256(ownedBody)
		if len(ownedBody) != int(metadata.Size) || hex.EncodeToString(digest[:]) != body.BodySHA256 {
			clear(ownedBody)
			return nil, errs.New(errs.KindInternal, "Script body generation content is corrupt")
		}
		artifacts.Bodies = append(artifacts.Bodies, &agentpb.ScriptBodyArtifact{
			Metadata: proto.Clone(metadata).(*agentpb.ScriptBodyArtifactMetadata), Body: ownedBody,
		})
	}
	return artifacts, nil
}

func validateScriptExecutionSources(sources ScriptExecutionSources, execution ScriptExecutionRecord) error {
	if sources.Revision <= 0 || sources.Tenant.ReadRevision != sources.Revision ||
		sources.Project.ReadRevision != sources.Revision || sources.Environment.ReadRevision != sources.Revision ||
		sources.Service.ReadRevision != sources.Revision || sources.ScriptSet.ReadRevision != sources.Revision ||
		sources.Script.ReadRevision != sources.Revision ||
		sources.BodyGeneration.ReadRevision != sources.Revision || sources.RenderInput.ReadRevision != sources.Revision ||
		sources.DesiredHead.ReadRevision != sources.Revision || sources.DesiredHead.Revision <= 0 ||
		sources.DesiredProjection.ReadRevision != sources.Revision || sources.DesiredProjection.Revision <= 0 ||
		sources.Release.Revision != sources.Revision || sources.Script.Record.Desired.ID != execution.ScriptID ||
		sources.ScriptSet.Revision <= 0 || sources.ScriptSet.Record.EnvironmentID != execution.EnvironmentID ||
		sources.ScriptSet.Record.GenerationID != execution.ScriptSetGeneration ||
		sources.Script.Record.ScriptSetGeneration != execution.ScriptSetGeneration ||
		sources.Script.Record.ActiveGeneration != execution.ScriptGeneration ||
		sources.BodyGeneration.Record.ScriptID != execution.ScriptID ||
		sources.BodyGeneration.Record.Generation != execution.ScriptGeneration ||
		sources.Environment.Record.ID != execution.EnvironmentID || sources.Service.Record.Desired.ID != execution.ServiceID ||
		sources.Release.Intent.ID != execution.ReleaseID || sources.RenderInput.Record.ReleaseID != execution.ReleaseID ||
		sources.RenderInput.Record.Projection.RenderGeneration != execution.RenderGeneration ||
		sources.DesiredHead.Record.EnvironmentID != execution.EnvironmentID ||
		sources.DesiredHead.Record.RevisionID == "" ||
		sources.DesiredProjection.Record.EnvironmentID != execution.EnvironmentID ||
		sources.DesiredProjection.Record.RevisionID == "" ||
		sources.DesiredProjection.Record.RenderGeneration == 0 ||
		sources.BodyGeneration.Record.BodySHA256 != execution.BodySHA256 {
		return errs.New(errs.KindValidationFailed, "Script execution sources do not match the execution")
	}
	desiredZoneNames := make(map[string]string, len(sources.DesiredProjection.Record.DesiredZones))
	for _, desired := range sources.DesiredProjection.Record.DesiredZones {
		if _, duplicate := desiredZoneNames[desired.Desired.ID]; duplicate {
			return errs.New(errs.KindValidationFailed, "Script execution desired zone identity is duplicated")
		}
		desiredZoneNames[desired.Desired.ID] = desired.Desired.Name
	}
	if len(sources.Networks) != len(desiredZoneNames) {
		return errs.New(errs.KindValidationFailed, "Script execution network sources are incomplete")
	}
	for _, network := range sources.Networks {
		identity, found := desiredZoneNames[network.Record.Desired.ID]
		if network.ReadRevision != sources.Revision || network.Revision != sources.DesiredProjection.Revision ||
			network.Record.EnvironmentID != execution.EnvironmentID || !found || network.Record.Desired.Name != identity {
			return errs.New(errs.KindValidationFailed, "Script execution network source revision is invalid")
		}
	}
	return nil
}

func scriptExecutionProjectionConditions(sources ScriptExecutionSources) []Condition {
	conditions := []Condition{
		{
			Key:         environmentBlueprintHeadKey(sources.Environment.Record.ID),
			ModRevision: sources.DesiredHead.Revision,
		},
		{
			Key: environmentBlueprintRootKey(
				sources.Environment.Record.ID, sources.DesiredProjection.Record.RevisionID,
			),
			ModRevision: sources.DesiredProjection.Revision,
		},
	}
	for _, network := range sources.Networks {
		conditions = append(conditions, Condition{
			Key: deletionTombstoneKey(string(DeletionTargetZone), network.Record.Desired.ID),
		})
	}
	return conditions
}

func validateScriptExecutionRecord(record ScriptExecutionRecord) error {
	if !validRawScriptExecutionID(record.ID) || !validRawScriptExecutionID(record.SnapshotID) ||
		ids.Validate(ids.KindOperation, record.OperationID) != nil ||
		ids.Validate(ids.KindTask, record.CurrentTaskID) != nil || ids.Validate(ids.KindStep, record.StepID) != nil ||
		ids.Validate(ids.KindScript, record.ScriptID) != nil ||
		record.ScriptGeneration == 0 || ids.Validate(ids.KindEnvironment, record.EnvironmentID) != nil ||
		ids.Validate(ids.KindService, record.ServiceID) != nil || ids.Validate(ids.KindDeployment, record.ReleaseID) != nil ||
		record.RenderGeneration == 0 || !validLowerSHA256(record.PlanHash) || !validLowerSHA256(record.SnapshotSHA256) ||
		!validLowerSHA256(record.BodySHA256) || !validLowerSHA256(record.RunnerProjectionSHA256) ||
		len(record.Plan) == 0 || len(record.Plan) > executionplan.MaximumPlanBytes || len(record.Snapshot) == 0 ||
		!validScriptExecutionState(record.State) || record.CreatedAt.IsZero() || record.UpdatedAt.Before(record.CreatedAt) ||
		validateScriptExecutionCheckpointShape(record) != nil {
		return errs.New(errs.KindValidationFailed, "Script execution record is invalid")
	}
	return nil
}

func validScriptExecutionState(state ScriptExecutionState) bool {
	switch state {
	case ScriptExecutionNotStarted, ScriptExecutionStartAuthorized, ScriptExecutionBodyPrepared,
		ScriptExecutionContainerCreated, ScriptExecutionOutcomeRecorded, ScriptExecutionCleanupProven:
		return true
	default:
		return false
	}
}

func classifyScriptExecutionPublication(expected int) idempotencyPlanClassifier {
	return func(_ int64, values []*KeyValue) error {
		if len(values) != expected {
			return errs.New(errs.KindInternal, "Script execution publication compare evidence is incomplete")
		}
		if values[6] != nil {
			return errs.New(errs.KindStateConflict, "Script operation already has an active Task")
		}
		for index, value := range values {
			if index < 8 && value != nil {
				return errs.New(errs.KindInternal, "Script execution identity collided with durable state")
			}
			if index >= 8 && value == nil {
				return errs.New(errs.KindStateConflict, "Script execution source changed before publication")
			}
		}
		return errs.New(errs.KindStateConflict, "Script execution source changed before publication")
	}
}

func scriptExecutionKey(executionID string) string { return scriptExecutionPrefix + executionID }

func scriptRunnerSnapshotKey(snapshotID string) string {
	return scriptRunnerSnapshotPrefix + snapshotID
}

func scriptSetBodyForwardReferenceKey(environmentID, setGeneration, scriptID string, generation uint64, executionID string) string {
	return scriptSetBodyGenerationKey(environmentID, setGeneration, scriptID, generation) + scriptBodyForwardRefSegment + executionID
}

func scriptBodyReverseReferenceKey(executionID string) string {
	return scriptExecutionKey(executionID) + scriptBodyReverseRefSegment
}

func validRawScriptExecutionID(value string) bool {
	if len(value) != 26 || value != strings.ToUpper(value) {
		return false
	}
	_, err := ulid.ParseStrict(value)
	return err == nil
}

func validLowerSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}
