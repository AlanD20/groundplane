package etcd

import (
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

	scriptExecutionPrefix       = "/v1/script-executions/"
	scriptRunnerSnapshotPrefix  = "/v1/script-runner-snapshots/"
	scriptBodyForwardRefSegment = "/references/"
	scriptBodyReverseRefSegment = "/body-reference"
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

func NewScriptExecutionRecord(
	task TaskRecord,
	plan *agentpb.ExecutionPlan,
	at time.Time,
) (ScriptExecutionRecord, error) {
	validated, err := executionplan.Validate(plan)
	if err != nil {
		return ScriptExecutionRecord{}, err
	}
	if task.Type != TaskScript || task.Executor != TaskExecutorAgent || task.Status != TaskStatusPending ||
		len(validated.Steps) != 1 || len(validated.ScriptRunnerSnapshots) != 1 ||
		len(validated.ScriptRunnerProjections) != 1 || len(validated.ScriptBodyArtifacts) != 1 ||
		validated.RenderGeneration > math.MaxInt32 {
		return ScriptExecutionRecord{}, errs.New(errs.KindValidationFailed, "Script Task and execution plan shape do not match")
	}
	run := validated.Steps[0].GetRunScript()
	snapshot := validated.ScriptRunnerSnapshots[0]
	if run == nil || task.Target != run.ScriptId || task.PlanID != validated.PlanId ||
		task.RenderGeneration != int32(validated.RenderGeneration) || len(task.Steps) != 1 ||
		task.Steps[0].ID != validated.Steps[0].StepId || task.TimeoutSeconds != executionplan.ScriptExecutionTimeoutSeconds ||
		task.PlanHash != hex.EncodeToString(validated.PlanHash) || task.Params[ScriptExecutionIDParam] != run.ScriptExecutionId ||
		task.Params[ScriptGenerationParam] != strconv.FormatUint(run.ScriptGeneration, 10) {
		return ScriptExecutionRecord{}, errs.New(errs.KindValidationFailed, "Script Task does not bind its sealed execution plan")
	}
	planBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(validated)
	if err != nil {
		return ScriptExecutionRecord{}, errs.Wrap(errs.KindInternal, err)
	}
	snapshotBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(snapshot)
	if err != nil {
		clear(planBytes)
		return ScriptExecutionRecord{}, errs.Wrap(errs.KindInternal, err)
	}
	record := ScriptExecutionRecord{
		ID: run.ScriptExecutionId, SnapshotID: run.RunnerSnapshotId,
		OperationID: task.OperationID, CurrentTaskID: task.ID, StepID: task.Steps[0].ID,
		ScriptID: run.ScriptId, ScriptGeneration: run.ScriptGeneration,
		EnvironmentID: run.EnvironmentId, ServiceID: run.ServiceId, ReleaseID: run.ReleaseId,
		RenderGeneration: run.RenderGeneration, PlanHash: hex.EncodeToString(validated.PlanHash),
		SnapshotSHA256:         hex.EncodeToString(run.RunnerSnapshotSha256),
		BodySHA256:             hex.EncodeToString(run.BodySha256),
		RunnerProjectionSHA256: hex.EncodeToString(snapshot.RunnerProjectionSha256),
		Plan:                   planBytes, Snapshot: snapshotBytes,
		State: ScriptExecutionNotStarted, ActiveReference: true, CreatedAt: at.UTC(), UpdatedAt: at.UTC(),
	}
	if err := validateScriptExecutionRecord(record); err != nil {
		clear(record.Plan)
		clear(record.Snapshot)
		return ScriptExecutionRecord{}, err
	}
	return record, nil
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
		{Key: serviceKey(execution.ServiceID), ModRevision: sources.Service.Revision},
		{Key: releaseProjectionKey(execution.ServiceID), ModRevision: sources.Release.ProjectionRevision},
		{Key: releaseIntentStagingKey("", execution.ReleaseID), ModRevision: sources.Release.IntentRevision},
		{Key: releaseRenderInputStagingKey("", execution.ReleaseID), ModRevision: sources.RenderInput.Revision},
		{Key: environmentComposeProjectionKey(execution.EnvironmentID), ModRevision: sources.AppliedProjection.Revision},
	}
	for _, network := range sources.Networks {
		conditions = append(conditions, Condition{Key: zoneKey(network.Record.Desired.ID), ModRevision: network.Revision})
	}
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
	if repository == nil || repository.store == nil || task.Type != TaskScript ||
		len(validated.ScriptBodyArtifacts) != 1 || len(validated.Steps) != 1 ||
		hex.EncodeToString(validated.PlanHash) != task.PlanHash {
		return nil, errs.New(errs.KindValidationFailed, "Script assignment artifact request is invalid")
	}
	executionID := task.Params[ScriptExecutionIDParam]
	executionRead, err := repository.store.Get(ctx, scriptExecutionKey(executionID))
	if err != nil {
		return nil, err
	}
	if executionRead == nil || executionRead.Entry == nil {
		return nil, errs.New(errs.KindStateConflict, "Script execution record is missing")
	}
	execution, err := decodeEnvelope[ScriptExecutionRecord](executionRead.Entry.Value, "script-execution")
	if err != nil || validateScriptExecutionRecord(execution) != nil || execution.CurrentTaskID != task.ID ||
		execution.OperationID != task.OperationID || execution.StepID != task.Steps[0].ID ||
		execution.PlanHash != task.PlanHash || !execution.ActiveReference {
		return nil, errs.New(errs.KindInternal, "Script execution record is corrupt")
	}
	metadata := validated.ScriptBodyArtifacts[0]
	bodyRead, err := repository.store.Get(ctx, scriptSetBodyGenerationKey(
		execution.EnvironmentID, execution.ScriptSetGeneration, metadata.ScriptId, metadata.Generation,
	))
	if err != nil {
		return nil, err
	}
	if bodyRead == nil || bodyRead.Entry == nil {
		return nil, errs.New(errs.KindInternal, "Script body generation is missing")
	}
	body, err := decodeScriptBodyGeneration(bodyRead.Entry.Value)
	if err != nil || body.ScriptID != metadata.ScriptId || body.Generation != metadata.Generation ||
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
	return &agentpb.ScriptAssignmentArtifacts{Bodies: []*agentpb.ScriptBodyArtifact{{
		Metadata: proto.Clone(metadata).(*agentpb.ScriptBodyArtifactMetadata), Body: ownedBody,
	}}}, nil
}

func validateScriptExecutionSources(sources ScriptExecutionSources, execution ScriptExecutionRecord) error {
	if sources.Revision <= 0 || sources.Tenant.ReadRevision != sources.Revision ||
		sources.Project.ReadRevision != sources.Revision || sources.Environment.ReadRevision != sources.Revision ||
		sources.Service.ReadRevision != sources.Revision || sources.Script.ReadRevision != sources.Revision ||
		sources.BodyGeneration.ReadRevision != sources.Revision || sources.RenderInput.ReadRevision != sources.Revision ||
		sources.AppliedProjection.ReadRevision != sources.Revision || sources.AppliedProjection.Revision <= 0 ||
		sources.Release.Revision != sources.Revision || sources.Script.Record.Desired.ID != execution.ScriptID ||
		sources.Script.Record.ScriptSetGeneration != execution.ScriptSetGeneration ||
		sources.Script.Record.ActiveGeneration != execution.ScriptGeneration ||
		sources.BodyGeneration.Record.ScriptID != execution.ScriptID ||
		sources.BodyGeneration.Record.Generation != execution.ScriptGeneration ||
		sources.Environment.Record.ID != execution.EnvironmentID || sources.Service.Record.Desired.ID != execution.ServiceID ||
		sources.Release.Intent.ID != execution.ReleaseID || sources.RenderInput.Record.ReleaseID != execution.ReleaseID ||
		sources.RenderInput.Record.Projection.RenderGeneration != execution.RenderGeneration ||
		sources.AppliedProjection.Record.EnvironmentID != execution.EnvironmentID ||
		sources.AppliedProjection.Record.RevisionID == "" || sources.AppliedProjection.Record.RenderGeneration == 0 ||
		sources.BodyGeneration.Record.BodySHA256 != execution.BodySHA256 {
		return errs.New(errs.KindValidationFailed, "Script execution sources do not match the execution")
	}
	for _, network := range sources.Networks {
		if network.ReadRevision != sources.Revision || network.Revision <= 0 {
			return errs.New(errs.KindValidationFailed, "Script execution network source revision is invalid")
		}
	}
	return nil
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
