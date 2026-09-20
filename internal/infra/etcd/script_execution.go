package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
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

type ScriptControllerCleanupAuthority string

const (
	ScriptExecutionNotStarted       ScriptExecutionState = "not_started"
	ScriptExecutionStartAuthorized  ScriptExecutionState = "start_authorized"
	ScriptExecutionBodyPrepared     ScriptExecutionState = "body_prepared"
	ScriptExecutionContainerCreated ScriptExecutionState = "container_created"
	ScriptExecutionOutcomeRecorded  ScriptExecutionState = "outcome_recorded"
	ScriptExecutionCleanupProven    ScriptExecutionState = "cleanup_proven"

	ScriptControllerCleanupBlueprintPendingAbort        ScriptControllerCleanupAuthority = "blueprint_pending_abort"
	ScriptControllerCleanupManualPendingAbort           ScriptControllerCleanupAuthority = "manual_pending_abort"
	ScriptControllerCleanupManualAssignedAbort          ScriptControllerCleanupAuthority = "manual_assigned_abort"
	ScriptControllerCleanupManualRetryExpiry            ScriptControllerCleanupAuthority = "manual_retry_expiry"
	ScriptControllerCleanupReleaseRecoveryParentFailure ScriptControllerCleanupAuthority = "release_recovery_parent_failure"
)

// ScriptExecutionRecord is the durable recovery authority for one one-off
// Script container. Plan and Snapshot contain no Script body or secret bytes.
type ScriptExecutionRecord struct {
	ID                     string                           `json:"id"`
	SnapshotID             string                           `json:"snapshot_id"`
	OperationID            string                           `json:"operation_id"`
	CurrentTaskID          string                           `json:"current_task_id"`
	AssignmentID           string                           `json:"assignment_id,omitempty"`
	StepID                 string                           `json:"step_id"`
	ScriptID               string                           `json:"script_id"`
	ScriptGeneration       uint64                           `json:"script_generation"`
	ScriptSetGeneration    string                           `json:"script_set_generation"`
	EnvironmentID          string                           `json:"environment_id"`
	ServiceID              string                           `json:"service_id"`
	ReleaseID              string                           `json:"release_id"`
	RenderGeneration       uint64                           `json:"render_generation"`
	PlanHash               string                           `json:"plan_hash"`
	SourceMembershipCount  uint64                           `json:"source_membership_count,omitempty"`
	SourceMembershipSHA256 string                           `json:"source_membership_sha256,omitempty"`
	SnapshotSHA256         string                           `json:"snapshot_sha256"`
	BodySHA256             string                           `json:"body_sha256"`
	RunnerProjectionSHA256 string                           `json:"runner_projection_sha256"`
	Plan                   []byte                           `json:"plan"`
	Snapshot               []byte                           `json:"snapshot"`
	State                  ScriptExecutionState             `json:"state"`
	StartAuthorized        bool                             `json:"start_authorized"`
	BodyPrepared           *ScriptBodyPreparedEvidence      `json:"body_prepared,omitempty"`
	ContainerCreated       *ScriptContainerCreatedEvidence  `json:"container_created,omitempty"`
	Outcome                *ScriptOutcomeEvidence           `json:"outcome,omitempty"`
	Cleanup                *ScriptCleanupEvidence           `json:"cleanup,omitempty"`
	ControllerCleanup      ScriptControllerCleanupAuthority `json:"controller_cleanup,omitempty"`
	LastCheckpointSHA256   string                           `json:"last_checkpoint_sha256,omitempty"`
	ReconciliationRequired bool                             `json:"reconciliation_required"`
	ActiveReference        bool                             `json:"active_reference"`
	CreatedAt              time.Time                        `json:"created_at"`
	UpdatedAt              time.Time                        `json:"updated_at"`
}

type releaseHookExecutionStep struct {
	stepID      string
	executionID string
}

type releaseHookExecutionRetryTransfer struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
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
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(steps) {
		return false, corruptReleaseRecord()
	}
	type activeHook struct {
		step   releaseHookExecutionStep
		record ScriptExecutionRecord
		value  *etcdstore.KeyValue
	}
	active := make([]activeHook, 0, maximumReleaseHookTerminalBatch)
	seenScripts := make(map[string]struct{}, len(steps))
	for index, step := range steps {
		value := read.Values[index]
		if value == nil {
			return false, corruptReleaseRecord()
		}
		record, decodeErr := recordcodec.Decode[ScriptExecutionRecord](value.Value, "script-execution")
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
			return false, errs.New(
				errs.KindStateConflict,
				"release hook execution has not reached a releasable checkpoint",
			)
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
	details, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: detailKeys, Revision: revision})
	if err != nil {
		return false, err
	}
	if details == nil || details.ReadRevision != revision || len(details.Values) != len(detailKeys) {
		return false, corruptReleaseRecord()
	}
	conditions := make([]etcdstore.Condition, 0, len(active)*4)
	mutations := make([]etcdstore.Mutation, 0, len(active)*4)
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
		executionValue, encodeErr := recordcodec.Encode("script-execution", next)
		if encodeErr != nil {
			return false, encodeErr
		}
		scriptValue, encodeErr := decrementStoredScriptActiveReferences(values[0].Value, hook.record.ScriptID)
		if encodeErr != nil {
			clear(executionValue)
			return false, encodeErr
		}
		conditions = append(conditions,
			etcdstore.Condition{Key: hook.value.Key, ModRevision: hook.value.ModRevision},
			etcdstore.Condition{Key: values[0].Key, ModRevision: values[0].ModRevision},
			etcdstore.Condition{Key: values[1].Key, ModRevision: values[1].ModRevision},
			etcdstore.Condition{Key: values[2].Key, ModRevision: values[2].ModRevision},
		)
		mutations = append(mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: hook.value.Key, Value: executionValue},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: values[0].Key, Value: scriptValue},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: values[1].Key},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: values[2].Key},
		)
	}
	if len(conditions)+len(mutations) > etcdstore.MaximumOperations {
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
	if err != nil {
		return releaseHookExecutionRetryTransfer{}, scriptRetryUnsafe("Script retry barrier Task authority is invalid")
	}
	if len(steps) == 0 {
		return releaseHookExecutionRetryTransfer{}, nil
	}
	if retry.PlanID != source.PlanID || retry.PlanHash != source.PlanHash || len(retry.Steps) != len(source.Steps) {
		return releaseHookExecutionRetryTransfer{}, scriptRetryUnsafe("Script retry barrier plan lineage changed")
	}
	for index, step := range source.Steps {
		if retry.Steps[index].ID != step.ID ||
			retry.Params[ReleaseHookStepExecutionParam(step.ID)] != source.Params[ReleaseHookStepExecutionParam(step.ID)] {
			return releaseHookExecutionRetryTransfer{}, scriptRetryUnsafe("Script retry barrier step lineage changed")
		}
	}
	keys := make([]string, len(steps))
	for index, step := range steps {
		keys[index] = scriptExecutionKey(step.executionID)
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return releaseHookExecutionRetryTransfer{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(steps) {
		return releaseHookExecutionRetryTransfer{}, scriptRetryUnsafe("Script retry barrier read is incomplete")
	}
	transfer := releaseHookExecutionRetryTransfer{
		conditions: make([]etcdstore.Condition, 0, len(steps)), mutations: make([]etcdstore.Mutation, 0, len(steps)),
	}
	seenExecutions := make(map[string]struct{}, len(steps))
	for index, step := range steps {
		value := read.Values[index]
		if value == nil {
			transfer.clear()
			return releaseHookExecutionRetryTransfer{}, scriptRetryUnsafe("Script execution state is unknown")
		}
		record, decodeErr := recordcodec.Decode[ScriptExecutionRecord](value.Value, "script-execution")
		if decodeErr != nil || validateScriptExecutionRecord(record) != nil {
			transfer.clear()
			return releaseHookExecutionRetryTransfer{}, scriptRetryUnsafe("Script execution state is unknown")
		}
		if record.ID != step.executionID ||
			record.CurrentTaskID != source.ID || record.OperationID != source.OperationID ||
			record.StepID != step.stepID || record.PlanHash != source.PlanHash || !record.ActiveReference ||
			!retry.CreatedAt.After(record.UpdatedAt) {
			transfer.clear()
			return releaseHookExecutionRetryTransfer{}, scriptRetryUnsafe("Script retry barrier lineage changed")
		}
		if record.State != ScriptExecutionNotStarted || record.StartAuthorized {
			transfer.clear()
			return releaseHookExecutionRetryTransfer{}, scriptRetryUnsafe("Script execution may already have started")
		}
		if _, duplicate := seenExecutions[record.ID]; duplicate {
			transfer.clear()
			return releaseHookExecutionRetryTransfer{}, scriptRetryUnsafe("Script retry barrier lineage changed")
		}
		seenExecutions[record.ID] = struct{}{}
		next := record
		next.CurrentTaskID = retry.ID
		next.AssignmentID = ""
		next.UpdatedAt = retry.CreatedAt.UTC()
		encoded, encodeErr := recordcodec.Encode("script-execution", next)
		if encodeErr != nil {
			transfer.clear()
			return releaseHookExecutionRetryTransfer{}, encodeErr
		}
		transfer.conditions = append(transfer.conditions, etcdstore.Condition{Key: value.Key, ModRevision: value.ModRevision})
		transfer.mutations = append(transfer.mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: value.Key, Value: encoded})
	}
	return transfer, nil
}

func releaseHookExecutionSteps(task TaskRecord) ([]releaseHookExecutionStep, error) {
	if task.Type != TaskDeploy && task.Type != TaskRollback &&
		(task.Type != TaskUpdate || !blueprintScriptTaskShape(task)) {
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

// releaseScriptEffectEvidenceAtRevision returns only durable checkpoint
// evidence. A host observation is never used to choose recovery mode.
func (repository *TaskRepository) releaseScriptEffectEvidenceAtRevision(
	ctx context.Context,
	task TaskRecord,
	assignment TaskAssignmentRecord,
	revision int64,
) (bool, []etcdstore.Condition, error) {
	steps, err := releaseHookExecutionSteps(task)
	if err != nil {
		return false, nil, err
	}
	if len(steps) == 0 {
		return false, nil, nil
	}
	keys := make([]string, len(steps))
	for index, step := range steps {
		keys[index] = scriptExecutionKey(step.executionID)
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return false, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return false, nil, corruptReleaseRecord()
	}
	effect := false
	conditions := make([]etcdstore.Condition, len(keys))
	for index, value := range read.Values {
		if value == nil {
			return false, nil, corruptReleaseRecord()
		}
		record, decodeErr := recordcodec.Decode[ScriptExecutionRecord](value.Value, "script-execution")
		if decodeErr != nil || validateScriptExecutionRecord(record) != nil || record.ID != steps[index].executionID ||
			record.CurrentTaskID != task.ID || record.OperationID != task.OperationID ||
			record.StepID != steps[index].stepID || record.PlanHash != task.PlanHash ||
			record.State == ScriptExecutionNotStarted && record.AssignmentID != "" ||
			record.State != ScriptExecutionNotStarted && record.AssignmentID != assignment.AssignmentID {
			return false, nil, corruptReleaseRecord()
		}
		conditions[index] = etcdstore.Condition{Key: value.Key, ModRevision: value.ModRevision}
		effect = effect || record.State != ScriptExecutionNotStarted
	}
	return effect, conditions, nil
}

func scriptRetryUnsafe(detail string) error {
	return errs.New(errs.KindScriptRetryUnsafe, detail)
}

func blueprintScriptTaskShape(task TaskRecord) bool {
	return task.Type == TaskUpdate && task.Executor == TaskExecutorAgent &&
		validatePublicationID(task.Params[TaskReleasePublicationParam]) == nil &&
		task.Owner.EnvironmentID != "" && task.Target == task.Owner.EnvironmentID &&
		task.Params[TaskMaterializationEnvironmentParam] == task.Owner.EnvironmentID &&
		ids.Validate(ids.KindTask, task.Params[EnvironmentDesiredRevisionParam]) == nil
}

func (repository *ScriptRepository) validateBlueprintScriptExecutionAuthority(
	ctx context.Context,
	task TaskRecord,
	execution ScriptExecutionRecord,
	revision int64,
) error {
	_, err := repository.blueprintScriptExecutionAuthority(ctx, task, execution, revision)
	return err
}

func (repository *ScriptRepository) blueprintScriptExecutionAuthority(
	ctx context.Context,
	task TaskRecord,
	execution ScriptExecutionRecord,
	revision int64,
) ([]etcdstore.Condition, error) {
	if repository == nil || repository.store == nil || !blueprintScriptTaskShape(task) ||
		execution.CurrentTaskID != task.ID || execution.OperationID != task.OperationID ||
		execution.EnvironmentID != task.Owner.EnvironmentID ||
		execution.PlanHash != task.PlanHash ||
		task.Params[ReleaseHookStepExecutionParam(execution.StepID)] != execution.ID {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Script execution does not match its Task")
	}
	publicationID := task.Params[TaskReleasePublicationParam]
	keys := []string{releasePublicationKey(publicationID), releaseManifestStagingKey(publicationID)}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[1] == nil {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Script publication authority is unavailable")
	}
	marker, err := decodeReleaseRecord[ReleasePublicationMarker](read.Values[0].Value, "release-publication")
	if err != nil {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Script publication authority is corrupt")
	}
	manifest, err := decodeReleaseRecord[ReleaseStagedManifest](read.Values[1].Value, "release-staged-manifest")
	if err != nil || validateBlueprintCandidateManifest(task, marker, manifest) != nil {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Script manifest authority is corrupt")
	}
	var selected ReleaseStagedMemberRef
	memberBound := false
	for _, member := range manifest.Members {
		if member.ReleaseID == execution.ReleaseID && member.ServiceID == execution.ServiceID {
			selected = member
			memberBound = true
			break
		}
	}
	if !memberBound {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Script execution is outside the candidate manifest")
	}
	intentKey := releaseIntentStagingKey(publicationID, selected.ReleaseID)
	intentRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{intentKey}, Revision: revision})
	if err != nil {
		return nil, err
	}
	if intentRead == nil || intentRead.ReadRevision != revision ||
		len(intentRead.Values) != 1 || intentRead.Values[0] == nil {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Script candidate Intent is unavailable")
	}
	intent, err := decodeReleaseRecord[domain.Intent](intentRead.Values[0].Value, "release-intent")
	intentDigest, _ := domain.Digest(intent)
	if err != nil || domain.ValidateIntent(intent) != nil || intentDigest != selected.IntentDigest ||
		intent.ID != execution.ReleaseID || intent.ServiceID != execution.ServiceID ||
		intent.EnvironmentID != execution.EnvironmentID ||
		intent.OperationID != task.OperationID || intent.OperationKind != domain.OperationBlueprintApply {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Script candidate Intent changed")
	}
	return []etcdstore.Condition{
		{Key: keys[0], ModRevision: read.Values[0].ModRevision},
		{Key: keys[1], ModRevision: read.Values[1].ModRevision},
		{Key: intentKey, ModRevision: intentRead.Values[0].ModRevision},
	}, nil
}

func validateScriptBodyReference(value []byte, execution ScriptExecutionRecord) error {
	reference, err := recordcodec.Decode[struct {
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
	stored, err := recordcodec.Decode[storedScriptRecord](value, "script")
	if err != nil || stored.Desired.ID != scriptID || stored.ActiveReferences == 0 {
		return nil, corruptReleaseRecord()
	}
	stored.ActiveReferences--
	encoded, err := recordcodec.Encode("script", stored)
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
	releaseTask := task.Type == TaskDeploy || task.Type == TaskRollback ||
		(task.Type == TaskUpdate && blueprintScriptTaskShape(task) &&
			validated.Operation == agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY &&
			validated.TargetId == task.Target)
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
			SnapshotSHA256: hex.EncodeToString(
				run.RunnerSnapshotSha256,
			), BodySHA256: hex.EncodeToString(run.BodySha256),
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
	return validateStoredScriptContext(sources, execution)
}

func scriptExecutionProjectionConditions(sources ScriptExecutionSources) []etcdstore.Condition {
	conditions := []etcdstore.Condition{
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
		conditions = append(conditions, etcdstore.Condition{
			Key: deletionTombstoneKey(string(DeletionTargetZone), network.Record.Desired.ID),
		})
	}
	conditions = append(conditions, scriptAttachSourceConditions(sources.AttachSources)...)
	return conditions
}

func validateScriptExecutionRecord(record ScriptExecutionRecord) error {
	if (record.SourceMembershipCount == 0) != (record.SourceMembershipSHA256 == "") ||
		(record.SourceMembershipCount > 0 && !validLowerSHA256(record.SourceMembershipSHA256)) {
		return errs.New(errs.KindValidationFailed, "Script execution source membership is invalid")
	}
	if !validRawScriptExecutionID(record.ID) || !validRawScriptExecutionID(record.SnapshotID) ||
		ids.Validate(ids.KindOperation, record.OperationID) != nil ||
		ids.Validate(ids.KindTask, record.CurrentTaskID) != nil || ids.Validate(ids.KindStep, record.StepID) != nil ||
		ids.Validate(ids.KindScript, record.ScriptID) != nil ||
		record.ScriptGeneration == 0 || ids.Validate(ids.KindEnvironment, record.EnvironmentID) != nil ||
		ids.Validate(
			ids.KindService,
			record.ServiceID,
		) != nil || ids.Validate(ids.KindDeployment, record.ReleaseID) != nil ||
		record.RenderGeneration == 0 || !validLowerSHA256(record.PlanHash) || !validLowerSHA256(record.SnapshotSHA256) ||
		!validLowerSHA256(record.BodySHA256) || !validLowerSHA256(record.RunnerProjectionSHA256) ||
		len(record.Plan) == 0 || len(record.Plan) > executionplan.MaximumPlanBytes || len(record.Snapshot) == 0 ||
		!validScriptExecutionState(
			record.State,
		) || record.CreatedAt.IsZero() || record.UpdatedAt.Before(record.CreatedAt) ||
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

func scriptExecutionKey(executionID string) string { return scriptExecutionPrefix + executionID }

func scriptRunnerSnapshotKey(snapshotID string) string {
	return scriptRunnerSnapshotPrefix + snapshotID
}

func scriptSetBodyForwardReferenceKey(
	environmentID, setGeneration, scriptID string,
	generation uint64,
	executionID string,
) string {
	return scriptSetBodyGenerationKey(
		environmentID,
		setGeneration,
		scriptID,
		generation,
	) + scriptBodyForwardRefSegment + executionID
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
