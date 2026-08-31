package etcd

import (
	"bytes"
	"context"
	"encoding/hex"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ScriptOutcomeReason string

const (
	ScriptOutcomeNormalExit               ScriptOutcomeReason = "normal_exit"
	ScriptOutcomeStartFailure             ScriptOutcomeReason = "start_failure"
	ScriptOutcomeRuntimeFailure           ScriptOutcomeReason = "runtime_failure"
	ScriptOutcomeTimeout                  ScriptOutcomeReason = "timeout"
	ScriptOutcomeAbort                    ScriptOutcomeReason = "abort"
	ScriptOutcomeAbortBeforeStart         ScriptOutcomeReason = "abort_before_start"
	ScriptOutcomeExpiryBeforeStart        ScriptOutcomeReason = "expiry_before_start"
	ScriptOutcomeNoServingRelease         ScriptOutcomeReason = "no_serving_release"
	ScriptOutcomeRecoveryInvariantFailure ScriptOutcomeReason = "recovery_invariant_failure"
)

type ScriptStartAuthorizedEvidence struct{}

type ScriptBodyPreparedEvidence struct {
	BodySHA256 string `json:"body_sha256"`
	UID        uint32 `json:"uid"`
	GID        uint32 `json:"gid"`
	Device     uint64 `json:"device"`
	Inode      uint64 `json:"inode"`
	Leaf       string `json:"leaf"`
}

type ScriptContainerCreatedEvidence struct {
	ContainerID           string `json:"container_id"`
	OwnershipLabelsSHA256 string `json:"ownership_labels_sha256"`
}

type ScriptOutcomeEvidence struct {
	Reason          ScriptOutcomeReason `json:"reason"`
	ExitCode        *int32              `json:"exit_code,omitempty"`
	OutputTruncated bool                `json:"output_truncated"`
	ObservedAt      time.Time           `json:"observed_at"`
}

type ScriptCleanupEvidence struct {
	ContainerID              string `json:"container_id,omitempty"`
	BodyDevice               uint64 `json:"body_device"`
	BodyInode                uint64 `json:"body_inode"`
	BodyLeaf                 string `json:"body_leaf"`
	ContainerAbsent          bool   `json:"container_absent"`
	BodyAbsent               bool   `json:"body_absent"`
	ExecutionDirectoryAbsent bool   `json:"execution_directory_absent"`
}

// ScriptCheckpointEvidenceKind is the stable discriminator for one typed
// Script checkpoint payload. It is deliberately separate from
// ScriptExecutionState: a checkpoint carries one evidence member, while the
// durable record carries the accumulated state after applying it.
type ScriptCheckpointEvidenceKind string

const (
	ScriptCheckpointEvidenceStartAuthorized  ScriptCheckpointEvidenceKind = "start_authorized"
	ScriptCheckpointEvidenceBodyPrepared     ScriptCheckpointEvidenceKind = "body_prepared"
	ScriptCheckpointEvidenceContainerCreated ScriptCheckpointEvidenceKind = "container_created"
	ScriptCheckpointEvidenceOutcome          ScriptCheckpointEvidenceKind = "outcome"
	ScriptCheckpointEvidenceCleanup          ScriptCheckpointEvidenceKind = "cleanup"
)

// ScriptCheckpointEvidence is the concrete, typed checkpoint codec boundary.
// Exactly one member must be non-nil and it must match Kind. The members are
// pointers so an empty start-authorized evidence value remains distinguishable
// from an absent payload without introducing a local interface or dynamic
// recovery path.
type ScriptCheckpointEvidence struct {
	Kind             ScriptCheckpointEvidenceKind
	StartAuthorized  *ScriptStartAuthorizedEvidence
	BodyPrepared     *ScriptBodyPreparedEvidence
	ContainerCreated *ScriptContainerCreatedEvidence
	Outcome          *ScriptOutcomeEvidence
	Cleanup          *ScriptCleanupEvidence
}

type ScriptCheckpointInput struct {
	TaskID          string
	OperationID     string
	AssignmentID    string
	AgentID         string
	AgentGeneration uint64
	StepID          string
	ExecutionID     string
	PlanHash        string
	ExpectedState   ScriptExecutionState
	State           ScriptExecutionState
	PayloadSHA256   string
	Evidence        ScriptCheckpointEvidence
	At              time.Time
}

type scriptCheckpointAnchor struct {
	record     ScriptExecutionRecord
	revision   int64
	read       int64
	conditions []Condition
}

func (repository *ScriptRepository) GetScriptExecution(
	ctx context.Context,
	executionID string,
) (Versioned[ScriptExecutionRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[ScriptExecutionRecord]{}, err
	}
	if repository == nil || repository.store == nil || !validRawScriptExecutionID(executionID) {
		return Versioned[ScriptExecutionRecord]{}, errs.New(errs.KindValidationFailed, "Script execution request is invalid")
	}
	result, err := repository.store.Get(ctx, scriptExecutionKey(executionID))
	if err != nil {
		return Versioned[ScriptExecutionRecord]{}, err
	}
	if result == nil || result.Entry == nil {
		return Versioned[ScriptExecutionRecord]{}, errs.New(errs.KindStateConflict, "Script execution record is missing")
	}
	defer clear(result.Entry.Value)
	record, err := decodeEnvelope[ScriptExecutionRecord](result.Entry.Value, "script-execution")
	if err != nil || validateScriptExecutionRecord(record) != nil || record.ID != executionID {
		return Versioned[ScriptExecutionRecord]{}, errs.New(errs.KindInternal, "Script execution record is corrupt")
	}
	return Versioned[ScriptExecutionRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}

func (repository *ScriptRepository) CheckpointScriptExecution(
	ctx context.Context,
	input ScriptCheckpointInput,
) (Versioned[ScriptExecutionRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[ScriptExecutionRecord]{}, err
	}
	if repository == nil || repository.store == nil || validateScriptCheckpointInput(input) != nil {
		return Versioned[ScriptExecutionRecord]{}, errs.New(errs.KindValidationFailed, "Script checkpoint input is invalid")
	}
	for attempt := 0; attempt < 2; attempt++ {
		anchor, err := repository.loadScriptCheckpointAnchor(ctx, input)
		if err != nil {
			return Versioned[ScriptExecutionRecord]{}, err
		}
		if anchor.record.State == input.State && anchor.record.LastCheckpointSHA256 == input.PayloadSHA256 {
			return Versioned[ScriptExecutionRecord]{
				Record: anchor.record, Revision: anchor.revision, ReadRevision: anchor.read,
			}, nil
		}
		next, err := advanceScriptExecutionRecord(anchor.record, input)
		if err != nil {
			return Versioned[ScriptExecutionRecord]{}, err
		}
		value, err := encodeEnvelope("script-execution", next)
		if err != nil {
			return Versioned[ScriptExecutionRecord]{}, err
		}
		transaction, err := repository.store.Transact(ctx, anchor.conditions, []Mutation{{
			Type: MutationPut, Key: scriptExecutionKey(input.ExecutionID), Value: value,
		}})
		clear(value)
		clearKeyValues(transaction.FailureReads)
		if err != nil {
			return Versioned[ScriptExecutionRecord]{}, err
		}
		if transaction.Succeeded {
			return Versioned[ScriptExecutionRecord]{
				Record: next, Revision: transaction.Revision, ReadRevision: transaction.Revision,
			}, nil
		}
	}
	return Versioned[ScriptExecutionRecord]{}, errs.New(errs.KindStateConflict, "Script checkpoint changed concurrently")
}

func (repository *ScriptRepository) loadScriptCheckpointAnchor(
	ctx context.Context,
	input ScriptCheckpointInput,
) (scriptCheckpointAnchor, error) {
	primary, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		scriptExecutionKey(input.ExecutionID), taskKey(input.TaskID), taskAssignmentIndexKey(input.TaskID),
	}})
	if err != nil {
		return scriptCheckpointAnchor{}, err
	}
	if primary == nil || len(primary.Values) != 3 || primary.Values[0] == nil ||
		primary.Values[1] == nil || primary.Values[2] == nil {
		clearKeyValues(primary.Values)
		return scriptCheckpointAnchor{}, errs.New(errs.KindStateConflict, "Script checkpoint assignment is unavailable")
	}
	defer clearKeyValues(primary.Values)
	execution, executionErr := decodeEnvelope[ScriptExecutionRecord](primary.Values[0].Value, "script-execution")
	task, taskErr := decodeTaskRecord(primary.Values[1].Value)
	assignment, assignmentErr := decodeTaskAssignment(primary.Values[2].Value)
	if executionErr != nil || taskErr != nil || assignmentErr != nil || validateScriptExecutionRecord(execution) != nil {
		return scriptCheckpointAnchor{}, errs.New(errs.KindInternal, "Script checkpoint durable state is corrupt")
	}
	if execution.ID != input.ExecutionID || execution.CurrentTaskID != input.TaskID ||
		execution.OperationID != input.OperationID || execution.StepID != input.StepID || execution.PlanHash != input.PlanHash ||
		(execution.AssignmentID != "" && execution.AssignmentID != input.AssignmentID) ||
		task.ID != input.TaskID || task.OperationID != input.OperationID ||
		task.Status != TaskStatusRunning || task.Executor != TaskExecutorAgent || task.PlanHash != input.PlanHash ||
		!taskOwnsScriptExecution(task, execution) || assignment.TaskID != input.TaskID ||
		assignment.AssignmentID != input.AssignmentID || assignment.Executor != TaskExecutorAgent ||
		assignment.AgentID != input.AgentID || assignment.AgentGeneration != input.AgentGeneration {
		return scriptCheckpointAnchor{}, errs.New(errs.KindStateConflict, "Script checkpoint does not own the running assignment")
	}
	claimKey := taskExecutionClaimKey(TaskExecutorAgent, input.AgentID, input.TaskID)
	timeoutKey := taskTimeoutIndexKey(input.TaskID, assignment.Deadline)
	claim, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{claimKey, timeoutKey}, Revision: primary.ReadRevision,
	})
	if err != nil {
		return scriptCheckpointAnchor{}, err
	}
	if claim == nil || len(claim.Values) != 2 || claim.Values[0] == nil || claim.Values[1] == nil {
		clearKeyValues(claim.Values)
		return scriptCheckpointAnchor{}, errs.New(errs.KindStateConflict, "Script checkpoint execution claim changed")
	}
	defer clearKeyValues(claim.Values)
	if primary.Values[2].ModRevision != claim.Values[0].ModRevision ||
		primary.Values[2].ModRevision != claim.Values[1].ModRevision ||
		!bytes.Equal(primary.Values[2].Value, claim.Values[0].Value) ||
		!bytes.Equal(primary.Values[2].Value, claim.Values[1].Value) {
		return scriptCheckpointAnchor{}, errs.New(errs.KindInternal, "Script checkpoint assignment copies diverged")
	}
	return scriptCheckpointAnchor{
		record: execution, revision: primary.Values[0].ModRevision, read: primary.ReadRevision,
		conditions: []Condition{
			{Key: scriptExecutionKey(input.ExecutionID), ModRevision: primary.Values[0].ModRevision},
			{Key: taskKey(input.TaskID), ModRevision: primary.Values[1].ModRevision},
			{Key: taskAssignmentIndexKey(input.TaskID), ModRevision: primary.Values[2].ModRevision},
			{Key: claimKey, ModRevision: claim.Values[0].ModRevision},
			{Key: timeoutKey, ModRevision: claim.Values[1].ModRevision},
		},
	}, nil
}

func taskOwnsScriptExecution(task TaskRecord, execution ScriptExecutionRecord) bool {
	switch task.Type {
	case TaskScript:
		return task.Target == execution.ScriptID && len(task.Steps) == 1 &&
			task.Steps[0].ID == execution.StepID &&
			task.Params[ScriptExecutionIDParam] == execution.ID
	case TaskDeploy, TaskRollback:
		if task.Owner.EnvironmentID != execution.EnvironmentID ||
			task.Params[TaskReleasePublicationParam] == "" {
			return false
		}
		for _, step := range task.Steps {
			if step.ID == execution.StepID {
				return true
			}
		}
	}
	return false
}

func advanceScriptExecutionRecord(
	current ScriptExecutionRecord,
	input ScriptCheckpointInput,
) (ScriptExecutionRecord, error) {
	if current.State != input.ExpectedState {
		return ScriptExecutionRecord{}, errs.New(errs.KindStateConflict, "Script checkpoint expected state does not match")
	}
	next := current
	if next.AssignmentID == "" {
		next.AssignmentID = input.AssignmentID
	}
	switch input.Evidence.Kind {
	case ScriptCheckpointEvidenceStartAuthorized:
		if input.Evidence.StartAuthorized == nil || input.State != ScriptExecutionStartAuthorized ||
			current.State != ScriptExecutionNotStarted {
			return ScriptExecutionRecord{}, invalidScriptCheckpointTransition()
		}
		next.StartAuthorized = true
	case ScriptCheckpointEvidenceBodyPrepared:
		if input.Evidence.BodyPrepared == nil {
			return ScriptExecutionRecord{}, invalidScriptCheckpointTransition()
		}
		if input.State != ScriptExecutionBodyPrepared || current.State != ScriptExecutionStartAuthorized {
			return ScriptExecutionRecord{}, invalidScriptCheckpointTransition()
		}
		evidence := *input.Evidence.BodyPrepared
		next.BodyPrepared = &evidence
	case ScriptCheckpointEvidenceContainerCreated:
		if input.Evidence.ContainerCreated == nil {
			return ScriptExecutionRecord{}, invalidScriptCheckpointTransition()
		}
		if input.State != ScriptExecutionContainerCreated || current.State != ScriptExecutionBodyPrepared {
			return ScriptExecutionRecord{}, invalidScriptCheckpointTransition()
		}
		evidence := *input.Evidence.ContainerCreated
		next.ContainerCreated = &evidence
	case ScriptCheckpointEvidenceOutcome:
		if input.Evidence.Outcome == nil {
			return ScriptExecutionRecord{}, invalidScriptCheckpointTransition()
		}
		evidence := *input.Evidence.Outcome
		if evidence.ExitCode != nil {
			exitCode := *evidence.ExitCode
			evidence.ExitCode = &exitCode
		}
		if input.State != ScriptExecutionOutcomeRecorded || !validScriptOutcomeTransition(current.State, evidence) {
			return ScriptExecutionRecord{}, invalidScriptCheckpointTransition()
		}
		next.Outcome = &evidence
		next.ReconciliationRequired = evidence.Reason == ScriptOutcomeRecoveryInvariantFailure
	case ScriptCheckpointEvidenceCleanup:
		if input.Evidence.Cleanup == nil {
			return ScriptExecutionRecord{}, invalidScriptCheckpointTransition()
		}
		if input.State != ScriptExecutionCleanupProven || current.State != ScriptExecutionOutcomeRecorded {
			return ScriptExecutionRecord{}, invalidScriptCheckpointTransition()
		}
		evidence := *input.Evidence.Cleanup
		next.Cleanup = &evidence
	default:
		return ScriptExecutionRecord{}, invalidScriptCheckpointTransition()
	}
	next.State = input.State
	next.LastCheckpointSHA256 = input.PayloadSHA256
	next.UpdatedAt = input.At.UTC()
	if !next.UpdatedAt.After(current.UpdatedAt) {
		next.UpdatedAt = current.UpdatedAt.Add(time.Nanosecond)
	}
	if err := validateScriptExecutionRecord(next); err != nil {
		return ScriptExecutionRecord{}, err
	}
	return next, nil
}

func validateScriptCheckpointInput(input ScriptCheckpointInput) error {
	if ids.Validate(ids.KindTask, input.TaskID) != nil || ids.Validate(ids.KindOperation, input.OperationID) != nil ||
		ids.Validate(ids.KindAssignment, input.AssignmentID) != nil || ids.Validate(ids.KindAgent, input.AgentID) != nil ||
		input.AgentGeneration == 0 || ids.Validate(ids.KindStep, input.StepID) != nil ||
		!validRawScriptExecutionID(input.ExecutionID) || !validLowerSHA256(input.PlanHash) ||
		!validLowerSHA256(input.PayloadSHA256) || !validScriptExecutionState(input.ExpectedState) ||
		!validScriptExecutionState(input.State) || !validScriptCheckpointEvidenceShape(input.Evidence) || input.At.IsZero() {
		return errs.New(errs.KindValidationFailed, "Script checkpoint input is invalid")
	}
	return nil
}

func validScriptCheckpointEvidenceShape(evidence ScriptCheckpointEvidence) bool {
	switch evidence.Kind {
	case ScriptCheckpointEvidenceStartAuthorized:
		return evidence.StartAuthorized != nil && evidence.BodyPrepared == nil && evidence.ContainerCreated == nil &&
			evidence.Outcome == nil && evidence.Cleanup == nil
	case ScriptCheckpointEvidenceBodyPrepared:
		return evidence.StartAuthorized == nil && evidence.BodyPrepared != nil && evidence.ContainerCreated == nil &&
			evidence.Outcome == nil && evidence.Cleanup == nil
	case ScriptCheckpointEvidenceContainerCreated:
		return evidence.StartAuthorized == nil && evidence.BodyPrepared == nil && evidence.ContainerCreated != nil &&
			evidence.Outcome == nil && evidence.Cleanup == nil
	case ScriptCheckpointEvidenceOutcome:
		return evidence.StartAuthorized == nil && evidence.BodyPrepared == nil && evidence.ContainerCreated == nil &&
			evidence.Outcome != nil && evidence.Cleanup == nil
	case ScriptCheckpointEvidenceCleanup:
		return evidence.StartAuthorized == nil && evidence.BodyPrepared == nil && evidence.ContainerCreated == nil &&
			evidence.Outcome == nil && evidence.Cleanup != nil
	default:
		return false
	}
}

func validateScriptExecutionCheckpointShape(record ScriptExecutionRecord) error {
	if record.State == ScriptExecutionNotStarted {
		if record.AssignmentID != "" || record.StartAuthorized || record.BodyPrepared != nil ||
			record.ContainerCreated != nil || record.Outcome != nil || record.Cleanup != nil ||
			record.LastCheckpointSHA256 != "" || record.ReconciliationRequired {
			return invalidScriptCheckpointRecord()
		}
		return nil
	}
	if ids.Validate(ids.KindAssignment, record.AssignmentID) != nil || !validLowerSHA256(record.LastCheckpointSHA256) ||
		(record.BodyPrepared != nil && !record.StartAuthorized) ||
		(record.ContainerCreated != nil && record.BodyPrepared == nil) ||
		(record.Cleanup != nil && record.Outcome == nil) {
		return invalidScriptCheckpointRecord()
	}
	if record.BodyPrepared != nil && (record.BodyPrepared.BodySHA256 != record.BodySHA256 ||
		record.BodyPrepared.Device == 0 || record.BodyPrepared.Inode == 0 || record.BodyPrepared.Leaf != "body") {
		return invalidScriptCheckpointRecord()
	}
	if record.ContainerCreated != nil && (!validScriptContainerID(record.ContainerCreated.ContainerID) ||
		!validLowerSHA256(record.ContainerCreated.OwnershipLabelsSHA256)) {
		return invalidScriptCheckpointRecord()
	}
	if record.Outcome != nil && (!validScriptOutcomeEvidence(*record.Outcome) ||
		record.ReconciliationRequired != (record.Outcome.Reason == ScriptOutcomeRecoveryInvariantFailure)) {
		return invalidScriptCheckpointRecord()
	}
	if record.Cleanup != nil && (!record.Cleanup.ContainerAbsent || !record.Cleanup.BodyAbsent ||
		!record.Cleanup.ExecutionDirectoryAbsent ||
		(record.ContainerCreated == nil && record.Cleanup.ContainerID != "") ||
		(record.ContainerCreated != nil && record.Cleanup.ContainerID != record.ContainerCreated.ContainerID) ||
		(record.BodyPrepared == nil && (record.Cleanup.BodyDevice != 0 || record.Cleanup.BodyInode != 0 || record.Cleanup.BodyLeaf != "")) ||
		(record.BodyPrepared != nil && (record.Cleanup.BodyDevice != record.BodyPrepared.Device ||
			record.Cleanup.BodyInode != record.BodyPrepared.Inode || record.Cleanup.BodyLeaf != record.BodyPrepared.Leaf))) {
		return invalidScriptCheckpointRecord()
	}
	switch record.State {
	case ScriptExecutionStartAuthorized:
		if !record.StartAuthorized || record.BodyPrepared != nil || record.ContainerCreated != nil || record.Outcome != nil || record.Cleanup != nil {
			return invalidScriptCheckpointRecord()
		}
	case ScriptExecutionBodyPrepared:
		if !record.StartAuthorized || record.BodyPrepared == nil || record.ContainerCreated != nil || record.Outcome != nil || record.Cleanup != nil {
			return invalidScriptCheckpointRecord()
		}
	case ScriptExecutionContainerCreated:
		if !record.StartAuthorized || record.BodyPrepared == nil || record.ContainerCreated == nil || record.Outcome != nil || record.Cleanup != nil {
			return invalidScriptCheckpointRecord()
		}
	case ScriptExecutionOutcomeRecorded:
		if record.Outcome == nil || record.Cleanup != nil {
			return invalidScriptCheckpointRecord()
		}
	case ScriptExecutionCleanupProven:
		if record.Outcome == nil || record.Cleanup == nil {
			return invalidScriptCheckpointRecord()
		}
	default:
		return invalidScriptCheckpointRecord()
	}
	return nil
}

func validScriptOutcomeTransition(from ScriptExecutionState, evidence ScriptOutcomeEvidence) bool {
	if !validScriptOutcomeEvidence(evidence) {
		return false
	}
	if evidence.Reason == ScriptOutcomeNormalExit {
		return from == ScriptExecutionContainerCreated
	}
	switch from {
	case ScriptExecutionNotStarted:
		return evidence.Reason == ScriptOutcomeAbortBeforeStart || evidence.Reason == ScriptOutcomeExpiryBeforeStart ||
			evidence.Reason == ScriptOutcomeNoServingRelease || evidence.Reason == ScriptOutcomeRecoveryInvariantFailure
	case ScriptExecutionStartAuthorized, ScriptExecutionBodyPrepared:
		return evidence.Reason == ScriptOutcomeStartFailure || evidence.Reason == ScriptOutcomeTimeout ||
			evidence.Reason == ScriptOutcomeAbortBeforeStart || evidence.Reason == ScriptOutcomeRecoveryInvariantFailure
	case ScriptExecutionContainerCreated:
		return evidence.Reason == ScriptOutcomeRuntimeFailure || evidence.Reason == ScriptOutcomeTimeout ||
			evidence.Reason == ScriptOutcomeAbort || evidence.Reason == ScriptOutcomeRecoveryInvariantFailure
	default:
		return false
	}
}

func validScriptOutcomeEvidence(evidence ScriptOutcomeEvidence) bool {
	if evidence.ObservedAt.IsZero() {
		return false
	}
	if evidence.Reason == ScriptOutcomeNormalExit {
		return evidence.ExitCode != nil && *evidence.ExitCode >= 0
	}
	if evidence.ExitCode != nil {
		return false
	}
	switch evidence.Reason {
	case ScriptOutcomeStartFailure, ScriptOutcomeRuntimeFailure, ScriptOutcomeTimeout, ScriptOutcomeAbort,
		ScriptOutcomeAbortBeforeStart, ScriptOutcomeExpiryBeforeStart, ScriptOutcomeNoServingRelease,
		ScriptOutcomeRecoveryInvariantFailure:
		return true
	default:
		return false
	}
}

func validScriptContainerID(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}

func invalidScriptCheckpointTransition() error {
	return errs.New(errs.KindStateConflict, "Script checkpoint transition is invalid")
}

func invalidScriptCheckpointRecord() error {
	return errs.New(errs.KindValidationFailed, "Script execution checkpoint evidence is invalid")
}
