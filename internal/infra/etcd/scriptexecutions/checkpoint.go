package scriptexecutions

import (
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
	ScriptOutcomeParentFailureBeforeStart ScriptOutcomeReason = "parent_failure_before_start"
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

func AdvanceScriptExecutionRecord(
	current ScriptExecutionRecord,
	input ScriptCheckpointInput,
) (ScriptExecutionRecord, error) {
	if current.State != input.ExpectedState {
		return ScriptExecutionRecord{}, errs.New(
			errs.KindStateConflict,
			"Script checkpoint expected state does not match",
		)
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
	if err := ValidateScriptExecutionRecord(next); err != nil {
		return ScriptExecutionRecord{}, err
	}
	return next, nil
}

func ValidateScriptCheckpointInput(input ScriptCheckpointInput) error {
	if ids.Validate(ids.KindTask, input.TaskID) != nil || ids.Validate(ids.KindOperation, input.OperationID) != nil ||
		ids.Validate(
			ids.KindAssignment,
			input.AssignmentID,
		) != nil || ids.Validate(ids.KindAgent, input.AgentID) != nil ||
		input.AgentGeneration == 0 || ids.Validate(ids.KindStep, input.StepID) != nil ||
		!ValidRawScriptExecutionID(input.ExecutionID) || !ValidLowerSHA256(input.PlanHash) ||
		!ValidLowerSHA256(input.PayloadSHA256) || !validScriptExecutionState(input.ExpectedState) ||
		!validScriptExecutionState(
			input.State,
		) || !validScriptCheckpointEvidenceShape(input.Evidence) || input.At.IsZero() {
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
			record.ControllerCleanup != "" || record.LastCheckpointSHA256 != "" || record.ReconciliationRequired {
			return invalidScriptCheckpointRecord()
		}
		return nil
	}
	if record.State == ScriptExecutionCleanupProven && record.AssignmentID == "" {
		controllerCleanupMatches := record.Outcome != nil &&
			(record.Outcome.Reason == ScriptOutcomeAbortBeforeStart &&
				(record.ControllerCleanup == ScriptControllerCleanupBlueprintPendingAbort ||
					record.ControllerCleanup == ScriptControllerCleanupManualPendingAbort ||
					record.ControllerCleanup == ScriptControllerCleanupManualAssignedAbort) ||
				record.Outcome.Reason == ScriptOutcomeExpiryBeforeStart &&
					record.ControllerCleanup == ScriptControllerCleanupManualRetryExpiry ||
				record.Outcome.Reason == ScriptOutcomeParentFailureBeforeStart &&
					record.ControllerCleanup == ScriptControllerCleanupReleaseRecoveryParentFailure)
		if record.StartAuthorized || record.BodyPrepared != nil || record.ContainerCreated != nil ||
			record.Outcome == nil || !controllerCleanupMatches ||
			record.Outcome.ExitCode != nil || record.Outcome.OutputTruncated || record.Outcome.ObservedAt.IsZero() ||
			record.Cleanup == nil || record.Cleanup.ContainerID != "" || record.Cleanup.BodyDevice != 0 ||
			record.Cleanup.BodyInode != 0 || record.Cleanup.BodyLeaf != "" || !record.Cleanup.ContainerAbsent ||
			!record.Cleanup.BodyAbsent || !record.Cleanup.ExecutionDirectoryAbsent ||
			!ValidLowerSHA256(record.LastCheckpointSHA256) || record.ReconciliationRequired || record.ActiveReference {
			return invalidScriptCheckpointRecord()
		}
		return nil
	}
	if record.ControllerCleanup != "" {
		return invalidScriptCheckpointRecord()
	}
	if ids.Validate(ids.KindAssignment, record.AssignmentID) != nil || !ValidLowerSHA256(record.LastCheckpointSHA256) ||
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
		!ValidLowerSHA256(record.ContainerCreated.OwnershipLabelsSHA256)) {
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
		if !record.StartAuthorized || record.BodyPrepared != nil || record.ContainerCreated != nil ||
			record.Outcome != nil ||
			record.Cleanup != nil {
			return invalidScriptCheckpointRecord()
		}
	case ScriptExecutionBodyPrepared:
		if !record.StartAuthorized || record.BodyPrepared == nil || record.ContainerCreated != nil ||
			record.Outcome != nil ||
			record.Cleanup != nil {
			return invalidScriptCheckpointRecord()
		}
	case ScriptExecutionContainerCreated:
		if !record.StartAuthorized || record.BodyPrepared == nil || record.ContainerCreated == nil ||
			record.Outcome != nil ||
			record.Cleanup != nil {
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
		ScriptOutcomeParentFailureBeforeStart, ScriptOutcomeRecoveryInvariantFailure:
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
