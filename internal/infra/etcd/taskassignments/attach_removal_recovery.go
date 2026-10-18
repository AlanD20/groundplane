package taskassignments

import (
	"math"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const attachRemovalRecoveryPrefix = "/v1/records/attach-removal-recoveries/"

// Authority binds recovery to the original native Detach, not a reconstructed
// Blueprint or a newly chosen backing runtime. Steps contain only journal IDs;
// compiled procedures, secrets and the Compose artifact stay in their pinned plan.
type AttachRemovalRecoveryAuthority struct {
	TaskID           string   `json:"task_id"`
	OperationID      string   `json:"operation_id"`
	PlanHash         string   `json:"plan_hash"`
	EnvironmentID    string   `json:"environment_id"`
	AttachID         string   `json:"attach_id"`
	BackingServiceID string   `json:"backing_service_id"`
	IntentSHA256     string   `json:"intent_sha256"`
	IntentRevision   int64    `json:"intent_revision"`
	StepIDs          []string `json:"step_ids"`
}

// The initial contiguous completion prefix is immutable evidence. Cursor is
// subsequent recovery progress; changing it cannot change dispatch authority.
// Custom hooks retain their captured plan; interrupted execution additionally
// requires an operator authorization owned by the hook checkpoint store.
type AttachRemovalRecoveryRecord struct {
	Schema                 int                               `json:"schema"`
	Authority              AttachRemovalRecoveryAuthority    `json:"authority"`
	AssignmentID           string                            `json:"assignment_id"`
	AgentID                string                            `json:"agent_id"`
	AgentGeneration        uint64                            `json:"agent_generation"`
	OriginalExecutionEpoch uint32                            `json:"original_execution_epoch"`
	PrimaryReportSHA256    string                            `json:"primary_report_sha256"`
	PrimaryStatus          taskjournal.TaskStatus            `json:"primary_status"`
	PrimaryResult          taskjournal.TaskResultData        `json:"primary_result"`
	RecoveryDeadline       time.Time                         `json:"recovery_deadline"`
	MutationEvidence       []ReleaseRecoveryMutationEvidence `json:"mutation_evidence"`
	InitialCursor          uint32                            `json:"initial_cursor"`
	Cursor                 uint32                            `json:"cursor"`
	EvidenceRevision       int64                             `json:"evidence_revision"`
}

func AttachRemovalRecoveryKey(taskID string) string { return attachRemovalRecoveryPrefix + taskID }

func ValidateAttachRemovalRecoveryAuthority(authority AttachRemovalRecoveryAuthority) error {
	if ids.Validate(ids.KindTask, authority.TaskID) != nil ||
		ids.Validate(ids.KindOperation, authority.OperationID) != nil ||
		ids.Validate(ids.KindEnvironment, authority.EnvironmentID) != nil ||
		ids.Validate(ids.KindAttach, authority.AttachID) != nil ||
		ids.Validate(ids.KindService, authority.BackingServiceID) != nil ||
		!recordcodec.ValidSHA256(authority.PlanHash) || !recordcodec.ValidSHA256(authority.IntentSHA256) ||
		authority.IntentRevision <= 0 || len(authority.StepIDs) == 0 ||
		len(authority.StepIDs) > 10 {
		return corruptAttachRemovalRecovery()
	}
	for index, stepID := range authority.StepIDs {
		if ids.Validate(ids.KindStep, stepID) != nil || slices.Contains(authority.StepIDs[:index], stepID) {
			return corruptAttachRemovalRecovery()
		}
	}
	return nil
}

func AttachRemovalRecoveryAuthoritySHA256(authority AttachRemovalRecoveryAuthority) (string, error) {
	if err := ValidateAttachRemovalRecoveryAuthority(authority); err != nil {
		return "", err
	}
	return domain.Digest(authority)
}

// Capture starts at the first incomplete original step. A completed receipt can
// never be replayed; a started but incomplete compiled action stays in the suffix.
// The caller must collect these receipts from the same revision as its intent.
func CaptureAttachRemovalRecovery(
	authority AttachRemovalRecoveryAuthority, assignment TaskAssignmentRecord,
	status taskjournal.TaskStatus, result taskjournal.TaskResultRecord,
	evidence []ReleaseRecoveryMutationEvidence, revision int64,
) (AttachRemovalRecoveryRecord, error) {
	authorityDigest, authorityErr := AttachRemovalRecoveryAuthoritySHA256(authority)
	if assignment.TaskID != authority.TaskID || assignment.Executor != taskjournal.TaskExecutorAgent ||
		assignment.ExecutionMode != TaskExecutionModeForward || result.ExecutionEpoch != assignment.ExecutionEpoch ||
		result.ReleaseRecoveryRecordSHA256 != "" || assignment.RestorationAuthority != nil ||
		assignment.ServiceAbsenceProcedureSHA256 != "" ||
		assignment.ManagedNetworkProcedureSHA256 != "" || assignment.BackupAuthorityFence != nil ||
		authorityErr != nil || authorityDigest != assignment.AttachRemovalAuthoritySHA256 {
		return AttachRemovalRecoveryRecord{}, corruptAttachRemovalRecovery()
	}
	primaryDigest, err := CanonicalPrimaryReportSHA256(status, result)
	if err != nil {
		return AttachRemovalRecoveryRecord{}, err
	}
	owned := authority
	owned.StepIDs = slices.Clone(authority.StepIDs)
	record := AttachRemovalRecoveryRecord{
		Schema: 1, Authority: owned, AssignmentID: assignment.AssignmentID,
		AgentID: assignment.AgentID, AgentGeneration: assignment.AgentGeneration,
		OriginalExecutionEpoch: assignment.ExecutionEpoch, PrimaryReportSHA256: primaryDigest,
		PrimaryStatus: status, PrimaryResult: *taskjournal.TaskResultToData(&result),
		RecoveryDeadline: assignment.RecoveryDeadline,
		MutationEvidence: slices.Clone(evidence), EvidenceRevision: revision,
	}
	for index, item := range record.MutationEvidence {
		if !item.Completed || index >= len(record.Authority.StepIDs) {
			break
		}
		record.InitialCursor++
	}
	record.Cursor = record.InitialCursor
	if err := validateAttachRemovalRecoveryRecord(record); err != nil {
		return AttachRemovalRecoveryRecord{}, err
	}
	return record, nil
}

func validateAttachRemovalRecoveryRecord(record AttachRemovalRecoveryRecord) error {
	if record.Schema != 1 || ValidateAttachRemovalRecoveryAuthority(record.Authority) != nil ||
		ids.Validate(
			ids.KindAssignment,
			record.AssignmentID,
		) != nil || ids.Validate(ids.KindAgent, record.AgentID) != nil ||
		record.AgentGeneration == 0 || record.OriginalExecutionEpoch == 0 || record.OriginalExecutionEpoch == math.MaxUint32 ||
		record.PrimaryStatus == taskjournal.TaskStatusCompleted || !ValidTerminalTaskStatus(record.PrimaryStatus) ||
		!record.PrimaryResult.ReconciliationRequired || record.PrimaryResult.Kind != taskjournal.TaskResultCompose ||
		record.PrimaryResult.ExecutionEpoch != record.OriginalExecutionEpoch || record.PrimaryResult.ReleaseRecoveryRecordSHA256 != "" ||
		!recordcodec.ValidSHA256(record.PrimaryReportSHA256) ||
		record.RecoveryDeadline.IsZero() || record.RecoveryDeadline.Location() != time.UTC ||
		record.EvidenceRevision <= 0 || int(record.InitialCursor) > len(record.Authority.StepIDs) ||
		record.Cursor < record.InitialCursor || int(record.Cursor) > len(record.Authority.StepIDs) ||
		len(record.MutationEvidence) == 0 || len(record.MutationEvidence) > len(record.Authority.StepIDs) {
		return corruptAttachRemovalRecovery()
	}
	primary, err := taskjournal.TaskResultFromData(&record.PrimaryResult)
	if err != nil || primary == nil {
		return corruptAttachRemovalRecovery()
	}
	steps := make([]taskjournal.TaskStepRecord, len(record.Authority.StepIDs))
	for index, stepID := range record.Authority.StepIDs {
		steps[index] = taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: stepID}
	}
	if taskjournal.ValidateTaskResult(*primary, steps, record.PrimaryStatus) != nil {
		return corruptAttachRemovalRecovery()
	}
	digest, err := CanonicalPrimaryReportSHA256(record.PrimaryStatus, *primary)
	if err != nil || digest != record.PrimaryReportSHA256 {
		return corruptAttachRemovalRecovery()
	}
	completed := uint32(0)
	for index, item := range record.MutationEvidence {
		if item.StepID != record.Authority.StepIDs[index] || !item.Running ||
			index < len(record.MutationEvidence)-1 && !item.Completed {
			return corruptAttachRemovalRecovery()
		}
		if item.Completed {
			completed++
		}
	}
	if completed != record.InitialCursor {
		return corruptAttachRemovalRecovery()
	}
	return nil
}

func EncodeAttachRemovalRecoveryRecord(record AttachRemovalRecoveryRecord) ([]byte, error) {
	if err := validateAttachRemovalRecoveryRecord(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("attach-removal-recovery", record)
}

func DecodeAttachRemovalRecoveryRecord(value []byte) (AttachRemovalRecoveryRecord, error) {
	record, err := recordcodec.Decode[AttachRemovalRecoveryRecord](value, "attach-removal-recovery")
	if err != nil || validateAttachRemovalRecoveryRecord(record) != nil {
		return AttachRemovalRecoveryRecord{}, corruptAttachRemovalRecovery()
	}
	return record, nil
}

func AttachRemovalRecoveryRecordSHA256(record AttachRemovalRecoveryRecord) (string, error) {
	if err := validateAttachRemovalRecoveryRecord(record); err != nil {
		return "", err
	}
	// Only progress is excluded. The original error, captured completion prefix,
	// native intent and deadline remain immutable throughout reconnects.
	record.Cursor = record.InitialCursor
	record.EvidenceRevision = 0
	return domain.Digest(record)
}

func ValidateAttachRemovalRecoveryAssignment(
	record AttachRemovalRecoveryRecord,
	assignment TaskAssignmentRecord,
) error {
	digest, digestErr := AttachRemovalRecoveryRecordSHA256(record)
	authorityDigest, authorityErr := AttachRemovalRecoveryAuthoritySHA256(record.Authority)
	if digestErr != nil || authorityErr != nil || digest != assignment.ReleaseRecoveryRecordSHA256 ||
		authorityDigest != assignment.AttachRemovalAuthoritySHA256 ||
		assignment.ExecutionMode != TaskExecutionModeRecoveryOnly || assignment.Executor != taskjournal.TaskExecutorAgent ||
		assignment.TaskID != record.Authority.TaskID || assignment.AssignmentID != record.AssignmentID ||
		assignment.AgentID != record.AgentID || assignment.ExecutionEpoch <= record.OriginalExecutionEpoch ||
		!assignment.RecoveryDeadline.Equal(record.RecoveryDeadline) {
		return corruptAttachRemovalRecovery()
	}
	return nil
}

func AttachRemovalRecoveryDirective(
	record AttachRemovalRecoveryRecord,
	assignment TaskAssignmentRecord,
) (*ReleaseRecoveryDirective, error) {
	if err := ValidateAttachRemovalRecoveryAssignment(record, assignment); err != nil {
		return nil, err
	}
	phase := ReleaseRecoveryPhaseCompensate
	if int(record.Cursor) == len(record.Authority.StepIDs) {
		phase = ReleaseRecoveryPhaseProven
	}
	return &ReleaseRecoveryDirective{
		Phase: phase, StepIDs: slices.Clone(record.Authority.StepIDs), Cursor: record.Cursor,
		RecordSHA256: assignment.ReleaseRecoveryRecordSHA256,
	}, nil
}

// Advance runs within the same transaction as journal append. The assignment
// owner must first verify Agent generation/epoch and exact record digest.
func AdvanceAttachRemovalRecoveryRecord(
	record AttachRemovalRecoveryRecord, assignment TaskAssignmentRecord,
	input taskjournal.TaskEventInput, revision int64,
) (AttachRemovalRecoveryRecord, bool, error) {
	if ValidateAttachRemovalRecoveryAssignment(record, assignment) != nil || revision < record.EvidenceRevision ||
		input.Identity.TaskID != assignment.TaskID || input.Identity.AssignmentID != assignment.AssignmentID ||
		input.Identity.AgentID != assignment.AgentID || input.Identity.AgentGeneration != assignment.AgentGeneration ||
		input.Identity.Attempt != assignment.ExecutionEpoch || input.Identity.Ordinal == 0 {
		return AttachRemovalRecoveryRecord{}, false, corruptAttachRemovalRecovery()
	}
	if int(record.Cursor) == len(record.Authority.StepIDs) ||
		input.Identity.StepID != record.Authority.StepIDs[record.Cursor] {
		return AttachRemovalRecoveryRecord{}, false, errs.New(errs.KindStateConflict,
			"Attach removal recovery event does not identify the next incomplete step")
	}
	switch input.State {
	case taskjournal.TaskEventStateRunning:
		if input.Identity.Ordinal != 1 {
			return AttachRemovalRecoveryRecord{}, false, errs.New(errs.KindStateConflict,
				"Attach removal recovery start ordinal is invalid")
		}
		return record, false, nil
	case taskjournal.TaskEventStateFailed,
		taskjournal.TaskEventStateAborted, taskjournal.TaskEventStateTimedOut:
		if input.Identity.Ordinal != 2 {
			return AttachRemovalRecoveryRecord{}, false, errs.New(errs.KindStateConflict,
				"Attach removal recovery outcome ordinal is invalid")
		}
		return record, false, nil
	case taskjournal.TaskEventStateCompleted:
		if input.Identity.Ordinal != 2 {
			return AttachRemovalRecoveryRecord{}, false, errs.New(errs.KindStateConflict,
				"Attach removal recovery completion must follow its durable start")
		}
		next := record
		next.Cursor++
		next.EvidenceRevision = revision
		return next, true, nil
	default:
		return AttachRemovalRecoveryRecord{}, false, errs.New(errs.KindStateConflict,
			"Attach removal recovery event state is invalid")
	}
}

func corruptAttachRemovalRecovery() error {
	return errs.New(errs.KindInternal, "Attach removal recovery authority is corrupt")
}
