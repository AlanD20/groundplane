package taskassignments

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const releaseRecoveryPrefix = "/v1/records/release-recoveries/"

type TaskExecutionMode string

const (
	TaskExecutionModeForward      TaskExecutionMode = "forward"
	TaskExecutionModeRecoveryOnly TaskExecutionMode = "recovery_only"
)

type ReleaseRestorationTarget string

const (
	ReleaseRestorationServingPredecessor ReleaseRestorationTarget = "serving_predecessor"
	ReleaseRestorationCandidateAbsence   ReleaseRestorationTarget = "candidate_absence"
)

type ReleaseRestorationCandidate struct {
	ServiceID string                   `json:"service_id"`
	ReleaseID string                   `json:"release_id"`
	Target    ReleaseRestorationTarget `json:"target"`
}

type ReleaseAppliedPredecessorAuthority struct {
	KeyRevision           int64  `json:"key_revision"`
	RevisionID            string `json:"revision_id"`
	RenderGeneration      uint64 `json:"render_generation"`
	ComposeArtifactSHA256 string `json:"compose_artifact_sha256"`
	ComposeArtifact       []byte `json:"compose_artifact"`
}

// ReleaseRestorationAuthority is the one concrete claim-time authority. It is
// embedded byte-identically in every assignment copy and is never duplicated
// into a recovery record.
type ReleaseRestorationAuthority struct {
	Schema              int                                 `json:"schema"`
	TaskID              string                              `json:"task_id"`
	OperationID         string                              `json:"operation_id"`
	PlanHash            string                              `json:"plan_hash"`
	EnvironmentID       string                              `json:"environment_id"`
	CandidateArtifactID string                              `json:"candidate_artifact_id"`
	Candidates          []ReleaseRestorationCandidate       `json:"candidates"`
	AppliedPredecessor  *ReleaseAppliedPredecessorAuthority `json:"applied_predecessor,omitempty"`
	NativePredecessors  []ReleaseNativePredecessorAuthority `json:"native_predecessors,omitempty"`
}

type ReleaseRecoveryPhase string

const (
	ReleaseRecoveryPhaseProbe      ReleaseRecoveryPhase = "probe"
	ReleaseRecoveryPhaseCompensate ReleaseRecoveryPhase = "compensate"
	ReleaseRecoveryPhaseProven     ReleaseRecoveryPhase = "proven"
)

type ReleaseRecoveryRecord struct {
	Schema                     int                               `json:"schema"`
	TaskID                     string                            `json:"task_id"`
	AssignmentID               string                            `json:"assignment_id"`
	OperationID                string                            `json:"operation_id"`
	PlanHash                   string                            `json:"plan_hash"`
	RestorationAuthoritySHA256 string                            `json:"restoration_authority_sha256"`
	PrimaryReportSHA256        string                            `json:"primary_report_sha256"`
	PrimaryStatus              taskjournal.TaskStatus            `json:"primary_status"`
	PrimaryResult              taskjournal.TaskResultRecord      `json:"primary_result"`
	RecoveryDeadline           time.Time                         `json:"recovery_deadline"`
	MutationEvidence           []ReleaseRecoveryMutationEvidence `json:"mutation_evidence"`
	RecoveryStepIDs            []string                          `json:"recovery_step_ids"`
	Cursor                     uint32                            `json:"cursor"`
	Phase                      ReleaseRecoveryPhase              `json:"phase"`
	EvidenceRevision           int64                             `json:"evidence_revision"`
}

type ReleaseRecoveryMutationEvidence struct {
	StepID    string `json:"step_id"`
	Running   bool   `json:"running"`
	Completed bool   `json:"completed"`
}

func ReleaseRecoveryKey(taskID string) string { return releaseRecoveryPrefix + taskID }

func ReleaseRestorationAuthoritySHA256(authority ReleaseRestorationAuthority) (string, error) {
	if err := ValidateReleaseRestorationAuthority(authority); err != nil {
		return "", err
	}
	return domain.Digest(authority)
}

func ValidateReleaseRestorationAuthority(authority ReleaseRestorationAuthority) error {
	if authority.Schema != 1 || ids.Validate(ids.KindTask, authority.TaskID) != nil ||
		ids.Validate(ids.KindOperation, authority.OperationID) != nil || !recordcodec.ValidSHA256(authority.PlanHash) ||
		ids.Validate(ids.KindEnvironment, authority.EnvironmentID) != nil ||
		ids.Validate(ids.KindConfig, authority.CandidateArtifactID) != nil || len(authority.Candidates) == 0 {
		return CorruptTaskAssignment()
	}
	previous := ""
	for _, candidate := range authority.Candidates {
		identity := candidate.ServiceID
		if ids.Validate(ids.KindService, candidate.ServiceID) != nil ||
			ids.Validate(ids.KindDeployment, candidate.ReleaseID) != nil ||
			identity <= previous {
			return CorruptTaskAssignment()
		}
		previous = identity
		if candidate.Target != ReleaseRestorationServingPredecessor &&
			candidate.Target != ReleaseRestorationCandidateAbsence {
			return CorruptTaskAssignment()
		}
	}
	if predecessor := authority.AppliedPredecessor; predecessor != nil {
		if predecessor.KeyRevision <= 0 || ids.Validate(ids.KindTask, predecessor.RevisionID) != nil ||
			predecessor.RenderGeneration == 0 || !recordcodec.ValidSHA256(predecessor.ComposeArtifactSHA256) ||
			len(
				predecessor.ComposeArtifact,
			) == 0 || len(predecessor.ComposeArtifact) > taskjournal.MaximumTaskRecordBytes {
			return CorruptTaskAssignment()
		}
		digest := sha256.Sum256(predecessor.ComposeArtifact)
		if hex.EncodeToString(digest[:]) != predecessor.ComposeArtifactSHA256 {
			return CorruptTaskAssignment()
		}
	}
	return validateNativeRestorationMemberWitness(authority)
}

func validateReleaseRecoveryRecord(record ReleaseRecoveryRecord) error {
	if record.Schema != 1 || ids.Validate(ids.KindTask, record.TaskID) != nil ||
		ids.Validate(
			ids.KindAssignment,
			record.AssignmentID,
		) != nil || ids.Validate(ids.KindOperation, record.OperationID) != nil ||
		!recordcodec.ValidSHA256(record.PlanHash) || !recordcodec.ValidSHA256(record.RestorationAuthoritySHA256) ||
		!recordcodec.ValidSHA256(
			record.PrimaryReportSHA256,
		) || record.PrimaryStatus == taskjournal.TaskStatusCompleted ||
		!ValidTerminalTaskStatus(record.PrimaryStatus) || len(record.RecoveryStepIDs) == 0 ||
		!record.RecoveryDeadline.Equal(record.RecoveryDeadline.UTC()) || record.RecoveryDeadline.IsZero() ||
		int(record.Cursor) > len(record.RecoveryStepIDs) || record.EvidenceRevision <= 0 {
		return errs.New(errs.KindInternal, "release recovery record is corrupt")
	}
	for index, stepID := range record.RecoveryStepIDs {
		if ids.Validate(ids.KindStep, stepID) != nil || slices.Contains(record.RecoveryStepIDs[:index], stepID) {
			return errs.New(errs.KindInternal, "release recovery record is corrupt")
		}
	}
	for index, evidence := range record.MutationEvidence {
		if ids.Validate(ids.KindStep, evidence.StepID) != nil || !evidence.Running ||
			evidence.Completed && !evidence.Running {
			return errs.New(errs.KindInternal, "release recovery record is corrupt")
		}
		for _, prior := range record.MutationEvidence[:index] {
			if prior.StepID == evidence.StepID {
				return errs.New(errs.KindInternal, "release recovery record is corrupt")
			}
		}
	}
	probeCount := len(record.RecoveryStepIDs) / 2
	if probeCount == 0 || len(record.RecoveryStepIDs)%2 != 0 {
		return errs.New(errs.KindInternal, "release recovery record is corrupt")
	}
	switch record.Phase {
	case ReleaseRecoveryPhaseProbe:
		if int(record.Cursor) >= probeCount {
			return errs.New(errs.KindInternal, "release recovery record is corrupt")
		}
	case ReleaseRecoveryPhaseCompensate:
		if int(record.Cursor) < probeCount || int(record.Cursor) >= len(record.RecoveryStepIDs) {
			return errs.New(errs.KindInternal, "release recovery record is corrupt")
		}
	case ReleaseRecoveryPhaseProven:
		if int(record.Cursor) != len(record.RecoveryStepIDs) {
			return errs.New(errs.KindInternal, "release recovery record is corrupt")
		}
	default:
		return errs.New(errs.KindInternal, "release recovery record is corrupt")
	}
	reportDigest, err := domain.Digest(struct {
		Status taskjournal.TaskStatus       `json:"status"`
		Result taskjournal.TaskResultRecord `json:"result"`
	}{record.PrimaryStatus, record.PrimaryResult})
	if err != nil || reportDigest != record.PrimaryReportSHA256 {
		return errs.New(errs.KindInternal, "release recovery record is corrupt")
	}
	return nil
}

func EncodeReleaseRecoveryRecord(record ReleaseRecoveryRecord) ([]byte, error) {
	if err := validateReleaseRecoveryRecord(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("release-recovery", record)
}

func DecodeReleaseRecoveryRecord(value []byte) (ReleaseRecoveryRecord, error) {
	record, err := recordcodec.Decode[ReleaseRecoveryRecord](value, "release-recovery")
	if err != nil || validateReleaseRecoveryRecord(record) != nil {
		return ReleaseRecoveryRecord{}, errs.New(errs.KindInternal, "release recovery record is corrupt")
	}
	return record, nil
}

func ReleaseRecoveryRecordSHA256(record ReleaseRecoveryRecord) (string, error) {
	if err := validateReleaseRecoveryRecord(record); err != nil {
		return "", err
	}
	// Cursor, phase, and their evidence revision are mutable progress. The
	// assignment carries the digest of the immutable recovery authority so one
	// dispatch digest remains valid while AppendTaskEvent advances the record.
	return domain.Digest(struct {
		Schema                     int                               `json:"schema"`
		TaskID                     string                            `json:"task_id"`
		AssignmentID               string                            `json:"assignment_id"`
		OperationID                string                            `json:"operation_id"`
		PlanHash                   string                            `json:"plan_hash"`
		RestorationAuthoritySHA256 string                            `json:"restoration_authority_sha256"`
		PrimaryReportSHA256        string                            `json:"primary_report_sha256"`
		PrimaryStatus              taskjournal.TaskStatus            `json:"primary_status"`
		PrimaryResult              taskjournal.TaskResultRecord      `json:"primary_result"`
		RecoveryDeadline           time.Time                         `json:"recovery_deadline"`
		MutationEvidence           []ReleaseRecoveryMutationEvidence `json:"mutation_evidence"`
		RecoveryStepIDs            []string                          `json:"recovery_step_ids"`
	}{
		record.Schema, record.TaskID, record.AssignmentID, record.OperationID,
		record.PlanHash, record.RestorationAuthoritySHA256, record.PrimaryReportSHA256,
		record.PrimaryStatus, record.PrimaryResult, record.RecoveryDeadline, record.MutationEvidence, record.RecoveryStepIDs,
	})
}

func AdvanceReleaseRecoveryRecord(
	record ReleaseRecoveryRecord,
	input taskjournal.TaskEventInput,
	revision int64,
) (ReleaseRecoveryRecord, bool, error) {
	if err := validateReleaseRecoveryRecord(record); err != nil || record.Phase == ReleaseRecoveryPhaseProven ||
		int(
			record.Cursor,
		) >= len(
			record.RecoveryStepIDs,
		) || input.Identity.StepID != record.RecoveryStepIDs[record.Cursor] {
		return ReleaseRecoveryRecord{}, false, errs.New(
			errs.KindStateConflict,
			"release recovery event is not the canonical next step",
		)
	}
	switch input.State {
	case taskjournal.TaskEventStateRunning,
		taskjournal.TaskEventStateFailed,
		taskjournal.TaskEventStateAborted,
		taskjournal.TaskEventStateTimedOut:
		return record, false, nil
	case taskjournal.TaskEventStateCompleted:
		next := record
		next.Cursor++
		next.EvidenceRevision = revision
		probeCount := uint32(len(next.RecoveryStepIDs) / 2)
		switch {
		case int(next.Cursor) == len(next.RecoveryStepIDs):
			next.Phase = ReleaseRecoveryPhaseProven
		case next.Cursor >= probeCount:
			next.Phase = ReleaseRecoveryPhaseCompensate
		default:
			next.Phase = ReleaseRecoveryPhaseProbe
		}
		if err := validateReleaseRecoveryRecord(next); err != nil {
			return ReleaseRecoveryRecord{}, false, err
		}
		return next, true, nil
	default:
		return ReleaseRecoveryRecord{}, false, errs.New(
			errs.KindStateConflict,
			"release recovery event state is invalid",
		)
	}
}

func ValidTerminalTaskStatus(status taskjournal.TaskStatus) bool {
	switch status {
	case taskjournal.TaskStatusFailed, taskjournal.TaskStatusAborted, taskjournal.TaskStatusTimedOut:
		return true
	default:
		return false
	}
}

func CanonicalPrimaryReportSHA256(status taskjournal.TaskStatus, result taskjournal.TaskResultRecord) (string, error) {
	return domain.Digest(struct {
		Status taskjournal.TaskStatus       `json:"status"`
		Result taskjournal.TaskResultRecord `json:"result"`
	}{status, result})
}

func cloneReleaseRestorationAuthority(value *ReleaseRestorationAuthority) *ReleaseRestorationAuthority {
	if value == nil {
		return nil
	}
	clone := *value
	clone.Candidates = slices.Clone(value.Candidates)
	clone.NativePredecessors = cloneReleaseNativePredecessors(value.NativePredecessors)
	if value.AppliedPredecessor != nil {
		predecessor := *value.AppliedPredecessor
		predecessor.ComposeArtifact = slices.Clone(value.AppliedPredecessor.ComposeArtifact)
		clone.AppliedPredecessor = &predecessor
	}
	return &clone
}
