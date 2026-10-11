package taskassignments

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const rollingReleasePrefix = "/v1/records/rolling-release-executions/"

// RollingReleaseRecord retains one member's forward cursor under its Task.
// Plan and deadlines are frozen; unknown in-flight effects cannot be cleared by
// replacing the assignment or reading newer desired state. The publisher must
// bind every referenced replica to the captured singleton Compose artifact.
type RollingReleaseRecord struct {
	TaskID                   string                  `json:"task_id"`
	AssignmentID             string                  `json:"assignment_id"`
	TaskPlanSHA256           string                  `json:"task_plan_sha256"`
	ProcedureSHA256          string                  `json:"procedure_sha256"`
	ExecutionEpoch           uint32                  `json:"execution_epoch"`
	TaskDeadline             time.Time               `json:"task_deadline"`
	Plan                     rollingrelease.Plan     `json:"plan"`
	Progress                 rollingrelease.Progress `json:"progress"`
	Failed                   bool                    `json:"failed"`
	CompletionProven         bool                    `json:"completion_proven"`
	FinalVerificationStarted bool                    `json:"final_verification_started"`
}

func NewRollingReleaseRecord(
	planHash string,
	assignment TaskAssignmentRecord,
	member *agentpb.CandidateReleaseMember,
) (RollingReleaseRecord, error) {
	if err := validateTaskAssignment(assignment); err != nil {
		return RollingReleaseRecord{}, err
	}
	plan, err := executionplan.OpenRollingPlan(member)
	if err != nil {
		return RollingReleaseRecord{}, err
	}
	progress, err := rollingrelease.NewProgress(plan)
	if err != nil {
		return RollingReleaseRecord{}, err
	}
	digest, err := RollingProcedureSHA256(plan)
	if err != nil {
		return RollingReleaseRecord{}, err
	}
	record := RollingReleaseRecord{TaskID: assignment.TaskID, AssignmentID: assignment.AssignmentID,
		TaskPlanSHA256: planHash, ProcedureSHA256: digest, ExecutionEpoch: assignment.ExecutionEpoch,
		TaskDeadline: assignment.Deadline, Plan: plan, Progress: progress}
	return record, ValidateRollingReleaseAssignment(record, assignment)
}

func RollingReleaseKey(taskID, serviceID string) string {
	return RollingReleaseTaskPrefix(taskID) + serviceID
}

func RollingReleaseTaskPrefix(taskID string) string {
	return rollingReleasePrefix + taskID + "/"
}

func RollingProcedureSHA256(plan rollingrelease.Plan) (string, error) {
	if err := plan.Validate(); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func ValidateRollingReleaseRecord(record RollingReleaseRecord) error {
	if ids.Validate(ids.KindTask, record.TaskID) != nil ||
		ids.Validate(ids.KindAssignment, record.AssignmentID) != nil ||
		!recordcodec.ValidSHA256(record.TaskPlanSHA256) ||
		record.ExecutionEpoch == 0 ||
		recordcodec.ValidateTimestamp("rolling Task deadline", record.TaskDeadline) != nil {
		return errs.New(errs.KindValidationFailed, "rolling execution identity is invalid")
	}
	digest, err := RollingProcedureSHA256(record.Plan)
	if err != nil {
		return err
	}
	if record.ProcedureSHA256 != digest {
		return errs.New(errs.KindValidationFailed, "rolling procedure digest does not match its captured plan")
	}
	if record.FinalVerificationStarted && (record.Progress.Phase != "" || record.Progress.InFlight) ||
		record.CompletionProven && (record.Failed || !record.FinalVerificationStarted) {
		return errs.New(
			errs.KindValidationFailed,
			"rolling completion proof conflicts with unfinished or failed progress",
		)
	}
	return record.Progress.Validate(record.Plan, record.TaskDeadline)
}

func ValidateRollingReleaseAssignment(record RollingReleaseRecord, assignment TaskAssignmentRecord) error {
	if err := ValidateRollingReleaseRecord(record); err != nil {
		return err
	}
	if record.TaskID != assignment.TaskID || record.AssignmentID != assignment.AssignmentID ||
		record.ExecutionEpoch != assignment.ExecutionEpoch || !record.TaskDeadline.Equal(assignment.Deadline) ||
		assignment.ExecutionMode != TaskExecutionModeForward || assignment.Executor != taskjournal.TaskExecutorAgent {
		return errs.New(errs.KindStateConflict, "rolling execution does not match the assigned Task")
	}
	return nil
}

func EncodeRollingReleaseRecord(record RollingReleaseRecord) ([]byte, error) {
	if err := ValidateRollingReleaseRecord(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("rolling-release-execution", record)
}

func DecodeRollingReleaseRecord(value []byte) (RollingReleaseRecord, error) {
	record, err := recordcodec.Decode[RollingReleaseRecord](value, "rolling-release-execution")
	if err != nil || ValidateRollingReleaseRecord(record) != nil {
		return RollingReleaseRecord{}, recordcodec.CorruptRecord()
	}
	return record, nil
}

func CloneRollingReleaseRecord(record RollingReleaseRecord) RollingReleaseRecord {
	record.Plan = rollingrelease.ClonePlan(record.Plan)
	return record
}
