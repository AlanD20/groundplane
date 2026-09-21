package etcd

import (
	"bytes"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

// TaskAssignmentRecord is the durable claim binding one running Task to one
// Agent daemon generation. ClaimedTaskRevision is the pending Task revision
// consumed by the assignment transaction, not the transaction's new revision.
type TaskAssignmentRecord struct {
	AssignmentID                string
	TaskID                      string
	Executor                    taskjournal.TaskExecutor
	AgentID                     string
	AgentGeneration             uint64
	ClaimedTaskRevision         int64
	AssignedAt                  time.Time
	Deadline                    time.Time
	RecoveryDeadline            time.Time
	RecoveryExecutionDeadline   time.Time
	ExecutionMode               TaskExecutionMode
	ExecutionEpoch              uint32
	RestorationAuthority        *ReleaseRestorationAuthority
	RestorationAuthoritySHA256  string
	ReleaseRecoveryRecordSHA256 string
}

// TaskAssignment contains both records created by one successful claim.
type TaskAssignment struct {
	Assignment      etcdstore.Versioned[TaskAssignmentRecord]
	Task            etcdstore.Versioned[TaskRecord]
	ReleaseRecovery *ReleaseRecoveryDirective
	// RecoveryProofRequired is private lifecycle state. It is true only after
	// the immutable recovery deadline expired without exact restoration proof.
	RecoveryProofRequired bool
}

type ReleaseRecoveryDirective struct {
	Phase                         ReleaseRecoveryPhase
	StepIDs                       []string
	Cursor                        uint32
	RecordSHA256                  string
	ApplicableCompensationStepIDs []string
}

type taskAssignmentJSON struct {
	Schema                      int                          `json:"schema"`
	AssignmentID                string                       `json:"assignment_id"`
	TaskID                      string                       `json:"task_id"`
	Executor                    taskjournal.TaskExecutor     `json:"executor"`
	AgentID                     string                       `json:"agent_id"`
	AgentGeneration             uint64                       `json:"agent_generation"`
	ClaimedTaskRevision         int64                        `json:"claimed_task_revision"`
	AssignedAt                  string                       `json:"assigned_at"`
	ForwardDeadline             string                       `json:"forward_deadline"`
	RecoveryDeadline            string                       `json:"recovery_deadline"`
	RecoveryExecutionDeadline   string                       `json:"recovery_execution_deadline,omitempty"`
	ExecutionMode               TaskExecutionMode            `json:"execution_mode"`
	ExecutionEpoch              uint32                       `json:"execution_epoch"`
	RestorationAuthority        *ReleaseRestorationAuthority `json:"restoration_authority,omitempty"`
	RestorationAuthoritySHA256  string                       `json:"restoration_authority_sha256,omitempty"`
	ReleaseRecoveryRecordSHA256 string                       `json:"release_recovery_record_sha256,omitempty"`
}

func encodeTaskAssignment(record TaskAssignmentRecord) ([]byte, error) {
	if err := validateTaskAssignment(record); err != nil {
		return nil, err
	}
	return json.Marshal(taskAssignmentJSON{
		Schema: 3, AssignmentID: record.AssignmentID, TaskID: record.TaskID,
		Executor: record.Executor, AgentID: record.AgentID,
		AgentGeneration: record.AgentGeneration, ClaimedTaskRevision: record.ClaimedTaskRevision,
		AssignedAt:       record.AssignedAt.Format(time.RFC3339Nano),
		ForwardDeadline:  record.Deadline.Format(time.RFC3339Nano),
		RecoveryDeadline: record.RecoveryDeadline.Format(time.RFC3339Nano),
		RecoveryExecutionDeadline: func() string {
			if record.RecoveryExecutionDeadline.IsZero() {
				return ""
			}
			return record.RecoveryExecutionDeadline.Format(time.RFC3339Nano)
		}(),
		ExecutionMode: record.ExecutionMode, ExecutionEpoch: record.ExecutionEpoch,
		RestorationAuthority:        cloneReleaseRestorationAuthority(record.RestorationAuthority),
		RestorationAuthoritySHA256:  record.RestorationAuthoritySHA256,
		ReleaseRecoveryRecordSHA256: record.ReleaseRecoveryRecordSHA256,
	})
}

func decodeTaskAssignment(value []byte) (TaskAssignmentRecord, error) {
	if recordcodec.RejectDuplicateFields(value) != nil {
		return TaskAssignmentRecord{}, corruptTaskAssignment()
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var data taskAssignmentJSON
	if err := decoder.Decode(&data); err != nil || recordcodec.RequireEOF(decoder) != nil || data.Schema != 3 {
		return TaskAssignmentRecord{}, corruptTaskAssignment()
	}
	assignedAt, err := recordcodec.ParseCanonicalTimestamp(data.AssignedAt)
	if err != nil {
		return TaskAssignmentRecord{}, corruptTaskAssignment()
	}
	deadline, err := recordcodec.ParseCanonicalTimestamp(data.ForwardDeadline)
	if err != nil {
		return TaskAssignmentRecord{}, corruptTaskAssignment()
	}
	recoveryDeadline, err := recordcodec.ParseCanonicalTimestamp(data.RecoveryDeadline)
	if err != nil {
		return TaskAssignmentRecord{}, corruptTaskAssignment()
	}
	var recoveryExecutionDeadline time.Time
	if data.RecoveryExecutionDeadline != "" {
		recoveryExecutionDeadline, err = recordcodec.ParseCanonicalTimestamp(data.RecoveryExecutionDeadline)
		if err != nil {
			return TaskAssignmentRecord{}, corruptTaskAssignment()
		}
	}
	record := TaskAssignmentRecord{
		AssignmentID: data.AssignmentID, TaskID: data.TaskID,
		Executor: data.Executor, AgentID: data.AgentID, AgentGeneration: data.AgentGeneration,
		ClaimedTaskRevision: data.ClaimedTaskRevision, AssignedAt: assignedAt, Deadline: deadline,
		RecoveryDeadline: recoveryDeadline, RecoveryExecutionDeadline: recoveryExecutionDeadline,
		ExecutionMode: data.ExecutionMode, ExecutionEpoch: data.ExecutionEpoch,
		RestorationAuthority:        cloneReleaseRestorationAuthority(data.RestorationAuthority),
		RestorationAuthoritySHA256:  data.RestorationAuthoritySHA256,
		ReleaseRecoveryRecordSHA256: data.ReleaseRecoveryRecordSHA256,
	}
	if err := validateTaskAssignment(record); err != nil {
		return TaskAssignmentRecord{}, corruptTaskAssignment()
	}
	return record, nil
}

func validateTaskAssignment(record TaskAssignmentRecord) error {
	if recordcodec.ValidateID(ids.KindAssignment, record.AssignmentID) != nil ||
		recordcodec.ValidateID(ids.KindTask, record.TaskID) != nil || !taskjournal.ValidExecutor(record.Executor) ||
		record.ClaimedTaskRevision <= 0 ||
		record.ExecutionEpoch == 0 ||
		recordcodec.ValidateTimestamp("task assignment assigned_at", record.AssignedAt) != nil ||
		recordcodec.ValidateTimestamp("task assignment deadline", record.Deadline) != nil ||
		!record.Deadline.After(record.AssignedAt) || !record.RecoveryDeadline.After(record.Deadline) {
		return corruptTaskAssignment()
	}
	if record.Executor == taskjournal.TaskExecutorAgent &&
		(recordcodec.ValidateID(ids.KindAgent, record.AgentID) != nil || record.AgentGeneration == 0) {
		return corruptTaskAssignment()
	}
	if record.Executor == taskjournal.TaskExecutorController && (record.AgentID != "" || record.AgentGeneration != 0) {
		return corruptTaskAssignment()
	}
	switch record.ExecutionMode {
	case TaskExecutionModeForward:
		if record.ReleaseRecoveryRecordSHA256 != "" || !record.RecoveryExecutionDeadline.IsZero() {
			return corruptTaskAssignment()
		}
	case TaskExecutionModeRecoveryOnly:
		if !recordcodec.ValidSHA256(record.ReleaseRecoveryRecordSHA256) ||
			(!record.RecoveryExecutionDeadline.IsZero() &&
				(recordcodec.ValidateTimestamp("task assignment recovery execution deadline", record.RecoveryExecutionDeadline) != nil ||
					!record.RecoveryExecutionDeadline.After(record.RecoveryDeadline))) {
			return corruptTaskAssignment()
		}
	default:
		return corruptTaskAssignment()
	}
	if record.RestorationAuthority == nil {
		if record.RestorationAuthoritySHA256 != "" || record.ExecutionMode == TaskExecutionModeRecoveryOnly {
			return corruptTaskAssignment()
		}
	} else {
		digest, err := releaseRestorationAuthoritySHA256(*record.RestorationAuthority)
		if err != nil || digest != record.RestorationAuthoritySHA256 || record.RestorationAuthority.TaskID != record.TaskID {
			return corruptTaskAssignment()
		}
	}
	return nil
}

func corruptTaskAssignment() error {
	return errs.New(errs.KindInternal, "task assignment record is corrupt")
}
