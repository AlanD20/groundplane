package backupruntime

import (
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"
)

const backupTerminalReceiptPrefix = "/v1/runtime/backup-terminal-receipts/"

// BackupTerminalSourceOutcome is the immutable terminal outcome of one
// ordered Backup source. The full terminal run is bound separately by
// DomainDigest so the receipt does not duplicate potentially large snapshots.
type BackupTerminalSourceOutcome struct {
	Ordinal                uint32                   `json:"ordinal"`
	SourceID               string                   `json:"source_id"`
	Kind                   BackupRuntimeSourceKind  `json:"kind"`
	TargetID               string                   `json:"target_id"`
	RecoveryPointID        string                   `json:"recovery_point_id"`
	RecoveryPointCreatedAt time.Time                `json:"recovery_point_created_at"`
	State                  BackupSourceAttemptState `json:"state"`
	Phase                  BackupSourceAttemptPhase `json:"phase"`
	SizeBytes              int64                    `json:"size_bytes,omitempty"`
	SHA256                 string                   `json:"sha256,omitempty"`
	FailureCode            BackupFailureCode        `json:"failure_code,omitempty"`
}

// BackupPruneTerminalPointOutcome is the immutable terminal result for one
// ordered Recovery Point in a Backup-prune Task.
type BackupPruneTerminalPointOutcome struct {
	Point     BackupRecoveryPointSnapshot `json:"point"`
	CreatedAt time.Time                   `json:"created_at"`
	Outcome   BackupPruneTerminalOutcome  `json:"outcome"`
}

type BackupPruneTerminalOutcome string

const (
	BackupPruneTerminalRemoved  BackupPruneTerminalOutcome = "removed_verified_absent"
	BackupPruneTerminalRetained BackupPruneTerminalOutcome = "retained_pending_unassigned"
)

// BackupTerminalTaskEvidence explicitly binds the terminal Task contract.
// TaskDigest additionally covers every persisted Task field, including future
// fields that are not repeated in this projection.
type BackupTerminalTaskEvidence struct {
	TaskID             string                                    `json:"task_id"`
	TaskType           taskjournal.TaskType                      `json:"task_type"`
	OperationID        string                                    `json:"operation_id"`
	RetryOf            string                                    `json:"retry_of,omitempty"`
	Owner              taskjournal.TaskOwner                     `json:"owner"`
	Actor              taskjournal.TaskActor                     `json:"actor"`
	Executor           taskjournal.TaskExecutor                  `json:"executor"`
	Target             string                                    `json:"target"`
	PlanID             string                                    `json:"plan_id"`
	PlanHash           string                                    `json:"plan_hash"`
	Status             taskjournal.TaskStatus                    `json:"status"`
	ResultDigest       string                                    `json:"result_digest"`
	TerminalAssignment *taskjournal.TaskTerminalAssignmentRecord `json:"terminal_assignment,omitempty"`
	CreatedAt          time.Time                                 `json:"created_at"`
	UpdatedAt          time.Time                                 `json:"updated_at"`
	StartedAt          *time.Time                                `json:"started_at,omitempty"`
	FinishedAt         time.Time                                 `json:"finished_at"`
	RetainUntil        time.Time                                 `json:"retain_until"`
	TaskDigest         string                                    `json:"task_digest"`
}

// BackupTerminalReceiptRecord is compaction-independent terminal evidence for
// both Backup and Backup-prune Tasks. The receipt is immutable and is written
// in the exact transaction that writes the terminal Task and domain outcome.
type BackupTerminalReceiptRecord struct {
	Task                          BackupTerminalTaskEvidence        `json:"task"`
	PriorTaskRevision             int64                             `json:"prior_task_revision"`
	PriorEnvironmentEpochRevision int64                             `json:"prior_environment_epoch_revision"`
	EnvironmentEpochDigest        string                            `json:"environment_epoch_digest"`
	DomainDigest                  string                            `json:"domain_digest"`
	Sources                       []BackupTerminalSourceOutcome     `json:"sources,omitempty"`
	Points                        []BackupPruneTerminalPointOutcome `json:"points,omitempty"`
	ReceiptDigest                 string                            `json:"receipt_digest"`
}

func BackupTerminalReceiptKey(taskID string) string {
	return backupTerminalReceiptPrefix + taskID
}

func EncodeBackupTerminalReceiptRecord(record BackupTerminalReceiptRecord) ([]byte, error) {
	return EncodeBackupRuntimeRecord(
		"backup-terminal-receipt",
		record,
		validateBackupTerminalReceiptRecord,
	)
}

func DecodeBackupTerminalReceiptRecord(value []byte) (BackupTerminalReceiptRecord, error) {
	return DecodeBackupRuntimeRecord(
		value,
		"backup-terminal-receipt",
		validateBackupTerminalReceiptRecord,
	)
}
