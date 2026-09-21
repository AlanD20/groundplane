package etcd

import (
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"
)

const backupTerminalReceiptPrefix = "/v1/runtime/backup-terminal-receipts/"

// BackupTerminalSourceOutcome is the immutable terminal outcome of one
// ordered Backup source. The full terminal run is bound separately by
// DomainDigest so the receipt does not duplicate potentially large snapshots.
type BackupTerminalSourceOutcome struct {
	Ordinal                uint32                                 `json:"ordinal"`
	SourceID               string                                 `json:"source_id"`
	Kind                   backupruntime.BackupRuntimeSourceKind  `json:"kind"`
	TargetID               string                                 `json:"target_id"`
	RecoveryPointID        string                                 `json:"recovery_point_id"`
	RecoveryPointCreatedAt time.Time                              `json:"recovery_point_created_at"`
	State                  backupruntime.BackupSourceAttemptState `json:"state"`
	Phase                  backupruntime.BackupSourceAttemptPhase `json:"phase"`
	SizeBytes              int64                                  `json:"size_bytes,omitempty"`
	SHA256                 string                                 `json:"sha256,omitempty"`
	FailureCode            backupruntime.BackupFailureCode        `json:"failure_code,omitempty"`
}

// BackupPruneTerminalPointOutcome is the immutable terminal result for one
// ordered Recovery Point in a Backup-prune Task.
type BackupPruneTerminalPointOutcome struct {
	Point     backupruntime.BackupRecoveryPointSnapshot `json:"point"`
	CreatedAt time.Time                                 `json:"created_at"`
	Outcome   BackupPruneTerminalOutcome                `json:"outcome"`
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
	TaskID             string                        `json:"task_id"`
	TaskType           taskjournal.TaskType          `json:"task_type"`
	OperationID        string                        `json:"operation_id"`
	RetryOf            string                        `json:"retry_of,omitempty"`
	Owner              TaskOwner                     `json:"owner"`
	Actor              TaskActor                     `json:"actor"`
	Executor           taskjournal.TaskExecutor      `json:"executor"`
	Target             string                        `json:"target"`
	PlanID             string                        `json:"plan_id"`
	PlanHash           string                        `json:"plan_hash"`
	Status             taskjournal.TaskStatus        `json:"status"`
	ResultDigest       string                        `json:"result_digest"`
	TerminalAssignment *TaskTerminalAssignmentRecord `json:"terminal_assignment,omitempty"`
	CreatedAt          time.Time                     `json:"created_at"`
	UpdatedAt          time.Time                     `json:"updated_at"`
	StartedAt          *time.Time                    `json:"started_at,omitempty"`
	FinishedAt         time.Time                     `json:"finished_at"`
	RetainUntil        time.Time                     `json:"retain_until"`
	TaskDigest         string                        `json:"task_digest"`
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

type backupTerminalReceiptPlan struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	record     BackupTerminalReceiptRecord
}

func (plan *backupTerminalReceiptPlan) clear() {
	clearBackupRuntimeMutations(plan.mutations)
	plan.conditions = nil
	plan.mutations = nil
	plan.record = BackupTerminalReceiptRecord{}
}

func backupTerminalReceiptKey(taskID string) string {
	return backupTerminalReceiptPrefix + taskID
}

func encodeBackupTerminalReceiptRecord(record BackupTerminalReceiptRecord) ([]byte, error) {
	return backupruntime.EncodeBackupRuntimeRecord(
		"backup-terminal-receipt",
		record,
		validateBackupTerminalReceiptRecord,
	)
}

func decodeBackupTerminalReceiptRecord(value []byte) (BackupTerminalReceiptRecord, error) {
	return backupruntime.DecodeBackupRuntimeRecord(
		value,
		"backup-terminal-receipt",
		validateBackupTerminalReceiptRecord,
	)
}
