package backupruntime

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"math"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type RestoreRetrySnapshot struct {
	Restore BackupRestoreRecord
	Plan    *agentpb.ExecutionPlan
	Guards  []etcdstore.Condition
}

// DecodeRestoreRetrySnapshot verifies the native, terminal, and sealed
// procedure evidence captured at one MVCC view. Task evidence is supplied by
// the task journal owner after decoding the source Task.
func DecodeRestoreRetrySnapshot(task BackupTerminalTaskEvidence, retryTaskID string,
	values []*etcdstore.KeyValue, at time.Time,
) (RestoreRetrySnapshot, error) {
	var zero RestoreRetrySnapshot
	if ids.Validate(ids.KindTask, task.TaskID) != nil || ids.Validate(ids.KindTask, retryTaskID) != nil ||
		task.TaskID == retryTaskID || !ValidBackupRuntimeInstant(at) || len(values) != 4 ||
		values[0] == nil || values[1] == nil || values[2] == nil || values[3] == nil {
		return zero, errs.New(errs.KindTaskNotRetryable, "Restore retry source is incomplete")
	}
	terminalRevision := values[0].ModRevision
	if terminalRevision <= 0 || values[1].ModRevision != terminalRevision ||
		values[2].ModRevision != terminalRevision || values[3].Version != 1 {
		return zero, errs.New(errs.KindStateConflict, "Restore retry source is not atomically terminal")
	}
	restore, restoreErr := DecodeBackupRestoreRecord(values[1].Value)
	receipt, receiptErr := DecodeBackupTerminalReceiptRecord(values[2].Value)
	plan, planErr := DecodeBackupExecutionPlan(values[3].Value)
	if restoreErr != nil || receiptErr != nil || planErr != nil ||
		task.TaskType != taskjournal.TaskRestore || task.Actor != taskjournal.TaskActorOperator ||
		task.Executor != taskjournal.TaskExecutorAgent ||
		task.Status != taskjournal.TaskStatusFailed && task.Status != taskjournal.TaskStatusAborted &&
			task.Status != taskjournal.TaskStatusTimedOut ||
		restore.TaskID != task.TaskID || restore.OperationID != task.OperationID ||
		restore.EnvironmentID != task.Owner.EnvironmentID || task.Target != restore.EnvironmentID ||
		restore.State != BackupRestoreFailedSafe || restore.MutationStarted || restore.UsesOldIdentity ||
		receipt.Restore == nil || !BackupRestoreRecordsEqual(restore, *receipt.Restore) ||
		ValidateRestoreExecutionPlan(restore, plan) != nil ||
		plan.PlanId != task.PlanID || hex.EncodeToString(plan.PlanHash) != task.PlanHash {
		return zero, errs.New(
			errs.KindTaskNotRetryable,
			"Restore retry requires a complete failed-safe terminal source",
		)
	}
	left, leftErr := json.Marshal(task)
	right, rightErr := json.Marshal(receipt.Task)
	if leftErr != nil || rightErr != nil || !bytes.Equal(left, right) {
		return zero, errs.New(errs.KindStateConflict, "Restore retry terminal Task evidence changed")
	}
	if at.Before(restore.UpdatedAt) {
		return zero, errs.New(errs.KindStateConflict, "Restore retry predates its source")
	}
	return RestoreRetrySnapshot{Restore: CloneBackupRestoreRecord(restore), Plan: proto.CloneOf(plan),
		Guards: []etcdstore.Condition{
			{Key: values[0].Key, ModRevision: terminalRevision},
			{Key: values[1].Key, ModRevision: terminalRevision},
			{Key: values[2].Key, ModRevision: terminalRevision},
			{Key: values[3].Key, ModRevision: values[3].ModRevision},
		}}, nil
}

// RestoreRetryConditions preserves selected source, target, credential, and
// key-era authority while assigning a fresh attempt number.
func RestoreRetryConditions(source RestoreRetrySnapshot, candidate BackupRestoreRecord,
	scope *agentpb.BackupPlanScope, authority *agentpb.BackupStepAuthority,
	artifacts []*agentpb.ComposeArtifact,
) ([]etcdstore.Condition, uint32, error) {
	if source.Restore.TaskID == "" || candidate.TaskID == source.Restore.TaskID ||
		candidate.OperationID != source.Restore.OperationID || source.Plan == nil ||
		len(source.Plan.Steps) != 1 || source.Plan.BackupScope == nil ||
		source.Plan.BackupScope.TaskAttempt == 0 || source.Plan.BackupScope.TaskAttempt == math.MaxUint32 ||
		scope == nil || authority == nil || authority.GetRestore() == nil ||
		len(artifacts) != len(source.Plan.Artifacts) {
		return nil, 0, errs.New(errs.KindStateConflict, "Restore retry authority changed")
	}
	expectedScope := proto.CloneOf(scope)
	expectedScope.TaskAttempt = source.Plan.BackupScope.TaskAttempt
	if !proto.Equal(expectedScope, source.Plan.BackupScope) {
		return nil, 0, errs.New(errs.KindStateConflict, "Restore retry scope authority changed")
	}
	oldRestore := source.Plan.Steps[0].GetBackupStep().GetRestore()
	newRestore := authority.GetRestore()
	if oldRestore == nil || !proto.Equal(oldRestore.SourceObject, newRestore.SourceObject) ||
		!proto.Equal(oldRestore.Encryption, newRestore.Encryption) ||
		!proto.Equal(oldRestore.ExpectedEvidence, newRestore.ExpectedEvidence) {
		return nil, 0, errs.New(errs.KindStateConflict, "Restore retry source authority changed")
	}
	for index := range artifacts {
		if !proto.Equal(artifacts[index], source.Plan.Artifacts[index]) {
			return nil, 0, errs.New(errs.KindStateConflict, "Restore retry Service authority changed")
		}
	}
	comparison := CloneBackupRestoreRecord(candidate)
	comparison.TaskID = source.Restore.TaskID
	comparison.RestoreGenerationID = source.Restore.RestoreGenerationID
	comparison.CreatedAt, comparison.UpdatedAt = source.Restore.CreatedAt, source.Restore.UpdatedAt
	comparison.State, comparison.Verification = source.Restore.State, source.Restore.Verification
	comparison.MutationStarted, comparison.Artifact = source.Restore.MutationStarted, source.Restore.Artifact
	comparison.StagedTreeManifestSHA256 = source.Restore.StagedTreeManifestSHA256
	comparison.ConfigProgress, comparison.VolumeProgress = source.Restore.ConfigProgress, source.Restore.VolumeProgress
	comparison.PostgresProgress = source.Restore.PostgresProgress
	if !BackupRestoreRecordsEqual(comparison, source.Restore) {
		return nil, 0, errs.New(errs.KindStateConflict, "Restore retry selected authority changed")
	}
	return append([]etcdstore.Condition(nil), source.Guards...), source.Plan.BackupScope.TaskAttempt + 1, nil
}

func BindRestoreRetry(source RestoreRetrySnapshot, candidate BackupRestoreRecord,
	scope *agentpb.BackupPlanScope, authority *agentpb.BackupStepAuthority,
	artifacts []*agentpb.ComposeArtifact, conditions []etcdstore.Condition, mutations []etcdstore.Mutation,
) ([]etcdstore.Condition, uint32, error) {
	guards, attempt, err := RestoreRetryConditions(source, candidate, scope, authority, artifacts)
	if err != nil {
		return nil, 0, err
	}
	combined := append(append([]etcdstore.Condition(nil), conditions...), guards...)
	if err := ValidateBackupRuntimeTransactionBounds(combined, mutations); err != nil {
		return nil, 0, err
	}
	return combined, attempt, nil
}
