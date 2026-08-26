package volumeremoval

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"math"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/volumeidentity"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	environmentVolumeRemovalRuntimePrefix  = "/v1/runtime/environment-volume-removals/"
	environmentVolumeRemovalTimeoutSeconds = int64(6 * time.Hour / time.Second)
	environmentVolumeRemovalMutationBudget = uint32(128)
	environmentVolumeRemovalCursorBytes    = 16 * 1024
	environmentVolumeRemovalResponseBytes  = 768 * 1024

	EnvironmentVolumeRemovalOriginTaskParam  = "volume_removal_origin_task_id"
	EnvironmentVolumeRemovalAttemptParam     = "volume_removal_attempt_ordinal"
	EnvironmentVolumeRemovalEnvironmentParam = "volume_removal_environment_id"
	EnvironmentVolumeRemovalKeyParam         = "volume_removal_key"
	EnvironmentVolumeRemovalImpactParam      = "volume_removal_impact_sha256"
	EnvironmentVolumeRemovalManifestParam    = "volume_removal_manifest_sha256"
	EnvironmentVolumeRemovalIntentParam      = "volume_removal_intent_sha256"
)

type EnvironmentVolumeRemovalCheckpoint uint8

const (
	EnvironmentVolumeRemovalIntentSealed EnvironmentVolumeRemovalCheckpoint = iota + 1
	EnvironmentVolumeRemovalRevisionStaged
	EnvironmentVolumeRemovalDesiredPublished
	EnvironmentVolumeRemovalConsumersDetached
	EnvironmentVolumeRemovalDirectoryAbsent
	EnvironmentVolumeRemovalRuntimeFinalized
)

type EnvironmentVolumeRemovalRuntimeRecord struct {
	OperationID            string
	EnvironmentID          string
	VolumeID               string
	Key                    string
	DesiredRevisionID      string
	DesiredGeneration      uint64
	ImpactSHA256           [sha256.Size]byte
	EvidenceManifestSHA256 [sha256.Size]byte
	IntentSHA256           [sha256.Size]byte
	RootLocator            etcd.IdempotencyLocator
	RootResponseSHA256     [sha256.Size]byte
	OriginTaskID           string
	CurrentTaskID          string
	PredecessorTaskID      string
	AttemptOrdinal         uint32
	StepID                 string
	Checkpoint             EnvironmentVolumeRemovalCheckpoint
	CreatedAt              time.Time
	UpdatedAt              time.Time
}

type EnvironmentVolumeRemovalAttemptRecord struct {
	OperationID       string
	OriginTaskID      string
	TaskID            string
	PredecessorTaskID string
	Ordinal           uint32
	CreatedAt         time.Time
}

type EnvironmentVolumeRemovalPathProgress struct {
	OperationID        string
	NextRequestOrdinal uint64
	ComponentStack     []string
	Cursor             []byte
	DirectoryAbsent    bool
	UpdatedAt          time.Time
}

type EnvironmentVolumeRemovalPendingPath struct {
	OperationID     string
	VolumeID        string
	Key             string
	IntentSHA256    [sha256.Size]byte
	RequestOrdinal  uint64
	MutationBudget  uint32
	ComponentStack  []string
	Cursor          []byte
	RequestSHA256   [sha256.Size]byte
	TaskID          string
	AssignmentID    string
	AgentID         string
	AgentGeneration uint64
	CreatedAt       time.Time
}

type EnvironmentVolumeRemovalPathCompletion struct {
	OperationID        string
	RequestOrdinal     uint64
	RequestSHA256      [sha256.Size]byte
	ResponseSHA256     [sha256.Size]byte
	ResponseBytes      uint32
	MutationCount      uint32
	NextComponentStack []string
	NextCursor         []byte
	DirectoryAbsent    bool
	CompletedAt        time.Time
}

type EnvironmentVolumeRemovalAssignment struct {
	OperationID     string
	TaskID          string
	AssignmentID    string
	AgentID         string
	AgentGeneration uint64
}

type EnvironmentVolumeRemovalPathResult struct {
	Assignment         EnvironmentVolumeRemovalAssignment
	RequestOrdinal     uint64
	RequestSHA256      [sha256.Size]byte
	ResponseSHA256     [sha256.Size]byte
	ResponseBytes      uint32
	MutationCount      uint32
	NextComponentStack []string
	NextCursor         []byte
	DirectoryAbsent    bool
	CompletedAt        time.Time
}

type EnvironmentVolumeRemovalResumeState struct {
	Runtime  etcd.Versioned[EnvironmentVolumeRemovalRuntimeRecord]
	Attempt  EnvironmentVolumeRemovalAttemptRecord
	Progress etcd.Versioned[EnvironmentVolumeRemovalPathProgress]
	Pending  *etcd.Versioned[EnvironmentVolumeRemovalPendingPath]
}

func environmentVolumeRemovalRoot(operationID string) string {
	return environmentVolumeRemovalRuntimePrefix + etcd.EncodeCapabilityKeySegment(operationID) + "/"
}

func environmentVolumeRemovalRuntimeKey(operationID string) string {
	return environmentVolumeRemovalRoot(operationID) + "runtime"
}

func environmentVolumeRemovalAttemptKey(operationID string, ordinal uint32) string {
	return environmentVolumeRemovalRoot(operationID) + "attempts/" + strconv.FormatUint(uint64(ordinal), 10)
}

func environmentVolumeRemovalProgressKey(operationID string) string {
	return environmentVolumeRemovalRoot(operationID) + "path/progress"
}

func environmentVolumeRemovalPendingPathKey(operationID string) string {
	return environmentVolumeRemovalRoot(operationID) + "path/pending"
}

func environmentVolumeRemovalCompletionKey(operationID string, ordinal uint64) string {
	return environmentVolumeRemovalRoot(operationID) + "path/completions/" +
		strconv.FormatUint(ordinal, 10)
}

func validateEnvironmentVolumeRemovalRuntime(record EnvironmentVolumeRemovalRuntimeRecord) error {
	if ids.Validate(ids.KindOperation, record.OperationID) != nil ||
		ids.Validate(ids.KindEnvironment, record.EnvironmentID) != nil ||
		ids.Validate(ids.KindVolume, record.VolumeID) != nil ||
		volumeidentity.ValidateKey(record.Key) != nil ||
		ids.Validate(ids.KindTask, record.DesiredRevisionID) != nil ||
		record.DesiredGeneration == 0 || record.DesiredGeneration > math.MaxInt32 ||
		zeroVolumeRemovalDigest(record.ImpactSHA256) ||
		zeroVolumeRemovalDigest(record.EvidenceManifestSHA256) ||
		zeroVolumeRemovalDigest(record.IntentSHA256) ||
		zeroVolumeRemovalDigest(record.RootResponseSHA256) ||
		ids.Validate(ids.KindTask, record.OriginTaskID) != nil ||
		ids.Validate(ids.KindTask, record.CurrentTaskID) != nil ||
		ids.Validate(ids.KindStep, record.StepID) != nil || record.AttemptOrdinal == 0 ||
		record.Checkpoint < EnvironmentVolumeRemovalIntentSealed ||
		record.Checkpoint > EnvironmentVolumeRemovalRuntimeFinalized ||
		etcd.ValidateCapabilityTimestamp("Volume removal created_at", record.CreatedAt) != nil ||
		etcd.ValidateCapabilityTimestamp("Volume removal updated_at", record.UpdatedAt) != nil ||
		record.UpdatedAt.Before(record.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal runtime is invalid")
	}
	if _, err := etcd.CapabilityIdempotencyMarkerKey(record.RootLocator); err != nil {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal replay locator is invalid")
	}
	if record.RootLocator.ScopeKind != etcd.IdempotencyScopeEnvironment ||
		record.RootLocator.ScopeID != record.EnvironmentID || record.RootLocator.Key == "" {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal replay scope is invalid")
	}
	if record.AttemptOrdinal == 1 {
		if record.CurrentTaskID != record.OriginTaskID || record.PredecessorTaskID != "" {
			return errs.New(errs.KindValidationFailed, "Environment Volume removal root attempt is invalid")
		}
	} else if ids.Validate(ids.KindTask, record.PredecessorTaskID) != nil ||
		record.CurrentTaskID == record.OriginTaskID {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal successor attempt is invalid")
	}
	return nil
}

func validateEnvironmentVolumeRemovalAttempt(record EnvironmentVolumeRemovalAttemptRecord) error {
	if ids.Validate(ids.KindOperation, record.OperationID) != nil ||
		ids.Validate(ids.KindTask, record.OriginTaskID) != nil ||
		ids.Validate(ids.KindTask, record.TaskID) != nil || record.Ordinal == 0 ||
		etcd.ValidateCapabilityTimestamp("Volume removal attempt created_at", record.CreatedAt) != nil {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal attempt is invalid")
	}
	if record.Ordinal == 1 {
		if record.TaskID != record.OriginTaskID || record.PredecessorTaskID != "" {
			return errs.New(errs.KindValidationFailed, "Environment Volume removal root attempt is invalid")
		}
	} else if ids.Validate(ids.KindTask, record.PredecessorTaskID) != nil ||
		record.TaskID == record.OriginTaskID {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal successor attempt is invalid")
	}
	return nil
}

func validateEnvironmentVolumeRemovalProgress(record EnvironmentVolumeRemovalPathProgress) error {
	if ids.Validate(ids.KindOperation, record.OperationID) != nil || record.NextRequestOrdinal == 0 ||
		etcd.ValidateCapabilityTimestamp("Volume removal path progress updated_at", record.UpdatedAt) != nil ||
		validateEnvironmentVolumeRemovalTraversal(record.ComponentStack, record.Cursor) != nil ||
		(record.DirectoryAbsent && (len(record.ComponentStack) != 0 || len(record.Cursor) != 0)) {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal path progress is invalid")
	}
	return nil
}

func validateEnvironmentVolumeRemovalPendingPath(record EnvironmentVolumeRemovalPendingPath) error {
	if ids.Validate(ids.KindOperation, record.OperationID) != nil ||
		ids.Validate(ids.KindVolume, record.VolumeID) != nil || volumeidentity.ValidateKey(record.Key) != nil ||
		zeroVolumeRemovalDigest(record.IntentSHA256) || record.RequestOrdinal == 0 ||
		record.MutationBudget != environmentVolumeRemovalMutationBudget ||
		zeroVolumeRemovalDigest(record.RequestSHA256) ||
		ids.Validate(ids.KindTask, record.TaskID) != nil ||
		ids.Validate(ids.KindAssignment, record.AssignmentID) != nil ||
		ids.Validate(ids.KindAgent, record.AgentID) != nil || record.AgentGeneration == 0 ||
		etcd.ValidateCapabilityTimestamp("Volume removal pending path created_at", record.CreatedAt) != nil ||
		validateEnvironmentVolumeRemovalTraversal(record.ComponentStack, record.Cursor) != nil {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal pending path is invalid")
	}
	if environmentVolumeRemovalPathRequestDigest(record) != record.RequestSHA256 {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal request digest is invalid")
	}
	return nil
}

func validateEnvironmentVolumeRemovalCompletion(record EnvironmentVolumeRemovalPathCompletion) error {
	if ids.Validate(ids.KindOperation, record.OperationID) != nil || record.RequestOrdinal == 0 ||
		zeroVolumeRemovalDigest(record.RequestSHA256) || zeroVolumeRemovalDigest(record.ResponseSHA256) ||
		record.ResponseBytes == 0 || record.ResponseBytes > environmentVolumeRemovalResponseBytes ||
		record.MutationCount > environmentVolumeRemovalMutationBudget ||
		etcd.ValidateCapabilityTimestamp("Volume removal path completed_at", record.CompletedAt) != nil ||
		validateEnvironmentVolumeRemovalTraversal(record.NextComponentStack, record.NextCursor) != nil ||
		(record.DirectoryAbsent && (len(record.NextComponentStack) != 0 || len(record.NextCursor) != 0)) {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal completion is invalid")
	}
	if environmentVolumeRemovalPathResponseDigest(record) != record.ResponseSHA256 {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal response digest is invalid")
	}
	if record.ResponseBytes != environmentVolumeRemovalPathResponseBytes(record) {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal response size is invalid")
	}
	return nil
}

func validateEnvironmentVolumeRemovalTraversal(stack []string, cursor []byte) error {
	encodedBytes := 4 + len(cursor)
	for _, component := range stack {
		if len(component) == 0 || len(component) > 255 || !utf8.ValidString(component) ||
			component == "." || component == ".." || bytes.IndexByte([]byte(component), '/') >= 0 ||
			bytes.IndexByte([]byte(component), 0) >= 0 {
			return errs.New(errs.KindValidationFailed, "Environment Volume removal component stack is invalid")
		}
		encodedBytes += 4 + len(component)
	}
	if encodedBytes > environmentVolumeRemovalCursorBytes {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal cursor exceeds 16 KiB")
	}
	return nil
}

func validateEnvironmentVolumeRemovalTask(
	task etcd.TaskRecord,
	runtime EnvironmentVolumeRemovalRuntimeRecord,
	attempt EnvironmentVolumeRemovalAttemptRecord,
) error {
	if err := etcd.ValidateCapabilityTaskRecord(task); err != nil {
		return err
	}
	if err := validateEnvironmentVolumeRemovalRuntime(runtime); err != nil {
		return err
	}
	if err := validateEnvironmentVolumeRemovalAttempt(attempt); err != nil {
		return err
	}
	expectedParams := environmentVolumeRemovalTaskParams(runtime, attempt)
	if task.ID != attempt.TaskID || task.ID != runtime.CurrentTaskID ||
		task.OperationID != runtime.OperationID || task.RetryOf != attempt.PredecessorTaskID ||
		task.Owner.EnvironmentID != runtime.EnvironmentID || task.Actor != etcd.TaskActorOperator ||
		task.Executor != etcd.TaskExecutorAgent || task.Type != etcd.TaskRemove || task.Target != runtime.VolumeID ||
		task.RenderGeneration != int32(runtime.DesiredGeneration) ||
		task.TimeoutSeconds != environmentVolumeRemovalTimeoutSeconds ||
		task.IdempotencyKey != runtime.RootLocator.Key || len(task.Steps) != 1 ||
		task.Steps[0].ID != runtime.StepID || !equalVolumeRemovalParams(task.Params, expectedParams) {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal Task inputs changed")
	}
	return nil
}

func environmentVolumeRemovalTaskParams(
	runtime EnvironmentVolumeRemovalRuntimeRecord,
	attempt EnvironmentVolumeRemovalAttemptRecord,
) map[string]string {
	return map[string]string{
		EnvironmentVolumeRemovalEnvironmentParam: runtime.EnvironmentID,
		etcd.EnvironmentDesiredRevisionParam:     runtime.DesiredRevisionID,
		EnvironmentVolumeRemovalOriginTaskParam:  runtime.OriginTaskID,
		EnvironmentVolumeRemovalAttemptParam:     strconv.FormatUint(uint64(attempt.Ordinal), 10),
		EnvironmentVolumeRemovalKeyParam:         runtime.Key,
		EnvironmentVolumeRemovalImpactParam:      hex.EncodeToString(runtime.ImpactSHA256[:]),
		EnvironmentVolumeRemovalManifestParam:    hex.EncodeToString(runtime.EvidenceManifestSHA256[:]),
		EnvironmentVolumeRemovalIntentParam:      hex.EncodeToString(runtime.IntentSHA256[:]),
	}
}

func equalVolumeRemovalParams(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range right {
		if left[key] != value {
			return false
		}
	}
	return true
}

func validateEnvironmentVolumeRemovalTaskResult(
	result etcd.TaskResultRecord,
	steps []etcd.TaskStepRecord,
	status etcd.TaskStatus,
) error {
	if err := etcd.ValidateCapabilityTaskResult(result, steps, status); err != nil {
		return err
	}
	if result.Kind != etcd.TaskResultEnvironmentDirectory ||
		result.Diagnostic != etcd.TaskResultDiagnosticNone || result.ReconciliationRequired ||
		len(result.Projects) != 0 || (status == etcd.TaskStatusCompleted && result.ExitCode != 0) {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal result is invalid")
	}
	return nil
}

func zeroVolumeRemovalDigest(value [sha256.Size]byte) bool {
	return value == [sha256.Size]byte{}
}
