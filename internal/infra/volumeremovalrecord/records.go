package volumeremovalrecord

import (
	"bytes"
	"crypto/sha256"
	"math"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/volumeidentity"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	environmentVolumeRemovalRuntimePrefix = "/v1/runtime/environment-volume-removals/"
	TimeoutSeconds                        = int64(6 * time.Hour / time.Second)
	MutationBudget                        = uint32(128)
	CursorBytes                           = 16 * 1024
	ResponseBytes                         = 768 * 1024

	OriginTaskParam  = "volume_removal_origin_task_id"
	AttemptParam     = "volume_removal_attempt_ordinal"
	EnvironmentParam = "volume_removal_environment_id"
	KeyParam         = "volume_removal_key"
	ImpactParam      = "volume_removal_impact_sha256"
	ManifestParam    = "volume_removal_manifest_sha256"
	IntentParam      = "volume_removal_intent_sha256"
)

type Checkpoint uint8

const (
	IntentSealed Checkpoint = iota + 1
	RevisionStaged
	DesiredPublished
	ConsumersDetached
	DirectoryAbsent
	RuntimeFinalized
)

type Runtime struct {
	OperationID            string
	EnvironmentID          string
	VolumeID               string
	Key                    string
	DesiredRevisionID      string
	DesiredGeneration      uint64
	ImpactSHA256           [sha256.Size]byte
	EvidenceManifestSHA256 [sha256.Size]byte
	IntentSHA256           [sha256.Size]byte
	RootLocator            ReplayLocator
	RootResponseSHA256     [sha256.Size]byte
	OriginTaskID           string
	CurrentTaskID          string
	PredecessorTaskID      string
	AttemptOrdinal         uint32
	StepID                 string
	Checkpoint             Checkpoint
	CreatedAt              time.Time
	UpdatedAt              time.Time
}

type Attempt struct {
	OperationID       string
	OriginTaskID      string
	TaskID            string
	PredecessorTaskID string
	Ordinal           uint32
	CreatedAt         time.Time
}

type Progress struct {
	OperationID        string
	NextRequestOrdinal uint64
	ComponentStack     []string
	Cursor             []byte
	DirectoryAbsent    bool
	UpdatedAt          time.Time
}

type PendingPath struct {
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

type Completion struct {
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

func Root(operationID string) string {
	return environmentVolumeRemovalRuntimePrefix + encodeSegment(operationID) + "/"
}

func RuntimeKey(operationID string) string {
	return Root(operationID) + "runtime"
}

func AttemptKey(operationID string, ordinal uint32) string {
	return Root(operationID) + "attempts/" + strconv.FormatUint(uint64(ordinal), 10)
}

func ProgressKey(operationID string) string {
	return Root(operationID) + "path/progress"
}

func PendingPathKey(operationID string) string {
	return Root(operationID) + "path/pending"
}

func CompletionKey(operationID string, ordinal uint64) string {
	return Root(operationID) + "path/completions/" +
		strconv.FormatUint(ordinal, 10)
}

func ValidateRuntime(record Runtime) error {
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
		record.Checkpoint < IntentSealed ||
		record.Checkpoint > RuntimeFinalized ||
		validateTimestamp("Volume removal created_at", record.CreatedAt) != nil ||
		validateTimestamp("Volume removal updated_at", record.UpdatedAt) != nil ||
		record.UpdatedAt.Before(record.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal runtime is invalid")
	}
	if err := validateReplayLocator(record.RootLocator); err != nil {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal replay locator is invalid")
	}
	if record.RootLocator.ScopeKind != "environment" ||
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

func ValidateAttempt(record Attempt) error {
	if ids.Validate(ids.KindOperation, record.OperationID) != nil ||
		ids.Validate(ids.KindTask, record.OriginTaskID) != nil ||
		ids.Validate(ids.KindTask, record.TaskID) != nil || record.Ordinal == 0 ||
		validateTimestamp("Volume removal attempt created_at", record.CreatedAt) != nil {
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

func ValidateProgress(record Progress) error {
	if ids.Validate(ids.KindOperation, record.OperationID) != nil || record.NextRequestOrdinal == 0 ||
		validateTimestamp("Volume removal path progress updated_at", record.UpdatedAt) != nil ||
		ValidateTraversal(record.ComponentStack, record.Cursor) != nil ||
		(record.DirectoryAbsent && (len(record.ComponentStack) != 0 || len(record.Cursor) != 0)) {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal path progress is invalid")
	}
	return nil
}

func ValidatePendingPath(record PendingPath) error {
	if ids.Validate(ids.KindOperation, record.OperationID) != nil ||
		ids.Validate(ids.KindVolume, record.VolumeID) != nil || volumeidentity.ValidateKey(record.Key) != nil ||
		zeroVolumeRemovalDigest(record.IntentSHA256) || record.RequestOrdinal == 0 ||
		record.MutationBudget != MutationBudget ||
		zeroVolumeRemovalDigest(record.RequestSHA256) ||
		ids.Validate(ids.KindTask, record.TaskID) != nil ||
		ids.Validate(ids.KindAssignment, record.AssignmentID) != nil ||
		ids.Validate(ids.KindAgent, record.AgentID) != nil || record.AgentGeneration == 0 ||
		validateTimestamp("Volume removal pending path created_at", record.CreatedAt) != nil ||
		ValidateTraversal(record.ComponentStack, record.Cursor) != nil {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal pending path is invalid")
	}
	if PathRequestDigest(record) != record.RequestSHA256 {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal request digest is invalid")
	}
	return nil
}

func ValidateCompletion(record Completion) error {
	if ids.Validate(ids.KindOperation, record.OperationID) != nil || record.RequestOrdinal == 0 ||
		zeroVolumeRemovalDigest(record.RequestSHA256) || zeroVolumeRemovalDigest(record.ResponseSHA256) ||
		record.ResponseBytes == 0 || record.ResponseBytes > ResponseBytes ||
		record.MutationCount > MutationBudget ||
		validateTimestamp("Volume removal path completed_at", record.CompletedAt) != nil ||
		ValidateTraversal(record.NextComponentStack, record.NextCursor) != nil ||
		(record.DirectoryAbsent && (len(record.NextComponentStack) != 0 || len(record.NextCursor) != 0)) {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal completion is invalid")
	}
	if PathResponseDigest(record) != record.ResponseSHA256 {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal response digest is invalid")
	}
	if record.ResponseBytes != PathResponseBytes(record) {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal response size is invalid")
	}
	return nil
}

func ValidateTraversal(stack []string, cursor []byte) error {
	encodedBytes := 4 + len(cursor)
	for _, component := range stack {
		if len(component) == 0 || len(component) > 255 || !utf8.ValidString(component) ||
			component == "." || component == ".." || bytes.IndexByte([]byte(component), '/') >= 0 ||
			bytes.IndexByte([]byte(component), 0) >= 0 {
			return errs.New(errs.KindValidationFailed, "Environment Volume removal component stack is invalid")
		}
		encodedBytes += 4 + len(component)
	}
	if encodedBytes > CursorBytes {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal cursor exceeds 16 KiB")
	}
	return nil
}

func zeroVolumeRemovalDigest(value [sha256.Size]byte) bool {
	return value == [sha256.Size]byte{}
}
