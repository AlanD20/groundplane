package backupruntime

import (
	"fmt"
)

const (
	backupCheckpointCursorPrefix = "/v1/runtime/backup-checkpoint-cursors/"
	backupCheckpointDedupPrefix  = "/v1/runtime/backup-checkpoint-dedup/"
)

type BackupCheckpointKind uint32

const (
	BackupCheckpointArtifactPrepared BackupCheckpointKind = iota + 1
	BackupCheckpointUploadVerified
	BackupCheckpointSourceCleanupCompleted
	BackupCheckpointRestoreArtifactValidated
	BackupCheckpointVolumeTreeStaged
	BackupCheckpointVolumeTreeExchanged
	BackupCheckpointVolumeReplacedTreeCleaned
	BackupCheckpointConfigGenerationStaged
	BackupCheckpointConfigGenerationActivated
	BackupCheckpointPostgresRestoreVerified
	BackupCheckpointRemoteObjectAbsent
	BackupCheckpointUploadCompleted
)

// BackupCheckpointPayload is a closed persistence input. Validation selects
// exactly the fields defined for Kind; unused fields must remain zero.
type BackupCheckpointPayload struct {
	Kind                          BackupCheckpointKind
	PointID                       string
	StoredSizeBytes               uint64
	StoredSHA256                  string
	DecodedSHA256                 string
	StagedTreeManifestSHA256      string
	LiveTreeManifestSHA256        string
	RestoreGenerationID           string
	EntryGenerationManifestSHA256 string
	RenderGeneration              uint64
}

type BackupCheckpointInput struct {
	TaskID          string
	AssignmentID    string
	AgentID         string
	AgentGeneration uint64
	StepID          string
	Sequence        uint64
	Payload         BackupCheckpointPayload
}

type BackupAssignmentInput struct {
	TaskID          string
	AssignmentID    string
	AgentID         string
	AgentGeneration uint64
	StepID          string
}

type BackupCheckpointCursorRecord struct {
	TaskID       string `json:"task_id"`
	AssignmentID string `json:"assignment_id"`
	StepID       string `json:"step_id"`
	NextSequence uint64 `json:"next_sequence"`
}

type BackupCheckpointDedupRecord struct {
	TaskID        string               `json:"task_id"`
	AssignmentID  string               `json:"assignment_id"`
	StepID        string               `json:"step_id"`
	Sequence      uint64               `json:"sequence"`
	Kind          BackupCheckpointKind `json:"kind"`
	PayloadSHA256 string               `json:"payload_sha256"`
}

func BackupCheckpointCursorKey(input BackupCheckpointInput) string {
	return BackupCheckpointCursorTaskPrefix(input.TaskID) + input.AssignmentID + "/" + input.StepID
}

func BackupCheckpointDedupKey(input BackupCheckpointInput) string {
	return BackupCheckpointDedupTaskPrefix(input.TaskID) + input.AssignmentID + "/" + input.StepID +
		"/" + fmt.Sprintf(
		"%020d",
		input.Sequence,
	)
}

func BackupCheckpointCursorTaskPrefix(taskID string) string {
	return backupCheckpointCursorPrefix + taskID + "/"
}

func BackupCheckpointDedupTaskPrefix(taskID string) string {
	return backupCheckpointDedupPrefix + taskID + "/"
}
