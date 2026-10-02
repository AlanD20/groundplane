package backupruntime

import (
	"fmt"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	backupCheckpointCursorPrefix = "/v1/runtime/backup-checkpoint-cursors/"
	backupCheckpointDedupPrefix  = "/v1/runtime/backup-checkpoint-dedup/"
)

type BackupCheckpointInput struct {
	TaskID                      string
	AssignmentID                string
	AgentID                     string
	AgentGeneration             uint64
	StepID                      string
	ExecutionID                 string
	AssignmentGeneration        uint64
	AuthoritySHA256             string
	PrecedingCheckpointRevision int64
	Sequence                    uint64
	Request                     *agentpb.BackupCheckpointRequest
}

type BackupAssignmentInput struct {
	TaskID          string
	AssignmentID    string
	AgentID         string
	AgentGeneration uint64
	StepID          string
}

type BackupCheckpointCursorRecord struct {
	TaskID               string `json:"task_id"`
	AssignmentID         string `json:"assignment_id"`
	StepID               string `json:"step_id"`
	ExecutionID          string `json:"execution_id"`
	AssignmentGeneration uint64 `json:"assignment_generation"`
	AuthoritySHA256      string `json:"authority_sha256"`
	NextSequence         uint64 `json:"next_sequence"`
}

type BackupCheckpointDedupRecord struct {
	TaskID                      string `json:"task_id"`
	AssignmentID                string `json:"assignment_id"`
	StepID                      string `json:"step_id"`
	ExecutionID                 string `json:"execution_id"`
	AssignmentGeneration        uint64 `json:"assignment_generation"`
	AuthoritySHA256             string `json:"authority_sha256"`
	PrecedingCheckpointRevision int64  `json:"preceding_checkpoint_revision"`
	Sequence                    uint64 `json:"sequence"`
	CheckpointTag               uint32 `json:"checkpoint_tag"`
	PayloadSHA256               string `json:"payload_sha256"`
	Request                     []byte `json:"request"`
}

func BackupCheckpointCursorKey(input BackupCheckpointInput) string {
	return BackupCheckpointCursorTaskPrefix(
		input.TaskID,
	) + input.AssignmentID + "/" + input.StepID + "/" + input.ExecutionID
}

func BackupCheckpointDedupKey(input BackupCheckpointInput) string {
	return BackupCheckpointDedupTaskPrefix(
		input.TaskID,
	) + input.AssignmentID + "/" + input.StepID + "/" + input.ExecutionID +
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
