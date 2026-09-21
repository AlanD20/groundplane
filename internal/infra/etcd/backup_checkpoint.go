package etcd

import (
	"fmt"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
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

type backupCheckpointCursorRecord struct {
	TaskID       string `json:"task_id"`
	AssignmentID string `json:"assignment_id"`
	StepID       string `json:"step_id"`
	NextSequence uint64 `json:"next_sequence"`
}

type backupCheckpointDedupRecord struct {
	TaskID        string               `json:"task_id"`
	AssignmentID  string               `json:"assignment_id"`
	StepID        string               `json:"step_id"`
	Sequence      uint64               `json:"sequence"`
	Kind          BackupCheckpointKind `json:"kind"`
	PayloadSHA256 string               `json:"payload_sha256"`
}

type backupCheckpointPlan struct {
	conditions     []etcdstore.Condition
	mutations      []etcdstore.Mutation
	readRevision   int64
	commitRevision int64
	digest         string
	duplicate      bool
}

type backupCheckpointBinding struct {
	taskType taskjournal.TaskType
	ordinal  uint32
	pointID  string
}

func (plan backupCheckpointPlan) composeTransaction(
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	composedConditions := append(append([]etcdstore.Condition(nil), conditions...), plan.conditions...)
	composedMutations := make([]etcdstore.Mutation, 0, len(mutations)+len(plan.mutations))
	for _, mutation := range append(append([]etcdstore.Mutation(nil), mutations...), plan.mutations...) {
		copyOfMutation := mutation
		copyOfMutation.Value = append([]byte(nil), mutation.Value...)
		composedMutations = append(composedMutations, copyOfMutation)
	}
	if err := validateBackupRuntimeTransactionBounds(
		composedConditions,
		composedMutations,
	); err != nil {
		clearBackupRuntimeMutations(composedMutations)
		return nil, nil, err
	}
	return composedConditions, composedMutations, nil
}

func backupCheckpointCursorKey(input BackupCheckpointInput) string {
	return backupCheckpointCursorTaskPrefix(input.TaskID) + input.AssignmentID + "/" + input.StepID
}

func backupCheckpointDedupKey(input BackupCheckpointInput) string {
	return backupCheckpointDedupTaskPrefix(input.TaskID) + input.AssignmentID + "/" + input.StepID +
		"/" + fmt.Sprintf(
		"%020d",
		input.Sequence,
	)
}

func backupCheckpointCursorTaskPrefix(taskID string) string {
	return backupCheckpointCursorPrefix + taskID + "/"
}

func backupCheckpointDedupTaskPrefix(taskID string) string {
	return backupCheckpointDedupPrefix + taskID + "/"
}

func (plan *backupCheckpointPlan) clear() {
	clearBackupRuntimeMutations(plan.mutations)
}
