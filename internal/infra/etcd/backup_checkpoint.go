package etcd

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
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
	conditions     []Condition
	mutations      []Mutation
	readRevision   int64
	commitRevision int64
	digest         string
	duplicate      bool
}

type backupCheckpointBinding struct {
	taskType TaskType
	ordinal  uint32
	pointID  string
}

func (plan backupCheckpointPlan) composeTransaction(
	conditions []Condition,
	mutations []Mutation,
) ([]Condition, []Mutation, error) {
	composedConditions := append(append([]Condition(nil), conditions...), plan.conditions...)
	composedMutations := make([]Mutation, 0, len(mutations)+len(plan.mutations))
	for _, mutation := range append(append([]Mutation(nil), mutations...), plan.mutations...) {
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

func (repository *BackupRuntimeRepository) loadBackupCheckpointPlan(
	ctx context.Context,
	input BackupCheckpointInput,
	revision int64,
	binding backupCheckpointBinding,
) (backupCheckpointPlan, error) {
	digest, err := backupCheckpointDigest(input.Payload)
	if err != nil || validateBackupCheckpointInput(input) != nil ||
		validateBackupCheckpointBinding(binding) != nil || revision <= 0 {
		return backupCheckpointPlan{}, errs.New(
			errs.KindValidationFailed,
			"backup checkpoint is invalid",
		)
	}
	taskRead, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{taskKey(input.TaskID)}, Revision: revision,
	})
	if err != nil {
		return backupCheckpointPlan{}, err
	}
	if taskRead == nil || taskRead.ReadRevision != revision || len(taskRead.Values) != 1 ||
		taskRead.Values[0] == nil {
		return backupCheckpointPlan{}, errs.New(
			errs.KindStateConflict,
			"backup task assignment changed",
		)
	}
	defer clearKeyValues(taskRead.Values)
	task, err := decodeTaskRecord(taskRead.Values[0].Value)
	if err != nil || task.ID != input.TaskID {
		return backupCheckpointPlan{}, corruptBackupRuntimeRecord()
	}
	if task.Status != TaskStatusRunning || !taskContainsStep(task, input.StepID) {
		return backupCheckpointPlan{}, errs.New(
			errs.KindStateConflict,
			"backup task assignment changed",
		)
	}
	if task.Type != binding.taskType || int(binding.ordinal) >= len(task.Steps) ||
		task.Steps[binding.ordinal].ID != input.StepID || input.Payload.PointID != binding.pointID {
		return backupCheckpointPlan{}, errs.New(
			errs.KindValidationFailed,
			"backup checkpoint step binding is invalid",
		)
	}
	claimKey := taskExecutionClaimKey(task.Executor, input.AgentID, input.TaskID)
	cursorKey := backupCheckpointCursorKey(input)
	dedupKey := backupCheckpointDedupKey(input)
	assignmentRead, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			claimKey,
			taskAssignmentIndexKey(input.TaskID),
			cursorKey,
			dedupKey,
		},
		Revision: revision,
	})
	if err != nil {
		return backupCheckpointPlan{}, err
	}
	if assignmentRead == nil || assignmentRead.ReadRevision != revision ||
		len(assignmentRead.Values) != 4 || assignmentRead.Values[0] == nil ||
		assignmentRead.Values[1] == nil {
		return backupCheckpointPlan{}, errs.New(
			errs.KindStateConflict,
			"backup task assignment changed",
		)
	}
	defer clearKeyValues(assignmentRead.Values)
	claimValue := assignmentRead.Values[0]
	indexValue := assignmentRead.Values[1]
	if claimValue.ModRevision != indexValue.ModRevision ||
		!bytes.Equal(claimValue.Value, indexValue.Value) {
		return backupCheckpointPlan{}, errs.New(
			errs.KindInternal,
			"backup task assignment copies differ",
		)
	}
	assignment, err := decodeTaskAssignment(claimValue.Value)
	if err != nil || assignment.AssignmentID != input.AssignmentID ||
		assignment.TaskID != input.TaskID ||
		assignment.Executor != task.Executor ||
		assignment.AgentID != input.AgentID ||
		assignment.AgentGeneration != input.AgentGeneration {
		return backupCheckpointPlan{}, errs.New(
			errs.KindStateConflict,
			"backup task assignment changed",
		)
	}
	timeoutRead, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{taskTimeoutIndexKey(input.TaskID, assignment.Deadline)}, Revision: revision,
	})
	if err != nil {
		return backupCheckpointPlan{}, err
	}
	if timeoutRead == nil || timeoutRead.ReadRevision != revision || len(timeoutRead.Values) != 1 ||
		timeoutRead.Values[0] == nil || timeoutRead.Values[0].ModRevision != claimValue.ModRevision ||
		!bytes.Equal(timeoutRead.Values[0].Value, claimValue.Value) {
		return backupCheckpointPlan{}, errs.New(
			errs.KindStateConflict,
			"backup task assignment changed",
		)
	}
	defer clearKeyValues(timeoutRead.Values)
	nextSequence := uint64(1)
	if assignmentRead.Values[2] != nil {
		cursor, decodeErr := decodeBackupCheckpointCursorRecord(assignmentRead.Values[2].Value)
		if decodeErr != nil || cursor.TaskID != input.TaskID ||
			cursor.AssignmentID != input.AssignmentID ||
			cursor.StepID != input.StepID {
			return backupCheckpointPlan{}, corruptBackupRuntimeRecord()
		}
		nextSequence = cursor.NextSequence
	}
	if assignmentRead.Values[3] != nil {
		cursorValue := assignmentRead.Values[2]
		dedupValue := assignmentRead.Values[3]
		if cursorValue == nil || dedupValue.Version != 1 || cursorValue.ModRevision <= 0 ||
			dedupValue.ModRevision != cursorValue.ModRevision {
			return backupCheckpointPlan{}, errs.New(
				errs.KindStateConflict,
				"backup checkpoint replay evidence changed",
			)
		}
		dedup, decodeErr := decodeBackupCheckpointDedupRecord(assignmentRead.Values[3].Value)
		if decodeErr != nil || dedup.TaskID != input.TaskID ||
			dedup.AssignmentID != input.AssignmentID ||
			dedup.StepID != input.StepID ||
			dedup.Sequence != input.Sequence ||
			dedup.Kind != input.Payload.Kind {
			return backupCheckpointPlan{}, corruptBackupRuntimeRecord()
		}
		if dedup.PayloadSHA256 != digest {
			return backupCheckpointPlan{}, errs.New(
				errs.KindStateConflict,
				"backup checkpoint digest changed",
			)
		}
		if input.Sequence == ^uint64(0) || nextSequence != input.Sequence+1 {
			return backupCheckpointPlan{}, errs.New(
				errs.KindStateConflict,
				"backup checkpoint sequence changed",
			)
		}
		return backupCheckpointPlan{
			readRevision: revision, commitRevision: dedupValue.ModRevision,
			digest: digest, duplicate: true,
		}, nil
	}
	if input.Sequence != nextSequence {
		return backupCheckpointPlan{}, errs.New(
			errs.KindStateConflict,
			"backup checkpoint sequence changed",
		)
	}
	cursor := backupCheckpointCursorRecord{
		TaskID: input.TaskID, AssignmentID: input.AssignmentID, StepID: input.StepID,
		NextSequence: input.Sequence + 1,
	}
	dedup := backupCheckpointDedupRecord{
		TaskID: input.TaskID, AssignmentID: input.AssignmentID, StepID: input.StepID,
		Sequence: input.Sequence, Kind: input.Payload.Kind, PayloadSHA256: digest,
	}
	cursorValue, err := encodeBackupCheckpointCursorRecord(cursor)
	if err != nil {
		return backupCheckpointPlan{}, err
	}
	dedupValue, err := encodeBackupCheckpointDedupRecord(dedup)
	if err != nil {
		clear(cursorValue)
		return backupCheckpointPlan{}, err
	}
	conditions := []Condition{
		{Key: taskKey(input.TaskID), ModRevision: taskRead.Values[0].ModRevision},
		{Key: claimKey, ModRevision: claimValue.ModRevision},
		{Key: taskAssignmentIndexKey(input.TaskID), ModRevision: indexValue.ModRevision},
		{
			Key:         taskTimeoutIndexKey(input.TaskID, assignment.Deadline),
			ModRevision: timeoutRead.Values[0].ModRevision,
		},
		{Key: dedupKey},
	}
	if assignmentRead.Values[2] == nil {
		conditions = append(conditions, Condition{Key: cursorKey})
	} else {
		conditions = append(conditions, Condition{
			Key: cursorKey, ModRevision: assignmentRead.Values[2].ModRevision,
		})
	}
	return backupCheckpointPlan{
		conditions: conditions,
		mutations: []Mutation{
			{Type: MutationPut, Key: cursorKey, Value: cursorValue},
			{Type: MutationPut, Key: dedupKey, Value: dedupValue},
		},
		readRevision: revision,
		digest:       digest,
	}, nil
}

func validateBackupCheckpointBinding(binding backupCheckpointBinding) error {
	if (binding.taskType != TaskBackup && binding.taskType != TaskBackupPrune) ||
		validateStableID(ids.KindRecoveryPoint, binding.pointID) != nil {
		return errs.New(errs.KindValidationFailed, "backup checkpoint binding is invalid")
	}
	return nil
}

func (repository *BackupRuntimeRepository) loadBackupAssignmentFence(
	ctx context.Context,
	input BackupAssignmentInput,
	revision int64,
) ([]Condition, error) {
	if validateStableID(ids.KindTask, input.TaskID) != nil ||
		validateStableID(ids.KindAssignment, input.AssignmentID) != nil ||
		validateStableID(ids.KindStep, input.StepID) != nil || revision <= 0 ||
		(input.AgentID == "" && input.AgentGeneration != 0) ||
		(input.AgentID != "" &&
			(validateStableID(ids.KindAgent, input.AgentID) != nil || input.AgentGeneration == 0)) {
		return nil, errs.New(errs.KindValidationFailed, "backup assignment fence is invalid")
	}
	taskResult, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{taskKey(input.TaskID)}, Revision: revision,
	})
	if err != nil {
		return nil, err
	}
	if taskResult == nil || taskResult.ReadRevision != revision || len(taskResult.Values) != 1 ||
		taskResult.Values[0] == nil {
		return nil, errs.New(errs.KindStateConflict, "backup task assignment changed")
	}
	defer clearKeyValues(taskResult.Values)
	task, err := decodeTaskRecord(taskResult.Values[0].Value)
	if err != nil || task.ID != input.TaskID {
		return nil, corruptBackupRuntimeRecord()
	}
	if task.Status != TaskStatusRunning || !taskContainsStep(task, input.StepID) {
		return nil, errs.New(errs.KindStateConflict, "backup task assignment changed")
	}
	claimKey := taskExecutionClaimKey(task.Executor, input.AgentID, input.TaskID)
	assignmentResult, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{claimKey, taskAssignmentIndexKey(input.TaskID)}, Revision: revision,
	})
	if err != nil {
		return nil, err
	}
	if assignmentResult == nil || assignmentResult.ReadRevision != revision ||
		len(assignmentResult.Values) != 2 || assignmentResult.Values[0] == nil ||
		assignmentResult.Values[1] == nil ||
		assignmentResult.Values[0].ModRevision != assignmentResult.Values[1].ModRevision ||
		!bytes.Equal(assignmentResult.Values[0].Value, assignmentResult.Values[1].Value) {
		return nil, errs.New(errs.KindStateConflict, "backup task assignment changed")
	}
	defer clearKeyValues(assignmentResult.Values)
	assignment, err := decodeTaskAssignment(assignmentResult.Values[0].Value)
	if err != nil || assignment.AssignmentID != input.AssignmentID ||
		assignment.TaskID != input.TaskID || assignment.Executor != task.Executor ||
		assignment.AgentID != input.AgentID || assignment.AgentGeneration != input.AgentGeneration {
		return nil, errs.New(errs.KindStateConflict, "backup task assignment changed")
	}
	timeoutResult, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{taskTimeoutIndexKey(input.TaskID, assignment.Deadline)}, Revision: revision,
	})
	if err != nil {
		return nil, err
	}
	if timeoutResult == nil || timeoutResult.ReadRevision != revision ||
		len(timeoutResult.Values) != 1 ||
		timeoutResult.Values[0] == nil ||
		timeoutResult.Values[0].ModRevision != assignmentResult.Values[0].ModRevision ||
		!bytes.Equal(timeoutResult.Values[0].Value, assignmentResult.Values[0].Value) {
		return nil, errs.New(errs.KindStateConflict, "backup task assignment changed")
	}
	defer clearKeyValues(timeoutResult.Values)
	return []Condition{
		{Key: taskKey(input.TaskID), ModRevision: taskResult.Values[0].ModRevision},
		{Key: claimKey, ModRevision: assignmentResult.Values[0].ModRevision},
		{
			Key:         taskAssignmentIndexKey(input.TaskID),
			ModRevision: assignmentResult.Values[1].ModRevision,
		},
		{
			Key:         taskTimeoutIndexKey(input.TaskID, assignment.Deadline),
			ModRevision: timeoutResult.Values[0].ModRevision,
		},
	}, nil
}

func backupAssignmentFromCheckpoint(input BackupCheckpointInput) BackupAssignmentInput {
	return BackupAssignmentInput{
		TaskID: input.TaskID, AssignmentID: input.AssignmentID, AgentID: input.AgentID,
		AgentGeneration: input.AgentGeneration, StepID: input.StepID,
	}
}

func (plan *backupCheckpointPlan) clear() {
	clearBackupRuntimeMutations(plan.mutations)
}

func validateBackupCheckpointInput(input BackupCheckpointInput) error {
	if validateStableID(ids.KindTask, input.TaskID) != nil ||
		validateStableID(ids.KindAssignment, input.AssignmentID) != nil ||
		validateStableID(ids.KindStep, input.StepID) != nil || input.Sequence == 0 {
		return errs.New(errs.KindValidationFailed, "backup checkpoint identity is invalid")
	}
	if input.AgentID == "" {
		if input.AgentGeneration != 0 {
			return errs.New(
				errs.KindValidationFailed,
				"controller checkpoint agent identity is invalid",
			)
		}
	} else if validateStableID(ids.KindAgent, input.AgentID) != nil || input.AgentGeneration == 0 {
		return errs.New(errs.KindValidationFailed, "agent checkpoint identity is invalid")
	}
	return nil
}

func backupCheckpointDigest(payload BackupCheckpointPayload) (string, error) {
	request := &agentpb.BackupCheckpointRequest{}
	remaining := payload
	remaining.Kind = 0
	remaining.PointID = ""
	decodeDigest := func(value string) ([]byte, error) {
		if !validSHA256(value) {
			return nil, errs.New(errs.KindValidationFailed, "backup checkpoint digest field is invalid")
		}
		decoded, err := hex.DecodeString(value)
		if err != nil {
			return nil, errs.New(errs.KindValidationFailed, "backup checkpoint digest field is invalid")
		}
		return decoded, nil
	}
	switch payload.Kind {
	case BackupCheckpointArtifactPrepared:
		storedSHA256, err := decodeDigest(payload.StoredSHA256)
		if err != nil {
			return "", err
		}
		request.Kind = agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_ARTIFACT_PREPARED
		request.Payload = &agentpb.BackupCheckpointRequest_ArtifactPrepared{
			ArtifactPrepared: &agentpb.BackupArtifactPreparedCheckpoint{
				PointId: payload.PointID, StoredSizeBytes: payload.StoredSizeBytes, StoredSha256: storedSHA256,
			},
		}
		remaining.StoredSizeBytes = 0
		remaining.StoredSHA256 = ""
	case BackupCheckpointUploadVerified:
		storedSHA256, err := decodeDigest(payload.StoredSHA256)
		if err != nil {
			return "", err
		}
		request.Kind = agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_UPLOAD_VERIFIED
		request.Payload = &agentpb.BackupCheckpointRequest_UploadVerified{
			UploadVerified: &agentpb.BackupUploadVerifiedCheckpoint{
				PointId: payload.PointID, StoredSizeBytes: payload.StoredSizeBytes, StoredSha256: storedSHA256,
			},
		}
		remaining.StoredSizeBytes = 0
		remaining.StoredSHA256 = ""
	case BackupCheckpointSourceCleanupCompleted:
		request.Kind = agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_SOURCE_CLEANUP_COMPLETED
		request.Payload = &agentpb.BackupCheckpointRequest_SourceCleanupCompleted{
			SourceCleanupCompleted: &agentpb.BackupSourceCleanupCompletedCheckpoint{PointId: payload.PointID},
		}
	case BackupCheckpointRestoreArtifactValidated:
		storedSHA256, err := decodeDigest(payload.StoredSHA256)
		if err != nil {
			return "", err
		}
		decodedSHA256, err := decodeDigest(payload.DecodedSHA256)
		if err != nil {
			return "", err
		}
		request.Kind = agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_RESTORE_ARTIFACT_VALIDATED
		request.Payload = &agentpb.BackupCheckpointRequest_RestoreArtifactValidated{
			RestoreArtifactValidated: &agentpb.BackupRestoreArtifactValidatedCheckpoint{
				PointId: payload.PointID, StoredSha256: storedSHA256, DecodedSha256: decodedSHA256,
			},
		}
		remaining.StoredSHA256 = ""
		remaining.DecodedSHA256 = ""
	case BackupCheckpointVolumeTreeStaged:
		manifestSHA256, err := decodeDigest(payload.StagedTreeManifestSHA256)
		if err != nil {
			return "", err
		}
		request.Kind = agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_VOLUME_TREE_STAGED
		request.Payload = &agentpb.BackupCheckpointRequest_VolumeTreeStaged{
			VolumeTreeStaged: &agentpb.BackupVolumeTreeStagedCheckpoint{
				PointId: payload.PointID, StagedTreeManifestSha256: manifestSHA256,
			},
		}
		remaining.StagedTreeManifestSHA256 = ""
	case BackupCheckpointVolumeTreeExchanged:
		manifestSHA256, err := decodeDigest(payload.LiveTreeManifestSHA256)
		if err != nil {
			return "", err
		}
		request.Kind = agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_VOLUME_TREE_EXCHANGED
		request.Payload = &agentpb.BackupCheckpointRequest_VolumeTreeExchanged{
			VolumeTreeExchanged: &agentpb.BackupVolumeTreeExchangedCheckpoint{
				PointId: payload.PointID, LiveTreeManifestSha256: manifestSHA256,
			},
		}
		remaining.LiveTreeManifestSHA256 = ""
	case BackupCheckpointVolumeReplacedTreeCleaned:
		request.Kind = agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_VOLUME_REPLACED_TREE_CLEANED
		request.Payload = &agentpb.BackupCheckpointRequest_VolumeReplacedTreeCleaned{
			VolumeReplacedTreeCleaned: &agentpb.BackupVolumeReplacedTreeCleanedCheckpoint{PointId: payload.PointID},
		}
	case BackupCheckpointConfigGenerationStaged:
		manifestSHA256, err := decodeDigest(payload.EntryGenerationManifestSHA256)
		if err != nil {
			return "", err
		}
		request.Kind = agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_CONFIG_GENERATION_STAGED
		request.Payload = &agentpb.BackupCheckpointRequest_ConfigGenerationStaged{
			ConfigGenerationStaged: &agentpb.BackupConfigGenerationStagedCheckpoint{
				PointId: payload.PointID, RestoreGenerationId: payload.RestoreGenerationID,
				EntryGenerationManifestSha256: manifestSHA256,
			},
		}
		remaining.RestoreGenerationID = ""
		remaining.EntryGenerationManifestSHA256 = ""
	case BackupCheckpointConfigGenerationActivated:
		request.Kind = agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_CONFIG_GENERATION_ACTIVATED
		request.Payload = &agentpb.BackupCheckpointRequest_ConfigGenerationActivated{
			ConfigGenerationActivated: &agentpb.BackupConfigGenerationActivatedCheckpoint{
				PointId: payload.PointID, RestoreGenerationId: payload.RestoreGenerationID,
				RenderGeneration: payload.RenderGeneration,
			},
		}
		remaining.RestoreGenerationID = ""
		remaining.RenderGeneration = 0
	case BackupCheckpointPostgresRestoreVerified:
		request.Kind = agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_POSTGRES_RESTORE_VERIFIED
		request.Payload = &agentpb.BackupCheckpointRequest_PostgresRestoreVerified{
			PostgresRestoreVerified: &agentpb.BackupPostgresRestoreVerifiedCheckpoint{PointId: payload.PointID},
		}
	case BackupCheckpointRemoteObjectAbsent:
		request.Kind = agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_REMOTE_OBJECT_ABSENT
		request.Payload = &agentpb.BackupCheckpointRequest_RemoteObjectAbsent{
			RemoteObjectAbsent: &agentpb.BackupRemoteObjectAbsentCheckpoint{PointId: payload.PointID},
		}
	case BackupCheckpointUploadCompleted:
		storedSHA256, err := decodeDigest(payload.StoredSHA256)
		if err != nil {
			return "", err
		}
		request.Kind = agentpb.BackupCheckpointKind_BACKUP_CHECKPOINT_KIND_UPLOAD_COMPLETED
		request.Payload = &agentpb.BackupCheckpointRequest_UploadCompleted{
			UploadCompleted: &agentpb.BackupUploadCompletedCheckpoint{
				PointId: payload.PointID, StoredSizeBytes: payload.StoredSizeBytes, StoredSha256: storedSHA256,
			},
		}
		remaining.StoredSizeBytes = 0
		remaining.StoredSHA256 = ""
	default:
		return "", errs.New(errs.KindValidationFailed, "backup checkpoint kind is invalid")
	}
	if remaining != (BackupCheckpointPayload{}) {
		return "", errs.New(errs.KindValidationFailed, "backup checkpoint payload has fields outside its kind")
	}
	digest, err := executionplan.ComputeBackupCheckpointPayloadDigest(request)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(digest), nil
}

func encodeBackupCheckpointCursorRecord(record backupCheckpointCursorRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord(
		"backup-checkpoint-cursor",
		record,
		validateBackupCheckpointCursorRecord,
	)
}

func decodeBackupCheckpointCursorRecord(value []byte) (backupCheckpointCursorRecord, error) {
	return decodeBackupRuntimeRecord(
		value,
		"backup-checkpoint-cursor",
		validateBackupCheckpointCursorRecord,
	)
}

func validateBackupCheckpointCursorRecord(record backupCheckpointCursorRecord) error {
	if validateStableID(ids.KindTask, record.TaskID) != nil ||
		validateStableID(ids.KindAssignment, record.AssignmentID) != nil ||
		validateStableID(ids.KindStep, record.StepID) != nil || record.NextSequence < 2 {
		return errs.New(errs.KindValidationFailed, "backup checkpoint cursor is invalid")
	}
	return nil
}

func encodeBackupCheckpointDedupRecord(record backupCheckpointDedupRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord(
		"backup-checkpoint-dedup",
		record,
		validateBackupCheckpointDedupRecord,
	)
}

func decodeBackupCheckpointDedupRecord(value []byte) (backupCheckpointDedupRecord, error) {
	return decodeBackupRuntimeRecord(
		value,
		"backup-checkpoint-dedup",
		validateBackupCheckpointDedupRecord,
	)
}

func validateBackupCheckpointDedupRecord(record backupCheckpointDedupRecord) error {
	if validateStableID(ids.KindTask, record.TaskID) != nil ||
		validateStableID(ids.KindAssignment, record.AssignmentID) != nil ||
		validateStableID(ids.KindStep, record.StepID) != nil || record.Sequence == 0 ||
		record.Kind < BackupCheckpointArtifactPrepared ||
		record.Kind > BackupCheckpointUploadCompleted || !validSHA256(record.PayloadSHA256) {
		return errs.New(
			errs.KindValidationFailed,
			"backup checkpoint deduplication record is invalid",
		)
	}
	return nil
}
