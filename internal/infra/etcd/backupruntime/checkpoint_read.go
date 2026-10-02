package backupruntime

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type BackupCommittedCheckpoint struct {
	Request *agentpb.BackupCheckpointRequest
	Fence   *agentpb.CheckpointFence
}

// VisitBackupStepCheckpoints replays the complete sealed cursor at the claim's
// revision. Each immutable receipt supplies its actual commit fence. Pages bound
// memory without inventing a maximum number of legitimate checkpoints.
func (reader *Reader) VisitBackupStepCheckpoints(ctx context.Context, authority *agentpb.BackupTaskAuthority,
	step *agentpb.BackupStepAuthority, revision int64, visit func(BackupCommittedCheckpoint) error,
) error {
	if revision <= 0 || visit == nil {
		return CorruptBackupRuntimeRecord()
	}
	if _, err := executionplan.BackupTaskAuthorityDigest(authority); err != nil {
		return err
	}
	sealed := false
	for _, candidate := range authority.Steps {
		if proto.Equal(candidate, step) {
			sealed = true
			break
		}
	}
	if !sealed || step == nil {
		return CorruptBackupRuntimeRecord()
	}
	input := BackupCheckpointInput{TaskID: authority.TaskId, AssignmentID: authority.AssignmentId,
		AssignmentGeneration: authority.AssignmentGeneration, StepID: step.StepId, ExecutionID: step.ExecutionId,
		AuthoritySHA256: hex.EncodeToString(step.StepDigest)}
	read, err := reader.ReadFixedKeys(ctx, []string{BackupCheckpointCursorKey(input)}, revision)
	if err != nil {
		return err
	}
	defer etcdstore.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil
	}
	cursor, err := DecodeBackupCheckpointCursorRecord(read.Values[0].Value)
	if err != nil || cursor.TaskID != input.TaskID || cursor.AssignmentID != input.AssignmentID ||
		cursor.StepID != input.StepID || cursor.ExecutionID != input.ExecutionID ||
		cursor.AssignmentGeneration != input.AssignmentGeneration || cursor.AuthoritySHA256 != input.AuthoritySHA256 {
		return CorruptBackupRuntimeRecord()
	}
	previous := int64(0)
	for sequence := uint64(1); sequence < cursor.NextSequence; {
		count := min(uint64(96), cursor.NextSequence-sequence)
		keys := make([]string, int(count))
		for index := range keys {
			input.Sequence = sequence + uint64(index)
			keys[index] = BackupCheckpointDedupKey(input)
		}
		page, err := reader.ReadFixedKeys(ctx, keys, revision)
		if err != nil {
			return err
		}
		next, err := visitBackupCheckpointPage(page.Values, input, sequence, previous, revision, visit)
		etcdstore.ClearValues(page.Values)
		if err != nil {
			return err
		}
		previous = next
		sequence += count
	}
	if previous != read.Values[0].ModRevision {
		return CorruptBackupRuntimeRecord()
	}
	return nil
}

func visitBackupCheckpointPage(values []*etcdstore.KeyValue, input BackupCheckpointInput, first uint64,
	previous, revision int64, visit func(BackupCommittedCheckpoint) error,
) (int64, error) {
	for index, value := range values {
		if value == nil || value.Version != 1 || value.ModRevision <= previous || value.ModRevision > revision {
			return 0, CorruptBackupRuntimeRecord()
		}
		record, err := DecodeBackupCheckpointDedupRecord(value.Value)
		if err != nil || record.TaskID != input.TaskID || record.AssignmentID != input.AssignmentID ||
			record.StepID != input.StepID || record.ExecutionID != input.ExecutionID ||
			record.AssignmentGeneration != input.AssignmentGeneration || record.AuthoritySHA256 != input.AuthoritySHA256 ||
			record.Sequence != first+uint64(index) || record.PrecedingCheckpointRevision != previous {
			return 0, CorruptBackupRuntimeRecord()
		}
		request := &agentpb.BackupCheckpointRequest{}
		if err := proto.Unmarshal(record.Request, request); err != nil {
			return 0, CorruptBackupRuntimeRecord()
		}
		digest, err := hex.DecodeString(input.AuthoritySHA256)
		if err != nil {
			return 0, CorruptBackupRuntimeRecord()
		}
		if err := visit(BackupCommittedCheckpoint{Request: request, Fence: &agentpb.CheckpointFence{
			AuthorityDigest: digest, DedupeKeyModRevision: value.ModRevision,
		}}); err != nil {
			return 0, err
		}
		previous = value.ModRevision
	}
	return previous, nil
}
