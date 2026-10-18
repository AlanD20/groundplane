package backupwork

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (repository *InputRepository) restoreProgressFences(ctx context.Context, view workqueue.View,
	input *agentpb.BackupWorkInput, stored []SourceProgress, sources []*agentpb.BackupStepResume, progressRevision int64,
) error {
	keys := make([]string, 0, len(sources))
	ordinals := make([]int, 0, len(sources))
	for ordinal, source := range sources {
		sequence, fence := sourceCheckpoint(source)
		if sequence == ^uint64(0) || fence != nil ||
			(sequence == 0 && stored[ordinal].CheckpointSHA256 != "") ||
			(sequence != 0 && len(stored[ordinal].CheckpointSHA256) != 64) {
			return conflict("Backup stored cursor must reference its immutable checkpoint receipt")
		}
		if sequence != 0 {
			keys = append(keys, CheckpointKey(view.Item.Item.ID, uint32(ordinal), sequence))
			ordinals = append(ordinals, ordinal)
		}
	}
	if len(keys) == 0 {
		return nil
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: view.ReadRevision})
	if err != nil {
		return err
	}
	if read == nil {
		return conflict("Backup cursor receipts are unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != view.ReadRevision || len(read.Values) != len(keys) {
		return conflict("Backup cursor receipt snapshot changed")
	}
	for index, value := range read.Values {
		ordinal := ordinals[index]
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > progressRevision ||
			len(value.Value) > executionplan.MaximumPlanBytes {
			return conflict("Backup cursor has no committed receipt")
		}
		request, source, err := decodeCheckpointReceipt(value.Value)
		if err != nil || request.Attempt == nil || !proto.Equal(source, sources[ordinal]) ||
			request.Attempt.TaskId != view.Task.Task.ID || request.Attempt.WorkItemId != view.Item.Item.ID ||
			request.SourceOrdinal != uint32(ordinal) {
			return conflict("Backup cursor receipt belongs to different work")
		}
		identity := workexecution.Identity{Executor: workexecution.Agent, HostID: view.Item.Item.Placement.HostID,
			AgentID: request.Attempt.AgentId, Generation: request.Attempt.ExecutorGeneration}
		if _, err := executionplan.ValidateBackupWorkCheckpoint(request, input, identity); err != nil {
			return err
		}
		digest, err := executionplan.BackupWorkCheckpointDigest(request)
		if err != nil {
			return err
		}
		sequence, _ := sourceCheckpoint(sources[ordinal])
		if request.CheckpointSequence != sequence || hex.EncodeToString(digest) != stored[ordinal].CheckpointSHA256 {
			return conflict("Backup cursor checkpoint digest changed")
		}
		setSourceFence(sources[ordinal], &agentpb.CheckpointFence{DedupeKeyModRevision: value.ModRevision,
			AuthorityDigest: append([]byte(nil), input.Steps[ordinal].StepDigest...)})
	}
	return nil
}

func setSourceFence(source *agentpb.BackupStepResume, fence *agentpb.CheckpointFence) {
	switch {
	case source.GetCapture() != nil:
		source.GetCapture().PrecedingCheckpoint = fence
	case source.GetRestore() != nil:
		source.GetRestore().PrecedingCheckpoint = fence
	case source.GetPrune() != nil:
		source.GetPrune().PrecedingCheckpoint = fence
	}
}
