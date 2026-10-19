package backupwork

import (
	"context"
	"fmt"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// CheckpointAdmission is bound to a fixed active-attempt snapshot. A new report
// still needs the source-specific transition and resource mutations joined to
// these guards before it may be acknowledged. Replay carries the original ack.
type CheckpointAdmission struct {
	Assignment *agentpb.BackupWorkAssignment
	Recovery   *agentpb.BackupRecoveryAssignment
	Request    *agentpb.BackupWorkCheckpointRequest
	Guards     []keyvalue.Condition
	Replay     *agentpb.BackupWorkCheckpointAck
	ReceiptKey string
}

func (admitted CheckpointAdmission) Input() *agentpb.BackupWorkInput {
	if admitted.Recovery != nil {
		return admitted.Recovery.Input
	}
	return admitted.Assignment.GetInput()
}

func (admitted CheckpointAdmission) Sources() []*agentpb.BackupStepResume {
	if admitted.Recovery != nil {
		return admitted.Recovery.Sources
	}
	return admitted.Assignment.GetSources()
}

func (admitted CheckpointAdmission) ConfigBinding(
	identity workexecution.Identity,
) (backupconfigtransfer.Binding, error) {
	if admitted.Request == nil {
		return backupconfigtransfer.Binding{}, conflict("Config checkpoint request is missing")
	}
	if admitted.Recovery != nil {
		return backupconfigtransfer.BindRecoverySource(admitted.Recovery, identity, admitted.Request.SourceOrdinal,
			agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE)
	}
	return backupconfigtransfer.Bind(admitted.Assignment, identity, admitted.Request.SourceOrdinal,
		agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE)
}

func CheckpointKey(workID string, ordinal uint32, sequence uint64) string {
	return fmt.Sprintf("%s%04d/%020d", checkpointPrefix(workID), ordinal, sequence)
}

func checkpointPrefix(workID string) string {
	return "/v1/runtime/backup-work-checkpoints/" + workID + "/"
}

func (repository *InputRepository) InspectCheckpoint(ctx context.Context, view workqueue.ResumeView,
	request *agentpb.BackupWorkCheckpointRequest,
) (CheckpointAdmission, error) {
	if view.Attempt.Attempt.StartedAt == nil {
		return CheckpointAdmission{}, conflict("Backup checkpoint requires a saved start grant")
	}
	assignment, guarded, err := repository.LoadAssignment(ctx, view)
	if err != nil {
		return CheckpointAdmission{}, err
	}
	validated, err := executionplan.ValidateBackupWorkCheckpoint(
		request,
		assignment.Input,
		view.Attempt.Attempt.Executor,
	)
	if err != nil {
		return CheckpointAdmission{}, err
	}
	if !proto.Equal(validated.Attempt, assignment.Attempt) {
		return CheckpointAdmission{}, conflict("Backup checkpoint belongs to another execution attempt")
	}
	return repository.inspectCheckpointReceipt(ctx, view, CheckpointAdmission{
		Assignment: assignment, Request: validated, Guards: guarded.Conditions,
	}, assignment.Input, assignment.Sources)
}

func (repository *InputRepository) InspectRestoreRecoveryCheckpoint(ctx context.Context, view workqueue.ResumeView,
	request *agentpb.BackupWorkCheckpointRequest,
) (CheckpointAdmission, error) {
	if view.Attempt.Attempt.StartedAt == nil {
		return CheckpointAdmission{}, conflict("Restore recovery checkpoint requires a saved start grant")
	}
	assignment, guarded, err := repository.LoadRecoveryAssignment(ctx, view)
	if err != nil {
		return CheckpointAdmission{}, err
	}
	validated, err := executionplan.ValidateBackupRestoreRecoveryCheckpoint(
		request,
		assignment,
		view.Attempt.Attempt.Executor,
	)
	if err != nil {
		return CheckpointAdmission{}, err
	}
	return repository.inspectCheckpointReceipt(ctx, view, CheckpointAdmission{
		Recovery: assignment, Request: validated, Guards: guarded.Conditions,
	}, assignment.Input, assignment.Sources)
}

func (repository *InputRepository) inspectCheckpointReceipt(ctx context.Context, view workqueue.ResumeView,
	result CheckpointAdmission, input *agentpb.BackupWorkInput, sources []*agentpb.BackupStepResume,
) (CheckpointAdmission, error) {
	validated := result.Request
	key := CheckpointKey(view.Work.Item.Item.ID, validated.SourceOrdinal, validated.CheckpointSequence)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.Work.ReadRevision},
	)
	if err != nil {
		return CheckpointAdmission{}, err
	}
	if read == nil {
		return CheckpointAdmission{}, conflict("Backup checkpoint receipt read is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 {
		return CheckpointAdmission{}, conflict("Backup checkpoint receipt snapshot changed")
	}
	result.ReceiptKey = key
	if saved := read.Values[0]; saved != nil {
		if saved.Key != key || saved.ModRevision <= 0 || saved.ModRevision > read.ReadRevision ||
			len(saved.Value) > executionplan.MaximumPlanBytes {
			return CheckpointAdmission{}, conflict("Backup checkpoint receipt is invalid")
		}
		original, _, err := decodeCheckpointReceipt(saved.Value)
		if err != nil || !proto.Equal(original, validated) {
			return CheckpointAdmission{}, conflict("Backup checkpoint sequence was already used by a different report")
		}
		sequence, fence := sourceCheckpoint(sources[validated.SourceOrdinal])
		if sequence < validated.CheckpointSequence || fence == nil ||
			fence.DedupeKeyModRevision < saved.ModRevision ||
			(sequence == validated.CheckpointSequence && fence.DedupeKeyModRevision != saved.ModRevision) {
			return CheckpointAdmission{}, conflict("Backup checkpoint receipt has no matching saved source progress")
		}
		if result.Recovery != nil {
			result.Replay, err = checkpointReceiptAcknowledgement(saved.Value, validated, input, saved.ModRevision)
			if err == nil {
				result.Replay, err = executionplan.ValidateBackupRestoreRecoveryCheckpointAck(
					result.Replay,
					validated,
					result.Recovery,
					view.Attempt.Attempt.Executor,
				)
			}
		} else {
			result.Replay, err = checkpointAcknowledgement(saved.Value, validated, input, view.Attempt.Attempt.Executor, saved.ModRevision)
		}
		if err != nil {
			return CheckpointAdmission{}, err
		}
		result.Guards = append(result.Guards, keyvalue.Condition{Key: key, ModRevision: saved.ModRevision})
		return result, nil
	}
	source := sources[validated.SourceOrdinal]
	sequence, predecessor := sourceCheckpoint(source)
	if sequence == ^uint64(0) || validated.CheckpointSequence != sequence+1 ||
		!proto.Equal(predecessor, validated.PrecedingCheckpoint) {
		return CheckpointAdmission{}, conflict("Backup checkpoint does not follow its saved source progress")
	}
	result.Guards = append(result.Guards, keyvalue.Condition{Key: key})
	return result, nil
}

func sourceCheckpoint(source *agentpb.BackupStepResume) (uint64, *agentpb.CheckpointFence) {
	switch {
	case source.GetCapture() != nil:
		return source.GetCapture().CheckpointSequence, source.GetCapture().PrecedingCheckpoint
	case source.GetRestore() != nil:
		return source.GetRestore().CheckpointSequence, source.GetRestore().PrecedingCheckpoint
	case source.GetPrune() != nil:
		return source.GetPrune().CheckpointSequence, source.GetPrune().PrecedingCheckpoint
	default:
		return ^uint64(0), nil
	}
}
